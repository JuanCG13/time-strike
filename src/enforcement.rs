//! Host-side enforcement for action leases issued by the MCP transport.
//!
//! The ledger is intentionally independent from the task core and the MCP
//! server. Hosts register the lease returned by `tick` and atomically consume
//! it immediately before executing the bound action.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

const MAX_ACTION_CHARS: usize = 160;
const MAX_TASK_ID_BYTES: usize = 256;
const MAX_LEASE_ID_BYTES: usize = 320;

/// Wire representation of an action lease returned by `tick`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ActionLeaseGrant {
    pub lease_id: String,
    pub task_id: String,
    pub action: String,
    pub duration_seconds: f64,
    pub expires_in_seconds: f64,
    pub expiry_anchor: String,
    pub one_shot: bool,
}

/// Deterministic rejection reasons produced by [`ActionLeaseLedger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionLeaseError {
    InvalidLease(&'static str),
    DuplicateLease,
    UnknownLease,
    Superseded,
    AlreadyConsumed,
    BindingMismatch(&'static str),
    Expired,
    WouldExceedDeadline,
    Unavailable,
}

impl fmt::Display for ActionLeaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLease(field) => write!(formatter, "invalid action lease field: {field}"),
            Self::DuplicateLease => {
                formatter.write_str("action lease identifier already registered")
            }
            Self::UnknownLease => formatter.write_str("action lease is not registered"),
            Self::Superseded => formatter.write_str("action lease was superseded"),
            Self::AlreadyConsumed => formatter.write_str("action lease was already consumed"),
            Self::BindingMismatch(field) => {
                write!(formatter, "action lease binding mismatch: {field}")
            }
            Self::Expired => formatter.write_str("action lease expired"),
            Self::WouldExceedDeadline => {
                formatter.write_str("action would exceed its lease deadline")
            }
            Self::Unavailable => formatter.write_str("action lease ledger is unavailable"),
        }
    }
}

impl std::error::Error for ActionLeaseError {}

#[derive(Debug)]
struct LeaseRecord {
    task_id: String,
    action: String,
    duration_seconds: f64,
    duration: Duration,
    expires_at: Duration,
    consumed: bool,
    superseded: bool,
}

#[derive(Debug, Default)]
struct LedgerState {
    leases: HashMap<String, LeaseRecord>,
    active_by_task: HashMap<String, String>,
}

/// Thread-safe, monotonic ledger for one-shot host action authority.
#[derive(Debug)]
pub struct ActionLeaseLedger {
    hard_deadline: Duration,
    state: Mutex<LedgerState>,
}

impl ActionLeaseLedger {
    /// Creates a ledger clamped to the host's absolute monotonic deadline.
    pub fn new(hard_deadline: Duration) -> Self {
        Self {
            hard_deadline,
            state: Mutex::new(LedgerState::default()),
        }
    }

    /// Registers a grant against the request that produced it.
    ///
    /// Duplicate detection, supersession, and insertion occur under one lock,
    /// so concurrent responses cannot both become authoritative.
    pub fn register(
        &self,
        request_started: Duration,
        expected_task_id: &str,
        expected_action: &str,
        expected_eta_seconds: f64,
        grant: &ActionLeaseGrant,
    ) -> Result<(), ActionLeaseError> {
        validate_grant(grant)?;
        let expected_action = normalize_action(expected_action)?;
        if !valid_task_id(expected_task_id) {
            return Err(ActionLeaseError::BindingMismatch("task_id"));
        }
        if grant.task_id != expected_task_id {
            return Err(ActionLeaseError::BindingMismatch("task_id"));
        }
        if grant.action != expected_action {
            return Err(ActionLeaseError::BindingMismatch("action"));
        }
        if checked_duration(expected_eta_seconds).is_none()
            || grant.duration_seconds.to_bits() != expected_eta_seconds.to_bits()
        {
            return Err(ActionLeaseError::BindingMismatch("duration_seconds"));
        }

        let duration = checked_duration(grant.duration_seconds)
            .ok_or(ActionLeaseError::InvalidLease("duration_seconds"))?;
        let relative_expiry = request_started
            .checked_add(
                checked_duration(grant.expires_in_seconds)
                    .ok_or(ActionLeaseError::InvalidLease("expires_in_seconds"))?,
            )
            .ok_or(ActionLeaseError::InvalidLease("expires_in_seconds"))?;
        let expires_at = relative_expiry.min(self.hard_deadline);
        if expires_at <= request_started {
            return Err(ActionLeaseError::Expired);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| ActionLeaseError::Unavailable)?;
        if state.leases.contains_key(&grant.lease_id) {
            return Err(ActionLeaseError::DuplicateLease);
        }
        if let Some(previous_id) = state.active_by_task.get(&grant.task_id).cloned()
            && let Some(previous) = state.leases.get_mut(&previous_id)
            && !previous.consumed
        {
            previous.superseded = true;
        }
        state.leases.insert(
            grant.lease_id.clone(),
            LeaseRecord {
                task_id: grant.task_id.clone(),
                action: grant.action.clone(),
                duration_seconds: grant.duration_seconds,
                duration,
                expires_at,
                consumed: false,
                superseded: false,
            },
        );
        state
            .active_by_task
            .insert(grant.task_id.clone(), grant.lease_id.clone());
        Ok(())
    }

    /// Atomically consumes a lease immediately before executing its action.
    pub fn consume(
        &self,
        lease_id: &str,
        task_id: &str,
        action: &str,
        eta_seconds: f64,
        now: Duration,
    ) -> Result<(), ActionLeaseError> {
        let action = normalize_action(action)?;
        if checked_duration(eta_seconds).is_none() {
            return Err(ActionLeaseError::BindingMismatch("duration_seconds"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ActionLeaseError::Unavailable)?;
        let registered_task;
        {
            let lease = state
                .leases
                .get_mut(lease_id)
                .ok_or(ActionLeaseError::UnknownLease)?;
            if lease.superseded {
                return Err(ActionLeaseError::Superseded);
            }
            if lease.consumed {
                return Err(ActionLeaseError::AlreadyConsumed);
            }
            if lease.task_id != task_id {
                return Err(ActionLeaseError::BindingMismatch("task_id"));
            }
            if lease.action != action {
                return Err(ActionLeaseError::BindingMismatch("action"));
            }
            if lease.duration_seconds.to_bits() != eta_seconds.to_bits() {
                return Err(ActionLeaseError::BindingMismatch("duration_seconds"));
            }
            if now >= lease.expires_at {
                return Err(ActionLeaseError::Expired);
            }
            if now
                .checked_add(lease.duration)
                .is_none_or(|finish| finish > lease.expires_at)
            {
                return Err(ActionLeaseError::WouldExceedDeadline);
            }
            lease.consumed = true;
            registered_task = lease.task_id.clone();
        }
        if state.active_by_task.get(&registered_task).map(String::as_str) == Some(lease_id) {
            state.active_by_task.remove(&registered_task);
        }
        Ok(())
    }
}

fn validate_grant(grant: &ActionLeaseGrant) -> Result<(), ActionLeaseError> {
    if grant.lease_id.trim().is_empty() || grant.lease_id.len() > MAX_LEASE_ID_BYTES {
        return Err(ActionLeaseError::InvalidLease("lease_id"));
    }
    if !valid_task_id(&grant.task_id) {
        return Err(ActionLeaseError::InvalidLease("task_id"));
    }
    let normalized_action = grant.action.trim();
    if normalized_action.is_empty()
        || normalized_action.chars().count() > MAX_ACTION_CHARS
        || grant.action != normalized_action
    {
        return Err(ActionLeaseError::InvalidLease("action"));
    }
    if checked_duration(grant.duration_seconds).is_none() {
        return Err(ActionLeaseError::InvalidLease("duration_seconds"));
    }
    if checked_duration(grant.expires_in_seconds).is_none() {
        return Err(ActionLeaseError::InvalidLease("expires_in_seconds"));
    }
    if grant.duration_seconds > grant.expires_in_seconds {
        return Err(ActionLeaseError::InvalidLease("duration_seconds"));
    }
    if grant.expiry_anchor != "tick_request_started" {
        return Err(ActionLeaseError::InvalidLease("expiry_anchor"));
    }
    if !grant.one_shot {
        return Err(ActionLeaseError::InvalidLease("one_shot"));
    }
    Ok(())
}

fn normalize_action(action: &str) -> Result<&str, ActionLeaseError> {
    let action = action.trim();
    if action.is_empty() || action.chars().count() > MAX_ACTION_CHARS {
        return Err(ActionLeaseError::BindingMismatch("action"));
    }
    Ok(action)
}

fn valid_task_id(task_id: &str) -> bool {
    !task_id.trim().is_empty() && task_id.len() <= MAX_TASK_ID_BYTES
}

fn checked_duration(value: f64) -> Option<Duration> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn grant(id: &str, task: &str, action: &str, seconds: f64) -> ActionLeaseGrant {
        ActionLeaseGrant {
            lease_id: id.into(),
            task_id: task.into(),
            action: action.into(),
            duration_seconds: seconds,
            expires_in_seconds: 5.0,
            expiry_anchor: "tick_request_started".into(),
            one_shot: true,
        }
    }

    #[test]
    fn one_shot_lease_rejects_replay() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(20));
        let lease = grant("lease-1", "task-1", "write output", 2.0);
        ledger
            .register(Duration::from_secs(1), "task-1", "write output", 2.0, &lease)
            .unwrap();
        ledger
            .consume("lease-1", "task-1", "write output", 2.0, Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            ledger.consume(
                "lease-1",
                "task-1",
                "write output",
                2.0,
                Duration::from_secs(2)
            ),
            Err(ActionLeaseError::AlreadyConsumed)
        );
    }

    #[test]
    fn binding_failure_does_not_burn_the_valid_lease() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(20));
        let lease = grant("lease-1", "task-1", "write output", 2.0);
        assert_eq!(
            ledger.register(Duration::ZERO, "other-task", "write output", 2.0, &lease),
            Err(ActionLeaseError::BindingMismatch("task_id"))
        );
        ledger
            .register(Duration::ZERO, "task-1", "write output", 2.0, &lease)
            .unwrap();
        assert_eq!(
            ledger.consume(
                "lease-1",
                "other-task",
                "write output",
                2.0,
                Duration::from_secs(1)
            ),
            Err(ActionLeaseError::BindingMismatch("task_id"))
        );
        assert_eq!(
            ledger.consume(
                "lease-1",
                "task-1",
                "search again",
                2.0,
                Duration::from_secs(1)
            ),
            Err(ActionLeaseError::BindingMismatch("action"))
        );
        ledger
            .consume("lease-1", "task-1", "write output", 2.0, Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn duplicate_registration_does_not_replace_active_authority() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(20));
        let lease = grant("lease-1", "task-1", "write", 1.0);
        ledger
            .register(Duration::ZERO, "task-1", "write", 1.0, &lease)
            .unwrap();
        assert_eq!(
            ledger.register(Duration::ZERO, "task-1", "write", 1.0, &lease),
            Err(ActionLeaseError::DuplicateLease)
        );
        ledger
            .consume("lease-1", "task-1", "write", 1.0, Duration::ZERO)
            .unwrap();
    }

    #[test]
    fn unrepresentable_duration_fails_closed_without_panicking() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(20));
        let mut lease = grant("lease-1", "task-1", "write", 1.0);
        lease.duration_seconds = f64::MAX;
        lease.expires_in_seconds = f64::MAX;
        assert_eq!(
            ledger.register(Duration::ZERO, "task-1", "write", f64::MAX, &lease),
            Err(ActionLeaseError::InvalidLease("duration_seconds"))
        );
    }

    #[test]
    fn newer_lease_supersedes_unconsumed_authority() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(20));
        let old = grant("old", "task-1", "inspect", 1.0);
        let new = grant("new", "task-1", "write", 1.0);
        ledger
            .register(Duration::ZERO, "task-1", "inspect", 1.0, &old)
            .unwrap();
        ledger
            .register(Duration::ZERO, "task-1", "write", 1.0, &new)
            .unwrap();
        assert_eq!(
            ledger.consume("old", "task-1", "inspect", 1.0, Duration::ZERO),
            Err(ActionLeaseError::Superseded)
        );
        ledger
            .consume("new", "task-1", "write", 1.0, Duration::ZERO)
            .unwrap();
    }

    #[test]
    fn hard_deadline_clamps_relative_expiry() {
        let ledger = ActionLeaseLedger::new(Duration::from_secs(4));
        let lease = grant("lease-1", "task-1", "write", 2.0);
        ledger
            .register(Duration::ZERO, "task-1", "write", 2.0, &lease)
            .unwrap();
        assert_eq!(
            ledger.consume(
                "lease-1",
                "task-1",
                "write",
                2.0,
                Duration::from_secs(3)
            ),
            Err(ActionLeaseError::WouldExceedDeadline)
        );
        ledger
            .consume(
                "lease-1",
                "task-1",
                "write",
                2.0,
                Duration::from_secs(2),
            )
            .unwrap();
    }

    #[test]
    fn concurrent_registration_accepts_exactly_one_copy() {
        let ledger = Arc::new(ActionLeaseLedger::new(Duration::from_secs(20)));
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let ledger = Arc::clone(&ledger);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let lease = grant("lease-1", "task-1", "write", 1.0);
                barrier.wait();
                ledger.register(Duration::ZERO, "task-1", "write", 1.0, &lease)
            }));
        }
        barrier.wait();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == Err(ActionLeaseError::DuplicateLease))
                .count(),
            1
        );
    }

    #[test]
    fn concurrent_consumption_authorizes_exactly_one_worker() {
        let ledger = Arc::new(ActionLeaseLedger::new(Duration::from_secs(20)));
        let lease = grant("lease-1", "task-1", "write", 1.0);
        ledger
            .register(Duration::ZERO, "task-1", "write", 1.0, &lease)
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let ledger = Arc::clone(&ledger);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                ledger.consume("lease-1", "task-1", "write", 1.0, Duration::ZERO)
            }));
        }
        barrier.wait();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == Err(ActionLeaseError::AlreadyConsumed))
                .count(),
            1
        );
    }
}
