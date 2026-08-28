//! Core runtime, budget policy, task lifecycle, and optional recovery store for
//! `time-strike`.
//!
//! The core deliberately has no async or MCP-specific state.  A monotonic clock
//! is injected so the same policy can be exercised deterministically in tests.

pub mod policy;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod clock {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    /// A clock used by the policy engine.  Values are monotonic durations from
    /// an implementation-defined origin, never wall-clock timestamps.
    pub trait Clock: Send + Sync {
        fn now(&self) -> Duration;
    }

    /// Production clock backed by [`Instant`].
    #[derive(Clone, Debug)]
    pub struct MonotonicClock {
        origin: Instant,
    }

    impl Default for MonotonicClock {
        fn default() -> Self {
            Self {
                origin: Instant::now(),
            }
        }
    }

    impl MonotonicClock {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl Clock for MonotonicClock {
        fn now(&self) -> Duration {
            self.origin.elapsed()
        }
    }

    /// Deterministic, thread-safe clock for tests and offline simulations.
    #[derive(Clone, Debug, Default)]
    pub struct ManualClock {
        nanos: Arc<AtomicU64>,
    }

    impl ManualClock {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn set_secs(&self, secs: f64) {
            assert!(
                secs.is_finite() && secs >= 0.0,
                "manual clock requires >= 0"
            );
            self.nanos.store(
                Duration::from_secs_f64(secs)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                Ordering::SeqCst,
            );
        }

        pub fn advance_secs(&self, secs: f64) {
            assert!(
                secs.is_finite() && secs >= 0.0,
                "manual clock requires >= 0"
            );
            let delta = Duration::from_secs_f64(secs)
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            self.nanos
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                    Some(old.saturating_add(delta))
                })
                .ok();
        }

        pub fn elapsed_secs(&self) -> f64 {
            self.now().as_secs_f64()
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
        }
    }
}

pub use clock::{Clock, ManualClock, MonotonicClock};

const SNAPSHOT_VERSION: u32 = 2;
const EPSILON: f64 = 1e-9;
const MAX_ID_LEN: usize = 256;
const MAX_NOTE_LEN: usize = 8 * 1024;
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn default_tick_interval() -> f64 {
    1.0
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Policy profiles alter reserve size and recommended work chunk size.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// General-purpose behavior.
    #[default]
    Balanced,
    /// Frequent small checkpoints and a larger finishing reserve.
    Interactive,
    /// Fewer, larger chunks and a smaller reserve.
    Deep,
    /// Strong finishing reserve and short control-loop intervals.
    Deadline,
}

impl Mode {
    pub fn reserve_ratio(&self) -> f64 {
        match self {
            Self::Balanced => 0.12,
            Self::Interactive => 0.20,
            Self::Deep => 0.07,
            Self::Deadline => 0.25,
        }
    }

    pub fn default_tick_interval_secs(&self) -> f64 {
        match self {
            Self::Balanced => 1.0,
            Self::Interactive => 0.25,
            Self::Deep => 2.0,
            Self::Deadline => 0.10,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::Interactive => "interactive",
            Self::Deep => "deep",
            Self::Deadline => "deadline",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Mode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "balanced" | "normal" | "default" => Ok(Self::Balanced),
            "interactive" | "fast" => Ok(Self::Interactive),
            "deep" | "thorough" => Ok(Self::Deep),
            "deadline" | "urgent" => Ok(Self::Deadline),
            other => Err(format!("unknown mode: {other}")),
        }
    }
}

/// One schedule phase. `until_secs` is measured from task start and is
/// cumulative; the final phase may use `f64::INFINITY` in Rust, but JSON users
/// should simply omit a later phase because the last phase extends forever.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SchedulePhase {
    pub name: String,
    pub until_secs: f64,
    #[serde(default)]
    pub mode: Option<Mode>,
    #[serde(default)]
    pub reserve_ratio: Option<f64>,
    #[serde(default)]
    pub tick_interval_secs: Option<f64>,
}

impl SchedulePhase {
    pub fn new(name: impl Into<String>, until_secs: f64) -> Self {
        Self {
            name: name.into(),
            until_secs,
            mode: None,
            reserve_ratio: None,
            tick_interval_secs: None,
        }
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = Some(mode);
        self
    }

    pub fn with_reserve_ratio(mut self, ratio: f64) -> Self {
        self.reserve_ratio = Some(ratio);
        self
    }

    pub fn with_tick_interval(mut self, secs: f64) -> Self {
        self.tick_interval_secs = Some(secs);
        self
    }
}

/// Optional relative schedule for a task.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Schedule {
    #[serde(default = "default_tick_interval")]
    pub tick_interval_secs: f64,
    #[serde(default)]
    pub phases: Vec<SchedulePhase>,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            tick_interval_secs: default_tick_interval(),
            phases: Vec::new(),
        }
    }
}

impl Schedule {
    pub fn new(tick_interval_secs: f64) -> Self {
        Self {
            tick_interval_secs,
            phases: Vec::new(),
        }
    }

    pub fn with_phases(mut self, phases: Vec<SchedulePhase>) -> Self {
        self.phases = phases;
        self
    }

    pub fn validate(&self) -> Result<(), TaskError> {
        if !self.tick_interval_secs.is_finite() || self.tick_interval_secs <= 0.0 {
            return Err(TaskError::Invalid(
                "schedule tick_interval_secs must be > 0".into(),
            ));
        }
        let mut previous = 0.0;
        for phase in &self.phases {
            if phase.name.trim().is_empty() {
                return Err(TaskError::Invalid(
                    "schedule phase name cannot be empty".into(),
                ));
            }
            if !phase.until_secs.is_finite() || phase.until_secs <= previous {
                return Err(TaskError::Invalid(
                    "schedule phase until_secs must be finite and strictly increasing".into(),
                ));
            }
            if let Some(ratio) = phase.reserve_ratio
                && (!ratio.is_finite() || !(0.0..=0.80).contains(&ratio))
            {
                return Err(TaskError::Invalid(
                    "schedule reserve_ratio must be between 0 and 0.80".into(),
                ));
            }
            if let Some(interval) = phase.tick_interval_secs
                && (!interval.is_finite() || interval <= 0.0)
            {
                return Err(TaskError::Invalid(
                    "schedule phase tick_interval_secs must be > 0".into(),
                ));
            }
            previous = phase.until_secs;
        }
        Ok(())
    }

    fn phase_at(&self, elapsed_secs: f64) -> Option<&SchedulePhase> {
        self.phases
            .iter()
            .find(|phase| elapsed_secs < phase.until_secs)
            .or_else(|| self.phases.last())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Active,
    Exhausted,
    Finished,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Exhausted => "exhausted",
            Self::Finished => "finished",
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StartTaskRequest {
    pub task_id: String,
    pub budget_secs: f64,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub schedule: Schedule,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl StartTaskRequest {
    pub fn new(task_id: impl Into<String>, budget_secs: f64) -> Self {
        Self {
            task_id: task_id.into(),
            budget_secs,
            parent_id: None,
            mode: Mode::Balanced,
            schedule: Schedule::default(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_parent(mut self, parent_id: impl Into<String>) -> Self {
        self.parent_id = Some(parent_id.into());
        self
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_schedule(mut self, schedule: Schedule) -> Self {
        self.schedule = schedule;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TickRequest {
    pub task_id: String,
}

impl TickRequest {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CheckpointRequest {
    pub task_id: String,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub progress: Option<f64>,
    #[serde(default)]
    pub estimated_remaining_work_secs: Option<f64>,
    #[serde(default)]
    pub plan_complete: bool,
    #[serde(default)]
    pub replan: bool,
}

impl CheckpointRequest {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            note: None,
            progress: None,
            estimated_remaining_work_secs: None,
            plan_complete: false,
            replan: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AdjustTaskRequest {
    pub task_id: String,
    #[serde(default)]
    pub budget_secs: Option<f64>,
    #[serde(default)]
    pub mode: Option<Mode>,
    #[serde(default)]
    pub schedule: Option<Schedule>,
}

impl AdjustTaskRequest {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            budget_secs: None,
            mode: None,
            schedule: None,
        }
    }

    pub fn with_budget(mut self, budget_secs: f64) -> Self {
        self.budget_secs = Some(budget_secs);
        self
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = Some(mode);
        self
    }

    pub fn with_schedule(mut self, schedule: Schedule) -> Self {
        self.schedule = Some(schedule);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinishTaskRequest {
    pub task_id: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub force: bool,
}

impl FinishTaskRequest {
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            reason: None,
            force: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CheckpointRecord {
    pub sequence: u64,
    pub elapsed_secs: f64,
    pub note: Option<String>,
    pub progress: Option<f64>,
    #[serde(default)]
    pub estimated_remaining_work_secs: Option<f64>,
    #[serde(default)]
    pub plan_complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskView {
    pub task_id: String,
    pub parent_id: Option<String>,
    pub budget_secs: f64,
    pub elapsed_secs: f64,
    pub remaining_secs: f64,
    pub child_reserved_secs: f64,
    pub adaptive_reserve_secs: f64,
    pub available_secs: f64,
    pub recommended_work_secs: f64,
    pub mode: Mode,
    pub phase: Option<String>,
    pub status: TaskStatus,
    pub ticks: u64,
    pub checkpoints: u64,
    pub last_checkpoint: Option<CheckpointRecord>,
    pub children: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub plan_submitted: bool,
}

impl TaskView {
    pub fn is_done(&self) -> bool {
        matches!(self.status, TaskStatus::Finished | TaskStatus::Exhausted)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StartTaskOutcome {
    pub task: TaskView,
    pub requested_budget_secs: f64,
    pub effective_budget_secs: f64,
    pub clamped: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TickOutcome {
    pub task: TaskView,
    pub tick: u64,
    pub phase_changed: bool,
}

/// Explicit deadline accounting returned by opt-in timing APIs.
///
/// This is separate from [`TaskView`] so existing Rust consumers keep the
/// original struct layout and `elapsed_secs` semantics.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskTiming {
    /// Monotonic runtime without deadline clamping.
    pub actual_elapsed_secs: f64,
    /// Runtime charged against the configured budget.
    pub accounted_elapsed_secs: f64,
    pub overrun_secs: f64,
    pub deadline_met: bool,
}

impl TaskTiming {
    fn new(actual_elapsed_secs: f64, budget_secs: f64) -> Self {
        Self {
            actual_elapsed_secs,
            accounted_elapsed_secs: actual_elapsed_secs.min(budget_secs.max(0.0)),
            overrun_secs: (actual_elapsed_secs - budget_secs).max(0.0),
            // Compare against the summed boundary to avoid cancellation around
            // the one-nanosecond tolerance.
            deadline_met: actual_elapsed_secs <= budget_secs + EPSILON,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CheckpointOutcome {
    pub task: TaskView,
    pub checkpoint: CheckpointRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AdjustTaskOutcome {
    pub task: TaskView,
    pub requested_budget_secs: Option<f64>,
    pub effective_budget_secs: f64,
    pub clamped: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FinishTaskOutcome {
    pub task: TaskView,
    pub reason: Option<String>,
    pub actual_elapsed_secs: f64,
    pub overrun_secs: f64,
}

impl FinishTaskOutcome {
    pub fn deadline_met(&self) -> bool {
        self.actual_elapsed_secs <= self.task.budget_secs + EPSILON
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    Invalid(String),
    AlreadyExists(String),
    NotFound(String),
    NotActive(String),
    ParentNotFound(String),
    ParentUnavailable(String),
    BudgetExhausted(String),
    ActiveChildren(String),
    EmptyAdjustment,
    Persistence(String),
    CorruptSnapshot(String),
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid request: {message}"),
            Self::AlreadyExists(id) => write!(f, "task already exists: {id}"),
            Self::NotFound(id) => write!(f, "task not found: {id}"),
            Self::NotActive(id) => write!(f, "task is not active: {id}"),
            Self::ParentNotFound(id) => write!(f, "parent task not found: {id}"),
            Self::ParentUnavailable(id) => write!(f, "parent task unavailable: {id}"),
            Self::BudgetExhausted(id) => write!(f, "no budget available for task: {id}"),
            Self::ActiveChildren(id) => write!(f, "task has active children: {id}"),
            Self::EmptyAdjustment => f.write_str("adjustment did not change anything"),
            Self::Persistence(message) => write!(f, "persistence error: {message}"),
            Self::CorruptSnapshot(message) => write!(f, "corrupt snapshot: {message}"),
        }
    }
}

impl std::error::Error for TaskError {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersistedState {
    pub version: u32,
    pub saved_at_unix_ms: u64,
    pub tasks: Vec<PersistedTask>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersistedTask {
    pub task_id: String,
    pub parent_id: Option<String>,
    pub budget_secs: f64,
    pub mode: Mode,
    pub schedule: Schedule,
    pub metadata: HashMap<String, String>,
    pub status: TaskStatus,
    pub elapsed_secs: f64,
    pub ticks: u64,
    pub checkpoints: u64,
    pub last_checkpoint: Option<CheckpointRecord>,
    #[serde(default)]
    pub plan_submitted: bool,
}

/// Optional state backend. Implementations must make `save` atomic from the
/// reader's point of view; the manager itself remains useful without a store.
pub trait SnapshotStore: Send + Sync {
    fn save(&self, state: &PersistedState) -> Result<(), String>;
    fn load(&self) -> Result<Option<PersistedState>, String>;
}

#[derive(Clone)]
struct Task {
    task_id: String,
    parent_id: Option<String>,
    budget_secs: f64,
    mode: Mode,
    schedule: Schedule,
    metadata: HashMap<String, String>,
    status: TaskStatus,
    started_at: Duration,
    base_elapsed_secs: f64,
    finished_elapsed_secs: Option<f64>,
    last_phase: Option<String>,
    child_reserved_secs: f64,
    children: BTreeSet<String>,
    ticks: u64,
    checkpoints: u64,
    last_checkpoint: Option<CheckpointRecord>,
    plan_submitted: bool,
}

impl Task {
    fn runtime_secs(&self, now: Duration) -> f64 {
        self.finished_elapsed_secs.unwrap_or_else(|| {
            self.base_elapsed_secs + now.saturating_sub(self.started_at).as_secs_f64()
        })
    }
}

/// Thread-safe task registry and policy engine.
pub struct TaskManager {
    clock: Arc<dyn Clock>,
    tasks: RwLock<HashMap<String, Task>>,
    store: Option<Arc<dyn SnapshotStore>>,
}

impl TaskManager {
    pub fn new<C>(clock: C) -> Self
    where
        C: Clock + 'static,
    {
        Self::from_clock(Arc::new(clock))
    }

    pub fn from_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            tasks: RwLock::new(HashMap::new()),
            store: None,
        }
    }

    pub fn with_store<C, S>(clock: C, store: Arc<S>) -> Result<Self, TaskError>
    where
        C: Clock + 'static,
        S: SnapshotStore + 'static,
    {
        let manager = Self {
            clock: Arc::new(clock),
            tasks: RwLock::new(HashMap::new()),
            store: Some(store),
        };
        manager.recover_from_store()?;
        Ok(manager)
    }

    pub fn task_count(&self) -> usize {
        self.tasks.read().expect("task lock poisoned").len()
    }

    pub fn get_task(&self, task_id: &str) -> Result<TaskView, TaskError> {
        let tasks = self.tasks.read().expect("task lock poisoned");
        let task = tasks
            .get(task_id)
            .ok_or_else(|| TaskError::NotFound(task_id.to_owned()))?;
        Ok(self.view_locked(task, &tasks, self.clock.now()))
    }

    pub fn list_tasks(&self) -> Vec<TaskView> {
        let tasks = self.tasks.read().expect("task lock poisoned");
        let now = self.clock.now();
        let mut views: Vec<_> = tasks
            .values()
            .map(|task| self.view_locked(task, &tasks, now))
            .collect();
        views.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        views
    }

    pub fn start_task(&self, request: StartTaskRequest) -> Result<StartTaskOutcome, TaskError> {
        validate_id(&request.task_id)?;
        validate_budget(request.budget_secs)?;
        request.schedule.validate()?;
        let mut tasks = self.tasks.write().expect("task lock poisoned");
        let before = tasks.clone();
        if tasks.contains_key(&request.task_id) {
            return Err(TaskError::AlreadyExists(request.task_id));
        }
        let now = self.clock.now();
        let mut effective_budget = request.budget_secs;
        if let Some(parent_id) = request.parent_id.as_deref() {
            let parent = tasks
                .get(parent_id)
                .ok_or_else(|| TaskError::ParentNotFound(parent_id.to_owned()))?;
            let parent_view = self.view_locked(parent, &tasks, now);
            if parent_view.is_done() {
                return Err(TaskError::ParentUnavailable(parent_id.to_owned()));
            }
            effective_budget = request.budget_secs.min(parent_view.available_secs);
            if effective_budget <= EPSILON {
                return Err(TaskError::BudgetExhausted(parent_id.to_owned()));
            }
        }

        let clamped = effective_budget + EPSILON < request.budget_secs;
        let task_id = request.task_id.clone();
        let parent_id = request.parent_id.clone();
        let initial_phase = request
            .schedule
            .phase_at(0.0)
            .map(|phase| phase.name.clone());
        let task = Task {
            task_id: task_id.clone(),
            parent_id: parent_id.clone(),
            budget_secs: effective_budget,
            mode: request.mode,
            schedule: request.schedule,
            metadata: request.metadata,
            status: TaskStatus::Active,
            started_at: now,
            base_elapsed_secs: 0.0,
            finished_elapsed_secs: None,
            last_phase: initial_phase,
            child_reserved_secs: 0.0,
            children: BTreeSet::new(),
            ticks: 0,
            checkpoints: 0,
            last_checkpoint: None,
            plan_submitted: false,
        };
        tasks.insert(task_id.clone(), task);
        if let Some(parent_id) = parent_id {
            let parent = tasks
                .get_mut(&parent_id)
                .expect("parent checked immediately before insertion");
            parent.children.insert(task_id.clone());
            parent.child_reserved_secs += effective_budget;
        }
        self.persist_or_rollback(&mut tasks, before, now)?;
        let task = tasks.get(&task_id).expect("task inserted");
        Ok(StartTaskOutcome {
            task: self.view_locked(task, &tasks, now),
            requested_budget_secs: request.budget_secs,
            effective_budget_secs: effective_budget,
            clamped,
        })
    }

    pub fn tick(&self, request: TickRequest) -> Result<TickOutcome, TaskError> {
        self.tick_with_timing(request).map(|(outcome, _)| outcome)
    }

    /// Tick once and return explicit actual/accounted deadline timing without
    /// changing the stable [`TaskView`] contract.
    pub fn tick_with_timing(
        &self,
        request: TickRequest,
    ) -> Result<(TickOutcome, TaskTiming), TaskError> {
        validate_id(&request.task_id)?;
        let mut tasks = self.tasks.write().expect("task lock poisoned");
        let now = self.clock.now();
        let (tick, phase_changed) = {
            let task = tasks
                .get_mut(&request.task_id)
                .ok_or_else(|| TaskError::NotFound(request.task_id.clone()))?;
            if matches!(task.status, TaskStatus::Finished) {
                return Err(TaskError::NotActive(request.task_id));
            }
            let runtime = task.runtime_secs(now);
            let current_phase = task
                .schedule
                .phase_at(runtime.max(0.0))
                .map(|phase| phase.name.clone());
            let phase_changed = task.last_phase != current_phase;
            task.last_phase = current_phase;
            task.ticks = task.ticks.saturating_add(1);
            if runtime + EPSILON >= task.budget_secs {
                task.status = TaskStatus::Exhausted;
            }
            (task.ticks, phase_changed)
        };
        let task = tasks.get(&request.task_id).expect("task checked");
        let view = self.view_locked(task, &tasks, now);
        let timing = TaskTiming::new(task.runtime_secs(now), task.budget_secs);
        Ok((
            TickOutcome {
                phase_changed,
                task: view,
                tick,
            },
            timing,
        ))
    }

    /// Return timing for active or completed tasks without mutating state.
    pub fn task_timing(&self, task_id: &str) -> Result<TaskTiming, TaskError> {
        validate_id(task_id)?;
        let tasks = self.tasks.read().expect("task lock poisoned");
        let task = tasks
            .get(task_id)
            .ok_or_else(|| TaskError::NotFound(task_id.to_owned()))?;
        Ok(TaskTiming::new(
            task.runtime_secs(self.clock.now()),
            task.budget_secs,
        ))
    }

    pub fn checkpoint(&self, request: CheckpointRequest) -> Result<CheckpointOutcome, TaskError> {
        validate_id(&request.task_id)?;
        if let Some(note) = &request.note
            && note.len() > MAX_NOTE_LEN
        {
            return Err(TaskError::Invalid("checkpoint note is too long".into()));
        }
        if let Some(progress) = request.progress
            && (!progress.is_finite() || !(0.0..=1.0).contains(&progress))
        {
            return Err(TaskError::Invalid(
                "checkpoint progress must be between 0 and 1".into(),
            ));
        }
        if let Some(eta) = request.estimated_remaining_work_secs
            && (!eta.is_finite() || eta <= 0.0)
        {
            return Err(TaskError::Invalid(
                "estimated_remaining_work_secs must be finite and > 0".into(),
            ));
        }
        let mut tasks = self.tasks.write().expect("task lock poisoned");
        let before = tasks.clone();
        let now = self.clock.now();
        let checkpoint = {
            let task = tasks
                .get_mut(&request.task_id)
                .ok_or_else(|| TaskError::NotFound(request.task_id.clone()))?;
            if matches!(task.status, TaskStatus::Finished) {
                return Err(TaskError::NotActive(request.task_id));
            }
            if !task.plan_submitted && !request.plan_complete {
                return Err(TaskError::Invalid(
                    "first checkpoint must submit a compact execution plan".into(),
                ));
            }
            if request.plan_complete {
                let note_is_valid = request
                    .note
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|note| note.len() >= 20);
                if !note_is_valid {
                    return Err(TaskError::Invalid(
                        "plan checkpoint requires a compact plan in note".into(),
                    ));
                }
                if !request
                    .estimated_remaining_work_secs
                    .is_some_and(|eta| eta.is_finite() && eta > 0.0)
                {
                    return Err(TaskError::Invalid(
                        "plan checkpoint requires estimated_remaining_work_secs > 0".into(),
                    ));
                }
            }
            if let Some(new_progress) = request.progress
                && let Some(previous_progress) = task
                    .last_checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.progress)
                && new_progress + EPSILON < previous_progress
                && !request.replan
            {
                return Err(TaskError::Invalid(
                    "progress cannot decrease unless replan=true".into(),
                ));
            }
            task.checkpoints = task.checkpoints.saturating_add(1);
            let checkpoint = CheckpointRecord {
                sequence: task.checkpoints,
                elapsed_secs: task.runtime_secs(now),
                note: request.note,
                progress: request.progress,
                estimated_remaining_work_secs: request.estimated_remaining_work_secs,
                plan_complete: request.plan_complete,
            };
            if request.plan_complete {
                task.plan_submitted = true;
            }
            task.last_checkpoint = Some(checkpoint.clone());
            checkpoint
        };
        self.persist_or_rollback(&mut tasks, before, now)?;
        let task = tasks
            .get(&request.task_id)
            .expect("task checked before persistence");
        Ok(CheckpointOutcome {
            task: self.view_locked(task, &tasks, now),
            checkpoint,
        })
    }

    pub fn adjust_task(&self, request: AdjustTaskRequest) -> Result<AdjustTaskOutcome, TaskError> {
        validate_id(&request.task_id)?;
        if request.budget_secs.is_none() && request.mode.is_none() && request.schedule.is_none() {
            return Err(TaskError::EmptyAdjustment);
        }
        if let Some(budget) = request.budget_secs {
            validate_budget(budget)?;
        }
        if let Some(schedule) = &request.schedule {
            schedule.validate()?;
        }
        let mut tasks = self.tasks.write().expect("task lock poisoned");
        let before = tasks.clone();
        let now = self.clock.now();
        let current_runtime;
        let old_budget;
        let parent_id;
        {
            let task = tasks
                .get(&request.task_id)
                .ok_or_else(|| TaskError::NotFound(request.task_id.clone()))?;
            if matches!(task.status, TaskStatus::Finished) {
                return Err(TaskError::NotActive(request.task_id));
            }
            current_runtime = task.runtime_secs(now);
            old_budget = task.budget_secs;
            parent_id = task.parent_id.clone();
        }
        let requested_budget = request.budget_secs;
        let mut effective_budget = old_budget;
        let mut clamped = false;
        if let Some(requested) = requested_budget {
            if requested + EPSILON < current_runtime {
                return Err(TaskError::Invalid(
                    "new budget cannot be less than elapsed runtime".into(),
                ));
            }
            effective_budget = requested;
            if let Some(parent_id) = parent_id.as_deref() {
                let (parent_budget, parent_runtime, parent_reserved) = {
                    let parent = tasks
                        .get(parent_id)
                        .ok_or_else(|| TaskError::ParentNotFound(parent_id.to_owned()))?;
                    (
                        parent.budget_secs,
                        parent.runtime_secs(now),
                        parent.child_reserved_secs,
                    )
                };
                let parent_available = self.available_secs(
                    parent_budget,
                    parent_runtime,
                    parent_reserved - old_budget,
                    &tasks[parent_id].mode,
                    &tasks[parent_id].schedule,
                );
                effective_budget = requested.min(parent_available);
                if effective_budget + EPSILON < current_runtime {
                    return Err(TaskError::BudgetExhausted(parent_id.to_owned()));
                }
            }
            clamped = effective_budget + EPSILON < requested;
        }
        if let Some(parent_id) = parent_id.as_deref() {
            let parent = tasks
                .get_mut(parent_id)
                .ok_or_else(|| TaskError::ParentNotFound(parent_id.to_owned()))?;
            if requested_budget.is_some() {
                parent.child_reserved_secs =
                    (parent.child_reserved_secs - old_budget + effective_budget).max(0.0);
            }
        }
        {
            let task = tasks
                .get_mut(&request.task_id)
                .expect("task checked before adjustment");
            if requested_budget.is_some() {
                task.budget_secs = effective_budget;
            }
            if let Some(mode) = request.mode {
                task.mode = mode;
            }
            if let Some(schedule) = request.schedule {
                task.schedule = schedule;
            }
        }
        self.persist_or_rollback(&mut tasks, before, now)?;
        let task = tasks
            .get(&request.task_id)
            .expect("task checked before persistence");
        Ok(AdjustTaskOutcome {
            task: self.view_locked(task, &tasks, now),
            requested_budget_secs: requested_budget,
            effective_budget_secs: effective_budget,
            clamped,
        })
    }

    pub fn finish_task(&self, request: FinishTaskRequest) -> Result<FinishTaskOutcome, TaskError> {
        validate_id(&request.task_id)?;
        if let Some(reason) = &request.reason
            && reason.len() > MAX_NOTE_LEN
        {
            return Err(TaskError::Invalid("finish reason is too long".into()));
        }
        let mut tasks = self.tasks.write().expect("task lock poisoned");
        let before = tasks.clone();
        let now = self.clock.now();
        let active_children: Vec<String> = {
            let task = tasks
                .get(&request.task_id)
                .ok_or_else(|| TaskError::NotFound(request.task_id.clone()))?;
            task.children
                .iter()
                .filter(|child_id| {
                    tasks
                        .get(*child_id)
                        .map(|child| matches!(child.status, TaskStatus::Active))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        };
        if !active_children.is_empty() && !request.force {
            return Err(TaskError::ActiveChildren(request.task_id));
        }
        if request.force {
            for child_id in &active_children {
                if let Some(child) = tasks.get_mut(child_id) {
                    child.parent_id = None;
                }
            }
        }
        let (parent_id, budget, runtime) = {
            let task = tasks
                .get_mut(&request.task_id)
                .ok_or_else(|| TaskError::NotFound(request.task_id.clone()))?;
            if matches!(task.status, TaskStatus::Finished) {
                return Err(TaskError::NotActive(request.task_id));
            }
            let runtime = task.runtime_secs(now);
            let parent_id = task.parent_id.clone();
            let budget = task.budget_secs;
            task.finished_elapsed_secs = Some(runtime);
            task.status = TaskStatus::Finished;
            task.children.clear();
            (parent_id, budget, runtime)
        };
        if let Some(parent_id) = parent_id.as_deref()
            && let Some(parent) = tasks.get_mut(parent_id)
        {
            parent.children.remove(&request.task_id);
            parent.child_reserved_secs = (parent.child_reserved_secs - budget).max(0.0);
        }
        self.persist_or_rollback(&mut tasks, before, now)?;
        let task = tasks
            .get(&request.task_id)
            .expect("task checked before persistence");
        let mut view = self.view_locked(task, &tasks, now);
        // Preserve the pre-existing finish contract: completed task views
        // report actual runtime, while live views remain budget-accounted.
        view.elapsed_secs = runtime;
        Ok(FinishTaskOutcome {
            task: view,
            reason: request.reason,
            actual_elapsed_secs: runtime,
            overrun_secs: (runtime - budget).max(0.0),
        })
    }

    pub fn snapshot(&self) -> PersistedState {
        let tasks = self.tasks.read().expect("task lock poisoned");
        self.snapshot_locked(&tasks, self.clock.now())
    }

    fn view_locked(&self, task: &Task, tasks: &HashMap<String, Task>, now: Duration) -> TaskView {
        let elapsed = task.runtime_secs(now).min(task.budget_secs.max(0.0));
        let remaining = (task.budget_secs - elapsed).max(0.0);
        let phase = task.schedule.phase_at(elapsed);
        let mode = phase
            .and_then(|phase| phase.mode.clone())
            .unwrap_or_else(|| task.mode.clone());
        let ratio = phase
            .and_then(|phase| phase.reserve_ratio)
            .unwrap_or_else(|| mode.reserve_ratio());
        let work_remaining = (remaining - task.child_reserved_secs).max(0.0);
        let reserve = if matches!(task.status, TaskStatus::Finished | TaskStatus::Exhausted) {
            0.0
        } else {
            (work_remaining * ratio).min(work_remaining)
        };
        let interval = phase.and_then(|phase| phase.tick_interval_secs).unwrap_or(
            task.schedule
                .tick_interval_secs
                .max(mode.default_tick_interval_secs()),
        );
        let status = if matches!(task.status, TaskStatus::Active) && remaining <= EPSILON {
            TaskStatus::Exhausted
        } else {
            task.status.clone()
        };
        let available = (work_remaining - reserve).max(0.0);
        let mut children: Vec<_> = task
            .children
            .iter()
            .filter(|child_id| tasks.contains_key(*child_id))
            .cloned()
            .collect();
        children.sort();
        TaskView {
            task_id: task.task_id.clone(),
            parent_id: task.parent_id.clone(),
            budget_secs: task.budget_secs,
            elapsed_secs: elapsed,
            remaining_secs: remaining,
            child_reserved_secs: task.child_reserved_secs,
            adaptive_reserve_secs: reserve,
            available_secs: available,
            recommended_work_secs: if matches!(status, TaskStatus::Active) {
                interval.min(available)
            } else {
                0.0
            },
            mode,
            phase: phase.map(|phase| phase.name.clone()),
            status,
            ticks: task.ticks,
            checkpoints: task.checkpoints,
            last_checkpoint: task.last_checkpoint.clone(),
            children,
            metadata: task.metadata.clone(),
            plan_submitted: task.plan_submitted,
        }
    }

    fn available_secs(
        &self,
        budget: f64,
        elapsed: f64,
        child_reserved: f64,
        mode: &Mode,
        schedule: &Schedule,
    ) -> f64 {
        let remaining = (budget - elapsed).max(0.0);
        let phase = schedule.phase_at(elapsed);
        let effective_mode = phase.and_then(|phase| phase.mode.as_ref()).unwrap_or(mode);
        let ratio = phase
            .and_then(|phase| phase.reserve_ratio)
            .unwrap_or_else(|| effective_mode.reserve_ratio());
        let work_remaining = (remaining - child_reserved).max(0.0);
        (work_remaining * (1.0 - ratio)).max(0.0)
    }

    fn snapshot_locked(&self, tasks: &HashMap<String, Task>, now: Duration) -> PersistedState {
        let mut persisted: Vec<_> = tasks
            .values()
            .map(|task| PersistedTask {
                task_id: task.task_id.clone(),
                parent_id: task.parent_id.clone(),
                budget_secs: task.budget_secs,
                mode: task.mode.clone(),
                schedule: task.schedule.clone(),
                metadata: task.metadata.clone(),
                status: task.status.clone(),
                elapsed_secs: task.runtime_secs(now),
                ticks: task.ticks,
                checkpoints: task.checkpoints,
                last_checkpoint: task.last_checkpoint.clone(),
                plan_submitted: task.plan_submitted,
            })
            .collect();
        persisted.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        PersistedState {
            version: SNAPSHOT_VERSION,
            saved_at_unix_ms: unix_ms(),
            tasks: persisted,
        }
    }

    fn persist_locked(
        &self,
        tasks: &HashMap<String, Task>,
        now: Duration,
    ) -> Result<(), TaskError> {
        if let Some(store) = &self.store {
            store
                .save(&self.snapshot_locked(tasks, now))
                .map_err(TaskError::Persistence)?;
        }
        Ok(())
    }

    fn persist_or_rollback(
        &self,
        tasks: &mut HashMap<String, Task>,
        before: HashMap<String, Task>,
        now: Duration,
    ) -> Result<(), TaskError> {
        if let Err(error) = self.persist_locked(tasks, now) {
            *tasks = before;
            return Err(error);
        }
        Ok(())
    }

    fn recover_from_store(&self) -> Result<(), TaskError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let Some(snapshot) = store.load().map_err(TaskError::Persistence)? else {
            return Ok(());
        };
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(TaskError::CorruptSnapshot(format!(
                "unsupported version {}",
                snapshot.version
            )));
        }
        let now = self.clock.now();
        let downtime_secs = unix_ms().saturating_sub(snapshot.saved_at_unix_ms) as f64 / 1000.0;
        let mut restored = HashMap::new();
        for persisted in snapshot.tasks {
            validate_id(&persisted.task_id)
                .map_err(|error| TaskError::CorruptSnapshot(error.to_string()))?;
            validate_budget(persisted.budget_secs)
                .map_err(|error| TaskError::CorruptSnapshot(error.to_string()))?;
            if !persisted.elapsed_secs.is_finite() || persisted.elapsed_secs < 0.0 {
                return Err(TaskError::CorruptSnapshot(
                    "elapsed_secs must be finite and >= 0".into(),
                ));
            }
            persisted
                .schedule
                .validate()
                .map_err(|error| TaskError::CorruptSnapshot(error.to_string()))?;
            if restored.contains_key(&persisted.task_id) {
                return Err(TaskError::CorruptSnapshot(format!(
                    "duplicate task {}",
                    persisted.task_id
                )));
            }
            let recovered_elapsed = if matches!(persisted.status, TaskStatus::Active) {
                persisted.elapsed_secs + downtime_secs
            } else {
                persisted.elapsed_secs
            };
            let status = if matches!(persisted.status, TaskStatus::Active)
                && recovered_elapsed + EPSILON >= persisted.budget_secs
            {
                TaskStatus::Exhausted
            } else {
                persisted.status.clone()
            };
            let recovered_phase = persisted
                .schedule
                .phase_at(recovered_elapsed)
                .map(|phase| phase.name.clone());
            restored.insert(
                persisted.task_id.clone(),
                Task {
                    task_id: persisted.task_id,
                    parent_id: persisted.parent_id,
                    budget_secs: persisted.budget_secs,
                    mode: persisted.mode,
                    schedule: persisted.schedule,
                    metadata: persisted.metadata,
                    status,
                    started_at: now,
                    base_elapsed_secs: recovered_elapsed,
                    finished_elapsed_secs: if matches!(persisted.status, TaskStatus::Finished) {
                        Some(recovered_elapsed)
                    } else {
                        None
                    },
                    last_phase: recovered_phase,
                    child_reserved_secs: 0.0,
                    children: BTreeSet::new(),
                    ticks: persisted.ticks,
                    checkpoints: persisted.checkpoints,
                    last_checkpoint: persisted.last_checkpoint,
                    plan_submitted: persisted.plan_submitted,
                },
            );
        }
        let ids: Vec<String> = restored.keys().cloned().collect();
        for task_id in ids {
            let parent_id = restored
                .get(&task_id)
                .and_then(|task| task.parent_id.clone());
            if let Some(parent_id) = parent_id {
                if parent_id == task_id || !restored.contains_key(&parent_id) {
                    return Err(TaskError::CorruptSnapshot(format!(
                        "invalid parent for {task_id}: {parent_id}"
                    )));
                }
                let is_active = restored
                    .get(&task_id)
                    .map(|task| matches!(task.status, TaskStatus::Active))
                    .unwrap_or(false);
                if is_active {
                    let budget = restored
                        .get(&task_id)
                        .map(|task| task.budget_secs)
                        .unwrap_or(0.0);
                    let parent = restored.get_mut(&parent_id).expect("parent checked");
                    parent.children.insert(task_id.clone());
                    parent.child_reserved_secs += budget;
                }
            }
        }
        validate_no_cycles(&restored)?;
        *self.tasks.write().expect("task lock poisoned") = restored;
        Ok(())
    }
}

fn validate_id(id: &str) -> Result<(), TaskError> {
    if id.trim().is_empty() || id.len() > MAX_ID_LEN {
        return Err(TaskError::Invalid(
            "task_id must be non-empty and at most 256 bytes".into(),
        ));
    }
    Ok(())
}

fn validate_budget(budget: f64) -> Result<(), TaskError> {
    if !budget.is_finite() || budget <= 0.0 {
        return Err(TaskError::Invalid(
            "budget_secs must be finite and > 0".into(),
        ));
    }
    Ok(())
}

fn validate_no_cycles(tasks: &HashMap<String, Task>) -> Result<(), TaskError> {
    for id in tasks.keys() {
        let mut seen = BTreeSet::new();
        let mut cursor = Some(id.as_str());
        while let Some(current) = cursor {
            if !seen.insert(current) {
                return Err(TaskError::CorruptSnapshot(format!(
                    "cycle involving task {id}"
                )));
            }
            cursor = tasks
                .get(current)
                .and_then(|task| task.parent_id.as_deref());
        }
    }
    Ok(())
}

/// An in-memory store useful for embedding and recovery tests.
#[derive(Default)]
pub struct MemoryStore {
    state: RwLock<Option<PersistedState>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> Option<PersistedState> {
        self.state
            .read()
            .expect("memory store lock poisoned")
            .clone()
    }
}

impl SnapshotStore for MemoryStore {
    fn save(&self, state: &PersistedState) -> Result<(), String> {
        *self.state.write().expect("memory store lock poisoned") = Some(state.clone());
        Ok(())
    }

    fn load(&self) -> Result<Option<PersistedState>, String> {
        Ok(self
            .state
            .read()
            .expect("memory store lock poisoned")
            .clone())
    }
}

/// POSIX-friendly atomic JSON snapshot store.  A temporary file is flushed and
/// renamed over the destination, so readers see either the old or new snapshot.
#[derive(Debug)]
pub struct FileStore {
    path: PathBuf,
    _lock: File,
}

impl FileStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state.json");
        let lock_path = path.with_file_name(format!(".{file_name}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(lock_path)
            .map_err(|error| error.to_string())?;
        lock.try_lock_exclusive()
            .map_err(|error| format!("state already has a writer: {error}"))?;
        Ok(Self { path, _lock: lock })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn temporary_path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state.json");
        self.path.with_file_name(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }
}

impl SnapshotStore for FileStore {
    fn save(&self, state: &PersistedState) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let temporary = self.temporary_path();
        let bytes = serde_json::to_vec_pretty(state).map_err(|error| error.to_string())?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)
                .map_err(|error| error.to_string())?;
            file.write_all(&bytes).map_err(|error| error.to_string())?;
            file.sync_all().map_err(|error| error.to_string())?;
            fs::rename(&temporary, &self.path).map_err(|error| error.to_string())?;
            if let Some(parent) = self.path.parent()
                && let Ok(directory) = File::open(parent)
            {
                let _ = directory.sync_all();
            }
            Ok::<(), String>(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn load(&self) -> Result<Option<PersistedState>, String> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_validation_rejects_non_monotonic_phases() {
        let schedule = Schedule::default().with_phases(vec![
            SchedulePhase::new("a", 2.0),
            SchedulePhase::new("b", 1.0),
        ]);
        assert!(schedule.validate().is_err());
    }

    #[test]
    fn manual_clock_is_monotonic_and_shared() {
        let clock = ManualClock::new();
        let other = clock.clone();
        other.advance_secs(1.25);
        assert!((clock.elapsed_secs() - 1.25).abs() < 1e-9);
    }
}
