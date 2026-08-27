use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeMode {
    Explore,
    Execute,
    Converge,
    Validate,
    Finalize,
    Emergency,
    Expired,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleStatus {
    Ahead,
    OnTrack,
    Behind,
    Critical,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PolicyInput {
    pub total_secs: f64,
    pub elapsed_secs: f64,
    pub progress_percent: Option<f64>,
    pub estimated_remaining_work_secs: Option<f64>,
    pub validation_reserve_percent: Option<f64>,
    pub finalization_reserve_percent: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PolicyDecision {
    pub remaining_secs: f64,
    pub remaining_percent: f64,
    pub validation_reserve_secs: f64,
    pub finalization_reserve_secs: f64,
    pub reserved_secs: f64,
    pub usable_work_secs: f64,
    pub mode: TimeMode,
    pub schedule: ScheduleStatus,
    pub max_new_action_secs: f64,
    pub next_check_secs: f64,
    pub must_converge: bool,
    pub must_validate: bool,
    pub must_finalize: bool,
    pub must_stop: bool,
    pub reason: &'static str,
}

pub fn adaptive_reserve_percents(total_secs: f64) -> (f64, f64) {
    if total_secs <= 60.0 {
        (15.0, 10.0)
    } else if total_secs <= 600.0 {
        (12.0, 8.0)
    } else if total_secs <= 14_400.0 {
        (10.0, 5.0)
    } else if total_secs <= 28_800.0 {
        (8.0, 4.0)
    } else {
        (6.0, 3.0)
    }
}

pub fn evaluate_time_policy(input: PolicyInput) -> PolicyDecision {
    let total = input.total_secs.max(0.001);
    let elapsed = input.elapsed_secs.clamp(0.0, total);
    let remaining = (total - elapsed).max(0.0);
    let remaining_percent = remaining / total * 100.0;
    let (default_validation, default_finalization) = adaptive_reserve_percents(total);
    let validation_percent = input
        .validation_reserve_percent
        .unwrap_or(default_validation)
        .clamp(5.0, 40.0);
    let finalization_percent = input
        .finalization_reserve_percent
        .unwrap_or(default_finalization)
        .clamp(3.0, 25.0);
    let validation_reserve = total * validation_percent / 100.0;
    let finalization_reserve = total * finalization_percent / 100.0;
    let reserved = (validation_reserve + finalization_reserve).min(total);
    let usable_work = (remaining - reserved).max(0.0);

    let progress = input.progress_percent.map(|p| p.clamp(0.0, 100.0));
    let expected_progress = (elapsed / (total - reserved).max(0.001) * 100.0).clamp(0.0, 100.0);
    let eta_critical = input
        .estimated_remaining_work_secs
        .is_some_and(|eta| eta.max(0.0) > usable_work.max(0.001));
    let schedule = if remaining <= f64::EPSILON || eta_critical {
        ScheduleStatus::Critical
    } else if let Some(progress) = progress {
        let delta = progress - expected_progress;
        if delta >= 12.0 {
            ScheduleStatus::Ahead
        } else if delta < -30.0 {
            ScheduleStatus::Critical
        } else if delta < -12.0 {
            ScheduleStatus::Behind
        } else {
            ScheduleStatus::OnTrack
        }
    } else if remaining <= reserved {
        ScheduleStatus::Critical
    } else {
        ScheduleStatus::OnTrack
    };

    let emergency_threshold = (total * 0.02).clamp(10.0, 30.0);
    let finalize_trigger = finalization_reserve.max((total * 0.10).min(300.0));
    let (mode, reason) = if remaining <= f64::EPSILON {
        (TimeMode::Expired, "hard_deadline_reached")
    } else if remaining <= emergency_threshold {
        (TimeMode::Emergency, "emergency_window")
    } else if remaining <= finalize_trigger {
        (TimeMode::Finalize, "finalization_window")
    } else if remaining <= reserved {
        (TimeMode::Validate, "validation_reserve")
    } else if matches!(schedule, ScheduleStatus::Behind | ScheduleStatus::Critical)
        || remaining_percent <= 25.0
    {
        (TimeMode::Converge, "schedule_pressure")
    } else if elapsed / total >= 0.25 {
        (TimeMode::Execute, "execution_window")
    } else {
        (TimeMode::Explore, "exploration_window")
    };

    let max_action = match mode {
        TimeMode::Explore => (usable_work * 0.35).min(600.0),
        TimeMode::Execute => (usable_work * 0.30).min(300.0),
        TimeMode::Converge => (usable_work * 0.25).min(120.0),
        TimeMode::Validate => (remaining - finalization_reserve).clamp(0.0, 120.0),
        TimeMode::Finalize => remaining.min(30.0) * 0.5,
        TimeMode::Emergency => remaining.min(10.0) * 0.5,
        TimeMode::Expired => 0.0,
    }
    .max(0.0);

    let base_next: f64 = if remaining > 7_200.0 {
        600.0
    } else if remaining > 1_800.0 {
        180.0
    } else if remaining > 600.0 {
        120.0
    } else if remaining > 180.0 {
        60.0
    } else if remaining > 60.0 {
        25.0
    } else {
        7.0
    };
    let next_check = if matches!(mode, TimeMode::Expired) {
        0.0
    } else {
        base_next.min(max_action.max(0.25)).min(remaining)
    };

    PolicyDecision {
        remaining_secs: remaining,
        remaining_percent,
        validation_reserve_secs: validation_reserve,
        finalization_reserve_secs: finalization_reserve,
        reserved_secs: reserved,
        usable_work_secs: usable_work,
        mode,
        schedule,
        max_new_action_secs: max_action,
        next_check_secs: next_check,
        must_converge: matches!(
            mode,
            TimeMode::Converge
                | TimeMode::Validate
                | TimeMode::Finalize
                | TimeMode::Emergency
                | TimeMode::Expired
        ),
        must_validate: matches!(mode, TimeMode::Validate),
        must_finalize: matches!(mode, TimeMode::Finalize | TimeMode::Emergency),
        must_stop: matches!(mode, TimeMode::Expired),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(total: f64, elapsed: f64, progress: Option<f64>) -> PolicyDecision {
        evaluate_time_policy(PolicyInput {
            total_secs: total,
            elapsed_secs: elapsed,
            progress_percent: progress,
            ..PolicyInput::default()
        })
    }

    #[test]
    fn required_scenarios() {
        let a = at(1800.0, 600.0, Some(45.0));
        assert_eq!(a.mode, TimeMode::Execute);
        assert!(matches!(
            a.schedule,
            ScheduleStatus::Ahead | ScheduleStatus::OnTrack
        ));

        let b = at(1800.0, 1200.0, Some(20.0));
        assert_eq!(b.mode, TimeMode::Converge);
        assert!(matches!(
            b.schedule,
            ScheduleStatus::Behind | ScheduleStatus::Critical
        ));

        assert_eq!(at(1800.0, 1620.0, None).mode, TimeMode::Finalize);
        assert_eq!(at(1800.0, 1780.0, None).mode, TimeMode::Emergency);
        let expired = at(1800.0, 1800.0, None);
        assert_eq!(expired.mode, TimeMode::Expired);
        assert!(expired.must_stop);
    }

    #[test]
    fn transitions_and_edges_are_safe() {
        let total = 1000.0;
        let modes = [
            at(total, 0.0, None).mode,
            at(total, 300.0, None).mode,
            at(total, 760.0, None).mode,
            at(total, 860.0, None).mode,
            at(total, 930.0, None).mode,
            at(total, 985.0, None).mode,
            at(total, 1000.0, None).mode,
        ];
        assert_eq!(modes[0], TimeMode::Explore);
        assert_eq!(modes[1], TimeMode::Execute);
        assert!(modes.contains(&TimeMode::Converge));
        assert!(modes.contains(&TimeMode::Validate));
        assert!(modes.contains(&TimeMode::Finalize));
        assert!(modes.contains(&TimeMode::Emergency));
        assert_eq!(modes[6], TimeMode::Expired);
        for budget in [1.0, 10.0, 300.0, 28_800.0, 259_200.0] {
            let decision = at(budget, budget * 2.0, Some(0.0));
            assert_eq!(decision.remaining_secs, 0.0);
            assert!(decision.must_stop);
        }
    }
}
