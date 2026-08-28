use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use time_strike::{
    AdjustTaskRequest, CheckpointRequest, FileStore, FinishTaskRequest, ManualClock, MemoryStore,
    Mode, PersistedState, Schedule, SchedulePhase, SnapshotStore, StartTaskRequest, TaskError,
    TaskManager, TaskStatus, TickRequest,
};

fn manager() -> (ManualClock, TaskManager) {
    let clock = ManualClock::new();
    let manager = TaskManager::new(clock.clone());
    (clock, manager)
}

#[test]
fn clock_and_budget_are_monotonic_and_reserve_adapts() {
    let (clock, manager) = manager();
    let started = manager
        .start_task(StartTaskRequest::new("root", 10.0))
        .unwrap();
    assert_eq!(started.task.status, TaskStatus::Active);
    assert!((started.task.elapsed_secs - 0.0).abs() < 1e-9);
    assert!((started.task.adaptive_reserve_secs - 1.2).abs() < 1e-9);
    assert!((started.task.available_secs - 8.8).abs() < 1e-9);

    clock.advance_secs(3.0);
    let tick = manager.tick(TickRequest::new("root")).unwrap();
    assert_eq!(tick.tick, 1);
    assert!((tick.task.elapsed_secs - 3.0).abs() < 1e-9);
    assert!((tick.task.remaining_secs - 7.0).abs() < 1e-9);
    assert!((tick.task.adaptive_reserve_secs - 0.84).abs() < 1e-9);
    assert!(tick.task.available_secs >= 6.15 && tick.task.available_secs <= 6.17);
}

#[test]
fn modes_change_policy_and_schedule_can_override_phase() {
    let (clock, manager) = manager();
    let balanced = manager
        .start_task(StartTaskRequest::new("balanced", 10.0).with_mode(Mode::Balanced))
        .unwrap();
    let interactive = manager
        .start_task(StartTaskRequest::new("interactive", 10.0).with_mode(Mode::Interactive))
        .unwrap();
    assert!(interactive.task.adaptive_reserve_secs > balanced.task.adaptive_reserve_secs);

    let schedule = Schedule::new(0.5).with_phases(vec![
        SchedulePhase::new("plan", 2.0),
        SchedulePhase::new("execute", 5.0)
            .with_mode(Mode::Deep)
            .with_reserve_ratio(0.02)
            .with_tick_interval(0.75),
    ]);
    manager
        .start_task(StartTaskRequest::new("scheduled", 10.0).with_schedule(schedule))
        .unwrap();
    assert_eq!(
        manager.get_task("scheduled").unwrap().phase.as_deref(),
        Some("plan")
    );
    clock.advance_secs(2.0);
    let tick = manager.tick(TickRequest::new("scheduled")).unwrap();
    assert!(tick.phase_changed);
    assert_eq!(tick.task.phase.as_deref(), Some("execute"));
    assert_eq!(tick.task.mode, Mode::Deep);
    assert!((tick.task.adaptive_reserve_secs - 0.16).abs() < 1e-9);
    assert!((tick.task.recommended_work_secs - 0.75).abs() < 1e-9);
}

#[test]
fn children_are_clamped_and_release_parent_budget() {
    let (_clock, manager) = manager();
    let root = manager
        .start_task(StartTaskRequest::new("root", 10.0))
        .unwrap();
    assert!((root.task.available_secs - 8.8).abs() < 1e-9);
    let child = manager
        .start_task(StartTaskRequest::new("child", 20.0).with_parent("root"))
        .unwrap();
    assert!(child.clamped);
    assert!((child.effective_budget_secs - 8.8).abs() < 1e-9);
    let root_with_child = manager.get_task("root").unwrap();
    assert!((root_with_child.child_reserved_secs - 8.8).abs() < 1e-9);
    let sibling = manager
        .start_task(StartTaskRequest::new("sibling", 0.1).with_parent("root"))
        .unwrap();
    assert!(!sibling.clamped);
    assert!(matches!(
        manager.finish_task(FinishTaskRequest::new("root")),
        Err(TaskError::ActiveChildren(_))
    ));
    manager
        .finish_task(FinishTaskRequest::new("child"))
        .unwrap();
    let root_after = manager.get_task("root").unwrap();
    assert!((root_after.child_reserved_secs - 0.1).abs() < 1e-9);
    let sibling_after_release = manager
        .start_task(StartTaskRequest::new("sibling-after-release", 0.1).with_parent("root"))
        .unwrap();
    assert!(!sibling_after_release.clamped);
}

#[test]
fn checkpoint_and_recovery_preserve_runtime_without_extra_time() {
    let clock = ManualClock::new();
    let store = Arc::new(MemoryStore::new());
    let manager = TaskManager::with_store(clock.clone(), store.clone()).unwrap();
    manager
        .start_task(StartTaskRequest::new("recover", 20.0))
        .unwrap();
    clock.advance_secs(2.5);
    manager
        .checkpoint(CheckpointRequest {
            task_id: "recover".into(),
            note: Some("Inspect state; persist plan; recover safely".into()),
            progress: Some(0.25),
            estimated_remaining_work_secs: Some(12.0),
            plan_complete: true,
            replan: false,
        })
        .unwrap();
    let before = manager.get_task("recover").unwrap();
    assert!((before.elapsed_secs - 2.5).abs() < 1e-9);

    let recovered = TaskManager::with_store(clock.clone(), store).unwrap();
    let after = recovered.get_task("recover").unwrap();
    assert!((after.elapsed_secs - before.elapsed_secs).abs() < 1e-9);
    assert_eq!(after.checkpoints, 1);
    assert_eq!(
        after.last_checkpoint.unwrap().note.as_deref(),
        Some("Inspect state; persist plan; recover safely")
    );
    assert!((clock.elapsed_secs() - 2.5).abs() < 1e-9);
}

#[test]
fn process_restart_charges_wall_clock_downtime() {
    let clock = ManualClock::new();
    let store = Arc::new(MemoryStore::new());
    let manager = TaskManager::with_store(clock.clone(), store.clone()).unwrap();
    manager
        .start_task(StartTaskRequest::new("restart", 10.0))
        .unwrap();
    clock.advance_secs(1.0);
    manager
        .checkpoint(CheckpointRequest {
            task_id: "restart".into(),
            note: Some("Persist initial plan before process restart".into()),
            progress: Some(0.0),
            estimated_remaining_work_secs: Some(8.0),
            plan_complete: true,
            replan: false,
        })
        .unwrap();

    let mut snapshot = store.state().unwrap();
    snapshot.saved_at_unix_ms = snapshot.saved_at_unix_ms.saturating_sub(2_000);
    store.save(&snapshot).unwrap();

    let restarted = TaskManager::with_store(ManualClock::new(), store).unwrap();
    let recovered = restarted.get_task("restart").unwrap();
    assert!(recovered.elapsed_secs >= 3.0);
    assert!(recovered.elapsed_secs < 3.2);
    assert!(recovered.remaining_secs <= 7.0);
}

struct FailingStore;

impl SnapshotStore for FailingStore {
    fn save(&self, _state: &PersistedState) -> Result<(), String> {
        Err("injected save failure".into())
    }

    fn load(&self) -> Result<Option<PersistedState>, String> {
        Ok(None)
    }
}

#[test]
fn persistence_failure_rolls_back_memory() {
    let manager = TaskManager::with_store(ManualClock::new(), Arc::new(FailingStore)).unwrap();
    assert!(matches!(
        manager.start_task(StartTaskRequest::new("rollback", 10.0)),
        Err(TaskError::Persistence(_))
    ));
    assert!(matches!(
        manager.get_task("rollback"),
        Err(TaskError::NotFound(_))
    ));
}

#[test]
fn file_store_rejects_a_second_writer() {
    let directory = std::env::temp_dir().join(format!(
        "time-strike-lock-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path = directory.join("state.json");
    let first = FileStore::new(&path).unwrap();
    assert!(FileStore::new(&path).is_err());
    drop(first);
    assert!(FileStore::new(&path).is_ok());
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn edge_validation_and_adjustment_are_explicit() {
    let (clock, manager) = manager();
    assert!(matches!(
        manager.start_task(StartTaskRequest::new("bad", 0.0)),
        Err(TaskError::Invalid(_))
    ));
    manager
        .start_task(StartTaskRequest::new("task", 5.0))
        .unwrap();
    assert!(matches!(
        manager.start_task(StartTaskRequest::new("task", 2.0)),
        Err(TaskError::AlreadyExists(_))
    ));
    assert_eq!(
        manager.adjust_task(AdjustTaskRequest::new("task")),
        Err(TaskError::EmptyAdjustment)
    );
    assert!(matches!(
        manager.checkpoint(CheckpointRequest {
            task_id: "task".into(),
            note: None,
            progress: Some(2.0),
            estimated_remaining_work_secs: None,
            plan_complete: false,
            replan: false,
        }),
        Err(TaskError::Invalid(_))
    ));
    clock.advance_secs(1.0);
    let adjusted = manager
        .adjust_task(
            AdjustTaskRequest::new("task")
                .with_budget(8.0)
                .with_mode(Mode::Deep),
        )
        .unwrap();
    assert!((adjusted.effective_budget_secs - 8.0).abs() < 1e-9);
    assert_eq!(adjusted.task.mode, Mode::Deep);
    assert!(matches!(
        manager.adjust_task(AdjustTaskRequest::new("task").with_budget(0.5)),
        Err(TaskError::Invalid(_))
    ));
}

#[test]
fn simulation_never_reports_negative_budget() {
    let (clock, manager) = manager();
    manager
        .start_task(StartTaskRequest::new("sim", 3.0))
        .unwrap();
    for _ in 0..400 {
        clock.advance_secs(0.01);
        let view = manager.tick(TickRequest::new("sim")).unwrap().task;
        assert!(view.remaining_secs >= 0.0);
        assert!(view.available_secs >= 0.0);
        assert!(view.adaptive_reserve_secs >= 0.0);
        if view.status == TaskStatus::Exhausted {
            break;
        }
    }
    assert_eq!(
        manager.get_task("sim").unwrap().status,
        TaskStatus::Exhausted
    );
}

#[test]
fn rwlock_manager_supports_concurrent_starts() {
    let (_clock, manager) = manager();
    let manager = Arc::new(manager);
    std::thread::scope(|scope| {
        for index in 0..16 {
            let manager = Arc::clone(&manager);
            scope.spawn(move || {
                manager
                    .start_task(StartTaskRequest::new(format!("task-{index}"), 1.0))
                    .unwrap();
            });
        }
    });
    assert_eq!(manager.task_count(), 16);
}

fn plan(task_id: &str, progress: f64, eta: f64) -> CheckpointRequest {
    CheckpointRequest {
        task_id: task_id.into(),
        note: Some("Inspect relevant files; implement minimal fix; run targeted tests".into()),
        progress: Some(progress),
        estimated_remaining_work_secs: Some(eta),
        plan_complete: true,
        replan: false,
    }
}

#[test]
fn first_checkpoint_must_submit_plan() {
    let (_, manager) = manager();
    manager
        .start_task(StartTaskRequest::new("plan", 30.0))
        .unwrap();
    assert!(matches!(
        manager.checkpoint(CheckpointRequest::new("plan")),
        Err(TaskError::Invalid(_))
    ));
    assert!(manager.checkpoint(plan("plan", 0.0, 20.0)).is_ok());
    assert!(manager.get_task("plan").unwrap().plan_submitted);
}

#[test]
fn plan_submitted_survives_recovery() {
    let store = Arc::new(MemoryStore::new());
    let manager = TaskManager::with_store(ManualClock::new(), store.clone()).unwrap();
    manager
        .start_task(StartTaskRequest::new("persist-plan", 30.0))
        .unwrap();
    manager.checkpoint(plan("persist-plan", 0.0, 20.0)).unwrap();
    let recovered = TaskManager::with_store(ManualClock::new(), store).unwrap();
    assert!(recovered.get_task("persist-plan").unwrap().plan_submitted);
}

#[test]
fn progress_regression_requires_replan() {
    let (_, manager) = manager();
    manager
        .start_task(StartTaskRequest::new("progress", 30.0))
        .unwrap();
    manager.checkpoint(plan("progress", 0.5, 15.0)).unwrap();
    let mut regressed = CheckpointRequest::new("progress");
    regressed.progress = Some(0.4);
    assert!(matches!(
        manager.checkpoint(regressed.clone()),
        Err(TaskError::Invalid(_))
    ));
    regressed.replan = true;
    assert!(manager.checkpoint(regressed).is_ok());
}

#[test]
fn finish_reports_real_overrun() {
    let (clock, manager) = manager();
    manager
        .start_task(StartTaskRequest::new("overrun", 10.0))
        .unwrap();
    clock.advance_secs(14.0);
    let outcome = manager
        .finish_task(FinishTaskRequest::new("overrun"))
        .unwrap();
    assert!((outcome.actual_elapsed_secs - 14.0).abs() < 1e-9);
    assert!((outcome.overrun_secs - 4.0).abs() < 1e-9);
}

#[test]
fn tick_preserves_actual_and_accounted_elapsed_after_deadline() {
    let (clock, manager) = manager();
    manager
        .start_task(StartTaskRequest::new("overdue-tick", 10.0))
        .unwrap();
    clock.advance_secs(14.0);

    let view = manager.tick(TickRequest::new("overdue-tick")).unwrap().task;

    assert!((view.actual_elapsed_secs - 14.0).abs() < 1e-9);
    assert!((view.accounted_elapsed_secs - 10.0).abs() < 1e-9);
    assert!((view.elapsed_secs - view.accounted_elapsed_secs).abs() < 1e-9);
    assert!((view.overrun_secs - 4.0).abs() < 1e-9);
    assert!(!view.deadline_met);
    assert_eq!(view.status, TaskStatus::Exhausted);
}

#[test]
fn finished_overrun_survives_recovery() {
    let clock = ManualClock::new();
    let store = Arc::new(MemoryStore::new());
    let manager = TaskManager::with_store(clock.clone(), store.clone()).unwrap();
    manager
        .start_task(StartTaskRequest::new("persisted-overrun", 10.0))
        .unwrap();
    clock.advance_secs(14.0);
    manager
        .finish_task(FinishTaskRequest::new("persisted-overrun"))
        .unwrap();

    let recovered = TaskManager::with_store(clock, store).unwrap();
    let view = recovered.get_task("persisted-overrun").unwrap();

    assert!((view.actual_elapsed_secs - 14.0).abs() < 1e-9);
    assert!((view.accounted_elapsed_secs - 10.0).abs() < 1e-9);
    assert!((view.overrun_secs - 4.0).abs() < 1e-9);
    assert!(!view.deadline_met);
}

struct CountingStore {
    state: std::sync::RwLock<Option<PersistedState>>,
    saves: AtomicUsize,
}
impl CountingStore {
    fn new() -> Self {
        Self {
            state: std::sync::RwLock::new(None),
            saves: AtomicUsize::new(0),
        }
    }
}
impl SnapshotStore for CountingStore {
    fn save(&self, state: &PersistedState) -> Result<(), String> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        *self.state.write().unwrap() = Some(state.clone());
        Ok(())
    }
    fn load(&self) -> Result<Option<PersistedState>, String> {
        Ok(self.state.read().unwrap().clone())
    }
}

#[test]
fn tick_does_not_write_snapshot() {
    let store = Arc::new(CountingStore::new());
    let manager = TaskManager::with_store(ManualClock::new(), store.clone()).unwrap();
    manager
        .start_task(StartTaskRequest::new("ticks", 30.0))
        .unwrap();
    assert_eq!(store.saves.load(Ordering::SeqCst), 1);
    for _ in 0..100 {
        manager.tick(TickRequest::new("ticks")).unwrap();
    }
    assert_eq!(store.saves.load(Ordering::SeqCst), 1);
    manager.checkpoint(plan("ticks", 0.0, 20.0)).unwrap();
    assert_eq!(store.saves.load(Ordering::SeqCst), 2);
}
