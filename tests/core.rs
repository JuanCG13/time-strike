use std::sync::Arc;
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
            note: Some("safe point".into()),
            progress: Some(0.25),
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
        Some("safe point")
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
        .checkpoint(CheckpointRequest::new("restart"))
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
