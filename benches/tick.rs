use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;
use time_strike::enforcement::{ActionLeaseGrant, ActionLeaseLedger};
use time_strike::{ManualClock, StartTaskRequest, TaskManager, TickRequest};

fn tick_core_10k(c: &mut Criterion) {
    let clock = ManualClock::new();
    let manager = TaskManager::new(clock.clone());
    manager
        .start_task(StartTaskRequest::new("bench", 1_000.0))
        .expect("benchmark task starts");
    c.bench_function("tick_core_10k", |bench| {
        bench.iter(|| {
            for _ in 0..10_000 {
                clock.advance_secs(0.00001);
                black_box(manager.tick(TickRequest::new("bench")).expect("tick"));
            }
        });
    });
}

fn action_lease_register_consume_10k(c: &mut Criterion) {
    c.bench_function("action_lease_register_consume_10k", |bench| {
        bench.iter_batched(
            || {
                let grants = (0..10_000)
                    .map(|index| ActionLeaseGrant {
                        lease_id: format!("bench-{index}"),
                        task_id: "bench".into(),
                        action: "benchmark action".into(),
                        duration_seconds: 0.000_001,
                        expires_in_seconds: 1.0,
                        expiry_anchor: "tick_request_started".into(),
                        one_shot: true,
                    })
                    .collect::<Vec<_>>();
                (ActionLeaseLedger::new(Duration::from_secs(100)), grants)
            },
            |(ledger, grants)| {
                for (index, grant) in grants.iter().enumerate() {
                    let request_started = Duration::from_micros(
                        u64::try_from(index).expect("10k benchmark index fits in u64"),
                    );
                    ledger
                        .register(
                            request_started,
                            "bench",
                            "benchmark action",
                            0.000_001,
                            grant,
                        )
                        .expect("benchmark lease registers");
                    ledger
                        .consume(
                            &grant.lease_id,
                            "bench",
                            "benchmark action",
                            0.000_001,
                            request_started,
                        )
                        .expect("benchmark lease consumes");
                }
                black_box(ledger);
            },
            BatchSize::LargeInput,
        );
    });
}

fn action_lease_revoke_all_10k(c: &mut Criterion) {
    c.bench_function("action_lease_revoke_all_10k_pending", |bench| {
        bench.iter_batched(
            || {
                let ledger = ActionLeaseLedger::new(Duration::from_secs(100));
                for index in 0..10_000 {
                    let task_id = format!("bench-{index}");
                    let grant = ActionLeaseGrant {
                        lease_id: format!("lease-{index}"),
                        task_id: task_id.clone(),
                        action: "benchmark action".into(),
                        duration_seconds: 0.000_001,
                        expires_in_seconds: 1.0,
                        expiry_anchor: "tick_request_started".into(),
                        one_shot: true,
                    };
                    ledger
                        .register(
                            Duration::ZERO,
                            &task_id,
                            "benchmark action",
                            0.000_001,
                            &grant,
                        )
                        .expect("benchmark lease registers");
                }
                ledger
            },
            |ledger| {
                ledger
                    .revoke_all()
                    .expect("benchmark ledger closes globally");
                black_box(ledger);
            },
            BatchSize::LargeInput,
        );
    });
}

fn action_lease_revoke_lease_10k(c: &mut Criterion) {
    c.bench_function("action_lease_revoke_lease_10k_pending", |bench| {
        bench.iter_batched(
            || {
                let ledger = ActionLeaseLedger::new(Duration::from_secs(100));
                for index in 0..10_000 {
                    let task_id = format!("bench-{index}");
                    let grant = ActionLeaseGrant {
                        lease_id: format!("lease-{index}"),
                        task_id: task_id.clone(),
                        action: "benchmark action".into(),
                        duration_seconds: 0.000_001,
                        expires_in_seconds: 1.0,
                        expiry_anchor: "tick_request_started".into(),
                        one_shot: true,
                    };
                    ledger
                        .register(
                            Duration::ZERO,
                            &task_id,
                            "benchmark action",
                            0.000_001,
                            &grant,
                        )
                        .expect("benchmark lease registers");
                }
                ledger
            },
            |ledger| {
                ledger
                    .revoke_lease("lease-9999")
                    .expect("benchmark lease revokes");
                black_box(ledger);
            },
            BatchSize::LargeInput,
        );
    });
}

fn action_lease_revoke_task_10k(c: &mut Criterion) {
    c.bench_function("action_lease_revoke_task_10k_pending", |bench| {
        bench.iter_batched(
            || {
                let ledger = ActionLeaseLedger::new(Duration::from_secs(100));
                for index in 0..10_000 {
                    let task_id = format!("bench-{index}");
                    let grant = ActionLeaseGrant {
                        lease_id: format!("lease-{index}"),
                        task_id: task_id.clone(),
                        action: "benchmark action".into(),
                        duration_seconds: 0.000_001,
                        expires_in_seconds: 1.0,
                        expiry_anchor: "tick_request_started".into(),
                        one_shot: true,
                    };
                    ledger
                        .register(
                            Duration::ZERO,
                            &task_id,
                            "benchmark action",
                            0.000_001,
                            &grant,
                        )
                        .expect("benchmark lease registers");
                }
                ledger
            },
            |ledger| {
                ledger
                    .revoke_task("bench-9999")
                    .expect("benchmark task revokes");
                black_box(ledger);
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    tick_core_10k,
    action_lease_register_consume_10k,
    action_lease_revoke_all_10k,
    action_lease_revoke_lease_10k,
    action_lease_revoke_task_10k
);
criterion_main!(benches);
