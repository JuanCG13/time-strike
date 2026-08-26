use criterion::{Criterion, black_box, criterion_group, criterion_main};
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

criterion_group!(benches, tick_core_10k);
criterion_main!(benches);
