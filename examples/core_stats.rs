use std::hint::black_box;
use std::time::Instant;
use time_strike::policy::{PolicyInput, evaluate_time_policy};

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn main() {
    const OPERATIONS: usize = 10_000;
    let mut samples = Vec::with_capacity(OPERATIONS);
    for index in 0..OPERATIONS {
        let input = PolicyInput {
            total_secs: 3_600.0,
            elapsed_secs: index as f64 * 0.25,
            progress_percent: Some((index % 101) as f64),
            estimated_remaining_work_secs: Some(600.0),
            ..PolicyInput::default()
        };
        let started = Instant::now();
        black_box(evaluate_time_policy(black_box(input)));
        samples.push(started.elapsed().as_nanos());
    }
    samples.sort_unstable();
    let mean = samples.iter().sum::<u128>() as f64 / samples.len() as f64;
    println!(
        "{{\"operations\":{OPERATIONS},\"unit\":\"ns\",\"mean\":{mean:.2},\"median\":{},\"p95\":{},\"p99\":{}}}",
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        percentile(&samples, 0.99)
    );
}
