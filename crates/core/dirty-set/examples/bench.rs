//! Ad-hoc benchmark: DirtySet<LIST_CAP> vs FnvHashSet<u32>.
//!
//! Run with:
//!   cargo run --release -p dirty-set --example bench
//!
//! Each scenario runs mark_dirty * N -> iter (collect sum) -> clear in a loop.
//! Both containers are reused across iterations, so allocations amortise.

use std::hint::black_box;
use std::time::Instant;

use dirty_set::{DirtySet, DirtySlotId};
use fnv::FnvHashSet;

fn bench_dirty<const CAP: usize>(n_subs: u32, ops: &[u32], iters: usize) -> f64 {
    let warmup = (iters / 50).max(10);
    let mut set = DirtySet::<CAP>::new(n_subs);
    let mut sum: u64 = 0;

    for _ in 0..warmup {
        for &id in ops {
            set.mark_dirty(DirtySlotId(id));
        }
        for d in set.iter() {
            sum = sum.wrapping_add(d.0 as u64);
        }
        set.clear();
    }
    black_box(sum);

    let start = Instant::now();
    let mut sum: u64 = 0;
    for _ in 0..iters {
        for &id in ops {
            set.mark_dirty(DirtySlotId(id));
        }
        for d in set.iter() {
            sum = sum.wrapping_add(d.0 as u64);
        }
        set.clear();
    }
    black_box(sum);
    let ns = start.elapsed().as_nanos() as f64;
    ns / iters as f64
}

fn bench_fnv(ops: &[u32], iters: usize) -> f64 {
    let warmup = (iters / 50).max(10);
    let mut set: FnvHashSet<u32> = FnvHashSet::default();
    let mut sum: u64 = 0;

    for _ in 0..warmup {
        for &id in ops {
            set.insert(id);
        }
        for d in &set {
            sum = sum.wrapping_add(*d as u64);
        }
        set.clear();
    }
    black_box(sum);

    let start = Instant::now();
    let mut sum: u64 = 0;
    for _ in 0..iters {
        for &id in ops {
            set.insert(id);
        }
        for d in &set {
            sum = sum.wrapping_add(*d as u64);
        }
        set.clear();
    }
    black_box(sum);
    let ns = start.elapsed().as_nanos() as f64;
    ns / iters as f64
}

fn row(name: &str, dirty_ns: f64, fnv_ns: f64) {
    let ratio = fnv_ns / dirty_ns;
    println!(
        "{:<55}  DirtySet: {:>8.1} ns  FnvHashSet: {:>8.1} ns   {:>5.2}x",
        name, dirty_ns, fnv_ns, ratio
    );
}

fn main() {
    // Small scenarios — per-iteration cost is cheap, so many iters for stable timing.
    const SMALL: usize = 50_000;
    const MEDIUM: usize = 5_000;
    const LARGE: usize = 500;
    const HUGE: usize = 50;

    let ops: Vec<u32> = (0..8).collect();
    row(
        "A1  8 marks, no dupes, no overflow (CAP=32, n_subs=256)",
        bench_dirty::<32>(256, &ops, SMALL),
        bench_fnv(&ops, SMALL),
    );

    let ops: Vec<u32> = (0..32).collect();
    row(
        "A2  32 marks, no dupes, no overflow (CAP=64, n_subs=256)",
        bench_dirty::<64>(256, &ops, SMALL),
        bench_fnv(&ops, SMALL),
    );

    let ops: Vec<u32> = (0..48).collect();
    row(
        "B1  48 marks, no dupes, overflow (CAP=32, n_subs=256)",
        bench_dirty::<32>(256, &ops, SMALL),
        bench_fnv(&ops, SMALL),
    );

    let ops: Vec<u32> = (0..1000).map(|i| (i * 7) % 64).collect();
    row(
        "C1  1000 marks, ~64 distinct (heavy dupes) (CAP=32, n_subs=256)",
        bench_dirty::<32>(256, &ops, MEDIUM),
        bench_fnv(&ops, MEDIUM),
    );

    let ops: Vec<u32> = (0..1000).map(|i| (i * 2654435761u32) % 4096).collect();
    row(
        "D1  1000 marks, ~1000 distinct over 4096 subs (CAP=32)",
        bench_dirty::<32>(4096, &ops, MEDIUM),
        bench_fnv(&ops, MEDIUM),
    );

    let ops: Vec<u32> = (0..10000).map(|i| (i * 2654435761u32) % 4096).collect();
    row(
        "D2  10k marks, ~4096 distinct over 4096 subs (CAP=32)",
        bench_dirty::<32>(4096, &ops, MEDIUM),
        bench_fnv(&ops, MEDIUM),
    );

    let ops: Vec<u32> = (0..200).collect();
    row(
        "E1  200 distinct marks, overflow (CAP=128, n_subs=4096)",
        bench_dirty::<128>(4096, &ops, SMALL),
        bench_fnv(&ops, SMALL),
    );

    let ops: Vec<u32> = (0..200).collect();
    row(
        "E2  200 distinct marks, CAP=1 (always bitmap)  n_subs=4096",
        bench_dirty::<1>(4096, &ops, SMALL),
        bench_fnv(&ops, SMALL),
    );

    // ── Large scenarios ──
    let ops: Vec<u32> = (0..100_000).map(|i| (i * 2654435761u32) % 1_000_000).collect();
    row(
        "F1  100k marks, ~100k distinct over 1M subs (CAP=32)",
        bench_dirty::<32>(1_000_000, &ops, LARGE),
        bench_fnv(&ops, LARGE),
    );

    let ops: Vec<u32> = (0..100_000).collect();
    row(
        "F2  100k distinct marks, dense (n_subs=100k, CAP=32)",
        bench_dirty::<32>(100_000, &ops, LARGE),
        bench_fnv(&ops, LARGE),
    );

    let ops: Vec<u32> = (0..100_000).map(|i| (i * 2654435761u32) % 10_000).collect();
    row(
        "F3  100k marks, ~10k distinct (heavy dupes, n_subs=100k)",
        bench_dirty::<32>(100_000, &ops, LARGE),
        bench_fnv(&ops, LARGE),
    );

    let ops: Vec<u32> = (0..1_000_000).map(|i| (i * 2654435761u32) % 100_000).collect();
    row(
        "F4  1M marks, ~100k distinct (n_subs=100k, CAP=32)",
        bench_dirty::<32>(100_000, &ops, HUGE),
        bench_fnv(&ops, HUGE),
    );
}
