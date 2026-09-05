//! The server must keep answering while it is writing.
//!
//! `GET /` returns a constant string and touches neither the engine nor
//! its lock, so its latency measures one thing only: whether a Tokio
//! worker thread was free to run it. That makes it an exact probe for
//! runtime starvation, with the database's own contention factored out.
//!
//! The failure this guards against is not hypothetical — it was measured
//! on this codebase. Taking the engine's `std::sync::RwLock` inside an
//! `async fn` blocks the worker thread, not just the task, and the
//! engine then `fsync`s while holding it. With Tokio's worker pool sized
//! to the core count, a few concurrent writers park every worker on the
//! same lock and nothing is left to serve reads:
//!
//! ```text
//!   GET / under 32 concurrent writers, real filesystem
//!
//!   lock taken on the reactor      p50  193.0 ms   p95  598.0 ms   max 1053.9 ms
//!   lock taken via spawn_blocking  p50    1.1 ms   p95    1.7 ms   max    2.1 ms
//! ```
//!
//! Writes still serialize on the lock — that is the engine's concurrency
//! model and this does not change it. They serialize somewhere that does
//! not starve everything else.

mod common;

use common::{fsync_cost_micros, free_port, node_body, scratch_on_disk, Server};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Concurrent writers. Comfortably more than the core count on any
/// machine this runs on, which is the condition that empties the pool.
const WRITERS: usize = 32;

/// Read probes taken while the write load is running.
const PROBES: usize = 60;

/// The ceiling `GET /` must stay under. Two orders of magnitude below
/// the 598 ms that the blocking version produced, and two above the
/// 1.7 ms the fixed one does — wide enough not to be flaky on a loaded
/// machine, tight enough that the regression cannot slip through.
const P95_BUDGET: Duration = Duration::from_millis(100);

#[test]
fn reads_stay_responsive_while_the_engine_is_writing() {
    let dir = scratch_on_disk("reactor");
    let port = free_port();

    // On a RAM-backed filesystem a write never blocks long enough for
    // starvation to appear, so the test would pass without testing
    // anything. Say so rather than claim a result.
    let fsync = fsync_cost_micros(&dir);

    if fsync < 200.0 {
        eprintln!(
            "reactor_liveness: skipped — {} has {fsync:.0} µs fsync, too fast to \
             starve the runtime. Run where `target/` is on real storage.",
            dir.display(),
        );
        return;
    }

    let server = Arc::new(Server::start(&dir, port));
    let stop = Arc::new(AtomicBool::new(false));

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let server = Arc::clone(&server);
            let stop = Arc::clone(&stop);

            std::thread::spawn(move || {
                let mut n = 0u32;

                while !stop.load(Ordering::Relaxed) {
                    let body = node_body(&format!("Load:{w}:{n}"), "Load", "payload");
                    let _ = server.post("/node", &body);
                    n += 1;
                }
            })
        })
        .collect();

    // Let the write load actually saturate before measuring.
    std::thread::sleep(Duration::from_secs(2));

    let mut latencies: Vec<Duration> = Vec::with_capacity(PROBES);

    for _ in 0..PROBES {
        let start = Instant::now();
        let response = server.get("/");
        let elapsed = start.elapsed();

        assert_eq!(response.status, 200, "GET / answered while writes were in flight");
        latencies.push(elapsed);

        std::thread::sleep(Duration::from_millis(20));
    }

    stop.store(true, Ordering::Relaxed);

    for w in writers {
        let _ = w.join();
    }

    latencies.sort_unstable();

    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];

    assert!(
        p95 <= P95_BUDGET,
        "GET / touches no lock and no data, yet its p95 was {p95:?} (p50 {p50:?}, \
         max {:?}) under {WRITERS} concurrent writers on a filesystem with \
         {fsync:.0} µs fsync. That is runtime starvation: the write path is \
         blocking Tokio worker threads instead of the blocking pool. See \
         `Database::with_engine_mut`.",
        latencies.last().expect("probes"),
    );
}
