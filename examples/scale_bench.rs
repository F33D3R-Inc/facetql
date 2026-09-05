//! The scale baseline.
//!
//! Phase 0's whole point: stop asserting where the ceiling is and
//! measure it. Nothing in this repo had ever run above about sixty rows,
//! so every number below is the first of its kind.
//!
//! Run it in release. In debug the per-page AES dominates everything and
//! the numbers describe the compiler, not the engine — the segment-roll
//! test took **894 s** in debug and **3.2 s** in release, which is the
//! whole reason scale work had never happened here.
//!
//! ```text
//! cargo run --release --example scale_bench                 # 100k rows
//! cargo run --release --example scale_bench -- 1000000      # 1M rows
//! cargo run --release --example scale_bench -- 100000 /tmp/x  # elsewhere
//! ```
//!
//! # Not in `/tmp`
//!
//! The default data directory is `target/bench-data`, deliberately, and
//! the run refuses to be silent about where it landed. The first version
//! of this file defaulted to `std::env::temp_dir()`, which on this
//! machine — and most Linux machines — is **tmpfs**: a RAM disk where
//! `fsync` costs 4 µs instead of 24 ms. Every write number it produced
//! was therefore measured with durability free, and the difference
//! between one fsync per record and one per transaction was invisible.
//! A storage benchmark on a RAM disk measures the allocator.
//!
//! The fsync cost of whichever directory is used is probed and printed
//! first, so no number here can be read out of that context again.
//!
//! What each number is for:
//!
//! * **insert (single)** is the write ceiling Phase 1 has to beat. Every
//!   mutation takes the global write lock and fsyncs its own WAL record,
//!   unbatched — this is that, measured.
//! * **insert (batched)** is the same work under one transaction frame.
//!   The gap between the two is what group commit is worth.
//! * the read rows are the access paths a feed actually issues.

use std::time::{Duration, Instant};

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::storage::engine::{StorageEngine, TxOperation};
use facetql::storage::index::IndexDef;

const KIND: &str = "Post";
const OWNER: &str = "bench";

fn post(n: u64) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        format!("Post:{n:019}"),
        KIND.to_string(),
        OWNER.to_string(),
    );

    // Shaped like the real workload: an author to filter on, a
    // timestamp to order by, a body to carry weight.
    node.data = format!(
        r#"{{"author":"u{}","created":{},"body":"post {n} {}"}}"#,
        n % 1_000,
        1_700_000_000 + n,
        "x".repeat(180),
    );
    node.visibility = Visibility::Public;

    node
}

fn rate(label: &str, n: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();

    println!(
        "  {label:<34} {:>10.0} ops/s   {:>9.3} ms total   {:>8.1} µs/op",
        n as f64 / secs,
        secs * 1000.0,
        secs * 1_000_000.0 / n as f64,
    );
}

fn timed(label: &str, n: u64, f: impl FnOnce()) {
    let start = Instant::now();
    f();
    rate(label, n, start.elapsed());
}

/// What one durable write costs on this filesystem, in microseconds.
///
/// Printed with every run because it is the number that decides whether
/// any of the write results below mean anything: on tmpfs it is single
/// digits and the engine is CPU-bound, on a real filesystem with
/// barriers it can be tens of milliseconds and the engine is fsync-bound.
/// The same code has completely different bottlenecks in those two
/// regimes.
fn probe_fsync(dir: &std::path::Path) -> f64 {
    use std::io::Write;

    let path = dir.join(".fsync_probe");
    let mut file = std::fs::File::create(&path).expect("probe file");

    file.write_all(b"warm").expect("write");
    file.sync_data().expect("sync");

    const N: u32 = 20;
    let start = Instant::now();

    for _ in 0..N {
        file.write_all(b"probe-------------------").expect("write");
        file.sync_data().expect("sync");
    }

    let per = start.elapsed().as_secs_f64() * 1_000_000.0 / f64::from(N);

    let _ = std::fs::remove_file(&path);
    per
}

fn main() {
    let mut args = std::env::args().skip(1);

    let rows: u64 = args
        .next()
        .and_then(|a| a.parse().ok())
        .unwrap_or(100_000);

    // Under `target/`, not `/tmp` — see the module docs.
    let dir = match args.next() {
        Some(path) => std::path::PathBuf::from(path),
        None => std::path::PathBuf::from("target").join("bench-data"),
    };

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    let dir = dir.canonicalize().expect("resolve bench dir");
    facetql::config::set_data_dir(dir.clone());

    let fsync_us = probe_fsync(&dir);

    println!("\nFacetQL scale baseline — {rows} rows");
    println!("  data dir: {}", dir.display());
    println!(
        "  fsync:    {fsync_us:.1} µs per durable write{}\n",
        if fsync_us < 100.0 {
            "  ← RAM-backed; write results are CPU-bound, not durability-bound"
        } else {
            ""
        },
    );

    let engine = StorageEngine::open().expect("open engine");

    // The single-write sample is sized to the filesystem, not to `rows`.
    // At 24 ms per fsync, a third of 100 000 records is five and a half
    // hours; the interesting quantity is cost per durable write, and a
    // few hundred of them measure that just as well.
    let batch_size = 500u64;
    let single = if fsync_us > 500.0 {
        200.min(rows / 3)
    } else {
        rows / 3
    };

    println!("write  (single = {single} records, batch = {batch_size}/transaction)");

    timed("insert (single)", single, || {
        for n in 0..single {
            engine.insert(post(n)).expect("insert");
        }
    });

    let batched_sample = (batch_size * 8).min(rows - single);

    timed("insert (batched)", batched_sample, || {
        let mut n = single;
        let end = single + batched_sample;

        while n < end {
            let upto = (n + batch_size).min(end);
            let ops: Vec<TxOperation> =
                (n..upto).map(|i| TxOperation::InsertNode(post(i))).collect();

            engine.execute_transaction(ops).expect("transaction");
            n = upto;
        }
    });

    // Fill the rest in batches so the read section has a full dataset.
    let mut n = single + batched_sample;

    while n < rows {
        let upto = (n + batch_size).min(rows);
        let ops: Vec<TxOperation> =
            (n..upto).map(|i| TxOperation::InsertNode(post(i))).collect();

        engine.execute_transaction(ops).expect("transaction");
        n = upto;
    }

    // ---- group commit -------------------------------------------------
    //
    // The single-writer number above is one fsync per record and cannot
    // improve: one record is one transaction. What *can* improve is what
    // happens when several writers commit at once — they share a flush
    // rather than queueing for their own. That is invisible to a
    // single-threaded benchmark, which is why this section exists.
    println!("\nwrite — concurrent writers (group commit)");

    let engine = std::sync::Arc::new(engine);
    let mut base = rows;

    for writers in [1usize, 4, 16, 64] {
        let per_writer = 25u64;
        let total = per_writer * writers as u64;
        let start = Instant::now();

        std::thread::scope(|scope| {
            for w in 0..writers {
                let engine = std::sync::Arc::clone(&engine);
                let first = base + (w as u64 * per_writer);

                scope.spawn(move || {
                    for i in 0..per_writer {
                        engine.insert(post(first + i)).expect("insert");
                    }
                });
            }
        });

        let elapsed = start.elapsed();
        base += total;

        let (flushes, records) = facetql::storage::wal::flush_stats();
        let shared = if flushes > 0 { records as f64 / flushes as f64 } else { 0.0 };

        rate(&format!("{writers:>3} writer(s), {per_writer} each"), total, elapsed);
        println!("      {flushes} fsyncs so far, {shared:.1} records per fsync");
    }

    let engine = std::sync::Arc::try_unwrap(engine)
        .unwrap_or_else(|_| panic!("writers still running"));

    println!("\nread — no index declared");

    let probes = (rows / 20).clamp(1, 20_000);

    timed("point (address)", probes, || {
        for i in 0..probes {
            let n = (i * 7_919) % rows;
            engine
                .get(&format!("Post:{n:019}"))
                .expect("get")
                .expect("present");
        }
    });

    timed("range (kind, first 50 by address)", 1, || {
        let page = engine
            .query_where(Some(KIND), None, None, None, "item", None, false, None, 50, 0)
            .expect("query");

        assert_eq!(page.nodes.len(), 50);
    });

    timed("range (kind, descending)", 1, || {
        engine
            .query_where(Some(KIND), None, None, None, "item", None, true, None, 50, 0)
            .expect("query");
    });

    timed("count (whole kind, index-only)", 1, || {
        let total = engine
            .count_where(Some(KIND), None, None, None, "item")
            .expect("count");

        assert!(total >= rows, "at least the seeded rows: {total} < {rows}");
    });

    timed("order by created (unindexed sort)", 1, || {
        engine
            .query_where(
                Some(KIND), None, None, None, "item",
                Some("created"), true, None, 50, 0,
            )
            .expect("ordered query");
    });

    // The canonical feed shape: filter on one field, order by another.
    // This is the query that was 1.83 s before the access path was
    // separated from the ordering claim, so it is the one worth watching.
    println!("\nread — with a declared index on `created`");

    let start = Instant::now();
    engine
        .create_index(IndexDef {
            name: "post_created".to_string(),
            kind: KIND.to_string(),
            field: "created".to_string(),
    unique: false,
})
        .expect("create index");
    rate("build index over existing rows", rows, start.elapsed());

    timed("order by created (indexed)", 1, || {
        engine
            .query_where(
                Some(KIND), None, None, None, "item",
                Some("created"), true, None, 50, 0,
            )
            .expect("ordered query");
    });

    timed("paginate 20 pages of 50 (keyset)", 20 * 50, || {
        let mut after: Option<String> = None;

        for _ in 0..20 {
            let page = engine
                .query_where(
                    Some(KIND), None, None, None, "item",
                    Some("created"), true, after.as_deref(), 50, 0,
                )
                .expect("page");

            if page.next.is_empty() {
                break;
            }

            after = Some(page.next.clone());
        }
    });

    let stats = engine.stats().expect("stats");

    println!(
        "\nstorage: {} nodes · {} pages · {} segments · {} obsolete bytes",
        stats.node_count,
        stats.storage.pages,
        stats.storage.segments,
        stats.storage.obsolete_bytes,
    );

    let bytes = walk_size(&dir);
    println!(
        "on disk: {:.1} MiB for {rows} rows ({:.0} bytes/row)\n",
        bytes as f64 / (1024.0 * 1024.0),
        bytes as f64 / rows as f64,
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn walk_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += if meta.is_dir() {
                    walk_size(&entry.path())
                } else {
                    meta.len()
                };
            }
        }
    }

    total
}
