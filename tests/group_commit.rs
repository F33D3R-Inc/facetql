//! Concurrent writers must share one flush.
//!
//! An `fsync` costs the same whether it flushes one record or a hundred
//! — measured on this project's hardware, 7–24 ms either way — so paying
//! one per writer caps durable writes at roughly `1 / fsync` regardless
//! of cores or clients. Sharing it is the single largest lever the write
//! path has.
//!
//! This test exists because the first implementation of that sharing
//! **looked correct and did nothing.** The coalescing logic was right,
//! but the flush ran while holding the same lock appends needed, so no
//! second writer could arrive to share it: 1600 concurrent writes cost
//! 1592 flushes. The fix was to flush on a cloned descriptor outside the
//! append lock. Nothing about the code's shape revealed the difference —
//! only counting the flushes did, which is why the count is asserted
//! here rather than the throughput.

use std::sync::Arc;

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::storage::engine::StorageEngine;
use facetql::storage::wal;

const WRITERS: usize = 32;
const PER_WRITER: u64 = 20;

/// Records per flush that concurrent writers must average.
///
/// One means no sharing at all. Perfect sharing would approach
/// `WRITERS`. Four is far enough above the broken case to be decisive
/// and far enough below the ideal to survive an unlucky schedule.
const MIN_SHARED: f64 = 4.0;

fn post(n: u64) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        format!("G:{n:012}"),
        "G".to_string(),
        "bench".to_string(),
    );

    node.data = format!(r#"{{"n":{n}}}"#);
    node.visibility = Visibility::Public;
    node
}

#[test]
fn concurrent_writers_share_one_flush() {
    let dir = std::path::PathBuf::from("target")
        .join(format!("it-groupcommit-{}", std::process::id()));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create data dir");

    let dir = dir.canonicalize().expect("resolve data dir");

    // Real storage, not tmpfs: with a microsecond fsync no writer is
    // ever still flushing when the next one arrives, so there is nothing
    // to share and the test would pass without testing anything.
    let probe = {
        use std::io::Write;

        let path = dir.join(".fsync_probe");
        let mut file = std::fs::File::create(&path).expect("probe");

        file.write_all(b"warm").expect("write");
        file.sync_data().expect("sync");

        let start = std::time::Instant::now();

        for _ in 0..20 {
            file.write_all(b"probe---------------").expect("write");
            file.sync_data().expect("sync");
        }

        let per = start.elapsed().as_secs_f64() * 1_000_000.0 / 20.0;
        let _ = std::fs::remove_file(&path);

        per
    };

    if probe < 200.0 {
        eprintln!(
            "group_commit: skipped — {} has {probe:.0} µs fsync, too fast for \
             writers to overlap. Run where `target/` is on real storage.",
            dir.display(),
        );
        return;
    }

    facetql::config::set_data_dir(dir.clone());

    let engine = Arc::new(StorageEngine::open().expect("open engine"));

    let (flushes_before, records_before) = wal::flush_stats();

    std::thread::scope(|scope| {
        for w in 0..WRITERS {
            let engine = Arc::clone(&engine);
            let first = w as u64 * PER_WRITER;

            scope.spawn(move || {
                for i in 0..PER_WRITER {
                    engine.insert(post(first + i)).expect("insert");
                }
            });
        }
    });

    let (flushes_after, records_after) = wal::flush_stats();

    let flushes = flushes_after - flushes_before;
    let records = records_after - records_before;
    let writes = WRITERS as u64 * PER_WRITER;

    assert!(flushes > 0, "something was flushed");

    let shared = records as f64 / flushes as f64;

    eprintln!(
        "  {writes} concurrent writes · {flushes} flushes · {shared:.1} records per flush \
         ({probe:.0} µs fsync)"
    );

    assert!(
        shared >= MIN_SHARED,
        "{writes} concurrent writes cost {flushes} flushes — {shared:.1} records \
         each. Writers are not sharing a flush. The usual cause is the flush \
         running while it holds a lock appends need, so no second writer can \
         arrive to join it: see `WalHandle::file` and `wal::sync_pending`.",
    );

    // Sharing a flush must not lose one. Every acknowledged write is
    // still readable.
    for n in 0..writes {
        assert!(
            engine.get(&format!("G:{n:012}")).expect("get").is_some(),
            "write {n} survived group commit",
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
