//! Identifiers have to come from somewhere race-free.
//!
//! The alternative an application reaches for — "what is the largest id
//! so far, add one" — has two costs, and the fct runtime pays both: it
//! must read every row to find the maximum, which is a large part of why
//! it holds the whole database in memory, and two callers that ask at the
//! same moment get the same answer.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use facetql::storage::engine::StorageEngine;

fn engine() -> &'static StorageEngine {
    static ENGINE: std::sync::OnceLock<StorageEngine> = std::sync::OnceLock::new();

    ENGINE.get_or_init(|| {
        let dir = std::path::PathBuf::from("target")
            .join(format!("it-seq-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        facetql::config::set_data_dir(dir.canonicalize().expect("resolve"));

        StorageEngine::open().expect("open engine")
    })
}

#[test]
fn a_fresh_sequence_starts_at_one_and_counts_up() {
    let e = engine();

    assert_eq!(e.sequence_next("fresh", 1, "app", false).expect("first"), 1);
    assert_eq!(e.sequence_next("fresh", 1, "app", false).expect("second"), 2);
    assert_eq!(e.sequence_next("fresh", 1, "app", false).expect("third"), 3);
}

#[test]
fn separate_sequences_do_not_share_a_counter() {
    let e = engine();

    assert_eq!(e.sequence_next("alpha", 1, "app", false).expect("a"), 1);
    assert_eq!(e.sequence_next("beta", 1, "app", false).expect("b"), 1);
    assert_eq!(e.sequence_next("alpha", 1, "app", false).expect("a"), 2);
}

#[test]
fn a_block_reserves_a_whole_range() {
    let e = engine();

    let first = e.sequence_next("blocks", 100, "app", false).expect("block");
    let next = e.sequence_next("blocks", 1, "app", false).expect("after");

    assert_eq!(next, first + 100, "the block is not handed out again");
}

#[test]
fn concurrent_callers_never_receive_the_same_value() {
    // The property the read-then-write approach cannot provide.
    let e = engine();
    let taken = Arc::new(Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        for _ in 0..24 {
            let taken = Arc::clone(&taken);

            scope.spawn(move || {
                for _ in 0..20 {
                    let value = e.sequence_next("race", 1, "app", false).expect("next");
                    taken.lock().expect("lock").push(value);
                }
            });
        }
    });

    let values = taken.lock().expect("lock").clone();
    let unique: HashSet<u64> = values.iter().copied().collect();

    assert_eq!(values.len(), 24 * 20, "every call returned");
    assert_eq!(
        unique.len(),
        values.len(),
        "no value was handed to two callers",
    );
}

#[test]
fn concurrent_blocks_do_not_overlap() {
    let e = engine();
    let ranges = Arc::new(Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        for _ in 0..16 {
            let ranges = Arc::clone(&ranges);

            scope.spawn(move || {
                let first = e.sequence_next("blockrace", 50, "app", false).expect("block");
                ranges.lock().expect("lock").push(first);
            });
        }
    });

    let mut firsts = ranges.lock().expect("lock").clone();
    firsts.sort_unstable();

    for pair in firsts.windows(2) {
        assert!(
            pair[1] - pair[0] >= 50,
            "blocks {} and {} overlap",
            pair[0],
            pair[1],
        );
    }
}

#[test]
fn a_sequence_belongs_to_whoever_created_it() {
    let e = engine();

    e.sequence_next("owned", 1, "alice", false).expect("alice creates it");

    let error = e
        .sequence_next("owned", 1, "bob", false)
        .expect_err("bob is refused");

    assert!(error.starts_with("not authorized"), "{error}");

    // An admin bypasses, the same way it does everywhere else.
    e.sequence_next("owned", 1, "bob", true).expect("admin advances it");
}

#[test]
fn a_bad_block_or_name_is_refused() {
    let e = engine();

    assert!(e.sequence_next("zero", 0, "app", false).is_err(), "count 0");
    assert!(
        e.sequence_next("huge", 1_000_000, "app", false).is_err(),
        "block past the bound",
    );
    assert!(e.sequence_next("", 1, "app", false).is_err(), "empty name");
    assert!(
        e.sequence_next("has:colon", 1, "app", false).is_err(),
        "a colon would collide with the address separator",
    );
}

#[test]
fn a_sequence_survives_a_checkpoint() {
    let e = engine();

    let before = e.sequence_next("durable", 1, "app", false).expect("first");

    e.checkpoint().expect("checkpoint");

    let after = e.sequence_next("durable", 1, "app", false).expect("second");

    assert_eq!(after, before + 1, "the counter did not restart");
}
