//! Direct tests for the on-disk copy-on-write B+tree.
//!
//! Before this file the tree had **zero** direct tests — it was exercised
//! only through `StorageEngine`, whose largest fixture is about sixty
//! rows. Sixty entries fit in a single leaf, so nothing in the suite had
//! ever caused a split, grown a branch level, reused a freed page, or
//! reopened a tree across a generation flip. Those are the paths that
//! lose data when they are wrong, and they were the paths with no
//! coverage.
//!
//! Every test here works on its own file in its own temp directory, so
//! they run in parallel and share nothing.

use std::path::PathBuf;

use facetql::storage::btree::{BTree, SeekMode, MAX_KEY_LEN, MAX_VALUE_LEN};

/// A private directory for one test, named after it.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "facetql-btree-{}-{}-{:?}",
        std::process::id(),
        name,
        std::thread::current().id(),
    ));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    dir
}

fn tree(name: &str) -> (BTree, PathBuf) {
    let dir = scratch(name);
    let path = dir.join("index.fqi");
    let t = BTree::open(&path).expect("open tree");

    (t, path)
}

/// Keys are fixed-width so that byte order and numeric order agree —
/// the tree orders by bytes, and a test that used unpadded decimal would
/// be asserting against lexical order without saying so.
fn key(n: u32) -> Vec<u8> {
    format!("k{n:08}").into_bytes()
}

fn val(n: u32) -> Vec<u8> {
    format!("v{n}").into_bytes()
}

/// How many entries it takes to be sure the tree is no longer one leaf.
///
/// A 16 KiB page holds on the order of a thousand short entries, so ten
/// thousand guarantees several levels of branch nodes and thousands of
/// splits. This is the number that separates "the tree works" from "the
/// tree works in a single page".
const MANY: u32 = 10_000;

#[test]
fn ten_thousand_keys_survive_splits_and_read_back() {
    let (t, _p) = tree("splits");

    for n in 0..MANY {
        t.put(&key(n), &val(n)).expect("put");
    }

    assert_eq!(t.len(), u64::from(MANY), "every key counted once");

    for n in 0..MANY {
        assert_eq!(
            t.get(&key(n)).expect("get"),
            Some(val(n)),
            "key {n} readable after {MANY} inserts",
        );
    }
}

#[test]
fn keys_inserted_in_reverse_still_read_back() {
    // Descending insertion drives splits down the left edge instead of
    // the right, which is a different rebalancing path from the ascending
    // case above.
    let (t, _p) = tree("reverse-insert");

    for n in (0..MANY).rev() {
        t.put(&key(n), &val(n)).expect("put");
    }

    assert_eq!(t.len(), u64::from(MANY));

    for n in 0..MANY {
        assert_eq!(t.get(&key(n)).expect("get"), Some(val(n)));
    }
}

#[test]
fn full_scan_returns_every_key_in_sorted_order() {
    let (t, _p) = tree("scan-order");

    // Insert in an order that is neither ascending nor descending, so a
    // scan that accidentally returned insertion order would be visibly
    // wrong rather than accidentally right.
    for n in (0..MANY).step_by(7) {
        t.put(&key(n), &val(n)).expect("put");
    }
    for n in (0..MANY).step_by(7).rev() {
        let n = n + 1;
        if n < MANY {
            t.put(&key(n), &val(n)).expect("put");
        }
    }

    let mut seen: Vec<Vec<u8>> = Vec::new();
    t.for_each_range(b"k", None, false, |k, _v| {
        seen.push(k.to_vec());
        Ok(true)
    })
    .expect("scan");

    assert_eq!(seen.len() as u64, t.len(), "scan visits every entry exactly once");

    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "scan is in ascending byte order");

    sorted.dedup();
    assert_eq!(sorted.len(), seen.len(), "scan yields no duplicates");
}

#[test]
fn reverse_scan_is_the_forward_scan_backwards() {
    let (t, _p) = tree("scan-reverse");

    for n in 0..MANY {
        t.put(&key(n), &val(n)).expect("put");
    }

    let mut forward: Vec<Vec<u8>> = Vec::new();
    t.for_each_range(b"k", None, false, |k, _v| {
        forward.push(k.to_vec());
        Ok(true)
    })
    .expect("forward");

    let mut backward: Vec<Vec<u8>> = Vec::new();
    t.for_each_range(b"k", None, true, |k, _v| {
        backward.push(k.to_vec());
        Ok(true)
    })
    .expect("reverse");

    backward.reverse();

    assert_eq!(
        forward, backward,
        "reverse iteration visits the same entries in the opposite order",
    );
}

#[test]
fn a_prefix_scan_returns_the_prefix_and_nothing_else() {
    // This is the access path every `kind=` and `owner=` query uses, and
    // the one a composite-key index depends on for correctness.
    let (t, _p) = tree("prefix");

    for n in 0..2_000 {
        t.put(format!("aaa:{n:06}").as_bytes(), b"x").expect("put");
        t.put(format!("aab:{n:06}").as_bytes(), b"x").expect("put");
        t.put(format!("aa:{n:06}").as_bytes(), b"x").expect("put");
    }

    let mut hits = 0usize;
    t.for_each_range(b"aaa:", None, false, |k, _v| {
        assert!(
            k.starts_with(b"aaa:"),
            "prefix scan yielded {:?}, which is not under the prefix",
            String::from_utf8_lossy(k),
        );
        hits += 1;
        Ok(true)
    })
    .expect("prefix scan");

    assert_eq!(hits, 2_000, "every key under the prefix, and only those");
}

#[test]
fn a_cursor_resumes_without_repeating_or_skipping() {
    // Keyset pagination is the engine's only unbounded read path, so a
    // cursor that repeats or skips an entry is a wrong answer that looks
    // like a right one.
    let (t, _p) = tree("cursor");

    for n in 0..MANY {
        t.put(&key(n), &val(n)).expect("put");
    }

    let page = 250usize;
    let mut collected: Vec<Vec<u8>> = Vec::new();
    let mut after: Option<Vec<u8>> = None;

    loop {
        let mut batch: Vec<Vec<u8>> = Vec::new();

        t.for_each_range(b"k", after.as_deref(), false, |k, _v| {
            batch.push(k.to_vec());
            Ok(batch.len() < page)
        })
        .expect("page");

        if batch.is_empty() {
            break;
        }

        after = Some(batch.last().expect("non-empty").clone());
        collected.extend(batch);
    }

    assert_eq!(collected.len(), MANY as usize, "paging saw every entry");

    let mut deduped = collected.clone();
    deduped.dedup();
    assert_eq!(deduped.len(), collected.len(), "no entry paged twice");

    let expected: Vec<Vec<u8>> = (0..MANY).map(key).collect();
    assert_eq!(collected, expected, "paging preserved order");
}

#[test]
fn early_stop_does_not_read_the_whole_range() {
    // `visit` returning false is what makes LIMIT cheap. If it were
    // ignored, every limited query would still pay for a full scan.
    let (t, _p) = tree("early-stop");

    for n in 0..MANY {
        t.put(&key(n), &val(n)).expect("put");
    }

    let mut seen = 0usize;
    t.for_each_range(b"k", None, false, |_k, _v| {
        seen += 1;
        Ok(seen < 10)
    })
    .expect("scan");

    assert_eq!(seen, 10, "iteration stopped when the visitor said stop");
}

#[test]
fn seek_finds_the_neighbour_in_each_direction() {
    let (t, _p) = tree("seek");

    // Only even keys exist, so every odd probe has a distinct neighbour
    // on each side and the four modes cannot be confused for each other.
    for n in (0..1_000).step_by(2) {
        t.put(&key(n), &val(n)).expect("put");
    }

    let probe = key(501);

    assert_eq!(t.seek(&probe, SeekMode::Ge).expect("ge").map(|(k, _)| k), Some(key(502)));
    assert_eq!(t.seek(&probe, SeekMode::Gt).expect("gt").map(|(k, _)| k), Some(key(502)));
    assert_eq!(t.seek(&probe, SeekMode::Le).expect("le").map(|(k, _)| k), Some(key(500)));
    assert_eq!(t.seek(&probe, SeekMode::Lt).expect("lt").map(|(k, _)| k), Some(key(500)));

    // On an exact hit, the inclusive and exclusive modes must differ.
    let exact = key(500);

    assert_eq!(t.seek(&exact, SeekMode::Ge).expect("ge").map(|(k, _)| k), Some(key(500)));
    assert_eq!(t.seek(&exact, SeekMode::Gt).expect("gt").map(|(k, _)| k), Some(key(502)));
    assert_eq!(t.seek(&exact, SeekMode::Le).expect("le").map(|(k, _)| k), Some(key(500)));
    assert_eq!(t.seek(&exact, SeekMode::Lt).expect("lt").map(|(k, _)| k), Some(key(498)));
}

#[test]
fn seek_past_the_ends_yields_nothing() {
    let (t, _p) = tree("seek-ends");

    for n in 0..100 {
        t.put(&key(n), &val(n)).expect("put");
    }

    assert!(t.seek(b"z", SeekMode::Ge).expect("past end").is_none());
    assert!(t.seek(b"a", SeekMode::Lt).expect("before start").is_none());
}

#[test]
fn overwriting_a_key_replaces_it_rather_than_duplicating_it() {
    // The tree stores no duplicate keys; multiplicity is encoded into
    // composite keys by the index layer. If `put` ever appended instead
    // of replacing, `len` would drift and a scan would yield a key twice.
    let (t, _p) = tree("overwrite");

    for n in 0..1_000 {
        t.put(&key(n), b"first").expect("put");
    }
    for n in 0..1_000 {
        t.put(&key(n), b"second").expect("overwrite");
    }

    assert_eq!(t.len(), 1_000, "overwrites did not grow the tree");
    assert_eq!(t.get(&key(7)).expect("get"), Some(b"second".to_vec()));

    let mut count = 0usize;
    t.for_each_range(b"k", None, false, |_k, v| {
        assert_eq!(v, b"second");
        count += 1;
        Ok(true)
    })
    .expect("scan");

    assert_eq!(count, 1_000);
}

#[test]
fn removal_leaves_the_survivors_intact() {
    let (t, _p) = tree("remove");

    for n in 0..MANY {
        t.put(&key(n), &val(n)).expect("put");
    }

    // Remove two thirds, keeping every third key.
    for n in 0..MANY {
        if n % 3 != 0 {
            assert!(t.remove(&key(n)).expect("remove"), "key {n} was present");
        }
    }

    let survivors = (0..MANY).filter(|n| n % 3 == 0).count() as u64;
    assert_eq!(t.len(), survivors, "count reflects the removals");

    for n in 0..MANY {
        let got = t.get(&key(n)).expect("get");

        if n % 3 == 0 {
            assert_eq!(got, Some(val(n)), "survivor {n} still readable");
        } else {
            assert_eq!(got, None, "removed {n} is gone");
        }
    }
}

#[test]
fn removing_an_absent_key_reports_it_and_changes_nothing() {
    let (t, _p) = tree("remove-absent");

    t.put(&key(1), &val(1)).expect("put");

    assert!(!t.remove(&key(999)).expect("remove absent"));
    assert_eq!(t.len(), 1);
    assert_eq!(t.get(&key(1)).expect("get"), Some(val(1)));
}

#[test]
fn emptying_the_tree_completely_leaves_it_usable() {
    // Collapsing every level back to an empty root is the least-travelled
    // path in the rebalancing code and the easiest one to leave dangling.
    let (t, _p) = tree("drain");

    for n in 0..2_000 {
        t.put(&key(n), &val(n)).expect("put");
    }
    for n in 0..2_000 {
        assert!(t.remove(&key(n)).expect("remove"));
    }

    assert_eq!(t.len(), 0);
    assert!(t.seek(b"k", SeekMode::Ge).expect("seek empty").is_none());

    let mut seen = 0usize;
    t.for_each_range(b"k", None, false, |_k, _v| {
        seen += 1;
        Ok(true)
    })
    .expect("scan empty");
    assert_eq!(seen, 0);

    // And it still accepts new writes afterwards.
    t.put(&key(42), &val(42)).expect("put after drain");
    assert_eq!(t.get(&key(42)).expect("get"), Some(val(42)));
    assert_eq!(t.len(), 1);
}

#[test]
fn a_committed_tree_reopens_with_everything_in_it() {
    // This is the durability contract of the meta-page flip: commit
    // publishes a generation, and opening picks the valid meta with the
    // higher generation.
    let dir = scratch("reopen");
    let path = dir.join("index.fqi");

    {
        let t = BTree::open(&path).expect("open");

        for n in 0..MANY {
            t.put(&key(n), &val(n)).expect("put");
        }

        t.commit().expect("commit");
    }

    let t = BTree::open(&path).expect("reopen");

    assert_eq!(t.len(), u64::from(MANY), "count survived the reopen");

    for n in 0..MANY {
        assert_eq!(t.get(&key(n)).expect("get"), Some(val(n)), "key {n} survived");
    }
}

#[test]
fn successive_commits_each_publish_a_whole_generation() {
    // Two commits means the meta pages have alternated at least once, so
    // this covers the flip in both directions rather than only the first.
    let dir = scratch("generations");
    let path = dir.join("index.fqi");

    {
        let t = BTree::open(&path).expect("open");
        for n in 0..1_000 {
            t.put(&key(n), b"gen1").expect("put");
        }
        t.commit().expect("commit 1");

        for n in 0..1_000 {
            t.put(&key(n), b"gen2").expect("put");
        }
        t.commit().expect("commit 2");

        for n in 1_000..2_000 {
            t.put(&key(n), b"gen3").expect("put");
        }
        t.commit().expect("commit 3");
    }

    let t = BTree::open(&path).expect("reopen");

    assert_eq!(t.len(), 2_000);
    assert_eq!(t.get(&key(0)).expect("get"), Some(b"gen2".to_vec()));
    assert_eq!(t.get(&key(1_500)).expect("get"), Some(b"gen3".to_vec()));
}

#[test]
fn uncommitted_writes_do_not_survive_a_reopen() {
    // The other half of the same contract: if a write that was never
    // committed came back, the meta flip would not be an atomicity
    // boundary at all.
    let dir = scratch("uncommitted");
    let path = dir.join("index.fqi");

    {
        let t = BTree::open(&path).expect("open");
        t.put(&key(1), b"durable").expect("put");
        t.commit().expect("commit");

        t.put(&key(2), b"lost").expect("put");
        // deliberately no commit
    }

    let t = BTree::open(&path).expect("reopen");

    assert_eq!(t.get(&key(1)).expect("get"), Some(b"durable".to_vec()));
    assert_eq!(t.get(&key(2)).expect("get"), None, "uncommitted write did not persist");
    assert_eq!(t.len(), 1);
}

#[test]
fn churn_across_generations_reuses_pages_without_losing_data() {
    // Repeated write/delete cycles with commits between them are what
    // exercise the free list: pages superseded in one generation become
    // reusable two generations later. If reuse were premature, a live
    // page would be handed out and the data under it would vanish.
    let dir = scratch("churn");
    let path = dir.join("index.fqi");

    let t = BTree::open(&path).expect("open");

    for round in 0..12u32 {
        for n in 0..1_500 {
            t.put(&key(n), format!("r{round}").as_bytes()).expect("put");
        }

        t.commit().expect("commit");

        for n in 0..1_500 {
            if n % 2 == 0 {
                t.remove(&key(n)).expect("remove");
            }
        }

        t.commit().expect("commit");
    }

    // Whatever the free list did, the surviving half must be exactly the
    // odd keys carrying the last round's value.
    assert_eq!(t.len(), 750);

    for n in 0..1_500 {
        let got = t.get(&key(n)).expect("get");

        if n % 2 == 0 {
            assert_eq!(got, None, "even key {n} stayed removed");
        } else {
            assert_eq!(got, Some(b"r11".to_vec()), "odd key {n} kept its last value");
        }
    }

    drop(t);

    let reopened = BTree::open(&path).expect("reopen after churn");
    assert_eq!(reopened.len(), 750, "churned tree reopens intact");
}

#[test]
fn keys_and_values_at_the_documented_bounds_are_accepted() {
    let (t, _p) = tree("bounds-ok");

    let k = vec![b'k'; MAX_KEY_LEN];
    let v = vec![b'v'; MAX_VALUE_LEN];

    t.put(&k, &v).expect("a key and value exactly at the bound are legal");
    assert_eq!(t.get(&k).expect("get"), Some(v));
}

#[test]
fn keys_and_values_past_the_bounds_are_refused_not_truncated() {
    // Silently truncating either one would corrupt the index: two
    // distinct keys would collide, and a value would decode as garbage.
    let (t, _p) = tree("bounds-refuse");

    let too_long_key = vec![b'k'; MAX_KEY_LEN + 1];
    let too_long_value = vec![b'v'; MAX_VALUE_LEN + 1];

    assert!(t.put(&too_long_key, b"x").is_err(), "oversized key refused");
    assert!(t.put(b"k", &too_long_value).is_err(), "oversized value refused");

    assert_eq!(t.len(), 0, "a refused write left nothing behind");
}

#[test]
fn an_empty_key_is_handled_consistently() {
    let (t, _p) = tree("empty-key");

    match t.put(b"", b"value") {
        Ok(()) => {
            assert_eq!(t.get(b"").expect("get"), Some(b"value".to_vec()));
            assert_eq!(t.len(), 1);
        }
        Err(_) => {
            assert_eq!(t.len(), 0, "if refused, nothing was written");
        }
    }
}

#[test]
fn binary_keys_order_by_bytes_including_high_bytes() {
    // Keys are arbitrary bytes, not UTF-8. A comparison that went through
    // a signed type would sort 0x80 below 0x00 and quietly scramble the
    // index for any non-ASCII address.
    let (t, _p) = tree("binary-order");

    let keys: Vec<Vec<u8>> = vec![
        vec![0x00, 0x01],
        vec![0x01, 0x00],
        vec![0x7f, 0xff],
        vec![0x80, 0x00],
        vec![0xfe, 0x00],
        vec![0xff, 0xff],
    ];

    for k in &keys {
        t.put(k, b"x").expect("put");
    }

    let mut seen: Vec<Vec<u8>> = Vec::new();
    t.for_each_range(b"", None, false, |k, _v| {
        seen.push(k.to_vec());
        Ok(true)
    })
    .expect("scan");

    assert_eq!(seen, keys, "unsigned byte order, high bytes last");
}

// ---------------------------------------------------------------------
// Snapshots
//
// Copy-on-write already meant a committed page was never overwritten.
// What was missing was any guarantee that a *reader* finished before its
// pages were handed back to the allocator, which is the only reason
// reads had to exclude writes. These pin that guarantee.
// ---------------------------------------------------------------------

#[test]
fn a_snapshot_does_not_see_commits_taken_after_it() {
    let (t, _p) = tree("snapshot-isolation");

    for n in 0..1_000 {
        t.put(&key(n), b"before").expect("put");
    }
    t.commit().expect("commit");

    let view = t.snapshot();

    // Overwrite everything and add more, twice, committing each time.
    for n in 0..1_000 {
        t.put(&key(n), b"after").expect("put");
    }
    for n in 1_000..2_000 {
        t.put(&key(n), b"after").expect("put");
    }
    t.commit().expect("commit");

    for n in 0..1_000 {
        t.put(&key(n), b"later").expect("put");
    }
    t.commit().expect("commit");

    // The pinned view still reads the generation it was taken at.
    assert_eq!(view.get(&key(0)).expect("get"), Some(b"before".to_vec()));
    assert_eq!(view.get(&key(999)).expect("get"), Some(b"before".to_vec()));
    assert_eq!(
        view.get(&key(1_500)).expect("get"),
        None,
        "a key added after the snapshot is not in it",
    );

    // While the tree itself has moved on.
    assert_eq!(t.get(&key(0)).expect("get"), Some(b"later".to_vec()));
    assert_eq!(t.get(&key(1_500)).expect("get"), Some(b"after".to_vec()));
}

#[test]
fn a_snapshot_scan_is_consistent_across_commits_that_land_mid_scan() {
    // The tree has no leaf sibling pointers, so a range scan re-descends
    // from the root for every entry. Without a pinned root, a commit
    // partway through would move the tree under the scan and the results
    // would be a mix of two generations. This is the case the engine's
    // global lock has been covering.
    let (t, _p) = tree("snapshot-scan");

    for n in 0..2_000 {
        t.put(&key(n), b"gen1").expect("put");
    }
    t.commit().expect("commit");

    let view = t.snapshot();
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut values_changed_midway = false;

    view.for_each_range(b"k", None, false, |k, v| {
        seen.push(k.to_vec());

        // Commit a whole new generation while the scan is in flight.
        if seen.len() == 500 {
            for n in 0..2_000 {
                t.put(&key(n), b"gen2").expect("put");
            }
            for n in 2_000..3_000 {
                t.put(&key(n), b"gen2").expect("put");
            }
            t.commit().expect("commit");
            values_changed_midway = true;
        }

        assert_eq!(v, b"gen1", "the scan never saw the newer generation");
        Ok(true)
    })
    .expect("scan");

    assert!(values_changed_midway, "the interfering commit actually happened");
    assert_eq!(seen.len(), 2_000, "the scan saw its own generation, whole");

    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted, "and in order");
}

#[test]
fn a_live_snapshot_holds_page_reuse_back_and_releasing_it_restores_reuse() {
    // The mechanism that makes the isolation above true: while a reader
    // is pinned below the generation that freed a page, that page is not
    // handed out again. The observable consequence is that the file
    // grows instead of reusing, and stops growing once the reader lets go.
    let dir = scratch("snapshot-reclaim");
    let path = dir.join("index.fqi");

    let churn = |t: &BTree| {
        for round in 0..6u32 {
            for n in 0..400 {
                t.put(&key(n), format!("r{round}").as_bytes()).expect("put");
            }
            t.commit().expect("commit");

            for n in 0..400 {
                if n % 2 == 0 {
                    t.remove(&key(n)).expect("remove");
                }
            }
            t.commit().expect("commit");
        }
    };

    let pinned_growth = {
        let t = BTree::open(&path).expect("open");

        for n in 0..400 {
            t.put(&key(n), b"seed").expect("put");
        }
        t.commit().expect("commit");

        let view = t.snapshot();
        let before = std::fs::metadata(&path).expect("stat").len();

        churn(&t);

        let after = std::fs::metadata(&path).expect("stat").len();

        // Releasing the pin must let reuse resume. A registry entry left
        // behind by a dropped snapshot would stall reclamation for the
        // life of the process — a leak with no symptom except a file
        // that never stops growing.
        drop(view);

        let resumed_from = std::fs::metadata(&path).expect("stat").len();
        churn(&t);
        let resumed_to = std::fs::metadata(&path).expect("stat").len();

        assert!(
            resumed_to - resumed_from < after - before,
            "with the snapshot released, the same churn should grow the file \
             less: pinned grew {}, released grew {}",
            after - before,
            resumed_to - resumed_from,
        );

        after - before
    };

    let unpinned_growth = {
        let dir = scratch("snapshot-reclaim-free");
        let path = dir.join("index.fqi");
        let t = BTree::open(&path).expect("open");

        for n in 0..400 {
            t.put(&key(n), b"seed").expect("put");
        }
        t.commit().expect("commit");

        let before = std::fs::metadata(&path).expect("stat").len();

        churn(&t);

        std::fs::metadata(&path).expect("stat").len() - before
    };

    assert!(
        pinned_growth > unpinned_growth,
        "a held snapshot should suppress page reuse: pinned grew {pinned_growth} \
         bytes, unpinned grew {unpinned_growth}",
    );
}

#[test]
fn a_reader_and_a_writer_run_concurrently_without_excluding_each_other() {
    // The point of the whole exercise. One thread scans a pinned
    // snapshot while another commits generation after generation over
    // the top. Neither takes a lock against the other, and the reader's
    // answers must be exactly the generation it pinned.
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let dir = scratch("snapshot-concurrent");
    let path = dir.join("index.fqi");

    let tree = Arc::new(BTree::open(&path).expect("open"));

    for n in 0..3_000 {
        tree.put(&key(n), b"pinned").expect("put");
    }
    tree.commit().expect("commit");

    // Pinned *before* the writer exists. A snapshot taken later would
    // legitimately see whatever generation was current when it was taken
    // — that is what a snapshot is — so the pin has to predate the
    // writes for the assertion below to mean anything.
    let view = tree.snapshot();

    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);

        std::thread::spawn(move || {
            let mut round = 0u32;

            while !stop.load(Ordering::Relaxed) {
                for n in 0..3_000 {
                    tree.put(&key(n), format!("w{round}").as_bytes())
                        .expect("put");
                }
                tree.commit().expect("commit");
                round += 1;
            }

            round
        })
    };

    // Scan the pinned generation repeatedly while that runs.
    for _ in 0..5 {
        let mut count = 0usize;

        view.for_each_range(b"k", None, false, |_k, v| {
            assert_eq!(
                v, b"pinned",
                "a snapshot pinned before the writer started must keep \
                 reading its own generation",
            );
            count += 1;
            Ok(true)
        })
        .expect("scan");

        assert_eq!(count, 3_000, "and see all of it");
    }

    stop.store(true, Ordering::Relaxed);
    let rounds = writer.join().expect("writer thread");

    assert!(rounds > 0, "the writer actually committed while reads ran");
}
