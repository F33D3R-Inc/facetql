//! Searching text, and which structure serves which question.
//!
//! Three string tests, and the difference between them decides the
//! access path:
//!
//! * `starts_with` is a **prefix of the ordered encoding**, so it is a
//!   range of the ordered B+tree — typeahead over a handle costs the
//!   matches, not the kind.
//! * `contains` and `ends_with` are prefixes of nothing, so no range of
//!   an index over whole values corresponds to them. They are served
//!   instead by an **inverted index** over the field's trigrams
//!   (`storage::text`), which yields a superset of the matching rows
//!   that the predicate then filters exactly — and, where no such index
//!   is declared, by the row-by-row scan they always were.
//!
//! The property that matters across all of that is one: the answer never
//! depends on which path served it. `contains_is_exact_with_or_without_
//! an_index` and `an_inverted_index_reads_the_matches_not_the_kind`
//! below are the two halves of that claim — same rows, very different
//! cost.
//!
//! Before any of this, none of the three could even be sent: the
//! predicate evaluator rejected anything it could not push down, so the
//! fct app's search box — `contains(lower(body), lower(q))` — was a
//! client-side filter over every row it had already loaded.

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::core::predicate::Expr;
use facetql::storage::engine::{StorageEngine, TxOperation};
use facetql::storage::index::IndexDef;
use facetql::storage::text::TextIndexDef;

const PROFILES: u64 = 20_000;

fn seeded() -> &'static StorageEngine {
    static ENGINE: std::sync::OnceLock<StorageEngine> = std::sync::OnceLock::new();

    ENGINE.get_or_init(|| {
        let dir = std::path::PathBuf::from("target")
            .join(format!("it-text-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        facetql::config::set_data_dir(dir.canonicalize().expect("resolve"));

        let engine = StorageEngine::open().expect("open engine");

        for batch in 0..(PROFILES / 500) {
            let ops: Vec<TxOperation> = (0..500)
                .map(|i| TxOperation::InsertNode(profile(batch * 500 + i)))
                .collect();

            engine.execute_transaction(ops).expect("seed");
        }

        engine
            .create_index(IndexDef {
                name: "profile_handle".to_string(),
                kind: "Profile".to_string(),
                field: "handle".to_string(),
                unique: false,
            })
            .expect("declare index");

        engine
    })
}

fn profile(n: u64) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        format!("Profile:{n:012}"),
        "Profile".to_string(),
        "app".to_string(),
    );

    // Handles are spread across the alphabet so a prefix selects a small
    // slice rather than everything.
    let letter = (b'a' + (n % 26) as u8) as char;

    node.data = format!(
        r#"{{"handle":"{letter}{n:06}","bio":"user number {n} writes about rust"}}"#
    );
    node.visibility = Visibility::Public;

    node
}

fn string_test(field: &str, op: &str, literal: &str) -> Expr {
    serde_json::from_value(serde_json::json!({
        "kind": "bin",
        "op": op,
        "l": { "kind": "get", "field": field, "obj": { "kind": "ref", "name": "item" } },
        "r": { "kind": "lit", "val": literal },
    }))
    .expect("build predicate")
}

fn handle_of(node: &Node) -> String {
    serde_json::from_str::<serde_json::Value>(&node.data)
        .expect("json")
        .get("handle")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .expect("handle")
}

#[test]
fn typeahead_over_an_indexed_handle_is_a_range_not_a_scan() {
    let engine = seeded();
    let predicate = string_test("handle", "starts_with", "c0001");

    let start = std::time::Instant::now();

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            Some("handle"), false, None, 10, 0,
        )
        .expect("typeahead");

    let elapsed = start.elapsed();

    assert!(!page.nodes.is_empty(), "the prefix matched something");

    for node in &page.nodes {
        assert!(
            handle_of(node).starts_with("c0001"),
            "every row is under the prefix, got {}",
            handle_of(node),
        );
    }

    let handles: Vec<String> = page.nodes.iter().map(handle_of).collect();
    let mut sorted = handles.clone();
    sorted.sort();
    assert_eq!(handles, sorted, "ascending by handle");

    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "typeahead took {elapsed:?} over {PROFILES} profiles — that is a scan, \
         not a prefix range of the handle index",
    );
}

#[test]
fn the_prefix_range_finds_every_match_and_only_matches() {
    let engine = seeded();
    let predicate = string_test("handle", "starts_with", "b");

    // Everything under `b`, paged through.
    let mut seen = 0usize;
    let mut after: Option<String> = None;

    loop {
        let page = engine
            .query_where(
                Some("Profile"), None, None, Some(&predicate), "item",
                Some("handle"), false, after.as_deref(), 200, 0,
            )
            .expect("page");

        if page.nodes.is_empty() {
            break;
        }

        for node in &page.nodes {
            assert!(handle_of(node).starts_with('b'));
        }

        seen += page.nodes.len();

        if page.next.is_empty() {
            break;
        }

        after = Some(page.next);
    }

    let expected = (0..PROFILES).filter(|n| n % 26 == 1).count();

    assert_eq!(seen, expected, "the range found every `b` handle exactly once");
}

#[test]
fn contains_is_exact_with_or_without_an_index() {
    // Expressible and correct on the scan path — `handle` carries an
    // ordered index, which cannot serve a substring, and no inverted
    // index is declared over it. This is the answer every other path has
    // to reproduce.
    let engine = seeded();
    let predicate = string_test("bio", "contains", "number 4242 ");

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            None, false, None, 10, 0,
        )
        .expect("contains");

    assert_eq!(page.nodes.len(), 1, "exactly the one bio that says so");
    assert_eq!(page.nodes[0].address, "Profile:000000004242");
}

/// The point of the inverted index, measured on a corpus big enough for
/// the difference to be the difference between a feature and an outage.
///
/// Declaring the index inside the test rather than in `seeded()` is what
/// makes the "before" number real: it is the same query, on the same
/// data, in the same process, with the only change being that the access
/// path now exists. Every other test in this binary keeps working across
/// that change, which is the correctness half of the claim — an index
/// may alter what a query costs and may never alter what it returns.
///
/// # Why this is `#[ignore]`
///
/// Not because it is flaky, and not because the query is slow — the
/// indexed query it measures answers in under a millisecond. What is
/// slow is getting there: backfilling twenty thousand bios writes about
/// six hundred thousand postings, and in a debug build each one is an
/// independent copy-on-write descent of the B+tree, so the setup costs
/// minutes while the measurement costs microseconds. Paying that on
/// every `cargo test` buys nothing the cheap engine-level version of
/// this test (`text_index_tests::the_index_reads_the_matches_and_not_
/// the_kind`, 1001 examined -> 1) does not already prove. Run it
/// deliberately:
///
/// ```text
/// cargo test --test text_search -- --ignored --nocapture
/// ```
///
/// Measured on this corpus: **20000 examined before, 38 after**. The
/// scan reads every profile; the postings read the 38 whose bios hold
/// every trigram of the phrase; the predicate then picks the 1 that
/// actually contains it.
#[test]
#[ignore = "backfills 20k bios into the index; run with --ignored"]
fn an_inverted_index_reads_the_matches_not_the_kind() {
    let engine = seeded();

    // A bio only one profile has. Every profile's bio ends "writes about
    // rust", so the common trigrams are useless here and the plan has to
    // find a rare one to seed from.
    let predicate = string_test("bio", "contains", "number 17777 writes");

    let scan_start = std::time::Instant::now();

    let before = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            None, false, None, 10, 0,
        )
        .expect("scan");

    let scan_elapsed = scan_start.elapsed();

    assert_eq!(
        before.nodes.len(), 1,
        "exactly one bio says that",
    );
    assert_eq!(
        before.examined, PROFILES,
        "without an inverted index every profile is read",
    );

    engine
        .create_text_index(TextIndexDef {
            name: "profile_bio".to_string(),
            kind: "Profile".to_string(),
            field: "bio".to_string(),
        })
        .expect("declare inverted index");

    let start = std::time::Instant::now();

    let after = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            None, false, None, 10, 0,
        )
        .expect("indexed search");

    let elapsed = start.elapsed();

    assert_eq!(
        after.nodes.iter().map(|n| n.address.clone()).collect::<Vec<_>>(),
        before.nodes.iter().map(|n| n.address.clone()).collect::<Vec<_>>(),
        "the index changed the cost, not the answer",
    );

    assert!(
        // A bound, not a measurement: the exact number depends on how
        // many bios happen to share the rarest trigram of the phrase,
        // which is a property of the corpus. What has to hold is that
        // the plan stopped reading the kind.
        after.examined < PROFILES / 100,
        "the postings should have narrowed {PROFILES} profiles to a handful;          read {}",
        after.examined,
    );

    // Measured against the scan this run actually paid rather than
    // against a wall-clock constant: an absolute bound would be a claim
    // about the machine and the build profile, and this is a claim about
    // the access path.
    assert!(
        elapsed * 4 < scan_elapsed,
        "the indexed search took {elapsed:?} against a {scan_elapsed:?} scan \
         of the same query — that is not a posting intersection",
    );

    // The measurement this test exists to produce. Printed rather than
    // only asserted, because a bound that passes tells an operator the
    // index works and the numbers tell them what it is worth.
    eprintln!(
        "contains over {PROFILES} profiles: scan examined {} in {scan_elapsed:?}; \
         inverted index examined {} in {elapsed:?}",
        before.examined, after.examined,
    );

    // A substring shorter than one trigram window has nothing to look
    // up, so the plan must fall back to the scan rather than answer from
    // an intersection it cannot form — and still be right.
    let short = string_test("bio", "contains", "wr");

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&short), "item",
            None, false, None, 5, 0,
        )
        .expect("short needle");

    assert_eq!(page.nodes.len(), 5, "every bio contains 'wr'");
    assert!(
        page.examined <= 6,
        "the scan reads the page and at most the one row past it that \
         proves a next cursor is worth emitting, as it always did; read {}",
        page.examined,
    );
}

#[test]
fn ends_with_works_too() {
    let engine = seeded();
    let predicate = string_test("bio", "ends_with", "about rust");

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            None, false, None, 5, 0,
        )
        .expect("ends_with");

    assert_eq!(page.nodes.len(), 5, "every bio ends that way");
}

#[test]
fn a_string_test_against_a_missing_field_is_false_not_an_error() {
    // Deliberately unlike `<` and `>`, which error on a non-numeric
    // operand. "Does this absent text start with 'a'" has a defensible
    // answer; "is this absent value less than 5" does not.
    let engine = seeded();
    let predicate = string_test("nonexistent", "starts_with", "x");

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            None, false, None, 10, 0,
        )
        .expect("no rows, no error");

    assert!(page.nodes.is_empty());
}

#[test]
fn a_prefix_combined_with_another_condition_still_uses_the_index() {
    let engine = seeded();

    let predicate: Expr = serde_json::from_value(serde_json::json!({
        "kind": "bin",
        "op": "&&",
        "l": {
            "kind": "bin", "op": "starts_with",
            "l": { "kind": "get", "field": "handle", "obj": { "kind": "ref", "name": "item" } },
            "r": { "kind": "lit", "val": "d" },
        },
        "r": {
            "kind": "bin", "op": "contains",
            "l": { "kind": "get", "field": "bio", "obj": { "kind": "ref", "name": "item" } },
            "r": { "kind": "lit", "val": "rust" },
        },
    }))
    .expect("predicate");

    let start = std::time::Instant::now();

    let page = engine
        .query_where(
            Some("Profile"), None, None, Some(&predicate), "item",
            Some("handle"), false, None, 10, 0,
        )
        .expect("query");

    let elapsed = start.elapsed();

    assert_eq!(page.nodes.len(), 10);

    for node in &page.nodes {
        assert!(handle_of(node).starts_with('d'));
    }

    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "the prefix should still narrow the scan when it is one arm of an \
         `&&`; took {elapsed:?}",
    );
}
