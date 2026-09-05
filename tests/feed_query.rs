//! The query a social platform is actually made of.
//!
//! `f33d3r/feed-engine`, the Postgres implementation of this same
//! product, writes its home feed as:
//!
//! ```sql
//! WHERE author_id IN (SELECT following_id FROM follows WHERE follower_id = $1)
//! ORDER BY created_at DESC LIMIT $3
//! ```
//!
//! A membership test against the follow set, ordered by time, windowed.
//! Until `in` existed this engine could not express it at all: the fct
//! app fell back to a correlated `exists()` that could not be pushed
//! down, so "posts by people I follow" became a full scan of every post
//! with a per-row check against an in-memory collection.
//!
//! With a declared index on the ordering field the plan is a descending
//! index scan that stops as soon as the page is full, which is what
//! Postgres does when the follow set is large. What distinguishes that
//! from a scan of everything is how many rows the plan reads, which the
//! page reports — a clock would measure the machine as much as the
//! plan, and fail on a loaded CI box whatever the planner was doing.

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::core::predicate::Expr;
use facetql::storage::engine::{StorageEngine, TxOperation};
use facetql::storage::index::IndexDef;

const POSTS: u64 = 20_000;
const AUTHORS: u64 = 1_000;

/// One engine, seeded once, shared by every test in this binary.
///
/// The data directory is process-wide (`config` resolves it through a
/// `OnceLock`) and only one process may hold it, so tests cannot each
/// build their own. Sharing is now straightforward rather than a
/// compromise: every engine method takes `&self`, so concurrent tests
/// read the same engine the way concurrent requests do — which is also a
/// small end-to-end check that they can.
fn seeded() -> &'static StorageEngine {
    static ENGINE: std::sync::OnceLock<StorageEngine> = std::sync::OnceLock::new();

    ENGINE.get_or_init(|| {
        let dir = std::path::PathBuf::from("target")
            .join(format!("it-feed-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        facetql::config::set_data_dir(dir.canonicalize().expect("resolve"));

        let engine = StorageEngine::open().expect("open engine");

        for batch in 0..(POSTS / 500) {
            let ops: Vec<TxOperation> = (0..500)
                .map(|i| TxOperation::InsertNode(post(batch * 500 + i)))
                .collect();

            engine.execute_transaction(ops).expect("seed");
        }

        engine
            .create_index(IndexDef {
                name: "post_created".to_string(),
                kind: "Post".to_string(),
                field: "created".to_string(),
    unique: false,
})
            .expect("declare index");

        engine
    })
}

fn post(n: u64) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        format!("Post:{n:012}"),
        "Post".to_string(),
        "app".to_string(),
    );

    node.data = format!(
        r#"{{"author":"u{}","created":{},"body":"post {n}"}}"#,
        n % AUTHORS,
        1_700_000_000 + n,
    );
    node.visibility = Visibility::Public;

    node
}

/// `item.<field> <op> <set>`
fn membership(field: &str, op: &str, set: &[&str]) -> Expr {
    serde_json::from_value(serde_json::json!({
        "kind": "bin",
        "op": op,
        "l": { "kind": "get", "field": field, "obj": { "kind": "ref", "name": "item" } },
        "r": { "kind": "lit", "val": set },
    }))
    .expect("build predicate")
}


fn created_of(node: &Node) -> i64 {
    serde_json::from_str::<serde_json::Value>(&node.data)
        .expect("data is json")
        .get("created")
        .and_then(|v| v.as_i64())
        .expect("created")
}

fn author_of(node: &Node) -> String {
    serde_json::from_str::<serde_json::Value>(&node.data)
        .expect("data is json")
        .get("author")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .expect("author")
}

#[test]
fn the_home_feed_query_runs_and_stops_at_the_page() {
    let engine = seeded();

    let following: Vec<&str> = vec![
        "u3", "u17", "u42", "u99", "u150", "u267", "u400", "u512", "u777", "u901",
    ];
    let predicate = membership("author", "in", &following);

    let page = engine
        .query_where(
            Some("Post"), None, None, Some(&predicate), "item",
            Some("created"), true, None, 20, 0,
        )
        .expect("feed query");

    assert_eq!(page.nodes.len(), 20, "a full page");

    for node in &page.nodes {
        assert!(
            following.contains(&author_of(node).as_str()),
            "every row is by someone in the set, got {}",
            author_of(node),
        );
    }

    let times: Vec<i64> = page.nodes.iter().map(created_of).collect();
    let mut sorted = times.clone();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(times, sorted, "newest first");

    // The newest matching post overall must be the first row: a plan
    // that stopped early in the *wrong* order would still return twenty
    // valid rows.
    let newest = (0..POSTS)
        .rev()
        .find(|n| following.contains(&format!("u{}", n % AUTHORS).as_str()))
        .expect("some post matches");
    assert_eq!(times[0], 1_700_000_000 + newest as i64, "starts at the newest match");

    // One in a hundred posts is by someone in this set, so a plan that
    // walks the ordering index backwards and stops at the page reads
    // about two thousand rows to find twenty. A plan that reads the kind
    // reads all twenty thousand. The bound sits between those, far from
    // both: it is not a performance target, it is the difference between
    // the two plans.
    assert!(
        page.examined < POSTS / 4,
        "the feed query read {} of {POSTS} posts — that is a scan of \
         everything, not a descending index scan that stops at the page. The \
         ordering index is what makes this bounded.",
        page.examined,
    );
}

#[test]
fn the_feed_pages_forward_without_repeating_or_skipping() {
    let engine = seeded();

    let following: Vec<&str> = vec!["u5", "u55", "u555"];
    let predicate = membership("author", "in", &following);

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;

    for _ in 0..10 {
        let page = engine
            .query_where(
                Some("Post"), None, None, Some(&predicate), "item",
                Some("created"), true, after.as_deref(), 7, 0,
            )
            .expect("page");

        if page.nodes.is_empty() {
            break;
        }

        seen.extend(page.nodes.iter().map(|n| n.address.clone()));

        if page.next.is_empty() {
            break;
        }

        after = Some(page.next);
    }

    assert!(seen.len() >= 60, "paged through several pages, got {}", seen.len());

    let mut deduped = seen.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(deduped.len(), seen.len(), "no row returned on two pages");
}

#[test]
fn not_in_excludes_exactly_the_named_set() {
    let engine = seeded();

    let blocked: Vec<&str> = vec!["u1", "u2", "u3"];
    let predicate = membership("author", "not in", &blocked);

    let page = engine
        .query_where(
            Some("Post"), None, None, Some(&predicate), "item",
            Some("created"), true, None, 50, 0,
        )
        .expect("query");

    assert_eq!(page.nodes.len(), 50);

    for node in &page.nodes {
        assert!(
            !blocked.contains(&author_of(node).as_str()),
            "a blocked author leaked through: {}",
            author_of(node),
        );
    }
}

#[test]
fn an_oversized_set_is_refused_rather_than_scanned() {
    // A set is compared against once per candidate row, so its size
    // multiplies the scan. Refusing is the same posture the rest of the
    // engine takes toward unbounded work.
    let engine = seeded();

    let huge: Vec<String> = (0..2_000).map(|n| format!("u{n}")).collect();
    let refs: Vec<&str> = huge.iter().map(String::as_str).collect();
    let predicate = membership("author", "in", &refs);

    let outcome = engine.query_where(
        Some("Post"), None, None, Some(&predicate), "item",
        Some("created"), true, None, 20, 0,
    );

    let error = outcome.expect_err("an oversized set is refused");

    assert!(
        error.contains("maximum"),
        "the refusal names the bound: {error}",
    );
}

#[test]
fn a_scalar_on_the_right_of_in_is_an_error_not_a_substring_test() {
    let engine = seeded();

    let predicate: Expr = serde_json::from_value(serde_json::json!({
        "kind": "bin",
        "op": "in",
        "l": { "kind": "get", "field": "author", "obj": { "kind": "ref", "name": "item" } },
        "r": { "kind": "lit", "val": "u3" },
    }))
    .expect("build predicate");

    let outcome = engine.query_where(
        Some("Post"), None, None, Some(&predicate), "item",
        None, false, None, 20, 0,
    );

    assert!(
        outcome.is_err(),
        "`x in \"string\"` is refused rather than read as a substring test",
    );
}

// ---------------------------------------------------------------------
// Batched point reads
// ---------------------------------------------------------------------

#[test]
fn multi_get_returns_the_named_nodes_in_the_order_asked() {
    let engine = seeded();

    let wanted: Vec<String> = [900u64, 12, 19_999, 4_242, 0]
        .iter()
        .map(|n| format!("Post:{n:012}"))
        .collect();

    let got = engine.multi_get(&wanted, None).expect("multi get");

    assert_eq!(got.len(), wanted.len());

    for (node, address) in got.iter().zip(&wanted) {
        assert_eq!(&node.address, address, "order preserved");
    }
}

#[test]
fn multi_get_skips_what_is_not_there_rather_than_failing() {
    let engine = seeded();

    let wanted = vec![
        "Post:000000000007".to_string(),
        "Post:999999999999".to_string(),
        "Nope:1".to_string(),
        "Post:000000000008".to_string(),
    ];

    let got = engine.multi_get(&wanted, None).expect("multi get");

    let addresses: Vec<&str> = got.iter().map(|n| n.address.as_str()).collect();

    assert_eq!(
        addresses,
        vec!["Post:000000000007", "Post:000000000008"],
        "absent addresses are gaps, not errors",
    );
}

#[test]
fn multi_get_answers_a_whole_page_of_enrichment_in_one_call() {
    // The shape this exists for: take a page of rows, then fetch the
    // related records for all of them at once instead of per row.
    let engine = seeded();

    let page = engine
        .query_where(
            Some("Post"), None, None, None, "item",
            Some("created"), true, None, 20, 0,
        )
        .expect("page");

    let related: Vec<String> = page
        .nodes
        .iter()
        .map(|n| n.address.clone())
        .collect();

    let start = std::time::Instant::now();
    let got = engine.multi_get(&related, None).expect("enrich");
    let elapsed = start.elapsed();

    assert_eq!(got.len(), 20);
    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "enriching a page took {elapsed:?}",
    );
}

#[test]
fn multi_get_refuses_a_batch_past_the_bound() {
    let engine = seeded();

    let huge: Vec<String> = (0..2_000).map(|n| format!("Post:{n:012}")).collect();
    let error = engine.multi_get(&huge, None).expect_err("refused");

    assert!(error.contains("maximum"), "the refusal names the bound: {error}");
}

#[test]
fn multi_get_does_not_reveal_nodes_the_caller_cannot_read() {
    // A private node must be a gap, not a 403: distinguishing "forbidden"
    // from "absent" is itself the disclosure.
    let engine = seeded();

    // Its own kind, so it cannot appear in the feed queries above: the
    // engine is shared by every test in this binary, and a `Post` here
    // would be a `Post` there.
    let mut private = Node::new(
        Coordinate::new(0, 0, 0, 0),
        "Secret:1".to_string(),
        "Secret".to_string(),
        "someone-else".to_string(),
    );
    private.data = r#"{"body":"private"}"#.to_string();
    private.visibility = Visibility::Private;
    let hidden = private.address.clone();

    engine.insert(private).expect("insert private");

    let wanted = vec![hidden.clone(), "Post:000000000001".to_string()];

    let as_owner = engine.multi_get(&wanted, Some("someone-else")).expect("owner");
    assert_eq!(as_owner.len(), 2, "the owner sees their own private node");

    let as_other = engine.multi_get(&wanted, Some("nobody")).expect("other");
    assert_eq!(as_other.len(), 1, "another identity sees only the public one");
    assert_ne!(as_other[0].address, hidden);
}
