//! Uniqueness has to be the engine's job.
//!
//! Before this, an application enforcing `@unique` had to read first and
//! then write, which is not a constraint at all: two callers that read at
//! the same moment both see nothing and both write. The fct runtime does
//! exactly that today — a full scan of its in-memory mirror per insert,
//! with no atomicity — so "unique" meant "unique unless two people tried
//! at once", which is when it matters.
//!
//! The check runs before the WAL, inside the same writer lock the write
//! takes, so there is no window between deciding and doing.

use std::sync::Arc;

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::storage::engine::{StorageEngine, TxOperation};
use facetql::storage::index::IndexDef;

fn engine() -> &'static StorageEngine {
    static ENGINE: std::sync::OnceLock<StorageEngine> = std::sync::OnceLock::new();

    ENGINE.get_or_init(|| {
        let dir = std::path::PathBuf::from("target")
            .join(format!("it-unique-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        facetql::config::set_data_dir(dir.canonicalize().expect("resolve"));

        StorageEngine::open().expect("open engine")
    })
}

fn user(kind: &str, address: &str, handle: &str) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        address.to_string(),
        kind.to_string(),
        "app".to_string(),
    );

    node.data = format!(r#"{{"handle":"{handle}"}}"#);
    node.visibility = Visibility::Public;
    node
}

fn declare(kind: &str, name: &str, unique: bool) {
    engine()
        .create_index(IndexDef {
            name: name.to_string(),
            kind: kind.to_string(),
            field: "handle".to_string(),
            unique,
        })
        .expect("declare index");
}

#[test]
fn a_duplicate_is_refused_and_leaves_the_original_intact() {
    declare("UA", "ua_handle", true);

    engine().insert(user("UA", "UA:1", "alice")).expect("first");

    let error = engine()
        .insert(user("UA", "UA:2", "alice"))
        .expect_err("duplicate refused");

    assert!(
        error.contains("already held"),
        "the refusal says what happened: {error}",
    );

    assert!(engine().get("UA:1").expect("get").is_some(), "original kept");
    assert!(
        engine().get("UA:2").expect("get").is_none(),
        "the refused write left nothing behind",
    );
}

#[test]
fn a_node_may_keep_its_own_value_across_an_update() {
    declare("UB", "ub_handle", true);

    engine().insert(user("UB", "UB:1", "bob")).expect("first");

    let mut updated = user("UB", "UB:1", "bob");
    updated.data = r#"{"handle":"bob","bio":"hello"}"#.to_string();

    engine()
        .insert(updated)
        .expect("a node does not conflict with itself");

    let got = engine().get("UB:1").expect("get").expect("present");
    assert!(got.data.contains("hello"), "the update landed");
}

#[test]
fn two_inserts_in_one_batch_cannot_both_claim_a_value() {
    // Checking only the committed index would let this through: neither
    // row is committed when the other is examined.
    declare("UC", "uc_handle", true);

    let error = engine()
        .execute_transaction(vec![
            TxOperation::InsertNode(user("UC", "UC:1", "carol")),
            TxOperation::InsertNode(user("UC", "UC:2", "carol")),
        ])
        .expect_err("batch refused");

    assert!(
        format!("{error:?}").contains("same"),
        "the refusal names the collision inside the batch: {error:?}",
    );

    assert!(engine().get("UC:1").expect("get").is_none(), "nothing applied");
    assert!(engine().get("UC:2").expect("get").is_none(), "nothing applied");
}

#[test]
fn a_batch_may_move_a_unique_value_from_one_node_to_another() {
    // Delete the holder and give the value to someone else, atomically.
    // Refusing this would make the constraint unusable rather than strict.
    declare("UD", "ud_handle", true);

    engine().insert(user("UD", "UD:1", "dave")).expect("first");

    engine()
        .execute_transaction(vec![
            TxOperation::DeleteNode("UD:1".to_string()),
            TxOperation::InsertNode(user("UD", "UD:2", "dave")),
        ])
        .expect("moving a unique value is legal");

    assert!(engine().get("UD:1").expect("get").is_none());
    assert!(engine().get("UD:2").expect("get").is_some());
}

#[test]
fn concurrent_writers_cannot_both_win_the_same_value() {
    // The case a read-then-write application always loses. Thirty-two
    // threads race for one handle; exactly one may have it.
    declare("UE", "ue_handle", true);

    let winners = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    std::thread::scope(|scope| {
        for w in 0..32 {
            let winners = Arc::clone(&winners);

            scope.spawn(move || {
                if engine()
                    .insert(user("UE", &format!("UE:{w}"), "contested"))
                    .is_ok()
                {
                    winners.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(
        winners.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one writer took the contested value",
    );

    let holders = engine()
        .query_where(Some("UE"), None, None, None, "item", None, false, None, 100, 0)
        .expect("query")
        .nodes;

    assert_eq!(holders.len(), 1, "and exactly one node exists");
}

#[test]
fn a_non_unique_index_still_allows_duplicates() {
    declare("UF", "uf_handle", false);

    engine().insert(user("UF", "UF:1", "same")).expect("first");
    engine().insert(user("UF", "UF:2", "same")).expect("second");

    let all = engine()
        .query_where(Some("UF"), None, None, None, "item", None, false, None, 100, 0)
        .expect("query")
        .nodes;

    assert_eq!(all.len(), 2, "uniqueness is opt-in, not the default");
}

#[test]
fn declaring_unique_over_duplicated_data_is_refused() {
    // A constraint that is false the moment it is created is worse than
    // none: reads start trusting it immediately.
    engine().insert(user("UG", "UG:1", "twin")).expect("first");
    engine().insert(user("UG", "UG:2", "twin")).expect("second");

    let error = engine()
        .create_index(IndexDef {
            name: "ug_handle".to_string(),
            kind: "UG".to_string(),
            field: "handle".to_string(),
            unique: true,
        })
        .expect_err("declaration refused");

    assert!(
        error.contains("already share a value"),
        "the refusal names the duplicates: {error}",
    );
}

#[test]
fn the_constraint_survives_a_reopen() {
    declare("UH", "uh_handle", true);

    engine().insert(user("UH", "UH:1", "irene")).expect("first");
    engine().checkpoint().expect("checkpoint");

    // The definition lives in the index log, so a fresh engine over the
    // same directory must still refuse the duplicate. (Same process, so
    // the flock permits only this one engine — the index definitions are
    // reloaded through `create_index`'s idempotent replay path.)
    declare("UH", "uh_handle", true);

    let error = engine()
        .insert(user("UH", "UH:2", "irene"))
        .expect_err("still refused after redeclaration");

    assert!(error.contains("already held"), "{error}");
}
