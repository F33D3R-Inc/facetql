//! What a delete means when other nodes point at the node being deleted.
//!
//! Before this, the answer was "nothing": FacetQL removed the node and
//! left every row referencing it behind, pointing at an address that no
//! longer resolves. An application that cared had to find those rows
//! itself, which it could only do by holding the whole graph in memory
//! — and then delete them in a second transaction, so a crash between
//! the two produced exactly the orphans it was trying to avoid.
//!
//! These tests are the three referential actions, the declaration rules
//! that make them cheap, and the insert-side check that keeps a
//! reference meaning something in the first place.

use facetql::core::coordinate::Coordinate;
use facetql::core::node::{Node, Visibility};
use facetql::core::predicate::Expr;
use facetql::storage::engine::{StorageEngine, TransactionError, TxOperation};
use facetql::storage::index::IndexDef;
use facetql::storage::reference::{ReferenceDef, ReferentialAction};

/// One engine, shared by every test in this binary — the data directory
/// is process-wide, so tests cannot each build their own. Every test
/// uses its own kinds, so they neither see nor delete each other's rows.
fn engine() -> &'static StorageEngine {
    static ENGINE: std::sync::OnceLock<StorageEngine> = std::sync::OnceLock::new();

    ENGINE.get_or_init(|| {
        let dir = std::path::PathBuf::from("target")
            .join(format!("it-refs-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create data dir");

        facetql::config::set_data_dir(dir.canonicalize().expect("resolve"));

        StorageEngine::open().expect("open engine")
    })
}

fn put(kind: &str, address: &str, data: &str) {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        address.to_string(),
        kind.to_string(),
        "app".to_string(),
    );

    node.data = data.to_string();
    node.visibility = Visibility::Public;

    engine().insert(node).expect("insert");
}

fn node(kind: &str, address: &str, data: &str) -> Node {
    let mut node = Node::new(
        Coordinate::new(0, 0, 0, 0),
        address.to_string(),
        kind.to_string(),
        "app".to_string(),
    );

    node.data = data.to_string();
    node.visibility = Visibility::Public;

    node
}

fn index(name: &str, kind: &str, field: &str, unique: bool) {
    engine()
        .create_index(IndexDef {
            name: name.to_string(),
            kind: kind.to_string(),
            field: field.to_string(),
            unique,
        })
        .expect("declare index");
}

fn reference(
    name: &str,
    kind: &str,
    field: &str,
    parent_kind: &str,
    on_delete: ReferentialAction,
) -> Result<(), String> {
    engine().create_reference(ReferenceDef {
        name: name.to_string(),
        kind: kind.to_string(),
        field: field.to_string(),
        parent_kind: parent_kind.to_string(),
        parent_field: None,
        on_delete,
    })
}

fn exists(address: &str) -> bool {
    engine().get(address).expect("read").is_some()
}

#[test]
fn a_delete_cascades_through_the_whole_reference_graph() {
    index("i_cc_comment", "CcComment", "post", false);
    index("i_cc_like", "CcLike", "comment", false);

    reference("r_cc_comment", "CcComment", "post", "CcPost", ReferentialAction::Cascade)
        .expect("declare reference");
    reference("r_cc_like", "CcLike", "comment", "CcComment", ReferentialAction::Cascade)
        .expect("declare reference");

    put("CcPost", "CcPost:1", r#"{"body":"first"}"#);
    put("CcPost", "CcPost:2", r#"{"body":"second"}"#);

    put("CcComment", "CcComment:a", r#"{"post":"CcPost:1"}"#);
    put("CcComment", "CcComment:b", r#"{"post":"CcPost:1"}"#);
    put("CcComment", "CcComment:c", r#"{"post":"CcPost:2"}"#);

    put("CcLike", "CcLike:a1", r#"{"comment":"CcComment:a"}"#);
    put("CcLike", "CcLike:b1", r#"{"comment":"CcComment:b"}"#);
    put("CcLike", "CcLike:c1", r#"{"comment":"CcComment:c"}"#);

    engine().delete("CcPost:1").expect("delete");

    // Two levels down, both gone: the like referenced a comment that
    // referenced the post, and nothing named it directly.
    for gone in ["CcPost:1", "CcComment:a", "CcComment:b", "CcLike:a1", "CcLike:b1"] {
        assert!(!exists(gone), "{gone} should have been removed by the cascade");
    }

    // The other post's subtree is untouched — a cascade follows the
    // reference, it does not clear the kind.
    for alive in ["CcPost:2", "CcComment:c", "CcLike:c1"] {
        assert!(exists(alive), "{alive} was not part of the cascade");
    }
}

#[test]
fn every_node_a_cascade_removes_is_archived() {
    index("i_ar_comment", "ArComment", "post", false);
    reference("r_ar", "ArComment", "post", "ArPost", ReferentialAction::Cascade)
        .expect("declare reference");

    put("ArPost", "ArPost:1", r#"{"body":"gone soon"}"#);
    put("ArComment", "ArComment:a", r#"{"post":"ArPost:1","body":"mine"}"#);

    engine().delete("ArPost:1").expect("delete");

    // A cascaded delete is still a delete, so the final state has to be
    // recoverable the same way — otherwise a node removed by a rule
    // nobody typed is the one node with no history at all.
    let history = engine().history_for("ArComment:a").expect("history");

    assert!(
        history.iter().any(|entry| entry.node.data.contains("mine")),
        "the cascaded child's final state was never archived: {history:?}",
    );
}

#[test]
fn restrict_refuses_the_delete_and_applies_nothing() {
    index("i_rs_comment", "RsComment", "post", false);
    reference("r_rs", "RsComment", "post", "RsPost", ReferentialAction::Restrict)
        .expect("declare reference");

    put("RsPost", "RsPost:1", r#"{"body":"protected"}"#);
    put("RsComment", "RsComment:a", r#"{"post":"RsPost:1"}"#);

    let refused = engine().delete("RsPost:1").expect_err("restrict must refuse");

    // A conflict rather than a malformed request: the caller asked for
    // something well formed and the state said no.
    assert!(
        matches!(refused, TransactionError::Precondition(_)),
        "restrict is a precondition failure, not a storage error: {refused:?}",
    );

    let refused = refused.to_string();

    assert!(
        refused.contains("RsComment:a") && refused.contains("restrict"),
        "the refusal should name what still references it: {refused}",
    );

    assert!(exists("RsPost:1"), "a refused delete must apply nothing");
    assert!(exists("RsComment:a"));

    // Removing the reason removes the refusal.
    engine().delete("RsComment:a").expect("delete the child");
    engine().delete("RsPost:1").expect("now unreferenced");

    assert!(!exists("RsPost:1"));
}

#[test]
fn set_null_keeps_the_children_and_clears_the_field() {
    index("i_sn_comment", "SnComment", "post", false);
    reference("r_sn", "SnComment", "post", "SnPost", ReferentialAction::SetNull)
        .expect("declare reference");

    put("SnPost", "SnPost:1", r#"{"body":"post"}"#);
    put("SnComment", "SnComment:a", r#"{"post":"SnPost:1","body":"kept"}"#);

    engine().delete("SnPost:1").expect("delete");

    let child = engine().get("SnComment:a").expect("read").expect("still there");

    let data: serde_json::Value =
        serde_json::from_str(&child.data).expect("data is json");

    assert!(data.get("body").is_some(), "the rest of the row is untouched");

    // Null rather than absent: a reader can still see that this kind
    // *has* a reference field and that it currently points at nothing.
    assert_eq!(
        data.get("post"),
        Some(&serde_json::Value::Null),
        "the reference field should be null, not removed: {data}",
    );
}

#[test]
fn a_bulk_delete_cascades_exactly_like_a_single_one() {
    index("i_bk_comment", "BkComment", "post", false);
    reference("r_bk", "BkComment", "post", "BkPost", ReferentialAction::Cascade)
        .expect("declare reference");

    put("BkPost", "BkPost:1", r#"{"draft":true}"#);
    put("BkPost", "BkPost:2", r#"{"draft":false}"#);
    put("BkComment", "BkComment:a", r#"{"post":"BkPost:1"}"#);
    put("BkComment", "BkComment:b", r#"{"post":"BkPost:2"}"#);

    let drafts: Expr = serde_json::from_value(serde_json::json!({
        "kind": "bin",
        "op": "==",
        "l": { "kind": "get", "field": "draft", "obj": { "kind": "ref", "name": "item" } },
        "r": { "kind": "lit", "val": true },
    }))
    .expect("build predicate");

    engine()
        .execute_transaction(vec![TxOperation::DeleteWhere {
            kind: "BkPost".to_string(),
            where_: Some(drafts),
            owner: "app".to_string(),
            is_admin: true,
        }])
        .expect("bulk delete");

    assert!(!exists("BkPost:1"));
    assert!(!exists("BkComment:a"), "a delete_where target cascades too");

    assert!(exists("BkPost:2"));
    assert!(exists("BkComment:b"));
}

#[test]
fn a_batch_that_removes_parent_and_child_together_is_not_a_double_delete() {
    index("i_tg_comment", "TgComment", "post", false);
    reference("r_tg", "TgComment", "post", "TgPost", ReferentialAction::Cascade)
        .expect("declare reference");

    put("TgPost", "TgPost:1", r#"{}"#);
    put("TgComment", "TgComment:a", r#"{"post":"TgPost:1"}"#);

    // The parent cascades onto the child, and the batch then names the
    // child itself. The second mention has to be a no-op rather than a
    // "delete target not found" that fails the whole batch.
    engine()
        .execute_transaction(vec![
            TxOperation::DeleteNode("TgPost:1".to_string()),
            TxOperation::DeleteNode("TgComment:a".to_string()),
        ])
        .expect("batch");

    assert!(!exists("TgPost:1"));
    assert!(!exists("TgComment:a"));
}

#[test]
fn a_cycle_in_the_reference_graph_terminates() {
    index("i_cy_a", "CyA", "b", false);
    index("i_cy_b", "CyB", "a", false);

    reference("r_cy_a", "CyA", "b", "CyB", ReferentialAction::Cascade)
        .expect("declare reference");
    reference("r_cy_b", "CyB", "a", "CyA", ReferentialAction::Cascade)
        .expect("declare reference");

    // Neither can be inserted before the other — each names a node that
    // does not exist yet — so a cycle can only be created in one batch,
    // which is exactly what the deferred check makes possible.
    engine()
        .execute_transaction(vec![
            TxOperation::InsertNode(node("CyA", "CyA:1", r#"{"b":"CyB:1"}"#)),
            TxOperation::InsertNode(node("CyB", "CyB:1", r#"{"a":"CyA:1"}"#)),
        ])
        .expect("a cycle takes one batch");

    // Each references the other, so a closure that revisited what it
    // had already removed would never finish.
    engine().delete("CyA:1").expect("delete");

    assert!(!exists("CyA:1"));
    assert!(!exists("CyB:1"), "the cycle's other half goes with it");
}

#[test]
fn an_insert_naming_a_parent_that_is_not_there_is_refused() {
    index("i_iv_comment", "IvComment", "post", false);
    reference("r_iv", "IvComment", "post", "IvPost", ReferentialAction::Cascade)
        .expect("declare reference");

    let orphan = node("IvComment", "IvComment:a", r#"{"post":"IvPost:missing"}"#);

    let refused = engine().insert(orphan).expect_err("must be refused");

    assert!(
        refused.contains("IvPost:missing"),
        "the refusal should name the value that did not resolve: {refused}",
    );

    assert!(!exists("IvComment:a"));

    // A node referencing nothing is always admissible — that is a
    // nullable foreign key, and it is what makes set_null usable.
    engine()
        .insert(node("IvComment", "IvComment:b", r#"{"post":null}"#))
        .expect("null references nothing");

    engine()
        .insert(node("IvComment", "IvComment:c", r#"{}"#))
        .expect("an absent field references nothing");
}

#[test]
fn a_reference_is_checked_against_the_batch_not_the_order_it_was_written_in() {
    index("i_df_comment", "DfComment", "post", false);
    reference("r_df", "DfComment", "post", "DfPost", ReferentialAction::Cascade)
        .expect("declare reference");

    // Child first. Checking in order would refuse this; the constraint
    // is a property of the data, not of the order a caller serialized
    // its writes in.
    engine()
        .execute_transaction(vec![
            TxOperation::InsertNode(node(
                "DfComment",
                "DfComment:a",
                r#"{"post":"DfPost:1"}"#,
            )),
            TxOperation::InsertNode(node("DfPost", "DfPost:1", r#"{}"#)),
        ])
        .expect("deferred to the end of the batch");

    assert!(exists("DfComment:a"));

    // A batch that creates a parent and then deletes it again is not a
    // violation: the delete cascades onto the child it created, so the
    // net effect is consistent — which is the answer the rule asks for,
    // and a stricter one than refusing it.
    engine()
        .execute_transaction(vec![
            TxOperation::InsertNode(node(
                "DfComment",
                "DfComment:b",
                r#"{"post":"DfPost:2"}"#,
            )),
            TxOperation::InsertNode(node("DfPost", "DfPost:2", r#"{}"#)),
            TxOperation::DeleteNode("DfPost:2".to_string()),
        ])
        .expect("the delete cascades onto what the same batch inserted");

    assert!(!exists("DfPost:2"));
    assert!(!exists("DfComment:b"), "the cascade reached the staged child");

    // A batch whose net effect leaves a child naming a parent nothing
    // creates is refused as a unit.
    let refused = engine()
        .execute_transaction(vec![
            TxOperation::InsertNode(node(
                "DfComment",
                "DfComment:c",
                r#"{"post":"DfPost:never"}"#,
            )),
            TxOperation::InsertNode(node("DfPost", "DfPost:3", r#"{}"#)),
        ])
        .expect_err("the parent is never created");

    assert!(
        format!("{refused:?}").contains("DfPost:never"),
        "the refusal should name the unresolved parent: {refused:?}",
    );

    assert!(!exists("DfComment:c"), "nothing applied");
    assert!(!exists("DfPost:3"), "not even the half that was fine");
}

#[test]
fn a_reference_by_data_field_resolves_through_the_parents_unique_index() {
    index("i_pk_user", "PkUser", "id", true);
    index("i_pk_post", "PkPost", "author", false);

    engine()
        .create_reference(ReferenceDef {
            name: "r_pk".to_string(),
            kind: "PkPost".to_string(),
            field: "author".to_string(),
            parent_kind: "PkUser".to_string(),
            parent_field: Some("id".to_string()),
            on_delete: ReferentialAction::Cascade,
        })
        .expect("declare reference");

    put("PkUser", "PkUser:7", r#"{"id":7,"handle":"ada"}"#);
    put("PkPost", "PkPost:1", r#"{"author":7}"#);
    put("PkPost", "PkPost:2", r#"{"author":7}"#);

    // The value is a number in `data`, not an address — the shape an
    // application has when its own ids live in the row.
    let refused = engine()
        .insert(node("PkPost", "PkPost:3", r#"{"author":9}"#))
        .expect_err("no user 9");

    assert!(refused.contains("PkUser"), "{refused}");

    engine().delete("PkUser:7").expect("delete");

    assert!(!exists("PkPost:1"));
    assert!(!exists("PkPost:2"));
}

#[test]
fn a_reference_needs_the_access_paths_that_make_it_cheap() {
    put("NxPost", "NxPost:1", r#"{}"#);

    // No index over the referencing field: enforcing this would mean
    // reading every NxComment on every NxPost delete.
    let refused = reference(
        "r_nx",
        "NxComment",
        "post",
        "NxPost",
        ReferentialAction::Cascade,
    )
    .expect_err("must be refused");

    assert!(
        refused.contains("index over NxComment.post"),
        "the refusal should name the index to declare: {refused}",
    );

    index("i_nx_comment", "NxComment", "post", false);

    // Now the referencing side is fine, but the referenced side is a
    // data field nothing keeps unique — a value two nodes can hold
    // names neither of them.
    index("i_nx_user_id", "NxUser", "id", false);

    let refused = engine()
        .create_reference(ReferenceDef {
            name: "r_nx2".to_string(),
            kind: "NxComment".to_string(),
            field: "post".to_string(),
            parent_kind: "NxUser".to_string(),
            parent_field: Some("id".to_string()),
            on_delete: ReferentialAction::Cascade,
        })
        .expect_err("must be refused");

    assert!(
        refused.contains("does not make unique"),
        "the refusal should say why a non-unique key cannot be referenced: {refused}",
    );
}

#[test]
fn a_reference_the_existing_data_already_breaks_is_refused() {
    index("i_bd_comment", "BdComment", "post", false);

    put("BdPost", "BdPost:1", r#"{}"#);
    put("BdComment", "BdComment:ok", r#"{"post":"BdPost:1"}"#);
    put("BdComment", "BdComment:orphan", r#"{"post":"BdPost:gone"}"#);

    let refused = reference(
        "r_bd",
        "BdComment",
        "post",
        "BdPost",
        ReferentialAction::Cascade,
    )
    .expect_err("must be refused");

    assert!(
        refused.contains("BdComment:orphan"),
        "the refusal should name the row that breaks it: {refused}",
    );

    // A rule accepted over data that already breaks it would be false
    // from the moment it was created, so it must not exist.
    assert!(
        engine().list_references().iter().all(|r| r.name != "r_bd"),
        "the refused reference must not have been declared",
    );

    engine().delete("BdComment:orphan").expect("remove the orphan");

    reference("r_bd", "BdComment", "post", "BdPost", ReferentialAction::Cascade)
        .expect("now the data satisfies it");
}

#[test]
fn the_index_a_reference_is_enforced_through_cannot_be_dropped() {
    index("i_dp_comment", "DpComment", "post", false);
    reference("r_dp", "DpComment", "post", "DpPost", ReferentialAction::Cascade)
        .expect("declare reference");

    let refused = engine().drop_index("i_dp_comment").expect_err("must be refused");

    assert!(
        refused.contains("r_dp"),
        "the refusal should name the reference that depends on it: {refused}",
    );

    // Dropping the rule first is how you get the index back.
    engine().drop_reference("r_dp").expect("drop reference");
    engine().drop_index("i_dp_comment").expect("now unreferenced");

    // And with the rule gone, a delete stops cascading.
    index("i_dp_comment2", "DpComment", "post", false);
    put("DpPost", "DpPost:1", r#"{}"#);
    put("DpComment", "DpComment:a", r#"{"post":"DpPost:1"}"#);

    engine().delete("DpPost:1").expect("delete");

    assert!(
        exists("DpComment:a"),
        "a dropped reference stops being enforced; the child survives as an orphan",
    );
}

#[test]
fn re_declaring_the_same_reference_converges_and_a_different_one_conflicts() {
    index("i_rd_comment", "RdComment", "post", false);

    reference("r_rd", "RdComment", "post", "RdPost", ReferentialAction::Cascade)
        .expect("declare");

    reference("r_rd", "RdComment", "post", "RdPost", ReferentialAction::Cascade)
        .expect("re-declaring the identical reference is how setup scripts re-run");

    let conflict =
        reference("r_rd", "RdComment", "post", "RdPost", ReferentialAction::Restrict)
            .expect_err("a different rule under the same name is a contradiction");

    assert!(conflict.contains("already exists"), "{conflict}");
}

/// A cascade must cost what it removes, not what the batch touched.
///
/// The lookup that finds referencing nodes has to consider the batch's
/// own writes as well as the index, and the obvious way to do that —
/// consider every address the batch has touched — costs the size of the
/// batch on every lookup. A cascade does one lookup per removed node,
/// and each removal joins the batch, so that version is quadratic in the
/// number of nodes it removes, spent inside the writer lock.
///
/// Timed, but not against a constant: a wall-clock bound measures the
/// machine as much as the code, and a threshold that holds on this box
/// is a guess about every other one. This runs the same cascade at two
/// sizes and compares it against **itself** — four times the nodes costs
/// about four times as much when the plan is linear and about sixteen
/// when it is quadratic, so the machine cancels out and the shape is
/// what is left. Fixed overheads inflate the smaller measurement, which
/// pushes the ratio down, so they can only make this test pass, never
/// fail it spuriously.
#[test]
fn a_cascade_costs_what_it_removes_not_the_size_of_the_batch() {
    const SMALL: usize = 1_250;
    const LARGE: usize = SMALL * 4;

    index("i_sc_child", "ScChild", "parent", false);
    reference("r_sc", "ScChild", "parent", "ScParent", ReferentialAction::Cascade)
        .expect("declare reference");

    let seed = |parent: &str, children: usize| {
        put("ScParent", parent, r#"{}"#);

        for batch in 0..(children / 250) {
            let ops: Vec<TxOperation> = (0..250)
                .map(|i| {
                    TxOperation::InsertNode(node(
                        "ScChild",
                        &format!("ScChild:{parent}:{:06}", batch * 250 + i),
                        &format!(r#"{{"parent":"{parent}"}}"#),
                    ))
                })
                .collect();

            engine().execute_transaction(ops).expect("seed");
        }
    };

    let time = |parent: &str| {
        let start = std::time::Instant::now();
        engine().delete(parent).expect("cascade");
        start.elapsed()
    };

    seed("ScParent:small", SMALL);
    seed("ScParent:large", LARGE);

    let small = time("ScParent:small");
    let large = time("ScParent:large");

    assert!(!exists(&format!("ScChild:ScParent:large:{:06}", LARGE - 1)));
    assert!(!exists("ScChild:ScParent:small:000000"));

    // Linear is 4x, quadratic is 16x. Half way between them in
    // multiplicative terms, so neither shape is near the line.
    assert!(
        large < small * 8,
        "cascading {LARGE} children took {large:?} against {small:?} for \
         {SMALL} — four times the nodes cost {:.1}x the time, which is the \
         batch being rescanned per removed node rather than one index \
         lookup per reference",
        large.as_secs_f64() / small.as_secs_f64().max(f64::MIN_POSITIVE),
    );
}
