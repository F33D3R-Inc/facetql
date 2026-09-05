//! A cascade that will not fit in one frame is refused, not split.
//!
//! The whole reason referential actions live in the engine is that the
//! parent and everything referencing it become durable together. That
//! guarantee has a size: a frame is staged in memory before it is
//! committed, so a delete whose closure runs to millions of nodes cannot
//! be honoured atomically. The only two answers are "refuse it" and
//! "apply half of it", and the second one is the failure this feature
//! exists to prevent.
//!
//! Its own test binary because the bound is read once per process from
//! the environment, and a realistic default would need tens of thousands
//! of rows to reach. Here the server is started with a small one, so the
//! refusal is exercised on four nodes instead of 25,000.

mod common;

use std::process::Command;

use common::{free_port, request, scratch, Server};

/// Six operations: a delete lowers to an archive and a removal per node,
/// so this admits a parent with two children and refuses one with three.
///
/// A `set_null` child costs the same two — an archive and the rewrite
/// that clears its field — so the same four-node shape is the boundary
/// on that path too.
const BOUND: &str = "6";

fn start(dir: &std::path::PathBuf, port: u16) -> Server {
    let child = Command::new(env!("CARGO_BIN_EXE_facetql"))
        .arg("start")
        .env("FACETQL_ENV", "test")
        .env("ENOCHIAN_DATA_DIR", dir)
        .env("ENOCHIAN_PORT", port.to_string())
        .env("FACETQL_MAX_TRANSACTION_OPS", BOUND)
        .env("FACETQL_RATE_READ", "off")
        .env("FACETQL_RATE_WRITE", "off")
        .env("FACETQL_RATE_BULK", "off")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn facetql");

    let server = Server { child, port, dir: dir.clone() };
    server.wait_ready();
    server
}

fn write(server: &Server, address: &str, kind: &str, data: &str) {
    let escaped = data.replace('"', "\\\"");

    let body = format!(
        r#"{{"address":"{address}","kind":"{kind}","x":0,"y":0,"z":0,"q":0,"data":"{escaped}","public":true}}"#
    );

    let created = server.post("/node", &body).expect("write");

    assert!(
        (200..300).contains(&created.status),
        "writing {address} answered {}: {}",
        created.status,
        created.body,
    );
}

#[test]
fn a_cascade_too_large_to_stage_atomically_is_refused_and_applies_nothing() {
    let dir = scratch("cascade-bound");
    let port = free_port();

    let server = start(&dir, port);

    let created = server
        .post(
            "/admin/indexes",
            r#"{"name":"cb_child","kind":"CbChild","field":"parent"}"#,
        )
        .expect("declare index");
    assert_eq!(created.status, 201, "{}", created.body);

    let created = server
        .post(
            "/admin/references",
            r#"{"name":"cb","kind":"CbChild","field":"parent",
                "parent_kind":"CbParent","on_delete":"cascade"}"#,
        )
        .expect("declare reference");
    assert_eq!(created.status, 201, "{}", created.body);

    // Fits: two nodes, four operations.
    write(&server, "CbParent:small", "CbParent", r#"{}"#);
    write(&server, "CbChild:s1", "CbChild", r#"{"parent":"CbParent:small"}"#);
    write(&server, "CbChild:s2", "CbChild", r#"{"parent":"CbParent:small"}"#);

    let deleted =
        request(port, "DELETE", "/node/CbParent:small", None).expect("delete");

    assert_eq!(deleted.status, 204, "{}", deleted.body);
    assert!(!server.exists("CbChild:s1"));
    assert!(!server.exists("CbChild:s2"));

    // Does not fit: one node past the bound.
    write(&server, "CbParent:big", "CbParent", r#"{}"#);

    for n in 1..=3 {
        write(
            &server,
            &format!("CbChild:b{n}"),
            "CbChild",
            r#"{"parent":"CbParent:big"}"#,
        );
    }

    let refused =
        request(port, "DELETE", "/node/CbParent:big", None).expect("delete");

    // 400, not 500: the request is well formed and will keep failing
    // until the caller changes something, so telling it to retry would
    // be a lie.
    assert_eq!(refused.status, 400, "{}", refused.body);

    assert!(
        refused.body.contains("cascades to more than"),
        "the refusal should say what it could not do: {}",
        refused.body,
    );

    // Nothing applied — not the parent, and not the children the closure
    // had already walked when it hit the bound.
    assert!(server.exists("CbParent:big"), "a refused delete applies nothing");

    for n in 1..=3 {
        assert!(server.exists(&format!("CbChild:b{n}")));
    }
}

/// The same bound, on the path that used to walk straight past it.
///
/// `set_null` is the one referential action that stages operations
/// *without* putting anything back on the work queue: the children are
/// rewritten in place inside the parent's own iteration. A bound checked
/// only against the parent's own archive and removal therefore never saw
/// them, and one delete could stage 2N+2 mutations into a frame that
/// admits six.
#[test]
fn a_set_null_closure_over_the_bound_is_refused_and_applies_nothing() {
    let dir = scratch("cascade-bound-set-null");
    let port = free_port();

    let server = start(&dir, port);

    let created = server
        .post(
            "/admin/indexes",
            r#"{"name":"sn_child","kind":"SnChild","field":"parent"}"#,
        )
        .expect("declare index");
    assert_eq!(created.status, 201, "{}", created.body);

    let created = server
        .post(
            "/admin/references",
            r#"{"name":"sn","kind":"SnChild","field":"parent",
                "parent_kind":"SnParent","on_delete":"set_null"}"#,
        )
        .expect("declare reference");
    assert_eq!(created.status, 201, "{}", created.body);

    // Fits: the parent's two operations and two per cleared child.
    write(&server, "SnParent:small", "SnParent", r#"{}"#);
    write(&server, "SnChild:s1", "SnChild", r#"{"parent":"SnParent:small"}"#);
    write(&server, "SnChild:s2", "SnChild", r#"{"parent":"SnParent:small"}"#);

    let deleted =
        request(port, "DELETE", "/node/SnParent:small", None).expect("delete");

    assert_eq!(deleted.status, 204, "{}", deleted.body);

    for n in 1..=2 {
        let child = server.get(&format!("/node/SnChild:s{n}"));

        assert_eq!(child.status, 200, "set_null keeps the child: {}", child.body);

        assert!(
            !child.body.contains("SnParent:small"),
            "the reference should have been cleared: {}",
            child.body,
        );
    }

    // Does not fit: eight operations for a frame that admits six.
    write(&server, "SnParent:big", "SnParent", r#"{}"#);

    for n in 1..=3 {
        write(
            &server,
            &format!("SnChild:b{n}"),
            "SnChild",
            r#"{"parent":"SnParent:big"}"#,
        );
    }

    let refused =
        request(port, "DELETE", "/node/SnParent:big", None).expect("delete");

    assert_eq!(refused.status, 400, "{}", refused.body);

    assert!(
        refused.body.contains("cascades to more than"),
        "the refusal should say what it could not do: {}",
        refused.body,
    );

    // Nothing applied: not the parent, and not the children whose field
    // the closure had already cleared when it hit the bound.
    assert!(server.exists("SnParent:big"), "a refused delete applies nothing");

    for n in 1..=3 {
        let child = server.get(&format!("/node/SnChild:b{n}"));

        assert_eq!(child.status, 200, "{}", child.body);

        assert!(
            child.body.contains("SnParent:big"),
            "a refused delete must leave the reference standing: {}",
            child.body,
        );
    }
}
