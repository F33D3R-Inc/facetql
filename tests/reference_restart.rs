//! A declared reference has to survive a restart, and it has to be
//! declarable over the wire.
//!
//! Everything about referential integrity is decided from the resident
//! set of definitions. That set is rebuilt at startup by replaying
//! `facetql.references`, so a reference that does not come back is a
//! rule the database silently stops enforcing — the failure would look
//! exactly like the orphans it exists to prevent, and only after a
//! restart. This is the one property the in-process tests cannot check,
//! because they never re-open the database.
//!
//! Driven through the real binary over HTTP, which also exercises the
//! three admin endpoints end to end.

mod common;

use common::{free_port, request, scratch, Server};

/// A node body whose `data` is JSON — `common::node_body` takes a plain
/// string, and every reference lives in a field.
fn body(address: &str, kind: &str, data: &str) -> String {
    let escaped = data.replace('"', "\\\"");

    format!(
        r#"{{"address":"{address}","kind":"{kind}","x":0,"y":0,"z":0,"q":0,"data":"{escaped}","public":true}}"#
    )
}

#[test]
fn a_declared_reference_still_cascades_after_a_restart() {
    let dir = scratch("reference-restart");
    let port = free_port();

    let mut server = Server::start(&dir, port);

    let created = server
        .post(
            "/admin/indexes",
            r#"{"name":"rr_comment","kind":"RrComment","field":"post"}"#,
        )
        .expect("declare index");

    assert_eq!(created.status, 201, "{}", created.body);

    let created = server
        .post(
            "/admin/references",
            r#"{"name":"rr","kind":"RrComment","field":"post",
                "parent_kind":"RrPost","on_delete":"cascade"}"#,
        )
        .expect("declare reference");

    assert_eq!(created.status, 201, "{}", created.body);

    // Re-declaring the identical rule converges; a different one under
    // the same name is a contradiction, not a repeat.
    let again = server
        .post(
            "/admin/references",
            r#"{"name":"rr","kind":"RrComment","field":"post",
                "parent_kind":"RrPost","on_delete":"cascade"}"#,
        )
        .expect("re-declare");

    assert_eq!(again.status, 201, "{}", again.body);

    let conflict = server
        .post(
            "/admin/references",
            r#"{"name":"rr","kind":"RrComment","field":"post",
                "parent_kind":"RrPost","on_delete":"restrict"}"#,
        )
        .expect("conflicting re-declare");

    assert_eq!(conflict.status, 409, "{}", conflict.body);

    for request_body in [
        body("RrPost:1", "RrPost", r#"{"body":"post"}"#),
        body("RrComment:a", "RrComment", r#"{"post":"RrPost:1"}"#),
    ] {
        let created = server.post("/node", &request_body).expect("write");
        assert!(
            (200..300).contains(&created.status),
            "{} → {}",
            request_body,
            created.body,
        );
    }

    // An orphan is refused over the wire too, not only in-process.
    let refused = server
        .post(
            "/node",
            &body("RrComment:x", "RrComment", r#"{"post":"RrPost:gone"}"#),
        )
        .expect("write");

    assert_eq!(refused.status, 400, "{}", refused.body);

    server.restart();

    // The definition has to be back before it can be enforced.
    let listed = server.get("/admin/references");

    assert_eq!(listed.status, 200, "{}", listed.body);
    assert!(
        listed.body.contains("\"rr\"") && listed.body.contains("cascade"),
        "the reference did not survive the restart: {}",
        listed.body,
    );

    let deleted = request(port, "DELETE", "/node/RrPost:1", None).expect("delete");

    assert!(
        (200..300).contains(&deleted.status),
        "delete answered {}: {}",
        deleted.status,
        deleted.body,
    );

    assert!(!server.exists("RrPost:1"));
    assert!(
        !server.exists("RrComment:a"),
        "the child survived a delete that a replayed reference should have \
         cascaded — the rule came back as a listing but not as enforcement",
    );

    // And the enforcement survives a second restart, from a state where
    // the cascade itself is part of what recovery replays.
    server.restart();

    assert!(!server.exists("RrComment:a"), "the cascade must not come undone");

    let dropped =
        request(port, "DELETE", "/admin/references/rr", None).expect("drop");

    assert_eq!(dropped.status, 204, "{}", dropped.body);

    server.restart();

    let listed = server.get("/admin/references");

    assert!(
        !listed.body.contains("\"rr\""),
        "a dropped reference came back: {}",
        listed.body,
    );
}
