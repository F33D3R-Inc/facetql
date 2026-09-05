//! Crash injection against the real server binary.
//!
//! Everything else in `tests/` exercises a subsystem in-process. This
//! file does the one thing that cannot be faked: it starts the actual
//! `facetql` binary, writes to it over HTTP, `SIGKILL`s it mid-workload,
//! restarts it, and checks what survived.
//!
//! Two invariants, and they are the whole durability contract:
//!
//! * **An acknowledged write survives.** If the server returned 2xx, the
//!   record is there after the crash. Anything less means the WAL fsync
//!   is not a durability boundary.
//! * **A transaction is all-or-nothing.** For every batch, either every
//!   record it wrote is present or none of them are. A batch that comes
//!   back half-applied means the BEGIN/COMMIT framing is decoration.
//!
//! The engine has carried framed transactions and crash-atomic replay for
//! a while, and reading the code says they are right. Nothing had ever
//! killed the process to find out.
//!
//! No HTTP client crate is used — these are a few plain requests, and a
//! hand-rolled one keeps the test free of dev-dependencies.

mod common;

use common::{free_port, node_body, scratch, Server};

use std::process::Command;

#[test]
fn every_acknowledged_write_survives_a_sigkill() {
    let dir = scratch("acknowledged");
    let port = free_port();

    let mut server = Server::start(&dir, port);
    let mut acknowledged: Vec<String> = Vec::new();

    for n in 0..300u32 {
        let address = format!("Crash:{n:06}");
        let body = node_body(&address, "Crash", &format!("payload-{n}"));

        match server.post("/node", &body) {
            Ok(r) if (200..300).contains(&r.status) => acknowledged.push(address),
            // A write we were never told succeeded proves nothing either
            // way — it is exactly the case the contract says nothing
            // about.
            _ => {}
        }
    }

    assert!(
        acknowledged.len() > 100,
        "expected the workload to get going; only {} writes acknowledged",
        acknowledged.len(),
    );

    server.kill();

    let server = Server::start(&dir, port);

    let mut missing = Vec::new();

    for address in &acknowledged {
        if !server.exists(address) {
            missing.push(address.clone());
        }
    }

    assert!(
        missing.is_empty(),
        "{} of {} acknowledged writes did not survive the crash: {:?}",
        missing.len(),
        acknowledged.len(),
        &missing[..missing.len().min(10)],
    );
}

#[test]
fn a_transaction_is_all_or_nothing_across_a_crash() {
    // The invariant that makes framed transactions worth having. Each
    // batch writes several records sharing a batch id; after the crash,
    // every batch must be entirely present or entirely absent. A batch
    // found half-applied means the BEGIN/COMMIT framing does not bound
    // anything.
    let dir = scratch("atomic");
    let port = free_port();

    let mut server = Server::start(&dir, port);

    const PER_BATCH: u32 = 12;
    let mut committed: Vec<u32> = Vec::new();

    for batch in 0..120u32 {
        let ops: Vec<String> = (0..PER_BATCH)
            .map(|i| {
                format!(
                    r#"{{"type":"insert_node","address":"Batch:{batch:04}:{i:02}","kind":"Batch","x":0,"y":0,"z":0,"q":0,"data":"b{batch}","public":true}}"#
                )
            })
            .collect();

        let body = format!(r#"{{"operations":[{}]}}"#, ops.join(","));

        match server.post("/transaction", &body) {
            Ok(r) if (200..300).contains(&r.status) => committed.push(batch),
            _ => {}
        }
    }

    assert!(
        committed.len() > 40,
        "expected the workload to get going; only {} batches committed",
        committed.len(),
    );

    server.kill();

    let server = Server::start(&dir, port);

    // Every batch that was ever issued — acknowledged or not — must be
    // whole or absent. The acknowledged ones must additionally be whole.
    for batch in 0..120u32 {
        let present = (0..PER_BATCH)
            .filter(|i| server.exists(&format!("Batch:{batch:04}:{i:02}")))
            .count() as u32;

        assert!(
            present == 0 || present == PER_BATCH,
            "batch {batch} came back half-applied: {present} of {PER_BATCH} records present",
        );

        if committed.contains(&batch) {
            assert_eq!(
                present, PER_BATCH,
                "batch {batch} was acknowledged as committed but only {present} records survived",
            );
        }
    }
}

#[test]
fn repeated_crashes_never_lose_a_previously_confirmed_write() {
    // One crash can be survived by luck — a checkpoint that happened to
    // land, a buffer that happened to be empty. Five crashes, each with
    // writes before and after, is the case where a recovery that
    // replays from the wrong position or advances the checkpoint too
    // eagerly shows itself.
    let dir = scratch("repeated");
    let port = free_port();

    let mut server = Server::start(&dir, port);
    let mut confirmed: Vec<String> = Vec::new();

    for round in 0..5u32 {
        for n in 0..80u32 {
            let address = format!("Round:{round}:{n:04}");
            let body = node_body(&address, "Round", &format!("r{round}n{n}"));

            if let Ok(r) = server.post("/node", &body)
                && (200..300).contains(&r.status)
            {
                confirmed.push(address);
            }
        }

        server.kill();
        server = Server::start(&dir, port);

        // Everything confirmed in this round *and every earlier one*
        // must still be readable.
        let mut missing = Vec::new();

        for address in &confirmed {
            if !server.exists(address) {
                missing.push(address.clone());
            }
        }

        assert!(
            missing.is_empty(),
            "after crash {round}, {} of {} confirmed writes were gone: {:?}",
            missing.len(),
            confirmed.len(),
            &missing[..missing.len().min(10)],
        );
    }

    assert!(confirmed.len() > 300, "the workload actually ran");
}

#[test]
fn a_clean_restart_preserves_everything_a_crash_would_have() {
    // The control for the tests above: if an ordinary restart lost data
    // too, the crash tests would be measuring the wrong thing.
    let dir = scratch("clean");
    let port = free_port();

    let mut server = Server::start(&dir, port);

    for n in 0..200u32 {
        let body = node_body(&format!("Clean:{n:04}"), "Clean", &format!("v{n}"));
        let r = server.post("/node", &body).expect("post");

        assert!((200..300).contains(&r.status), "write {n} accepted");
    }

    server.restart();

    for n in 0..200u32 {
        assert!(
            server.exists(&format!("Clean:{n:04}")),
            "record {n} survived a clean restart",
        );
    }
}

#[test]
fn the_data_directory_admits_only_one_process() {
    // The advisory flock is what stops two servers corrupting one
    // directory. If it ever stopped working, every other guarantee in
    // this file becomes unenforceable.
    let dir = scratch("flock");
    let port = free_port();

    let _first = Server::start(&dir, port);

    let second = Command::new(env!("CARGO_BIN_EXE_facetql"))
        .arg("start")
        .env("FACETQL_ENV", "test")
        .env("ENOCHIAN_DATA_DIR", &dir)
        .env("ENOCHIAN_PORT", free_port().to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .expect("run second server");

    assert!(
        !second.status.success(),
        "a second process on the same data directory was refused",
    );
}

// ---------------------------------------------------------------------
// The commit marker, removed on purpose.
//
// The two tests above kill the process at an arbitrary instant, so the
// window they are actually aiming at — the one between the last staged
// mutation and its COMMIT — is hit by luck if it is hit at all, and only
// ever for a batch of plain inserts. The two below remove the luck: they
// build a batch that touches every structure a transaction can move
// (nodes, an overwrite's history entry, an edge, a declared secondary
// index, and a reference whose cascade expands one delete into eight
// mutations), take the process down, and then delete exactly one line
// from the WAL — the COMMIT.
//
// That is the precise durable state a crash between `frame.stage` and
// `frame.commit` leaves behind, and it is the state the whole staging
// design exists to answer for. `discards_...` asserts the answer is
// "none of it", down to the secondary index and the history log;
// `replays_...` runs the identical batch with the log left alone and
// asserts the answer is "all of it". Neither is worth anything without
// the other: a server that lost the batch either way would pass the
// first, and a server that kept it either way would pass the second.
//
// `FACETQL_CHECKPOINT_INTERVAL` is raised out of reach so the engine
// never checkpoints. A checkpoint would push the batch into the heap and
// rewrite the WAL, at which point removing the COMMIT line proves
// nothing — the transaction would already be durable by a route the
// marker does not gate.
// ---------------------------------------------------------------------

/// Env that keeps the engine from checkpointing for the life of a test.
fn no_checkpoints() -> Vec<(&'static str, String)> {
    vec![("FACETQL_CHECKPOINT_INTERVAL", "1000000".to_string())]
}

/// Addresses of `kind`, in the order `GET /nodes` reports them.
fn addresses(server: &Server, kind: &str) -> Vec<String> {
    let body = server.get(&format!("/nodes?kind={kind}")).body;

    let nodes: Vec<serde_json::Value> =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("GET /nodes?kind={kind}: {e}: {body}"));

    nodes
        .iter()
        .map(|n| n["address"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Addresses of `kind` ordered by an indexed `data` field.
///
/// Read through `POST /nodes/query` with `order`, which is the access
/// path the declared index serves — so this observes the index itself,
/// not the heap. An entry the index kept for a node the transaction was
/// rolled back over would show up here and nowhere else.
fn by_index(server: &Server, kind: &str, field: &str) -> Vec<String> {
    let body = server
        .post(
            "/nodes/query",
            &format!(r#"{{"kind":"{kind}","order":"{field}","desc":true}}"#),
        )
        .expect("query")
        .body;

    let page: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("query: {e}: {body}"));

    page["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("query returned no nodes array: {body}"))
        .iter()
        .map(|n| n["address"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// One node's `data` string, or `None` when the address resolves to
/// nothing.
fn data_of(server: &Server, address: &str) -> Option<String> {
    let r = server.get(&format!("/node/{address}"));

    match r.status {
        404 => None,
        200 => {
            let node: serde_json::Value = serde_json::from_str(&r.body)
                .unwrap_or_else(|e| panic!("GET /node/{address}: {e}: {}", r.body));

            Some(node["data"].as_str().unwrap_or_default().to_string())
        }
        other => panic!("GET /node/{address} answered {other}: {}", r.body),
    }
}

fn history_len(server: &Server, address: &str) -> usize {
    let body = server.get(&format!("/node/{address}/history")).body;

    let entries: Vec<serde_json::Value> =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("history {address}: {e}: {body}"));

    entries.len()
}

fn out_edges(server: &Server, address: &str) -> usize {
    let body = server.get(&format!("/node/{address}/edges/out")).body;

    let edges: Vec<serde_json::Value> =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("edges {address}: {e}: {body}"));

    edges.len()
}

/// The state every one of these tests starts from: one parent, three
/// children referencing it, an edge, a declared index over the
/// children's `score`, and a declared cascade reference.
fn seed(server: &Server) {
    let ok = |r: std::io::Result<common::Response>, what: &str| {
        let r = r.expect(what);
        assert!(
            (200..300).contains(&r.status),
            "{what} answered {}: {}",
            r.status,
            r.body,
        );
    };

    ok(
        server.post(
            "/node",
            r#"{"address":"Par:1","kind":"Par","x":0,"y":0,"z":0,"q":0,"data":"{\"n\":\"a\"}","public":true}"#,
        ),
        "seed parent",
    );

    for i in 1..=3u32 {
        ok(
            server.post(
                "/node",
                &format!(
                    r#"{{"address":"Chi:{i}","kind":"Chi","x":0,"y":0,"z":0,"q":0,"data":"{{\"par\":\"Par:1\",\"score\":{i}}}","public":true}}"#
                ),
            ),
            "seed child",
        );
    }

    // Outside the cascade on purpose: something the batch overwrites but
    // does not delete, so the history entry the overwrite archives stays
    // readable afterwards. `GET /node/:address/history` resolves the
    // node first, so a deleted node's archives cannot be observed
    // through it at all.
    ok(
        server.post(
            "/node",
            r#"{"address":"Keep:1","kind":"Keep","x":0,"y":0,"z":0,"q":0,"data":"{\"v\":1}","public":true}"#,
        ),
        "seed keep",
    );

    ok(
        server.post("/edge", r#"{"from":"Par:1","to":"Chi:1","kind":"owns"}"#),
        "seed edge",
    );

    ok(
        server.post(
            "/admin/indexes",
            r#"{"name":"chi_score","kind":"Chi","field":"score"}"#,
        ),
        "declare index",
    );

    // The engine refuses a reference whose child side is unindexed —
    // without it a cascade would scan every node of the kind.
    ok(
        server.post(
            "/admin/indexes",
            r#"{"name":"chi_par","kind":"Chi","field":"par"}"#,
        ),
        "declare reference index",
    );

    ok(
        server.post(
            "/admin/references",
            r#"{"name":"chi_to_par","kind":"Chi","field":"par","parent_kind":"Par","on_delete":"cascade"}"#,
        ),
        "declare reference",
    );
}

/// The batch under test. Every arm moves a different structure:
/// an overwrite (archive + insert, and a `score` the index must
/// re-key), a fresh insert, an edge, and a delete that cascades through
/// the declared reference into all four children.
const WIDE_BATCH: &str = r#"{"operations":[
    {"type":"insert_node","address":"Keep:1","kind":"Keep","x":0,"y":0,"z":0,"q":0,"data":"{\"v\":2}","public":true},
    {"type":"insert_node","address":"Chi:1","kind":"Chi","x":0,"y":0,"z":0,"q":0,"data":"{\"par\":\"Par:1\",\"score\":50}","public":true},
    {"type":"insert_node","address":"Chi:9","kind":"Chi","x":0,"y":0,"z":0,"q":0,"data":"{\"par\":\"Par:1\",\"score\":9}","public":true},
    {"type":"insert_edge","from":"Chi:9","to":"Chi:2","kind":"peer"},
    {"type":"delete_node","address":"Par:1"}
]}"#;

/// The WAL's non-empty lines. One line is one frame, so this counts
/// durable records.
fn wal_lines(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("facetql.wal"))
        .expect("read wal")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect()
}

/// Remove the WAL's last line, which is the COMMIT of the last
/// transaction written. Returns the line count before and after so the
/// caller can assert something was actually removed.
fn drop_last_wal_line(dir: &std::path::Path) -> (usize, usize) {
    let path = dir.join("facetql.wal");

    let text = std::fs::read_to_string(&path).expect("read wal");

    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let before = lines.len();

    let kept = &lines[..before - 1];

    std::fs::write(
        &path,
        kept.iter()
            .map(|l| format!("{l}\n"))
            .collect::<String>(),
    )
    .expect("rewrite wal");

    (before, kept.len())
}

#[test]
fn a_frame_whose_commit_never_landed_leaves_every_structure_as_it_was() {
    let dir = scratch("nocommit");
    let port = free_port();

    let mut server = Server::start_with(&dir, port, &no_checkpoints());
    seed(&server);

    // The state the transaction must be rolled back to, read through
    // every access path it is about to touch.
    assert_eq!(addresses(&server, "Par"), vec!["Par:1"]);
    assert_eq!(addresses(&server, "Chi"), vec!["Chi:1", "Chi:2", "Chi:3"]);
    assert_eq!(by_index(&server, "Chi", "score"), vec!["Chi:3", "Chi:2", "Chi:1"]);
    assert_eq!(history_len(&server, "Chi:1"), 0);
    assert_eq!(history_len(&server, "Keep:1"), 0);
    assert_eq!(data_of(&server, "Keep:1").as_deref(), Some(r#"{"v":1}"#));
    assert_eq!(out_edges(&server, "Par:1"), 1);

    let r = server.post("/transaction", WIDE_BATCH).expect("transaction");
    assert!(
        (200..300).contains(&r.status),
        "the batch must commit before the marker can be removed: {} {}",
        r.status,
        r.body,
    );

    // It really did all of it, or removing the COMMIT proves nothing.
    assert!(addresses(&server, "Par").is_empty());
    assert!(addresses(&server, "Chi").is_empty());

    server.kill();

    let (before, after) = drop_last_wal_line(&dir);
    assert_eq!(after + 1, before, "exactly one WAL record was removed");

    let server = Server::start_with(&dir, port, &no_checkpoints());

    // Nodes: both the deletes and the inserts are gone.
    assert_eq!(
        addresses(&server, "Par"),
        vec!["Par:1"],
        "the cascade's root came back",
    );
    assert_eq!(
        addresses(&server, "Chi"),
        vec!["Chi:1", "Chi:2", "Chi:3"],
        "every cascaded child came back, and Chi:9 was never created",
    );

    // The declared index: same members, same order, and no entry for a
    // node the frame never committed. Chi:1 would sort first, not last,
    // if the overwrite to score 50 had survived in the index alone.
    assert_eq!(
        by_index(&server, "Chi", "score"),
        vec!["Chi:3", "Chi:2", "Chi:1"],
        "the secondary index rolled back with the transaction",
    );

    // The overwrite that was not deleted: back at its original value,
    // and with no archive of it. An `Archive` and the `Insert` that
    // supersedes it are two separate WAL records inside one frame, so
    // this is the pair that a non-atomic apply leaves inconsistent —
    // history claiming a value was superseded by a value that is not
    // there.
    assert_eq!(
        data_of(&server, "Keep:1").as_deref(),
        Some(r#"{"v":1}"#),
        "the overwrite rolled back",
    );

    // History: the archives the overwrite and the cascade staged must
    // not exist either.
    for address in ["Chi:1", "Chi:2", "Chi:3", "Par:1", "Keep:1"] {
        assert_eq!(
            history_len(&server, address),
            0,
            "{address} has no archive from the discarded frame",
        );
    }

    // Edges: the seeded one survives, the staged one does not.
    assert_eq!(out_edges(&server, "Par:1"), 1, "the seeded edge is intact");
    assert!(
        !server.exists("Chi:9"),
        "the node the staged edge pointed from was never created",
    );

    // The definitions themselves are not transaction state — they were
    // committed before the batch and must still be there, or the
    // assertions above would pass for the wrong reason.
    assert!(
        server.get("/admin/references").body.contains("chi_to_par"),
        "the reference declaration survived",
    );
}

#[test]
fn the_same_frame_with_its_commit_replays_all_of_it() {
    // The control. Identical batch, WAL left alone: recovery must apply
    // the whole frame, including the archives and the index re-keying.
    let dir = scratch("commit");
    let port = free_port();

    let mut server = Server::start_with(&dir, port, &no_checkpoints());
    seed(&server);

    let r = server.post("/transaction", WIDE_BATCH).expect("transaction");
    assert!((200..300).contains(&r.status), "batch committed: {}", r.body);

    server.kill();

    let server = Server::start_with(&dir, port, &no_checkpoints());

    assert!(
        addresses(&server, "Par").is_empty(),
        "the deleted parent stayed deleted across replay",
    );
    assert!(
        addresses(&server, "Chi").is_empty(),
        "every cascaded child stayed deleted across replay",
    );
    assert_eq!(
        data_of(&server, "Keep:1").as_deref(),
        Some(r#"{"v":2}"#),
        "the overwrite replayed",
    );
    assert_eq!(
        history_len(&server, "Keep:1"),
        1,
        "the archive staged beside that overwrite replayed with it",
    );
}


#[test]
fn a_committed_frame_missing_its_begin_refuses_to_start() {
    // The other half of the frame, and the opposite kind of failure.
    //
    // Removing the COMMIT asks recovery to discard a batch; removing the
    // BEGIN asks it to explain one. It cannot: a correct writer emits
    // BEGIN first and with the frame's lowest sequence, so a log holding
    // the COMMIT but not the BEGIN was cut through the middle of a live
    // frame. Replaying what is left would apply part of a batch, and
    // discarding it would silently drop one that was acknowledged to a
    // client. Recovery does neither and refuses to start, because the
    // only remedy is an operator's.
    //
    // Nothing in the engine can produce this: the checkpoint fence in
    // `storage::checkpoint` is what keeps a checkpoint or a WAL rotation
    // from ever cutting there. The test exists because the branch that
    // notices is worthless if nobody has run it — recovery used to
    // return "not committed" here, so the same damage started cleanly
    // and served a database with a committed transaction quietly missing
    // from it.
    let dir = scratch("nobegin");
    let port = free_port();

    let mut server = Server::start_with(&dir, port, &no_checkpoints());
    seed(&server);

    // The frame starts at the first line written after this point, so
    // its BEGIN is identified by counting rather than by guessing how
    // many mutations the batch lowers to.
    let before = wal_lines(&dir).len();

    let r = server.post("/transaction", WIDE_BATCH).expect("transaction");
    assert!((200..300).contains(&r.status), "batch committed: {}", r.body);

    let lines = wal_lines(&dir);
    assert!(
        lines.len() > before + 2,
        "the frame is BEGIN, mutations and COMMIT: {} new lines",
        lines.len() - before,
    );

    server.kill();

    let kept: String = lines
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != before)
        .map(|(_, l)| format!("{l}\n"))
        .collect();

    std::fs::write(dir.join("facetql.wal"), kept).expect("rewrite wal");

    let mut child = Command::new(env!("CARGO_BIN_EXE_facetql"))
        .arg("start")
        .env("FACETQL_ENV", "test")
        .env("ENOCHIAN_DATA_DIR", &dir)
        .env("ENOCHIAN_PORT", free_port().to_string())
        .env("FACETQL_CHECKPOINT_INTERVAL", "1000000")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn server");

    // A server that came up would never exit on its own, so waiting for
    // it unconditionally would hang instead of failing. Give startup a
    // generous window, then treat "still running" as the failure it is.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

    let status = loop {
        match child.try_wait().expect("wait") {
            Some(status) => break Some(status),
            None if std::time::Instant::now() >= deadline => break None,
            None => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    };

    let Some(status) = status else {
        let _ = child.kill();
        let _ = child.wait();

        panic!(
            "the server started on a log whose committed frame has no BEGIN; \
             that batch is silently missing from the database it is now serving",
        );
    };

    let mut said = String::new();

    if let Some(mut stderr) = child.stderr.take() {
        use std::io::Read;
        let _ = stderr.read_to_string(&mut said);
    }

    assert!(
        !status.success(),
        "startup must fail on a frame with no BEGIN; it said: {said}",
    );

    assert!(
        said.contains("no BEGIN record"),
        "the refusal names what is wrong with the log; it said: {said}",
    );
}
