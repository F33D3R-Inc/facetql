//! Telemetry bypasses the in-flight cap: with every admission slot held,
//! `GET /stats` and the `GET /` liveness probe still answer, while ordinary
//! work is refused with the concurrency 503. A supervisor must be able to
//! see a saturated instance — that is when it most needs to.

mod common;

use common::{free_port, request, scratch, Server};
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn stats_and_the_liveness_probe_answer_while_every_slot_is_held() {
    let dir = scratch("telemetry-admission");
    let port = free_port();

    let server = Server::start_with(
        &dir,
        port,
        &[
            ("FACETQL_MAX_CONCURRENT_REQUESTS", "1".to_string()),
            ("FACETQL_REQUEST_TIMEOUT_SECS", "30".to_string()),
        ],
    );

    // Hold the one slot: a write whose body never finishes arriving.
    let mut stalled = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stalled
        .write_all(
            format!(
                "POST /node HTTP/1.1\r\nHost: x\r\nx-api-key: {}\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{{\"address\":",
                common::TOKEN
            )
            .as_bytes(),
        )
        .expect("write");
    std::thread::sleep(Duration::from_millis(300));

    let work = server.get("/nodes");
    assert_eq!(work.status, 503, "ordinary work is refused while the slot is held: {}", work.body);

    let stats = server.get("/stats");
    assert_eq!(stats.status, 200, "/stats answers while saturated: {}", stats.body);
    assert!(stats.body.contains("\"in_flight\":1"), "the held request is the one in flight: {}", stats.body);

    let probe = request(port, "GET", "/", None).expect("probe");
    assert_eq!(probe.status, 200, "the liveness probe answers while saturated");

    drop(stalled);
}
