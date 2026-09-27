//! `facetql start --port 0`: the kernel picks the port, and the banner
//! names the port that was bound rather than the zero that was asked for.
//!
//! This is how a supervisor starts a server without a port race: asking
//! for a free port first and passing it along leaves a window in which
//! another process can take it. The banner is the contract a harness
//! reads the port from, so it is pinned here, over the real binary.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn port_zero_binds_an_ephemeral_port_and_the_banner_names_it() {
    let dir = std::env::temp_dir().join(format!("facetql-port0-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");

    let mut child = Command::new(env!("CARGO_BIN_EXE_facetql"))
        .arg("start")
        .env("FACETQL_ENV", "test")
        .env("FACETQL_DATA_DIR", &dir)
        .env("FACETQL_PORT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server");

    let stdout = child.stdout.take().expect("stdout");
    let mut lines = BufReader::new(stdout).lines();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut port: Option<u16> = None;

    while Instant::now() < deadline {
        match lines.next() {
            Some(Ok(line)) => {
                if let Some(rest) = line.strip_prefix("FacetQL Server Running on port ") {
                    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                    port = digits.parse().ok();
                    break;
                }
            }
            _ => break,
        }
    }

    let port = match port {
        Some(p) => p,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the banner never named a port");
        }
    };

    assert_ne!(port, 0, "the banner names the bound port, not the requested zero");

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to the banner's port");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut body = String::new();
    let _ = stream.read_to_string(&mut body);

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(body.contains("FacetQL Online"), "the server answers on the port the banner named: {body}");
}
