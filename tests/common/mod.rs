//! A real `facetql` server, driven over HTTP.
//!
//! Shared by every integration test that needs the actual binary rather
//! than the engine in-process: `crash_recovery` kills it to check what
//! survived, `reactor_liveness` saturates it to check the runtime stays
//! responsive. Both need the same three things — a free port, a server
//! that is actually up, and a request function — so they live here
//! instead of being written twice and drifting.
//!
//! No HTTP client crate: these are a few plain requests, and a
//! hand-rolled one keeps the tests free of dev-dependencies.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const TOKEN: &str = "dev-local-key-change-me";

/// A free port, found by binding and immediately releasing one.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("addr").port();

    drop(listener);
    port
}

pub fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "facetql-crash-{}-{}",
        std::process::id(),
        name,
    ));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    dir
}

/// A scratch directory on **real storage**, under `target/`.
///
/// [`scratch`] uses the system temp directory, which on most Linux
/// machines is tmpfs — a RAM disk where `fsync` costs microseconds. That
/// is fine, and fast, for tests about what the engine *computes*. It is
/// useless for tests about what the engine *waits for*: with a free
/// fsync, a write never blocks long enough to demonstrate anything about
/// blocking.
pub fn scratch_on_disk(name: &str) -> PathBuf {
    let dir = PathBuf::from("target").join(format!("it-{}-{}", name, std::process::id()));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    dir.canonicalize().expect("resolve scratch dir")
}

/// Microseconds for one durable write in `dir`.
///
/// Tests that depend on a slow fsync check this first and say so when
/// the filesystem does not provide one, rather than passing vacuously.
pub fn fsync_cost_micros(dir: &std::path::Path) -> f64 {
    let path = dir.join(".fsync_probe");
    let mut file = std::fs::File::create(&path).expect("probe file");

    file.write_all(b"warm").expect("write");
    file.sync_data().expect("sync");

    const N: u32 = 20;
    let start = std::time::Instant::now();

    for _ in 0..N {
        file.write_all(b"probe----------------").expect("write");
        file.sync_data().expect("sync");
    }

    let per = start.elapsed().as_secs_f64() * 1_000_000.0 / f64::from(N);
    let _ = std::fs::remove_file(&path);

    per
}

pub struct Response {
    pub status: u16,
    pub body: String,
}

/// One HTTP/1.1 request, connection-per-request.
pub fn request(port: u16, method: &str, path: &str, body: Option<&str>) -> std::io::Result<Response> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;

    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    let payload = body.unwrap_or("");

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         x-api-key: {TOKEN}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        payload.len(),
    );

    stream.write_all(head.as_bytes())?;
    stream.write_all(payload.as_bytes())?;
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let text = String::from_utf8_lossy(&raw).into_owned();

    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();

    Ok(Response { status, body })
}

pub struct Server {
    pub child: Child,
    pub port: u16,
    pub dir: PathBuf,
}

/// Spawn the server process. Separate from [`Server`] so a restart can
/// replace the child in place — `Server` implements `Drop`, so it cannot
/// be destructured.
pub fn spawn_child(dir: &PathBuf, port: u16) -> Child {
    spawn_child_with(dir, port, &[])
}

/// [`spawn_child`] plus extra environment for the child.
///
/// One test needs `FACETQL_CHECKPOINT_INTERVAL` raised so far that the
/// engine never checkpoints: it edits the WAL by hand between runs, and
/// a checkpoint would both rewrite the file underneath it and make the
/// records it is editing already-durable in the heap — which is to say,
/// it would test nothing.
pub fn spawn_child_with(dir: &PathBuf, port: u16, env: &[(&str, String)]) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_facetql"));

    for (key, value) in env {
        command.env(key, value);
    }

    command
        .arg("start")
        .env("FACETQL_ENV", "test")
        .env("ENOCHIAN_DATA_DIR", dir)
        .env("ENOCHIAN_PORT", port.to_string())
        // Rate limiting is a per-identity control against a hostile
        // caller; this harness is one identity issuing thousands of
        // requests as fast as it can, which is exactly the shape the
        // limiter exists to refuse. Left on, the verification pass gets
        // 429s — and the first version of this file counted a 429 as
        // "the record is gone", which reported a half-applied
        // transaction that had never happened. The limits are off here
        // so that a non-200 means something about the data.
        .env("FACETQL_RATE_READ", "off")
        .env("FACETQL_RATE_WRITE", "off")
        .env("FACETQL_RATE_BULK", "off")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn facetql")
}

impl Server {
    pub fn start(dir: &PathBuf, port: u16) -> Server {
        Server::start_with(dir, port, &[])
    }

    /// [`Server::start`] with extra environment. See [`spawn_child_with`].
    pub fn start_with(dir: &PathBuf, port: u16, env: &[(&str, String)]) -> Server {
        let server = Server {
            child: spawn_child_with(dir, port, env),
            port,
            dir: dir.clone(),
        };

        server.wait_ready();
        server
    }

    /// Poll `GET /` until it answers. A server that never comes up is a
    /// recovery failure, and this is where it shows.
    pub fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);

        while Instant::now() < deadline {
            if let Ok(r) = request(self.port, "GET", "/", None)
                && r.status == 200
            {
                return;
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        panic!(
            "server on port {} never became ready — recovery refused to start",
            self.port,
        );
    }

    pub fn get(&self, path: &str) -> Response {
        request(self.port, "GET", path, None).expect("GET")
    }

    /// Is this record there?
    ///
    /// Strict on purpose: 200 is present, 404 is absent, and **anything
    /// else panics**. A durability test whose "absent" branch also
    /// catches rate limits, timeouts and 500s is not measuring
    /// durability — it reports data loss whenever the server declines to
    /// answer. This method exists because that is precisely the bug the
    /// first version of this file had.
    pub fn exists(&self, address: &str) -> bool {
        let r = self.get(&format!("/node/{address}"));

        match r.status {
            200 => true,
            404 => false,
            other => panic!(
                "GET /node/{address} answered {other}, which says nothing about \
                 whether the record survived: {}",
                r.body.chars().take(200).collect::<String>(),
            ),
        }
    }

    pub fn post(&self, path: &str, body: &str) -> std::io::Result<Response> {
        request(self.port, "POST", path, Some(body))
    }

    /// SIGKILL. Not a shutdown — the point is that nothing gets to run a
    /// destructor, flush a buffer or write a clean marker.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();

        // The next process needs the advisory flock released, which the
        // kernel does on exit; give it a moment to land.
        std::thread::sleep(Duration::from_millis(200));
    }

    /// An ordinary stop and start, as distinct from [`Server::kill`].
    pub fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();

        std::thread::sleep(Duration::from_millis(200));

        self.child = spawn_child(&self.dir, self.port);
        self.wait_ready();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn node_body(address: &str, kind: &str, data: &str) -> String {
    format!(
        r#"{{"address":"{address}","kind":"{kind}","x":0,"y":0,"z":0,"q":0,"data":"{data}","public":true}}"#
    )
}

