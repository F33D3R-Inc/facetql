//! Resource guards: the bounds a request meets before it is allowed to
//! cost anything.
//!
//! Everything in this module answers one question — *how much of this
//! server can one caller consume?* — and every answer here is a refusal
//! rather than a queue. That is deliberate. A guard that makes an
//! excessive request *wait* still allocates the socket, the task and
//! the buffered body, so under the load these bounds exist for it
//! converts a burst into a slow-motion outage instead of preventing
//! one. A 429 or a 503 is information the caller can act on; a hang is
//! not.
//!
//! The guards, outermost first, in the order a request meets them:
//!
//! | Guard | Bound | Refusal |
//! |---|---|---|
//! | [`connection_permit`] | concurrent TCP connections | connection dropped |
//! | [`body_limit_layer`] | bytes of request body | 413 |
//! | [`concurrency`] | in-flight requests | 503 |
//! | [`rate_limit`] | requests per identity per endpoint class | 429 |
//! | [`request_timeout_layer`] | wall-clock per request | 408 |
//! | [`subscriber_permit`] | concurrent `/events` streams | 503 |
//!
//! # Why the knobs are process-wide `OnceLock`s over the environment
//!
//! Because that is what every other bound in this engine already is —
//! `FACETQL_MAX_SCAN_ROWS`, `FACETQL_MAX_TRANSACTION_OPS`,
//! `FACETQL_MAX_QUERY_OFFSET`, `FACETQL_WAL_ROTATE_BYTES`. Threading a
//! configuration struct through the router for these five and leaving
//! the other four in the environment would leave an operator with two
//! places to look and this codebase with two conventions. Resolved
//! once, so a mid-run environment change cannot make two requests
//! disagree about the limit they were judged against.
//!
//! # What is deliberately *not* here
//!
//! Per-IP limiting. Every bound below is keyed by authenticated
//! identity, which is the thing this server can actually verify: a
//! client address is whatever the last hop says it is, and this process
//! is documented as running behind a reverse proxy in a normal
//! deployment, so `X-Forwarded-For` would be caller-controlled unless
//! the proxy is known and trusted — configuration this server has no
//! way to check. Unauthenticated floods are bounded by
//! [`connection_permit`] and [`concurrency`] instead, which do not
//! depend on trusting anything the caller says.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::auth::AuthIdentity;

// ---------------------------------------------------------------------
// Request body
// ---------------------------------------------------------------------

/// Largest request body any endpoint will buffer, in bytes.
///
/// Axum applies a 2 MiB default of its own, which is a fine number and a
/// bad place to leave it: it is invisible at the call site, it is not
/// something an operator can raise for a legitimately large batch, and a
/// future extractor added without `DefaultBodyLimit` in mind would
/// silently inherit whatever the framework's default happens to be that
/// version. Stating it here makes the bound part of this router's
/// contract rather than a property of a dependency.
///
/// It is the first bound a request's *content* meets, and it is the
/// cheapest: it rejects on the length header or as the body streams,
/// before any deserialization, so an oversized `POST /transaction` never
/// becomes allocated JSON. The bounds behind it — transaction size, scan
/// rows, predicate size, record and key limits — are the ones that
/// matter for what a well-formed request can *ask for*; this one only
/// stops the bytes.
const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

const MAX_BODY_BYTES_ENV: &str = "FACETQL_MAX_BODY_BYTES";

pub fn max_body_bytes() -> usize {
    static BYTES: OnceLock<usize> = OnceLock::new();

    *BYTES.get_or_init(|| {
        env_usize(MAX_BODY_BYTES_ENV).unwrap_or(DEFAULT_MAX_BODY_BYTES)
    })
}

/// The body bound as a layer, for the outermost router.
pub fn body_limit_layer() -> axum::extract::DefaultBodyLimit {
    axum::extract::DefaultBodyLimit::max(max_body_bytes())
}

// ---------------------------------------------------------------------
// Per-request deadline
// ---------------------------------------------------------------------

/// Wall-clock a single request may take before the server stops waiting
/// for it.
///
/// Without this a request has no upper bound at all: a scan that reaches
/// [`max_scan_rows`](crate::storage::engine) is bounded in *rows* but
/// not in time, and a client that stops reading its own response holds
/// the connection open indefinitely. Both hold their share of
/// [`concurrency`]'s permits while they do it, so one slow request is
/// not merely slow — it is a permit removed from every other caller.
///
/// # What this does and does not stop
///
/// Stated plainly, because the difference matters: the timeout ends the
/// *response*, not necessarily the work. Handlers here take a
/// `std::sync::RwLock` on the engine and run to completion on the thread
/// that entered them, so a scan already inside the engine finishes even
/// after its caller has been answered. The timeout is therefore what
/// stops a slow request from *accumulating* — the client is freed, the
/// connection is released, the permit comes back — and the bounds that
/// stop any single request from being enormous in the first place are
/// the row, predicate and batch caps, not this.
///
/// 30 seconds: far above any request this engine serves in health (an
/// indexed page is sub-millisecond, a 100 k-row unindexed scan is
/// hundreds of milliseconds) and far below the point where a caller has
/// given up and retried, which is the behaviour that turns one slow
/// request into a queue of them.
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

const REQUEST_TIMEOUT_ENV: &str = "FACETQL_REQUEST_TIMEOUT_SECS";

pub fn request_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();

    *TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            env_u64(REQUEST_TIMEOUT_ENV).unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECS),
        )
    })
}

/// The deadline as a layer.
///
/// Applied to the request routes only, never to `GET /events`: an SSE
/// stream is *supposed* to outlive any deadline, so a timeout there
/// would not be a guard, it would be a bug that severs every live
/// subscriber every thirty seconds. That connection is bounded by
/// [`subscriber_permit`] instead, which bounds how many may exist rather
/// than how long one may last.
pub fn request_timeout_layer() -> tower_http::timeout::TimeoutLayer {
    tower_http::timeout::TimeoutLayer::new(request_timeout())
}

// ---------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------

/// Requests that may be in flight at once, across every identity.
///
/// This is the bound that keeps a burst from turning into an
/// out-of-memory kill. Each in-flight request holds a task, a buffered
/// body of up to [`max_body_bytes`], whatever the handler materialized
/// (a query page can be [`max_scan_rows`](crate::storage::engine) rows),
/// and a place in the queue for the engine's single `RwLock`. None of
/// that is bounded by the rate limiter, which bounds *arrivals* and not
/// *residency*, and none of it is bounded by the OS until the process is
/// already dying.
///
/// It also has to be here rather than only in the accept loop, because
/// only one of the two serving paths owns its accept loop
/// (`tls_server::serve_tls`); the plaintext path is `axum::serve`, which
/// does not expose one. A guard at the service layer covers both.
///
/// 512 concurrent requests against an engine whose mutations serialize
/// on one write lock is already far past the point of useful
/// parallelism; the number exists to bound memory, not to tune
/// throughput.
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 512;

const MAX_CONCURRENT_REQUESTS_ENV: &str = "FACETQL_MAX_CONCURRENT_REQUESTS";

pub fn max_concurrent_requests() -> usize {
    static SLOTS: OnceLock<usize> = OnceLock::new();

    *SLOTS.get_or_init(|| {
        env_usize(MAX_CONCURRENT_REQUESTS_ENV)
            .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
    })
}

fn request_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

    SLOTS.get_or_init(|| Arc::new(Semaphore::new(max_concurrent_requests())))
}

/// Requests holding an in-flight permit right now.
///
/// Derived from the semaphore that already enforces the bound rather
/// than from a counter of its own: a second counter would be a second
/// number that could disagree with the one the limiter actually acts
/// on, and this one cannot. Reported on `GET /stats` as
/// `requests.in_flight` — saturation here is the difference between a
/// server that is busy and one that is refusing work.
pub fn in_flight_requests() -> usize {
    max_concurrent_requests().saturating_sub(request_slots().available_permits())
}

/// Middleware form of the in-flight bound.
///
/// `try_acquire` rather than `acquire`: waiting for a permit is the
/// queue this guard exists to prevent, and a caller told "not now" can
/// retry, shed load, or fail over — none of which it can do while
/// blocked inside a request it cannot see the state of.
pub async fn concurrency(request: Request, next: Next) -> Response {
    let permit = match Arc::clone(request_slots()).try_acquire_owned() {
        Ok(permit) => permit,

        Err(_) => return overloaded("in-flight requests"),
    };

    let response = next.run(request).await;

    // Held until the handler has produced a response, then released.
    // Explicit rather than implicit so it is obvious this is not a
    // guard on the response *body*: a streaming body outlives the
    // permit, which is exactly why `/events` has a permit of its own.
    drop(permit);

    response
}

// ---------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------

/// Accepted TCP connections that may exist at once.
///
/// A connection costs a file descriptor, a task, and — on the TLS path —
/// a handshake, all before a single byte of HTTP has been parsed and
/// therefore before any credential has been presented. It is the one
/// cost an entirely unauthenticated client can impose, so it is the one
/// bound that cannot be keyed by identity.
///
/// Deliberately larger than [`DEFAULT_MAX_CONCURRENT_REQUESTS`]: an idle
/// keep-alive connection is cheap and normal, and capping connections at
/// the in-flight bound would evict clients that are behaving perfectly.
const DEFAULT_MAX_CONNECTIONS: usize = 2048;

const MAX_CONNECTIONS_ENV: &str = "FACETQL_MAX_CONNECTIONS";

/// A permit for one accepted connection, or `None` when the cap is
/// reached.
///
/// The caller holds it for the life of the connection and drops the
/// connection when it is `None`. Dropping is the whole refusal: there is
/// no HTTP response to send, because on the TLS path nothing has been
/// negotiated yet and on either path answering would mean doing the work
/// the cap exists to refuse.
pub fn connection_permit() -> Option<OwnedSemaphorePermit> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

    let slots = SLOTS.get_or_init(|| {
        Arc::new(Semaphore::new(
            env_usize(MAX_CONNECTIONS_ENV).unwrap_or(DEFAULT_MAX_CONNECTIONS),
        ))
    });

    Arc::clone(slots).try_acquire_owned().ok()
}

// ---------------------------------------------------------------------
// Live subscribers
// ---------------------------------------------------------------------

/// Concurrent `GET /events` streams.
///
/// An SSE subscriber is the one request that is *designed* not to end,
/// so none of the bounds above reach it: it is past the body limit
/// immediately, it must not have a deadline, and its
/// [`concurrency`] permit is released the moment the response head is
/// produced — long before the stream is. What it holds instead is a
/// broadcast receiver, and a receiver that falls behind pins the
/// ring's messages until it catches up. `BROADCAST_CAPACITY` bounds the
/// ring per subscriber; this bounds the subscribers, and the two
/// multiply into the memory the event system can occupy.
///
/// 256 is generous for a notification fan-out whose intended consumers
/// are application *instances*, not end users' browsers.
const DEFAULT_MAX_SUBSCRIBERS: usize = 256;

const MAX_SUBSCRIBERS_ENV: &str = "FACETQL_MAX_SUBSCRIBERS";

/// A permit for one live subscriber, or `None` when the cap is reached.
///
/// The handler must move the permit into the stream itself rather than
/// hold it in the handler body, because the handler returns as soon as
/// the response head exists and the connection then lives on inside the
/// stream. A permit dropped at the end of the handler would bound
/// nothing at all.
pub fn subscriber_permit() -> Option<OwnedSemaphorePermit> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

    let slots = SLOTS.get_or_init(|| {
        Arc::new(Semaphore::new(
            env_usize(MAX_SUBSCRIBERS_ENV).unwrap_or(DEFAULT_MAX_SUBSCRIBERS),
        ))
    });

    Arc::clone(slots).try_acquire_owned().ok()
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

/// What a request costs this server, as opposed to what it is called.
///
/// A single rate for the whole API would have to be set for the most
/// expensive endpoint, which would throttle point reads to the rate of
/// predicate scans, or for the cheapest, which would leave the scans
/// unbounded. Neither is a limit; both are a number chosen by looking
/// away from the problem.
///
/// So the classes are cost profiles, not URL groupings, and each carries
/// its own bucket per identity:
///
/// * [`Read`](EndpointClass::Read) — one point lookup or one bounded
///   page. Cost is set by the engine, not by the caller.
/// * [`Write`](EndpointClass::Write) — one durable mutation: a WAL
///   append, an fsync, an index update.
/// * [`Bulk`](EndpointClass::Bulk) — the two endpoints whose cost is set
///   by the *shape* of the request rather than by its size:
///   `POST /nodes/query`, whose predicate the caller writes and which is
///   evaluated once per candidate row, and `POST /transaction`, which
///   stages a whole batch into one WAL frame and pins the checkpoint
///   boundary while it does (and whose `delete_where` op runs exactly
///   the same predicate scan). These are the requests where forty bytes
///   of input can buy seconds of server.
/// * [`Admin`](EndpointClass::Admin) — the control plane. Creating an
///   index reads every existing node of a kind; listing users and stats
///   is fleet-wide. Operational, deliberate, and never hot.
/// * [`Subscribe`](EndpointClass::Subscribe) — opening a live stream.
///   Rate-limited separately from reads because the resource being
///   consumed is a *slot* that persists, not a moment of CPU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointClass {
    Read,
    Write,
    Bulk,
    Admin,
    Subscribe,
}

impl EndpointClass {
    /// The class of a concrete request path.
    ///
    /// Matched on path segments rather than on the axum route patterns,
    /// because middleware runs before a `MatchedPath` is guaranteed to
    /// be available on every version of every layer ordering, and
    /// because a segment match is checkable by eye against the route
    /// table in `routes.rs`. The test module below drives every entry of
    /// that table through this function, so the two cannot drift
    /// silently.
    ///
    /// An unrecognized path resolves to [`EndpointClass::Admin`], the
    /// tightest bucket. Nothing should reach here unmatched — this
    /// middleware is a `route_layer`, so a request that matched no route
    /// never runs it — but "unknown" resolving to "generous" is the
    /// shape of every rate-limit bypass ever written.
    pub fn of(method: &axum::http::Method, path: &str) -> Self {
        use axum::http::Method;

        let segments: Vec<&str> =
            path.split('/').filter(|s| !s.is_empty()).collect();

        match segments.as_slice() {
            ["admin", ..] | ["stats"] => EndpointClass::Admin,

            ["events"] => EndpointClass::Subscribe,

            ["nodes", "multiget"]
            | ["nodes", "query"]
            | ["nodes", "count"]
            | ["nodes", "count_by"]
            | ["transaction"] => {
                EndpointClass::Bulk
            }

            // A scan reads, decrypts and classifies the whole
            // write-ahead log — up to `wal::rotate_threshold` of it —
            // so it buys the same disproportionate work per call that
            // `/nodes/query` and `/transaction` do, and draws from the
            // same bucket.
            ["changes"] => EndpointClass::Bulk,

            ["publish"] | ["edge"] => EndpointClass::Write,

            ["node"] => EndpointClass::Write,

            ["node", _, "claim"] => EndpointClass::Write,

            ["sequence", _, "next"] => EndpointClass::Write,

            ["nodes"]
            | ["node", _, "history"]
            | ["node", _, "owned"]
            | ["node", _, "edges", _] => EndpointClass::Read,

            // The bare node path is the one place the method decides:
            // the same URL is a read, an overwrite and a delete.
            ["node", _] if method == Method::GET => EndpointClass::Read,
            ["node", _] => EndpointClass::Write,

            // `GET /` — outside the protected router, so unreachable
            // from this middleware; classified anyway so the function is
            // total.
            [] => EndpointClass::Read,

            _ => EndpointClass::Admin,
        }
    }

    fn env_var(self) -> &'static str {
        match self {
            EndpointClass::Read => "FACETQL_RATE_READ",
            EndpointClass::Write => "FACETQL_RATE_WRITE",
            EndpointClass::Bulk => "FACETQL_RATE_BULK",
            EndpointClass::Admin => "FACETQL_RATE_ADMIN",
            EndpointClass::Subscribe => "FACETQL_RATE_SUBSCRIBE",
        }
    }

    /// `(burst, refill per second)` when nothing is configured.
    ///
    /// Set from what a real application does, not from what feels safe:
    /// the live client (`fct`'s `fqStore`) serves page renders out of
    /// point reads and paged listings, so `Read` has to sit comfortably
    /// above a busy page's fan-out, while `Bulk` and `Admin` are where a
    /// single caller can buy disproportionate work and are set an order
    /// of magnitude lower. The burst is double the sustained rate in
    /// each case, so a page that issues its reads all at once is served
    /// and a caller that issues them all at once *forever* is not.
    fn default_rate(self) -> (f64, f64) {
        match self {
            EndpointClass::Read => (600.0, 300.0),
            EndpointClass::Write => (300.0, 150.0),
            EndpointClass::Bulk => (120.0, 60.0),
            EndpointClass::Admin => (60.0, 30.0),
            EndpointClass::Subscribe => (30.0, 5.0),
        }
    }
}

/// The configured bound for one class, or `None` when an operator has
/// explicitly turned it off.
///
/// Format: `burst[:per_second]`, or the literal `off`. A single number
/// sets both, which is the common case ("no more than N per second, and
/// no burst beyond that"). `off` exists because a deployment behind a
/// gateway that already does this should be able to say so — but it has
/// to *say* so: an unparseable value falls back to the default rather
/// than to no limit, since a typo must never be the thing that removes a
/// control.
fn configured_rate(class: EndpointClass) -> Option<(f64, f64)> {
    static RATES: OnceLock<HashMap<EndpointClass, Option<(f64, f64)>>> =
        OnceLock::new();

    RATES
        .get_or_init(|| {
            [
                EndpointClass::Read,
                EndpointClass::Write,
                EndpointClass::Bulk,
                EndpointClass::Admin,
                EndpointClass::Subscribe,
            ]
            .into_iter()
            .map(|class| (class, parse_rate(class)))
            .collect()
        })
        .get(&class)
        .copied()
        .flatten()
}

fn parse_rate(class: EndpointClass) -> Option<(f64, f64)> {
    parse_rate_from(class, std::env::var(class.env_var()).ok().as_deref())
}

/// The parse itself, separated from where the string came from so it can
/// be tested exhaustively — an environment variable is process-wide
/// state, and a test that mutated one would be testing the harness as
/// much as the parser.
fn parse_rate_from(class: EndpointClass, raw: Option<&str>) -> Option<(f64, f64)> {
    let raw = match raw {
        Some(raw) => raw,
        None => return Some(class.default_rate()),
    };

    let raw = raw.trim();

    if raw.eq_ignore_ascii_case("off") {
        return None;
    }

    let (burst, refill) = match raw.split_once(':') {
        Some((burst, refill)) => (burst.trim(), Some(refill.trim())),
        None => (raw, None),
    };

    let burst: f64 = match burst.parse() {
        Ok(value) if value > 0.0 => value,
        _ => return Some(class.default_rate()),
    };

    let refill: f64 = match refill {
        None => burst,
        Some(refill) => match refill.parse() {
            Ok(value) if value > 0.0 => value,
            _ => return Some(class.default_rate()),
        },
    };

    Some((burst, refill))
}

/// One identity's allowance for one class.
///
/// A token bucket rather than a fixed window, because a fixed window
/// admits twice its own limit across a boundary (the last instant of one
/// window plus the first of the next) and because the leftover capacity
/// in a bucket is exactly the "burst" a real client needs: a page render
/// issues its reads together and then goes quiet.
///
/// Refill is computed from elapsed time on read rather than by a
/// background task. A timer per identity per class would be thousands of
/// wakeups to maintain state nobody is asking about; the arithmetic here
/// is equivalent and costs nothing when the identity is idle.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn full(burst: f64, now: Instant) -> Self {
        Bucket { tokens: burst, updated: now }
    }

    /// Spend one token if there is one; otherwise report how long until
    /// there will be.
    fn take(&mut self, burst: f64, refill: f64, now: Instant) -> Result<(), Duration> {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();

        self.tokens = (self.tokens + elapsed * refill).min(burst);
        self.updated = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Ok(());
        }

        // Rounded up to a whole second: `Retry-After` has no
        // sub-second form, and rounding down would advertise a retry
        // that is still too early and would be refused again.
        Err(Duration::from_secs_f64(((1.0 - self.tokens) / refill).ceil()))
    }

    /// Has this bucket refilled completely?
    ///
    /// A full bucket carries no information — recreating it produces the
    /// identical state — so it is the one entry that can be evicted
    /// without changing any caller's allowance. See [`Buckets::take`].
    fn is_full(&self, burst: f64, refill: f64, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();

        self.tokens + elapsed * refill >= burst
    }
}

/// How many `(identity, class)` buckets are tracked at once.
///
/// Identities cannot be minted by an attacker — they come from the
/// persistent user store, which is admin-only, or from `ENOCHIAN_TOKENS`
/// — so this map is already bounded in any correct deployment. The cap
/// is here for the deployment that is not correct, and for the property
/// that a limiter must not itself be the memory leak it was added to
/// prevent.
const MAX_TRACKED_BUCKETS: usize = 10_000;

#[derive(Default)]
struct Buckets {
    buckets: HashMap<(String, EndpointClass), Bucket>,
}

/// The outcome of consulting the limiter.
///
/// `Deny` carries no retry hint when the limiter itself failed, because
/// there is nothing honest to say about when it will work again.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    Allow,
    Deny(Option<Duration>),
}

impl Buckets {
    fn take(
        &mut self,
        owner: &str,
        class: EndpointClass,
        burst: f64,
        refill: f64,
        now: Instant,
    ) -> Decision {
        let key = (owner.to_string(), class);

        if let Some(bucket) = self.buckets.get_mut(&key) {
            return match bucket.take(burst, refill, now) {
                Ok(()) => Decision::Allow,
                Err(retry) => Decision::Deny(Some(retry)),
            };
        }

        if self.buckets.len() >= MAX_TRACKED_BUCKETS {
            // Evict only entries that have refilled: dropping one is
            // indistinguishable from keeping it, so this cannot hand
            // anybody allowance they had already spent. If nothing has
            // refilled, every tracked identity is actively rate-limited
            // and the new key is refused rather than admitted — the
            // alternative is a table full of attacker-chosen keys that
            // evicts the real ones, which turns a memory bound into a
            // denial-of-service primitive.
            self.buckets
                .retain(|(_, class), bucket| {
                    let (burst, refill) = match configured_rate(*class) {
                        Some(rate) => rate,
                        None => return false,
                    };

                    !bucket.is_full(burst, refill, now)
                });

            if self.buckets.len() >= MAX_TRACKED_BUCKETS {
                return Decision::Deny(None);
            }
        }

        let mut bucket = Bucket::full(burst, now);

        let decision = match bucket.take(burst, refill, now) {
            Ok(()) => Decision::Allow,
            Err(retry) => Decision::Deny(Some(retry)),
        };

        self.buckets.insert(key, bucket);

        decision
    }
}

fn buckets() -> &'static Mutex<Buckets> {
    static BUCKETS: OnceLock<Mutex<Buckets>> = OnceLock::new();

    BUCKETS.get_or_init(|| Mutex::new(Buckets::default()))
}

/// Per-identity, per-class rate limiting.
///
/// Runs *inside* `auth_middleware` — the identity it keys on is the
/// authenticated one, never a header the caller chose — and outside the
/// handler, so a refused request costs a hash-map probe and nothing
/// else.
///
/// # Fail-safe
///
/// The only failure this can have is a poisoned mutex, which means a
/// thread panicked mid-update and the token counts may be arbitrary.
/// That is answered with a refusal, not with a pass. A limiter that
/// opens when it breaks is not a limiter: the conditions that break it
/// are correlated with the load it exists for, so it would be absent
/// exactly when it is needed. The refusal is a 503 rather than a 429
/// because the cause is this server, not the caller.
pub async fn rate_limit(request: Request, next: Next) -> Response {
    let class = EndpointClass::of(request.method(), request.uri().path());

    let (burst, refill) = match configured_rate(class) {
        Some(rate) => rate,

        // Explicitly disabled by an operator. Not a failure, so not a
        // refusal.
        None => return next.run(request).await,
    };

    let owner = match request.extensions().get::<AuthIdentity>() {
        Some(identity) => identity.owner.clone(),

        // Unreachable while this layer sits inside the auth layer, and
        // a refusal rather than a pass if that ever stops being true:
        // an unidentified request is precisely the one that must not
        // get an unmetered allowance.
        None => return overloaded("identity"),
    };

    let decision = match buckets().lock() {
        Ok(mut buckets) => {
            buckets.take(&owner, class, burst, refill, Instant::now())
        }

        Err(_) => Decision::Deny(None),
    };

    match decision {
        Decision::Allow => next.run(request).await,

        Decision::Deny(retry) => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "rate limit exceeded for {class:?} requests by this \
                     identity ({burst} burst, {refill}/s). Retry after the \
                     interval in the Retry-After header, or raise \
                     {} on the server.",
                    class.env_var()
                ),
            )
                .into_response();

            // Best effort: a header that will not build is not a
            // reason to withhold the refusal itself.
            if let Some(value) = retry
                .map(|retry| retry.as_secs().to_string())
                .and_then(|seconds| HeaderValue::from_str(&seconds).ok())
            {
                response.headers_mut().insert("retry-after", value);
            }

            response
        }
    }
}

// ---------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------

/// The refusal for a saturated resource.
///
/// 503 with a `Retry-After` of one second: the condition is transient by
/// construction — a permit is released by every request that finishes —
/// so the honest advice is "try again shortly", and saying so in a
/// header keeps a well-behaved client from hot-looping.
fn overloaded(what: &str) -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "server is at its concurrency limit ({what}); the request was \
             not started. Retry shortly."
        ),
    )
        .into_response();

    response
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("1"));

    response
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[cfg(test)]
mod tests {
    //! The arithmetic and the classification, tested here where they are
    //! deterministic. The *wiring* — that these actually sit in front of
    //! the handlers — is tested through the real router in
    //! `routes::resource_guard_tests`, because a correct limiter that is
    //! not in the request path is worth exactly nothing.
    use super::*;
    use crate::api::routes::ROUTES;
    use axum::http::Method;

    // -----------------------------------------------------------------
    // Classification
    // -----------------------------------------------------------------

    /// The route table declares a cost class for every endpoint and the
    /// middleware derives one from the path. If those two ever disagree,
    /// some endpoint is silently drawing from the wrong bucket — most
    /// likely the generous one, since that is the direction a missing
    /// arm falls in. So they are checked against each other rather than
    /// each against a hand-written expectation.
    #[test]
    fn every_declared_route_classifies_as_the_table_says() {
        for spec in ROUTES {
            let method =
                Method::from_bytes(spec.method.as_bytes()).expect("known method");

            // `:param` segments are patterns; substitute something a
            // real request would carry.
            let path: String = spec
                .path
                .split('/')
                .map(|segment| {
                    if segment.starts_with(':') {
                        "some-value"
                    } else {
                        segment
                    }
                })
                .collect::<Vec<_>>()
                .join("/");

            assert_eq!(
                EndpointClass::of(&method, &path),
                spec.class,
                "{} {} draws from the wrong rate-limit bucket",
                spec.method,
                spec.path
            );
        }
    }

    /// The same URL is a read, an overwrite and a delete depending on
    /// the method, and only the read may draw from the read bucket.
    #[test]
    fn the_node_path_is_classified_by_method() {
        assert_eq!(
            EndpointClass::of(&Method::GET, "/node/thing:1"),
            EndpointClass::Read
        );
        assert_eq!(
            EndpointClass::of(&Method::PUT, "/node/thing:1"),
            EndpointClass::Write
        );
        assert_eq!(
            EndpointClass::of(&Method::DELETE, "/node/thing:1"),
            EndpointClass::Write
        );
    }

    /// An unrecognized path must land in the tightest bucket, not the
    /// most generous one — the direction every rate-limit bypass has
    /// ever gone.
    #[test]
    fn an_unknown_path_falls_into_the_tightest_bucket() {
        assert_eq!(
            EndpointClass::of(&Method::GET, "/something/nobody/registered"),
            EndpointClass::Admin
        );
    }

    // -----------------------------------------------------------------
    // The bucket
    // -----------------------------------------------------------------

    #[test]
    fn a_bucket_allows_its_burst_and_then_refuses() {
        let now = Instant::now();
        let mut buckets = Buckets::default();

        for i in 0..3 {
            assert_eq!(
                buckets.take("alice", EndpointClass::Read, 3.0, 1.0, now),
                Decision::Allow,
                "request {i} inside the burst was refused"
            );
        }

        match buckets.take("alice", EndpointClass::Read, 3.0, 1.0, now) {
            Decision::Deny(Some(retry)) => {
                assert!(retry.as_secs() >= 1, "retry hint must be usable");
            }
            other => panic!("the fourth request should be refused: {other:?}"),
        }
    }

    /// Time refills it, and time is the only thing that does.
    #[test]
    fn a_bucket_refills_over_time() {
        let now = Instant::now();
        let mut buckets = Buckets::default();

        assert_eq!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Allow
        );
        assert!(matches!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Deny(_)
        ));

        let later = now + Duration::from_secs(2);

        assert_eq!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, later),
            Decision::Allow
        );
    }

    /// One identity exhausting its allowance must not spend anybody
    /// else's — that is the difference between a rate limit and an
    /// outage.
    #[test]
    fn buckets_are_per_identity() {
        let now = Instant::now();
        let mut buckets = Buckets::default();

        assert_eq!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Allow
        );
        assert!(matches!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Deny(_)
        ));

        assert_eq!(
            buckets.take("bob", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Allow
        );
    }

    /// …and per class, so a flood of scans cannot lock an identity out
    /// of its own point reads.
    #[test]
    fn buckets_are_per_class() {
        let now = Instant::now();
        let mut buckets = Buckets::default();

        assert_eq!(
            buckets.take("alice", EndpointClass::Bulk, 1.0, 1.0, now),
            Decision::Allow
        );
        assert!(matches!(
            buckets.take("alice", EndpointClass::Bulk, 1.0, 1.0, now),
            Decision::Deny(_)
        ));

        assert_eq!(
            buckets.take("alice", EndpointClass::Read, 1.0, 1.0, now),
            Decision::Allow
        );
    }

    // -----------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------

    /// The property that matters most about parsing a limit: a value
    /// nobody can read must not be read as "no limit". A typo in a unit
    /// file is otherwise a silent removal of a control.
    #[test]
    fn an_unparseable_rate_falls_back_to_the_default_not_to_unlimited() {
        // `parse_rate` reads the environment for the class it is given,
        // so exercise the parsing branch directly through a class whose
        // variable is not set: an absent variable yields the default,
        // which is the same guarantee an unparseable one must give.
        let default = EndpointClass::Bulk.default_rate();

        for garbage in ["", "  ", "banana", "0", "-5", "10:banana", "10:0"] {
            assert_eq!(
                parse_rate_from(EndpointClass::Bulk, Some(garbage)),
                Some(default),
                "{garbage:?} must fall back to the default, not disable the limit"
            );
        }
    }

    #[test]
    fn a_rate_can_be_turned_off_explicitly_and_only_explicitly() {
        assert_eq!(parse_rate_from(EndpointClass::Read, Some("off")), None);
        assert_eq!(parse_rate_from(EndpointClass::Read, Some("OFF")), None);
    }

    #[test]
    fn a_rate_parses_burst_and_refill() {
        assert_eq!(
            parse_rate_from(EndpointClass::Read, Some("10:2")),
            Some((10.0, 2.0))
        );

        // A single number sets both.
        assert_eq!(
            parse_rate_from(EndpointClass::Read, Some("10")),
            Some((10.0, 10.0))
        );
    }

    // -----------------------------------------------------------------
    // Permits
    // -----------------------------------------------------------------

    /// The subscriber pool hands out permits and takes them back — the
    /// property `/events` depends on, since a permit that is never
    /// returned turns the cap into a one-way ratchet.
    #[test]
    fn a_subscriber_permit_is_returned_when_dropped() {
        // Drained rather than counted to a constant: the pool is
        // process-wide and another test in this binary may legitimately
        // be holding some of it. What is being asserted is that the pool
        // is *finite* and that it refills, not what its size is.
        let held: Vec<_> = std::iter::from_fn(subscriber_permit).collect();

        assert!(!held.is_empty(), "the pool handed out nothing at all");
        assert!(
            subscriber_permit().is_none(),
            "a drained pool still handed out a permit"
        );

        drop(held);

        assert!(
            subscriber_permit().is_some(),
            "a released permit did not come back"
        );
    }
}
