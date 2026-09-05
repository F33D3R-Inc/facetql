//! Operational metrics: what this process is *doing*, as opposed to what
//! it is *holding*.
//!
//! `GET /stats` already reported the shape of the data (node/edge/user
//! counts, per-kind breakdown, heap segments). That is a census, not a
//! workload: a control plane deciding whether a database is under
//! pressure needs throughput, latency, contention and resource use, and
//! none of those can be derived from a census. This module is that
//! second half, and everything in it is additive to the wire contract —
//! new fields on the `/stats` body, no change to any existing one.
//!
//! # The rule this module is written to
//!
//! **A metric that is missing is better than a metric that is wrong.**
//! Downstream (Fabric) *acts* on these numbers — it moves data between
//! instances because of them — so a plausible-looking invention is worse
//! than a hole. Every figure here is therefore either measured or
//! `null`; nothing is estimated into existence. Where a number is a
//! deliberate approximation (a percentile read off a bounded histogram,
//! a per-cell table that can overflow) the approximation is bounded,
//! documented, and reported *alongside the evidence of its own error* —
//! the overflow counters below exist so a consumer can tell a complete
//! attribution from a partial one instead of assuming.
//!
//! # What lives here and why it is one module
//!
//! Three different layers contribute to one answer:
//!
//! * the **HTTP layer** knows request latency and how many requests are
//!   in flight ([`observe`]);
//! * the **engine** knows which coordinate a record read or a mutation
//!   belonged to ([`CellTable`]) and when a writer had to queue behind
//!   another writer ([`enter_write_queue`]);
//! * the **process** knows its own CPU time and resident memory
//!   ([`ProcessStats`]).
//!
//! They meet in [`snapshot`], which is what `StorageEngine::stats`
//! embeds in its response. Keeping them in one module keeps one
//! vocabulary — "read", "write", "cell" mean the same thing in all three
//! — rather than three subsystems each with a private idea of what a
//! read is.
//!
//! # Cost
//!
//! Every hot-path write here is a **relaxed** atomic add on a counter
//! that nothing reads until `/stats` is called. Relaxed is exactly right
//! for a counter: there is no other memory whose visibility depends on
//! it, so there is nothing to order it against, and it compiles to a
//! bare `lock xadd` with no fence. Per request the cost is two
//! `Instant::now()` calls and three or four such adds; per record read
//! or written, one hashed probe and two adds. Nothing here takes a lock
//! on a request path, and nothing here allocates on a request path.
//!
//! The one mutex ([`Window`]) is taken only by `GET /stats` itself, so
//! its contention is bounded by how often an operator or a control plane
//! polls — never by traffic.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::Response;
use serde::Serialize;

use crate::core::coordinate::Coordinate;

// ─────────────────────────────────────────────────────────────────────
// Latency histogram
// ─────────────────────────────────────────────────────────────────────

/// Significant bits kept below the leading one bit of a latency sample.
///
/// Three bits means every bucket is at most 12.5% wider than its own
/// lower bound, so a percentile read off this histogram is within 12.5%
/// of the true value — and it is *reported as the bucket's upper bound*,
/// so the error only ever runs pessimistic. That is the deliberate
/// trade: an exact percentile needs every sample retained and sorted,
/// which is unbounded memory on a hot path, and a mean needs neither but
/// answers the wrong question. Latency distributions are long-tailed;
/// the mean of a database's request latency is dominated by the fast
/// path and moves barely at all when the tail collapses, which is
/// precisely the event a control plane is watching for.
const SUB_BITS: u32 = 3;

const SUB_BUCKETS: usize = 1 << SUB_BITS;

/// Enough octaves to cover the whole `u64` microsecond range, so no
/// sample can fall off the end and be silently dropped.
const BUCKET_COUNT: usize = 512;

/// A fixed-size, lock-free latency histogram in microseconds.
///
/// Log-linear ("HdrHistogram-shaped") bucketing: samples below
/// [`SUB_BUCKETS`] land in a bucket of their own, and above it each
/// octave is split into [`SUB_BUCKETS`] linear sub-buckets. Fixed size
/// is the point — 512 `u64`s, 4 KiB, allocated once and never grown, so
/// a latency spike costs no memory at all.
struct Histogram {
    buckets: [AtomicU64; BUCKET_COUNT],
}

/// Which bucket a microsecond sample belongs to. Monotone in `us`.
fn bucket_index(us: u64) -> usize {
    if us < SUB_BUCKETS as u64 {
        return us as usize;
    }

    // `us >= 8`, so the leading one bit is at position 3 or above and
    // the shift below cannot underflow.
    let msb = (63 - us.leading_zeros()) as usize;
    let sub = ((us >> (msb - SUB_BITS as usize)) & (SUB_BUCKETS as u64 - 1)) as usize;

    ((msb - SUB_BITS as usize + 1) * SUB_BUCKETS + sub).min(BUCKET_COUNT - 1)
}

/// The largest microsecond value that lands in `index`.
///
/// Reporting the *upper* bound rather than the midpoint is what makes
/// the approximation one-directional: "the 99th percentile is at most
/// this" is a claim that stays true under the bucket's own width.
fn bucket_upper_us(index: usize) -> u64 {
    if index < SUB_BUCKETS {
        return index as u64;
    }

    let octave = index / SUB_BUCKETS;
    let sub = (index % SUB_BUCKETS) as u64;
    let msb = octave + SUB_BITS as usize - 1;
    let width = 1u64 << (msb - SUB_BITS as usize);
    let base = (SUB_BUCKETS as u64 + sub) << (msb - SUB_BITS as usize);

    base.saturating_add(width - 1)
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKET_COUNT],
        }
    }

    /// One sample. A single relaxed add — the whole hot-path cost.
    fn record(&self, us: u64) {
        self.buckets[bucket_index(us)].fetch_add(1, Ordering::Relaxed);
    }

    /// Copy the cumulative counts out. Not atomic as a whole, and does
    /// not need to be: a sample landing mid-snapshot is attributed to
    /// this window or the next, never lost and never double-counted,
    /// because the window differences two snapshots of the same
    /// monotonic counters.
    fn snapshot(&self, out: &mut [u64; BUCKET_COUNT]) {
        for (slot, bucket) in out.iter_mut().zip(self.buckets.iter()) {
            *slot = bucket.load(Ordering::Relaxed);
        }
    }
}

/// The latency of one class of request over one window.
///
/// `p50` and `p99` rather than a mean, and both rather than one: p50 is
/// what a typical caller experienced, p99 is what saturation looks like
/// first, and the gap between them is the shape of the distribution. A
/// consumer scoring *pressure* should use `p99_us` — a database at its
/// limit shows it in the tail long before the median moves.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyStats {
    /// Requests of this class completed during the window.
    pub count: u64,
    /// Median, in microseconds, as the upper bound of the bucket it
    /// falls in. `null` when the window contained no requests — zero
    /// would read as "instant", which is not what "nothing happened"
    /// means.
    pub p50_us: Option<u64>,
    pub p99_us: Option<u64>,
    /// Upper bound of the highest occupied bucket: the slowest request
    /// in the window, to within one bucket width.
    pub max_us: Option<u64>,
}

impl LatencyStats {
    /// Percentiles over the per-bucket *deltas* of one window.
    fn from_delta(delta: &[u64; BUCKET_COUNT]) -> Self {
        let count: u64 = delta.iter().sum();

        if count == 0 {
            return Self::default();
        }

        let quantile = |q: f64| -> Option<u64> {
            // Rank of the sample that sits at `q`, 1-based, rounded up:
            // the smallest value with at least `q` of the mass at or
            // below it.
            let rank = ((count as f64) * q).ceil().max(1.0) as u64;
            let mut seen = 0u64;

            for (index, &n) in delta.iter().enumerate() {
                seen += n;

                if seen >= rank {
                    return Some(bucket_upper_us(index));
                }
            }

            None
        };

        let max_us = delta
            .iter()
            .rposition(|&n| n > 0)
            .map(bucket_upper_us);

        Self {
            count,
            p50_us: quantile(0.50),
            p99_us: quantile(0.99),
            max_us,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Request classification
// ─────────────────────────────────────────────────────────────────────

/// What a request counts as for the purposes of throughput and latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    /// Serves data out of the engine.
    Read,
    /// Changes data in the engine.
    Write,
    /// Deliberately outside the workload measurement: administration,
    /// the event fan-out, and `/stats` itself. Counted, so the exclusion
    /// is visible rather than silent, but kept out of the latency
    /// histograms — a `/stats` poll is not client traffic, and admin
    /// calls are not data workload (the same reason `writes_total` has
    /// never counted user-record writes).
    Excluded,
    /// A route the table below does not name.
    ///
    /// This bucket is the whole reason classification is a table and not
    /// a heuristic on the HTTP method: a route added later without a
    /// line here shows up *here*, as an explicit count of requests
    /// nobody classified, instead of being quietly filed as a read
    /// because it happened to be a `GET` — or, worse, a `POST /nodes/query`
    /// filed as a write because it happened to be a `POST`.
    Unclassified,
}

/// The route table. Keyed by the *matched path pattern*, not the
/// request URI, so `/node/abc` and `/node/def` are one entry and no
/// string parsing is involved.
fn classify(method: &Method, matched: &str) -> RequestClass {
    use RequestClass::{Excluded, Read, Unclassified, Write};

    match matched {
        "/node" => match *method {
            Method::POST => Write,
            _ => Unclassified,
        },

        "/node/:address" => match *method {
            Method::GET => Read,
            Method::PUT | Method::DELETE => Write,
            _ => Unclassified,
        },

        "/node/:address/history"
        | "/node/:address/owned"
        | "/node/:address/edges/out"
        | "/node/:address/edges/in"
        | "/nodes"
        | "/nodes/multiget"
        | "/nodes/query"
        | "/nodes/count"
        | "/nodes/count_by" => Read,

        "/node/:address/claim"
        | "/sequence/:name/next"
        | "/edge"
        | "/transaction" => Write,

        // `/changes` sits with `/events` rather than with the reads,
        // and the reason is circularity rather than taxonomy. This
        // split feeds the workload profile Fabric places cells by, and
        // the only caller of the change scan is a *migration* catching
        // up on the writes it has to copy. Counting its scans as reads
        // would make a cell look read-heavy exactly while it is being
        // moved, and the mover's own traffic would start steering the
        // decision that produced it.
        "/publish" | "/stats" | "/" | "/events" | "/changes" => Excluded,

        path if path.starts_with("/admin/") => Excluded,

        _ => Unclassified,
    }
}

// ─────────────────────────────────────────────────────────────────────
// The registry
// ─────────────────────────────────────────────────────────────────────

/// Process-wide operational counters.
///
/// Process-wide rather than per-engine because that is what they
/// describe: one process serves one HTTP surface with one CPU budget and
/// one memory budget, and a second engine in the same process (which
/// only tests create) does not get a second set of those. The per-cell
/// attribution, which *is* per-engine data, lives on the engine instead
/// — see [`CellTable`].
struct Registry {
    started: Instant,

    requests_total: AtomicU64,
    requests_read: AtomicU64,
    requests_write: AtomicU64,
    requests_excluded: AtomicU64,
    requests_unclassified: AtomicU64,

    read_latency: Histogram,
    write_latency: Histogram,

    /// Writers currently waiting to acquire the engine's writer mutex.
    ///
    /// This is the server's real queue: reads run concurrently, but
    /// every mutation serializes on one lock, so the number of threads
    /// parked on it is the contention signal that actually predicts
    /// write latency here.
    write_queue_depth: AtomicUsize,

    /// Times a writer found the lock already wanted by somebody else.
    /// A gauge sampled every few seconds misses short bursts entirely;
    /// this counter cannot.
    write_queue_contended: AtomicU64,

    window: Mutex<Window>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();

    REGISTRY.get_or_init(|| Registry {
        started: Instant::now(),
        requests_total: AtomicU64::new(0),
        requests_read: AtomicU64::new(0),
        requests_write: AtomicU64::new(0),
        requests_excluded: AtomicU64::new(0),
        requests_unclassified: AtomicU64::new(0),
        read_latency: Histogram::new(),
        write_latency: Histogram::new(),
        write_queue_depth: AtomicUsize::new(0),
        write_queue_contended: AtomicU64::new(0),
        window: Mutex::new(Window::new()),
    })
}

/// Start the clock behind `uptime_seconds`.
///
/// Called from `StorageEngine::open`, so uptime is measured from the
/// moment this process had a database — which is what an operator means
/// by it — rather than from whenever the first request happened to
/// touch a counter.
pub fn init() {
    let _ = registry();
}

// ─────────────────────────────────────────────────────────────────────
// The HTTP middleware
// ─────────────────────────────────────────────────────────────────────

/// Time and count every request that reaches a route.
///
/// One layer, applied once where the routes are declared, is the entire
/// instrumentation of the request path: there is no per-handler call to
/// forget, so a handler cannot under-report by omission.
///
/// Latency is measured **here**, around the whole handler, rather than
/// inside the engine. That is deliberate: a caller's latency includes
/// the queue for a blocking thread, the wait for the writer mutex and
/// the fsync that a mutation ends with, and those are exactly the parts
/// that grow when an instance is in trouble. An engine-internal timer
/// would report the healthy-looking half.
pub async fn observe(request: Request, next: Next) -> Response {
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string());

    let class = match &matched {
        Some(path) => classify(request.method(), path),
        None => RequestClass::Unclassified,
    };

    let registry = registry();
    registry.requests_total.fetch_add(1, Ordering::Relaxed);

    let counter = match class {
        RequestClass::Read => &registry.requests_read,
        RequestClass::Write => &registry.requests_write,
        RequestClass::Excluded => &registry.requests_excluded,
        RequestClass::Unclassified => &registry.requests_unclassified,
    };

    counter.fetch_add(1, Ordering::Relaxed);

    let started = Instant::now();
    let response = next.run(request).await;
    let micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;

    match class {
        RequestClass::Read => registry.read_latency.record(micros),
        RequestClass::Write => registry.write_latency.record(micros),
        RequestClass::Excluded | RequestClass::Unclassified => {}
    }

    response
}

// ─────────────────────────────────────────────────────────────────────
// Write-queue instrumentation
// ─────────────────────────────────────────────────────────────────────

/// Announce that this thread is about to block on the engine's writer
/// mutex. Must be paired with [`leave_write_queue`].
///
/// Two relaxed adds around a lock acquisition that already costs an
/// atomic RMW of its own, so the instrumentation is in the noise of the
/// thing it measures.
pub fn enter_write_queue() {
    let registry = registry();

    if registry.write_queue_depth.fetch_add(1, Ordering::Relaxed) > 0 {
        registry.write_queue_contended.fetch_add(1, Ordering::Relaxed);
    }
}

/// The lock has been acquired (or the attempt is over).
pub fn leave_write_queue() {
    registry()
        .write_queue_depth
        .fetch_sub(1, Ordering::Relaxed);
}

// ─────────────────────────────────────────────────────────────────────
// Per-cell attribution
// ─────────────────────────────────────────────────────────────────────

/// Slots in a [`CellTable`]. A power of two so the probe wraps with a
/// mask rather than a division.
///
/// 256 is the bound, and it is a hard one: the table is a fixed array
/// allocated once with the engine (roughly 10 KiB) and it never grows,
/// never rehashes and never allocates again. A map keyed by whatever
/// coordinates traffic happens to touch would grow without limit over a
/// long uptime — the same shape of bug as an observation vector that is
/// appended to on every poll and never trimmed.
const CELL_SLOTS: usize = 256;

/// How far a probe walks before giving up and charging the operation to
/// the overflow counters. Bounded so the hot path's worst case is
/// bounded: eight loads, not a scan of the table.
const CELL_PROBES: usize = 8;

/// Marks a slot as occupied. A coordinate packs into the low 32 bits,
/// and `(0, 0, 0, 0)` is a perfectly ordinary coordinate, so "occupied"
/// cannot be "non-zero key" without this bit.
const CELL_OCCUPIED: u64 = 1 << 32;

/// Per-coordinate traffic attribution, bounded and lock-free.
///
/// # What a "cell" is here, exactly
///
/// One FacetQL coordinate `(x, y, z, q)`. This is the finest unit the
/// engine can honestly attribute work to, because a coordinate is a
/// property of a *record*, and records are what reads and writes touch.
///
/// # What the numbers mean, exactly
///
/// * `reads` counts **node records read**, not requests. One query that
///   returns fifty nodes spread over three coordinates contributes fifty
///   reads across three cells. This includes the read-before-write an
///   update performs, because that read is real work done on that cell's
///   data — the alternative would be a number that undercounts the cost
///   of a write-heavy cell.
/// * `writes` counts **mutations applied to a node**, matching
///   `writes_total`'s per-mutation meaning. Edge and user mutations have
///   no coordinate and are charged to `unattributed_writes` rather than
///   guessed at.
/// * `bytes_read` / `bytes_written` are the record's `data` payload
///   length. That is the part whose size the application controls and
///   the part a migration would have to copy; it is not the on-disk
///   record size, which includes framing this counter cannot see from
///   here.
///
/// # How it stays bounded, and how it admits when it did
///
/// Open addressing with linear probing over a fixed 256-slot array. A
/// coordinate claims a slot on first sight, with a compare-and-exchange,
/// and keeps it for the life of the engine. When more than 256
/// coordinates (or an unlucky probe run of 8) are in play, the excess is
/// charged to `overflow_reads` / `overflow_writes` — a visible,
/// countable statement that the attribution is partial. A consumer that
/// sees overflow at zero knows the breakdown is complete; one that does
/// not, knows exactly how much it is missing. That is the whole reason
/// the overflow counters are on the wire.
///
/// Counters are cumulative for the life of the process, like
/// `reads_total`: the engine reports totals and the consumer differences
/// two samples into a rate. A window kept here instead would need
/// resetting, and a reset racing the hot path is how counters lose
/// updates.
pub struct CellTable {
    keys: [AtomicU64; CELL_SLOTS],
    reads: [AtomicU64; CELL_SLOTS],
    writes: [AtomicU64; CELL_SLOTS],
    bytes_read: [AtomicU64; CELL_SLOTS],
    bytes_written: [AtomicU64; CELL_SLOTS],

    overflow_reads: AtomicU64,
    overflow_writes: AtomicU64,
    unattributed_writes: AtomicU64,
}

impl Default for CellTable {
    fn default() -> Self {
        Self::new()
    }
}

fn pack(coordinate: Coordinate) -> u64 {
    ((coordinate.x as u64) << 24)
        | ((coordinate.y as u64) << 16)
        | ((coordinate.z as u64) << 8)
        | coordinate.q as u64
}

fn unpack(packed: u64) -> Coordinate {
    Coordinate::new(
        (packed >> 24) as u8,
        (packed >> 16) as u8,
        (packed >> 8) as u8,
        packed as u8,
    )
}

impl CellTable {
    pub const fn new() -> Self {
        Self {
            keys: [const { AtomicU64::new(0) }; CELL_SLOTS],
            reads: [const { AtomicU64::new(0) }; CELL_SLOTS],
            writes: [const { AtomicU64::new(0) }; CELL_SLOTS],
            bytes_read: [const { AtomicU64::new(0) }; CELL_SLOTS],
            bytes_written: [const { AtomicU64::new(0) }; CELL_SLOTS],
            overflow_reads: AtomicU64::new(0),
            overflow_writes: AtomicU64::new(0),
            unattributed_writes: AtomicU64::new(0),
        }
    }

    /// The slot holding `key`, claiming a free one if the coordinate has
    /// not been seen before. `None` when the probe run is exhausted.
    fn slot(&self, key: u64) -> Option<usize> {
        // Fibonacci hashing: one multiply, and it spreads the low bits
        // of a packed coordinate (which are the `q` axis, often 0)
        // across the whole index range.
        let mut index =
            ((key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize) % CELL_SLOTS;

        for _ in 0..CELL_PROBES {
            let current = self.keys[index].load(Ordering::Relaxed);

            if current == key {
                return Some(index);
            }

            if current == 0 {
                match self.keys[index].compare_exchange(
                    0,
                    key,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    // Claimed it.
                    Ok(_) => return Some(index),
                    // Lost the race; if the winner wanted the same
                    // coordinate the slot is still ours to use.
                    Err(actual) if actual == key => return Some(index),
                    Err(_) => {}
                }
            }

            index = (index + 1) % CELL_SLOTS;
        }

        None
    }

    /// Charge one node-record read to `coordinate`.
    pub fn record_read(&self, coordinate: Coordinate, bytes: u64) {
        let key = CELL_OCCUPIED | pack(coordinate);

        match self.slot(key) {
            Some(index) => {
                self.reads[index].fetch_add(1, Ordering::Relaxed);
                self.bytes_read[index].fetch_add(bytes, Ordering::Relaxed);
            }
            None => {
                self.overflow_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Charge one applied node mutation to `coordinate`.
    pub fn record_write(&self, coordinate: Coordinate, bytes: u64) {
        let key = CELL_OCCUPIED | pack(coordinate);

        match self.slot(key) {
            Some(index) => {
                self.writes[index].fetch_add(1, Ordering::Relaxed);
                self.bytes_written[index].fetch_add(bytes, Ordering::Relaxed);
            }
            None => {
                self.overflow_writes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// A mutation that has no coordinate at all: an edge, or a delete of
    /// an address that did not exist. Counted, never guessed at.
    pub fn record_unattributed_write(&self) {
        self.unattributed_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// The wire view: every occupied slot, busiest first.
    fn snapshot(&self) -> CellAttribution {
        let mut cells: Vec<CellStats> = (0..CELL_SLOTS)
            .filter_map(|index| {
                let key = self.keys[index].load(Ordering::Relaxed);

                if key == 0 {
                    return None;
                }

                let coordinate = unpack(key & 0xFFFF_FFFF);

                Some(CellStats {
                    x: coordinate.x,
                    y: coordinate.y,
                    z: coordinate.z,
                    q: coordinate.q,
                    reads: self.reads[index].load(Ordering::Relaxed),
                    writes: self.writes[index].load(Ordering::Relaxed),
                    bytes_read: self.bytes_read[index].load(Ordering::Relaxed),
                    bytes_written: self.bytes_written[index].load(Ordering::Relaxed),
                })
            })
            .collect();

        // Busiest first, with the coordinate as a deterministic tiebreak
        // so two polls of an idle instance return the same ordering.
        cells.sort_by(|a, b| {
            (b.reads + b.writes)
                .cmp(&(a.reads + a.writes))
                .then_with(|| (a.x, a.y, a.z, a.q).cmp(&(b.x, b.y, b.z, b.q)))
        });

        CellAttribution {
            capacity: CELL_SLOTS as u64,
            tracked: cells.len() as u64,
            overflow_reads: self.overflow_reads.load(Ordering::Relaxed),
            overflow_writes: self.overflow_writes.load(Ordering::Relaxed),
            unattributed_writes: self.unattributed_writes.load(Ordering::Relaxed),
            cells,
        }
    }
}

/// One coordinate's share of the traffic. See [`CellTable`] for what
/// each number counts.
#[derive(Debug, Clone, Serialize)]
pub struct CellStats {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
    pub reads: u64,
    pub writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// The per-cell breakdown, and the honest account of what it left out.
#[derive(Debug, Clone, Serialize)]
pub struct CellAttribution {
    /// Coordinates this table can track at once. A hard bound.
    pub capacity: u64,
    /// Coordinates it is currently tracking.
    pub tracked: u64,
    /// Record reads that could not be attributed because the table was
    /// full. Non-zero means `cells` is a partial account of `reads`.
    pub overflow_reads: u64,
    pub overflow_writes: u64,
    /// Mutations with no coordinate to attribute — edges, and deletes of
    /// addresses that were already gone.
    pub unattributed_writes: u64,
    /// Busiest cell first.
    pub cells: Vec<CellStats>,
}

// ─────────────────────────────────────────────────────────────────────
// Process resource use
// ─────────────────────────────────────────────────────────────────────

/// CPU seconds this process has consumed across all threads, user plus
/// system.
///
/// `getrusage` rather than `/proc/self/stat` because it is one syscall
/// with no parsing and it is portable across the Unixes; the value is
/// the same accounting either way.
#[cfg(unix)]
fn cpu_seconds_total() -> Option<f64> {
    // SAFETY: `getrusage` writes into a caller-provided `rusage`, which
    // is a plain C struct of integers; a zeroed one is a valid starting
    // state and the kernel overwrites what it uses.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };

    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return None;
    }

    let seconds = |t: libc::timeval| t.tv_sec as f64 + (t.tv_usec as f64 / 1_000_000.0);

    Some(seconds(usage.ru_utime) + seconds(usage.ru_stime))
}

/// No portable way to ask; `None` rather than a zero that would read as
/// an idle process.
#[cfg(not(unix))]
fn cpu_seconds_total() -> Option<f64> {
    None
}

/// Resident set size in bytes.
#[cfg(target_os = "linux")]
fn resident_bytes() -> Option<u64> {
    // `/proc/self/statm` field 2 is resident pages.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;

    // SAFETY: `sysconf` reads a static system parameter and returns -1
    // on failure, which the check below rejects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };

    if page_size <= 0 {
        return None;
    }

    Some(pages * page_size as u64)
}

/// Resident set size in bytes — unavailable without `/proc`.
#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> Option<u64> {
    None
}

/// The memory ceiling this process is actually held to, and where that
/// number came from.
///
/// A cgroup limit wins over the machine's total whenever it is smaller,
/// because in a container the machine's total is not the bound anyone
/// gets killed for exceeding. Reporting the source alongside the number
/// is what lets an operator tell "4 GiB because that is the box" from
/// "4 GiB because that is the quota" — two very different reasons for a
/// utilization figure to move.
///
/// Cached: neither number changes while the process runs, and reading
/// three files on every `/stats` poll to learn that would be waste.
fn memory_limit() -> Option<(u64, &'static str)> {
    static LIMIT: OnceLock<Option<(u64, &'static str)>> = OnceLock::new();

    *LIMIT.get_or_init(probe_memory_limit)
}

#[cfg(target_os = "linux")]
fn probe_memory_limit() -> Option<(u64, &'static str)> {
    let parse = |path: &str| -> Option<u64> {
        std::fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
    };

    let system = std::fs::read_to_string("/proc/meminfo").ok().and_then(|text| {
        text.lines()
            .find(|line| line.starts_with("MemTotal:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024)
    });

    let cgroup = parse("/sys/fs/cgroup/memory.max")
        .or_else(|| parse("/sys/fs/cgroup/memory/memory.limit_in_bytes"));

    match (cgroup, system) {
        // "max" in cgroup v2 does not parse at all, and v1 writes a
        // near-`u64::MAX` sentinel; either way an unlimited cgroup is
        // larger than the machine, so preferring the smaller of the two
        // picks the honest bound without special-casing the sentinel.
        (Some(limit), Some(total)) if limit < total => Some((limit, "cgroup")),
        (_, Some(total)) => Some((total, "system")),
        (Some(limit), None) => Some((limit, "cgroup")),
        (None, None) => None,
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_memory_limit() -> Option<(u64, &'static str)> {
    None
}

/// What the process is consuming, as opposed to what it is serving.
///
/// Every field is `Option` because every field is genuinely unavailable
/// on some platform, and `null` is the only truthful encoding of "this
/// host would not tell me". A zero here would read downstream as an idle
/// process.
#[derive(Debug, Clone, Serialize)]
pub struct ProcessStats {
    /// CPU seconds consumed since start, user plus system, all threads.
    ///
    /// A **monotonic counter**, not a utilization. This is the honest
    /// primitive: utilization is a rate, a rate needs a window, and the
    /// process cannot know which window the consumer cares about. The
    /// window-derived figure is in [`WindowStats::cpu_utilization`]; a
    /// consumer that wants its own window should difference this against
    /// its own previous sample, exactly as it already does with
    /// `reads_total`.
    pub cpu_seconds_total: Option<f64>,

    /// Hardware parallelism available to this process — the divisor that
    /// turns CPU seconds per wall second into a 0..1 utilization. On
    /// Linux this respects a cgroup CPU quota, so a process limited to
    /// half a core is not reported as using 1/64th of the machine.
    pub cpu_cores: Option<u64>,

    /// Resident set size. Linux only.
    pub resident_bytes: Option<u64>,

    /// The ceiling `resident_bytes` is measured against.
    pub memory_limit_bytes: Option<u64>,

    /// `"cgroup"` or `"system"` — see [`memory_limit`].
    pub memory_limit_source: Option<&'static str>,

    /// `resident_bytes / memory_limit_bytes`, clamped to 0..1.
    ///
    /// This is a *process* memory utilization, not the machine's: it
    /// answers "how close is this database to its own ceiling", which is
    /// the question a placement decision turns on. It does not include
    /// page cache the kernel holds on this process's behalf, which is
    /// memory the process benefits from but is not charged for.
    pub memory_utilization: Option<f64>,
}

fn process_stats() -> ProcessStats {
    let resident = resident_bytes();
    let limit = memory_limit();

    ProcessStats {
        cpu_seconds_total: cpu_seconds_total(),
        cpu_cores: std::thread::available_parallelism()
            .ok()
            .map(|n| n.get() as u64),
        resident_bytes: resident,
        memory_limit_bytes: limit.map(|(bytes, _)| bytes),
        memory_limit_source: limit.map(|(_, source)| source),
        memory_utilization: match (resident, limit) {
            (Some(resident), Some((limit, _))) if limit > 0 => {
                Some((resident as f64 / limit as f64).clamp(0.0, 1.0))
            }
            _ => None,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────
// The observation window
// ─────────────────────────────────────────────────────────────────────

/// Shortest interval that counts as a window.
///
/// Two things need a window rather than a counter — a CPU utilization
/// and a latency percentile — and both are meaningless over an interval
/// too short to contain traffic. One second is long enough to divide by
/// without amplifying clock noise, and short enough that a control plane
/// polling on any sane interval closes a window on every poll.
const MIN_WINDOW: Duration = Duration::from_secs(1);

/// The state behind one rolling observation window.
///
/// The window is advanced *by observation*: a `GET /stats` that finds
/// the open window at least [`MIN_WINDOW`] old closes it, publishes it,
/// and opens the next one. There is no background thread, no timer, and
/// nothing to keep running when nobody is watching.
///
/// The consequence, stated plainly because a consumer has to know it:
/// the window is bounded by the *polling* interval, not chosen by the
/// server. Two pollers on the same instance interleave and each sees
/// shorter windows than it asked for. Those windows are still true —
/// a percentile over five seconds is a real percentile over five seconds
/// — which is why `duration_ms` is on the wire: the consumer is told
/// exactly what interval it is being handed, rather than assuming one.
struct Window {
    opened: Instant,
    closed: Option<Instant>,
    cpu_at_open: Option<f64>,
    read_at_open: Box<[u64; BUCKET_COUNT]>,
    write_at_open: Box<[u64; BUCKET_COUNT]>,
    last: WindowStats,
}

impl Window {
    fn new() -> Self {
        Self {
            opened: Instant::now(),
            closed: None,
            cpu_at_open: cpu_seconds_total(),
            read_at_open: Box::new([0; BUCKET_COUNT]),
            write_at_open: Box::new([0; BUCKET_COUNT]),
            last: WindowStats::default(),
        }
    }
}

/// One closed observation window.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WindowStats {
    /// How long the window this describes actually ran. **Zero means no
    /// window has closed yet** — the instance has been up for less than
    /// [`MIN_WINDOW`], or this is the first poll. A consumer must treat
    /// zero as "no measurement", not as "no traffic".
    pub duration_ms: u64,

    /// How long ago it closed. Grows between polls; a large value means
    /// these figures describe an interval that has since passed.
    pub age_ms: u64,

    /// CPU seconds consumed during the window divided by (wall seconds ×
    /// cores), clamped to 0..1.
    ///
    /// `null` when no window has closed, or when the platform would not
    /// report CPU time or core count. Never fabricated: a process that
    /// has not been observed for a full window has no utilization, and
    /// saying `0.0` would be a claim that it was idle.
    pub cpu_utilization: Option<f64>,

    pub read_latency: LatencyStats,
    pub write_latency: LatencyStats,
}

/// Close the current window if it is old enough, and return the newest
/// closed one.
fn window_stats() -> WindowStats {
    let registry = registry();
    let now = Instant::now();

    let mut window = match registry.window.lock() {
        Ok(window) => window,
        // A panic inside this mutex could only have happened while
        // copying counters; nothing is left half-written that a
        // subsequent reader could misread, so recovering the guard is
        // sound and is better than failing a stats poll.
        Err(poisoned) => poisoned.into_inner(),
    };

    let elapsed = now.saturating_duration_since(window.opened);

    if elapsed >= MIN_WINDOW {
        let mut read_now = Box::new([0u64; BUCKET_COUNT]);
        let mut write_now = Box::new([0u64; BUCKET_COUNT]);

        registry.read_latency.snapshot(&mut read_now);
        registry.write_latency.snapshot(&mut write_now);

        let mut read_delta = [0u64; BUCKET_COUNT];
        let mut write_delta = [0u64; BUCKET_COUNT];

        for index in 0..BUCKET_COUNT {
            read_delta[index] = read_now[index].saturating_sub(window.read_at_open[index]);
            write_delta[index] =
                write_now[index].saturating_sub(window.write_at_open[index]);
        }

        let cpu_now = cpu_seconds_total();
        let cores = std::thread::available_parallelism().ok().map(|n| n.get() as f64);
        let seconds = elapsed.as_secs_f64();

        let cpu_utilization = match (window.cpu_at_open, cpu_now, cores) {
            (Some(before), Some(after), Some(cores)) if cores > 0.0 && seconds > 0.0 => {
                Some(((after - before) / (seconds * cores)).clamp(0.0, 1.0))
            }
            _ => None,
        };

        window.last = WindowStats {
            duration_ms: elapsed.as_millis().min(u64::MAX as u128) as u64,
            age_ms: 0,
            cpu_utilization,
            read_latency: LatencyStats::from_delta(&read_delta),
            write_latency: LatencyStats::from_delta(&write_delta),
        };

        window.opened = now;
        window.closed = Some(now);
        window.cpu_at_open = cpu_now;
        window.read_at_open = read_now;
        window.write_at_open = write_now;
    }

    let mut stats = window.last.clone();

    stats.age_ms = window
        .closed
        .map(|closed| now.saturating_duration_since(closed).as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0);

    stats
}

// ─────────────────────────────────────────────────────────────────────
// The snapshot
// ─────────────────────────────────────────────────────────────────────

/// HTTP-level throughput and contention.
///
/// These count *requests*. `EngineStats::reads_total` and
/// `writes_total` count engine operations and mutations respectively,
/// and the two do not have to agree: one `POST /transaction` is one
/// write request and any number of mutations. Both are on the wire
/// because they answer different questions — "how much is being asked of
/// this server" and "how much work did that turn into".
#[derive(Debug, Clone, Serialize)]
pub struct RequestStats {
    pub total: u64,
    pub read: u64,
    pub write: u64,
    /// Administration, `/publish`, `/events` and `/stats`: counted,
    /// deliberately outside the latency measurement.
    pub excluded: u64,
    /// Requests on a route the classifier does not name. Should be zero;
    /// a non-zero value means a route was added without a line in
    /// `classify`, and the read/write split above is missing it.
    pub unclassified: u64,
    /// Requests being served right now, from the concurrency limiter's
    /// own permits — the exact number, not a sample.
    pub in_flight: u64,
    /// The ceiling `in_flight` is measured against.
    pub max_concurrent: u64,
    /// Writers parked on the engine's writer mutex right now.
    ///
    /// This is the queue depth in the sense a queueing model means it:
    /// work that has arrived and cannot start. It is an instantaneous
    /// gauge, so a poll can miss a burst between samples — which is what
    /// `write_queue_contended_total` is for.
    pub write_queue_depth: u64,
    /// Times a writer arrived to find the writer mutex already
    /// contended. Monotonic, so no burst can hide between two polls.
    pub write_queue_contended_total: u64,
}

/// Everything this module knows, as one serializable block.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeStats {
    /// Seconds since the database opened in this process.
    pub uptime_seconds: u64,
    pub requests: RequestStats,
    pub window: WindowStats,
    pub process: ProcessStats,
}

/// Take the snapshot that `GET /stats` reports.
pub fn snapshot() -> RuntimeStats {
    let registry = registry();

    RuntimeStats {
        uptime_seconds: registry.started.elapsed().as_secs(),
        requests: RequestStats {
            total: registry.requests_total.load(Ordering::Relaxed),
            read: registry.requests_read.load(Ordering::Relaxed),
            write: registry.requests_write.load(Ordering::Relaxed),
            excluded: registry.requests_excluded.load(Ordering::Relaxed),
            unclassified: registry.requests_unclassified.load(Ordering::Relaxed),
            in_flight: crate::api::limits::in_flight_requests() as u64,
            max_concurrent: crate::api::limits::max_concurrent_requests() as u64,
            write_queue_depth: registry.write_queue_depth.load(Ordering::Relaxed) as u64,
            write_queue_contended_total: registry
                .write_queue_contended
                .load(Ordering::Relaxed),
        },
        window: window_stats(),
        process: process_stats(),
    }
}

/// The per-cell view of an engine's traffic.
pub fn cell_stats(cells: &CellTable) -> CellAttribution {
    cells.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bucketing has to be monotone and contiguous: if a larger sample
    /// could land in a lower bucket, every percentile read off the
    /// histogram would be wrong in a way nothing else would catch.
    #[test]
    fn bucket_indexes_are_monotone_and_bounded() {
        let mut previous = 0;

        for us in [0u64, 1, 7, 8, 15, 16, 31, 100, 1_000, 1_000_000, u64::MAX] {
            let index = bucket_index(us);
            assert!(index < BUCKET_COUNT, "{us} landed at {index}");
            assert!(index >= previous, "{us} went backwards to {index}");
            assert!(
                bucket_upper_us(index) >= us,
                "bucket {index} upper bound {} is below its own sample {us}",
                bucket_upper_us(index),
            );
            previous = index;
        }
    }

    /// A percentile must be a real bound on the samples, not a number
    /// near them.
    #[test]
    fn percentiles_bound_the_samples_they_summarize() {
        let histogram = Histogram::new();

        for us in 1..=1_000u64 {
            histogram.record(us);
        }

        let mut counts = [0u64; BUCKET_COUNT];
        histogram.snapshot(&mut counts);

        let stats = LatencyStats::from_delta(&counts);
        assert_eq!(stats.count, 1_000);

        let p50 = stats.p50_us.expect("p50 over 1000 samples");
        let p99 = stats.p99_us.expect("p99 over 1000 samples");

        // Within one bucket width (12.5%) of the true quantiles, and
        // never below them — the bound only runs pessimistic.
        assert!((500..=563).contains(&p50), "p50 was {p50}");
        assert!((990..=1_114).contains(&p99), "p99 was {p99}");
        assert!(stats.max_us.expect("max") >= 1_000);
    }

    /// An empty window reports absence, not zero latency.
    #[test]
    fn an_empty_window_has_no_percentile() {
        let stats = LatencyStats::from_delta(&[0u64; BUCKET_COUNT]);
        assert_eq!(stats.count, 0);
        assert!(stats.p50_us.is_none());
        assert!(stats.max_us.is_none());
    }

    /// Distinct coordinates must not share a slot, or one cell's traffic
    /// would be reported as another's.
    #[test]
    fn cells_are_attributed_to_their_own_coordinate() {
        let table = CellTable::new();

        table.record_read(Coordinate::new(1, 2, 3, 4), 100);
        table.record_read(Coordinate::new(1, 2, 3, 4), 100);
        table.record_write(Coordinate::new(9, 9, 9, 9), 7);
        table.record_unattributed_write();

        let snapshot = table.snapshot();
        assert_eq!(snapshot.tracked, 2);
        assert_eq!(snapshot.unattributed_writes, 1);
        assert_eq!(snapshot.overflow_reads, 0);

        let busiest = &snapshot.cells[0];
        assert_eq!((busiest.x, busiest.y, busiest.z, busiest.q), (1, 2, 3, 4));
        assert_eq!(busiest.reads, 2);
        assert_eq!(busiest.bytes_read, 200);

        let other = snapshot
            .cells
            .iter()
            .find(|cell| cell.x == 9)
            .expect("the written cell");
        assert_eq!(other.writes, 1);
        assert_eq!(other.bytes_written, 7);
    }

    /// The bound is the point: past capacity the table must keep
    /// counting into the overflow rather than growing, and must say so.
    #[test]
    fn a_full_table_overflows_instead_of_growing() {
        let table = CellTable::new();

        // Far more distinct coordinates than there are slots.
        for x in 0..64u8 {
            for y in 0..64u8 {
                table.record_read(Coordinate::new(x, y, 0, 0), 1);
            }
        }

        let snapshot = table.snapshot();
        assert!(snapshot.tracked <= snapshot.capacity);
        assert!(
            snapshot.overflow_reads > 0,
            "4096 coordinates into {} slots must overflow",
            snapshot.capacity,
        );

        let attributed: u64 = snapshot.cells.iter().map(|cell| cell.reads).sum();
        assert_eq!(
            attributed + snapshot.overflow_reads,
            64 * 64,
            "every read is either attributed or counted as overflow",
        );
    }

    /// The classifier is the only thing standing between a `POST` that
    /// reads and a write count that is wrong.
    #[test]
    fn reads_and_writes_are_classified_by_route_not_by_method() {
        assert_eq!(classify(&Method::POST, "/nodes/query"), RequestClass::Read);
        assert_eq!(classify(&Method::POST, "/nodes/count"), RequestClass::Read);
        assert_eq!(classify(&Method::POST, "/transaction"), RequestClass::Write);
        assert_eq!(classify(&Method::GET, "/node/:address"), RequestClass::Read);
        assert_eq!(
            classify(&Method::DELETE, "/node/:address"),
            RequestClass::Write
        );
        assert_eq!(classify(&Method::GET, "/stats"), RequestClass::Excluded);
        assert_eq!(
            classify(&Method::GET, "/admin/users"),
            RequestClass::Excluded
        );
        assert_eq!(
            classify(&Method::GET, "/something/new"),
            RequestClass::Unclassified
        );
    }
}
