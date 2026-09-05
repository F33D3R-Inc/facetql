//! Direct tests for the record heap.
//!
//! Two paths here had never executed in any test at any time:
//!
//! * **Overflow chains.** A record larger than one 16 KiB page is split
//!   across a chain of overflow pages and addressed by a 12-byte stub.
//!   Nothing in the suite had ever written a record that big, so the
//!   chain writer, the chain reader, the cycle guard and the length
//!   guard were all unexercised. A social platform stores post bodies
//!   and JSON payloads; oversized records are not an edge case for it.
//! * **Compaction accounting.** `mark_obsolete` and
//!   `compaction_candidates` decide when a segment's dead bytes justify
//!   rewriting it, and getting that wrong either leaks disk forever or
//!   rewrites a live segment.
//!
//! `Catalog` resolves its files through a process-wide `OnceLock`, so
//! every test in this binary shares one data directory and is serialized
//! against the others. Each test therefore uses its own address prefix,
//! the way the engine's own test modules use their own `kind`.

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use facetql::core::coordinate::Coordinate;
use facetql::core::edge::Edge;
use facetql::core::history::HistoryEntry;
use facetql::core::node::Node;
use facetql::storage::catalog::Catalog;
use facetql::storage::heap::{HeapRecord, RecordStore};

/// One data directory for this test binary, plus a lock that serializes
/// the tests that share it.
fn disk_guard() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        let dir = std::env::temp_dir()
            .join(format!("facetql-heap-test-{}", std::process::id()));

        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp data dir");

        facetql::config::set_data_dir(dir);
    });

    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn store() -> RecordStore {
    let catalog = Arc::new(Catalog::open().expect("open catalog"));

    RecordStore::open(catalog)
}

fn node(address: &str, data: String) -> Node {
    let mut n = Node::new(
        Coordinate::new(0, 0, 0, 0),
        address.to_string(),
        "Test".to_string(),
        "tester".to_string(),
    );

    n.data = data;
    n
}

#[test]
fn a_node_round_trips_through_the_heap() {
    let _g = disk_guard();
    let store = store();

    let original = node("roundtrip:1", r#"{"body":"hello"}"#.to_string());
    let location = store
        .append(&HeapRecord::Node(original.clone()))
        .expect("append");

    match store.read(location).expect("read") {
        HeapRecord::Node(got) => {
            assert_eq!(got.address, original.address);
            assert_eq!(got.data, original.data);
            assert_eq!(got.kind, original.kind);
            assert_eq!(got.owner, original.owner);
        }
        other => panic!("expected a node, got {other:?}"),
    }
}

#[test]
fn edges_and_history_round_trip_too() {
    let _g = disk_guard();
    let store = store();

    let edge = Edge::new(
        "eh:from".to_string(),
        "eh:to".to_string(),
        "follows".to_string(),
        "tester".to_string(),
    );

    let at = store.append(&HeapRecord::Edge(edge.clone())).expect("append edge");

    match store.read(at).expect("read edge") {
        HeapRecord::Edge(got) => assert_eq!(got.id(), edge.id()),
        other => panic!("expected an edge, got {other:?}"),
    }

    let entry = HistoryEntry {
        address: "eh:node".to_string(),
        archived_at_unix: 1_700_000_000,
        node: node("eh:node", "previous".to_string()),
        version: 7,
    };

    let at = store
        .append(&HeapRecord::History(entry.clone()))
        .expect("append history");

    match store.read(at).expect("read history") {
        HeapRecord::History(got) => {
            assert_eq!(got.address, entry.address);
            assert_eq!(got.version, entry.version);
            assert_eq!(got.node.data, entry.node.data);
        }
        other => panic!("expected history, got {other:?}"),
    }
}

#[test]
fn a_record_larger_than_a_page_survives_the_overflow_chain() {
    // The first test ever to write one. 64 KiB of payload is four pages
    // of chain past the stub.
    let _g = disk_guard();
    let store = store();

    let payload = "o".repeat(64 * 1024);
    let original = node("overflow:1", payload.clone());

    let location = store
        .append(&HeapRecord::Node(original))
        .expect("append oversized record");

    match store.read(location).expect("read oversized record") {
        HeapRecord::Node(got) => {
            assert_eq!(got.data.len(), payload.len(), "no bytes lost in the chain");
            assert_eq!(got.data, payload, "and none reordered");
            assert_eq!(got.address, "overflow:1");
        }
        other => panic!("expected a node, got {other:?}"),
    }
}

#[test]
fn overflow_records_of_many_sizes_all_read_back() {
    // Walks the boundary rather than picking one size: just under a page,
    // exactly around it, and several pages past it. The interesting
    // failures live within a few bytes of the page edge.
    let _g = disk_guard();
    let store = store();

    let sizes = [
        1usize,
        4_000,
        16_000,
        16_300,
        16_384,
        16_500,
        32_768,
        100_000,
        250_000,
    ];

    for (i, size) in sizes.iter().enumerate() {
        let payload = format!("{i:02}").repeat(size / 2 + 1);
        let payload = payload[..*size].to_string();

        let location = store
            .append(&HeapRecord::Node(node(&format!("sizes:{i}"), payload.clone())))
            .expect("append");

        match store.read(location).expect("read") {
            HeapRecord::Node(got) => {
                assert_eq!(got.data.len(), *size, "size {size} kept its length");
                assert_eq!(got.data, payload, "size {size} kept its bytes");
            }
            other => panic!("expected a node, got {other:?}"),
        }
    }
}

#[test]
fn many_records_spanning_many_pages_all_read_back() {
    // Enough records to fill dozens of pages, read back in an order
    // unrelated to the order they were written.
    let _g = disk_guard();
    let store = store();

    let mut located = Vec::new();

    for n in 0..2_000u32 {
        let payload = format!("{{\"n\":{n},\"pad\":\"{}\"}}", "p".repeat(200));

        let at = store
            .append(&HeapRecord::Node(node(&format!("many:{n}"), payload.clone())))
            .expect("append");

        located.push((n, at, payload));
    }

    // Strided read order, so page locality does not hide a mix-up.
    let stride = 617usize;
    let mut i = 0usize;

    for _ in 0..located.len() {
        let (n, at, payload) = &located[i];

        match store.read(*at).expect("read") {
            HeapRecord::Node(got) => {
                assert_eq!(got.address, format!("many:{n}"));
                assert_eq!(got.data, *payload);
            }
            other => panic!("expected a node, got {other:?}"),
        }

        i = (i + stride) % located.len();
    }
}

#[test]
fn a_scan_visits_every_record_written_to_a_segment() {
    // `scan_segment` is how compaction discovers what a segment holds, so
    // a scan that skipped a record would let compaction drop a live one.
    let _g = disk_guard();
    let store = store();

    let mut written = Vec::new();

    for n in 0..500u32 {
        let at = store
            .append(&HeapRecord::Node(node(&format!("scan:{n}"), format!("d{n}"))))
            .expect("append");

        written.push(at);
    }

    let segment = written[0].segment;
    let expected = written.iter().filter(|l| l.segment == segment).count();

    let mut seen = 0usize;
    store
        .scan_segment(segment, |_location, _record| {
            seen += 1;
            Ok(())
        })
        .expect("scan");

    assert!(
        seen >= expected,
        "the scan saw at least the {expected} records this test put in segment {segment} (saw {seen})",
    );
}

#[test]
fn the_segment_being_appended_to_is_never_a_compaction_candidate() {
    // Discovered by writing the test: no matter how much of the active
    // segment is dead, it is excluded. That is correct — draining the
    // segment currently receiving appends would move records out from
    // under the writer — and it is the reason a single-segment database
    // never compacts at all.
    let _g = disk_guard();
    let store = store();

    let mut located = Vec::new();

    for n in 0..800u32 {
        let at = store
            .append(&HeapRecord::Node(node(
                &format!("obsolete:{n}"),
                "x".repeat(500),
            )))
            .expect("append");

        located.push(at);
    }

    let segment = located[0].segment;

    assert!(
        !store.compaction_candidates(0.5).contains(&segment),
        "a fully live segment is not a candidate",
    );

    for at in located.iter().filter(|l| l.segment == segment) {
        store.mark_obsolete(*at);
    }

    assert!(
        !store.compaction_candidates(0.0).contains(&segment),
        "the active segment stays excluded even when entirely dead",
    );
}

#[test]
#[ignore = "writes 64 MiB to roll a segment; run with --ignored"]
fn rolling_a_segment_makes_the_retired_one_compactable() {
    // A segment caps at 4096 pages (64 MiB), so this is the first test
    // anywhere to produce a second segment — and therefore the first to
    // reach the compaction path at all, since the active segment is
    // always excluded.
    //
    // Records are deliberately oversized so each one consumes a chain of
    // pages and the roll arrives in hundreds of appends rather than
    // hundreds of thousands.
    let _g = disk_guard();
    let store = store();

    let chunk = "r".repeat(240 * 1024);
    let mut located = Vec::new();
    let first_segment = {
        let at = store
            .append(&HeapRecord::Node(node("roll:0", chunk.clone())))
            .expect("append");
        located.push(at);
        at.segment
    };

    let mut n = 1u32;
    while located.last().expect("non-empty").segment == first_segment {
        assert!(
            n < 1_000,
            "a segment should have rolled long before this ({n} oversized records \
             written with no roll)",
        );

        let at = store
            .append(&HeapRecord::Node(node(&format!("roll:{n}"), chunk.clone())))
            .expect("append");

        located.push(at);
        n += 1;
    }

    assert_ne!(
        located.last().expect("non-empty").segment,
        first_segment,
        "the heap rolled onto a new segment",
    );

    // Every record written before the roll must still read back — the
    // roll must not disturb what the retired segment already holds.
    for at in located.iter().filter(|l| l.segment == first_segment) {
        match store.read(*at).expect("read across the roll") {
            HeapRecord::Node(got) => assert_eq!(got.data.len(), chunk.len()),
            other => panic!("expected a node, got {other:?}"),
        }
    }

    // Now that it is retired rather than active, writing it off makes it
    // a candidate.
    assert!(
        !store.compaction_candidates(0.5).contains(&first_segment),
        "still live, so not yet a candidate",
    );

    for at in located.iter().filter(|l| l.segment == first_segment) {
        store.mark_obsolete(*at);
    }

    assert!(
        store.compaction_candidates(0.5).contains(&first_segment),
        "a retired segment written off past the ratio is a candidate",
    );
}

#[test]
fn syncing_makes_records_readable_from_a_reopened_store() {
    // The durability boundary for records: `sync` flushes the pager and
    // fsyncs the catalog, and after that a fresh `RecordStore` over the
    // same directory must find everything.
    let _g = disk_guard();

    let payload = "durable-".repeat(1_000);
    let location = {
        let store = store();

        let at = store
            .append(&HeapRecord::Node(node("durable:1", payload.clone())))
            .expect("append");

        store.sync().expect("sync");
        at
    };

    let reopened = store();

    match reopened.read(location).expect("read after reopen") {
        HeapRecord::Node(got) => {
            assert_eq!(got.address, "durable:1");
            assert_eq!(got.data, payload);
        }
        other => panic!("expected a node, got {other:?}"),
    }
}

#[test]
fn a_location_encodes_and_decodes_to_the_same_place() {
    use facetql::storage::location::{RecordLocation, LOCATION_LEN};

    let original = RecordLocation {
        segment: 9,
        page: 4_095,
        slot: 61_000,
        length: 1_234_567,
    };

    let bytes = original.encode();
    assert_eq!(bytes.len(), LOCATION_LEN);

    assert_eq!(RecordLocation::decode(&bytes).expect("decode"), original);

    // A short buffer must be refused, not read past.
    assert!(RecordLocation::decode(&bytes[..LOCATION_LEN - 1]).is_err());
}
