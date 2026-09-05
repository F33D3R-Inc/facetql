//! Direct tests for the buffer pool.
//!
//! The pager is bounded at 256 pages per file by default (4 MiB). Every
//! test that existed before this file worked on fixtures of about sixty
//! records — three or four pages — so **eviction had never once run**.
//! That matters more than it sounds: eviction is the only path that
//! writes a dirty page back to disk outside an explicit `flush`, so a
//! bug there does not fail loudly, it silently discards writes that the
//! caller was told had succeeded.
//!
//! Every test below therefore works past the cache bound on purpose.

use std::path::PathBuf;
use std::sync::Arc;

use facetql::storage::page::{Page, PageKind};
use facetql::storage::pager::Pager;

/// Comfortably more pages than the 256-page default cache, so that by
/// the time the last one is written the first has certainly been evicted.
const PAST_CACHE: u32 = 1_000;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "facetql-pager-{}-{}",
        std::process::id(),
        name,
    ));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    dir
}

/// A page whose contents identify which page it is, so a mix-up between
/// two evicted pages shows up as a wrong value rather than as nothing.
fn stamped(id: u32, tag: &str) -> Page {
    let mut page = Page::new(PageKind::Heap);

    page.set_extra(id);
    page.push_cell(format!("page-{id}-{tag}").as_bytes())
        .expect("stamp fits");

    page
}

fn expect_stamp(page: &Arc<Page>, id: u32, tag: &str) {
    assert_eq!(page.extra(), id, "page {id} carries its own extra word");
    assert_eq!(
        page.cell(0),
        Some(format!("page-{id}-{tag}").as_bytes()),
        "page {id} carries its own contents",
    );
}

#[test]
fn allocate_hands_out_distinct_ids_and_tracks_the_count() {
    let dir = scratch("allocate");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    let mut ids = Vec::new();

    for _ in 0..64 {
        ids.push(pager.allocate());
    }

    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();

    assert_eq!(sorted.len(), ids.len(), "no id handed out twice");
    assert_eq!(pager.page_count(), 64, "the count follows allocation");
}

#[test]
fn a_page_reads_back_while_it_is_still_cached() {
    let dir = scratch("cached");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    let id = pager.allocate();
    pager.write(id, stamped(id, "hot")).expect("write");

    expect_stamp(&pager.read(id).expect("read"), id, "hot");
}

#[test]
fn every_page_survives_being_evicted_and_read_back() {
    // The core of the whole file. Writing a thousand pages through a
    // 256-page cache means roughly three quarters of them are evicted —
    // each one dirty — before anything is read. If the writeback on
    // eviction were wrong, the reads below would return stale or empty
    // pages while every `write` had returned `Ok`.
    let dir = scratch("evict-writeback");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    for id in 0..PAST_CACHE {
        let allocated = pager.allocate();
        assert_eq!(allocated, id, "ids are dense and ascending");

        pager.write(id, stamped(id, "v1")).expect("write");
    }

    // Read in the same order they were written, so the earliest pages —
    // the ones evicted longest ago — are checked first.
    for id in 0..PAST_CACHE {
        expect_stamp(&pager.read(id).expect("read"), id, "v1");
    }

    // And again in reverse, which re-evicts everything a second time.
    for id in (0..PAST_CACHE).rev() {
        expect_stamp(&pager.read(id).expect("read"), id, "v1");
    }
}

#[test]
fn rewriting_an_evicted_page_replaces_it_rather_than_resurrecting_the_old_one() {
    // A page written, evicted, then written again must not be able to
    // come back with its first contents — which is what happens if a
    // stale cache entry outlives its writeback.
    let dir = scratch("rewrite-evicted");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    for id in 0..PAST_CACHE {
        pager.allocate();
        pager.write(id, stamped(id, "v1")).expect("write v1");
    }

    // Rewrite every other page. The untouched ones are the control.
    for id in (0..PAST_CACHE).step_by(2) {
        pager.write(id, stamped(id, "v2")).expect("write v2");
    }

    for id in 0..PAST_CACHE {
        let tag = if id % 2 == 0 { "v2" } else { "v1" };
        expect_stamp(&pager.read(id).expect("read"), id, tag);
    }
}

#[test]
fn random_access_across_the_cache_bound_is_consistent() {
    // Sequential access is the kind case for an LRU. Strided access with
    // a stride coprime to the cache size defeats it deliberately, so
    // nearly every read is a miss and every miss must be correct.
    let dir = scratch("strided");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    for id in 0..PAST_CACHE {
        pager.allocate();
        pager.write(id, stamped(id, "v1")).expect("write");
    }

    let stride = 397u32; // coprime with 1000
    let mut id = 0u32;

    for _ in 0..(PAST_CACHE * 2) {
        expect_stamp(&pager.read(id).expect("read"), id, "v1");
        id = (id + stride) % PAST_CACHE;
    }
}

#[test]
fn a_flushed_pager_reopens_with_every_page_intact() {
    let dir = scratch("reopen");
    let path = dir.join("f.fqp");

    {
        let pager = Pager::open(&path).expect("open");

        for id in 0..PAST_CACHE {
            pager.allocate();
            pager.write(id, stamped(id, "durable")).expect("write");
        }

        pager.flush().expect("flush");
    }

    let pager = Pager::open(&path).expect("reopen");

    assert_eq!(pager.page_count(), PAST_CACHE, "the page count is on disk");

    for id in 0..PAST_CACHE {
        expect_stamp(&pager.read(id).expect("read"), id, "durable");
    }
}

#[test]
fn set_page_count_is_reflected_on_reopen() {
    // Truncation bookkeeping: the heap sets this when a segment's length
    // changes, and it has to survive a restart or the segment's tail
    // becomes unreachable.
    let dir = scratch("page-count");
    let path = dir.join("f.fqp");

    {
        let pager = Pager::open(&path).expect("open");

        for id in 0..16 {
            pager.allocate();
            pager.write(id, stamped(id, "x")).expect("write");
        }

        pager.set_page_count(16);
        pager.flush().expect("flush");
    }

    let pager = Pager::open(&path).expect("reopen");
    assert_eq!(pager.page_count(), 16);
}

#[test]
fn a_fresh_file_starts_empty() {
    let dir = scratch("fresh");
    let pager = Pager::open(&dir.join("brand-new.fqp")).expect("open");

    assert_eq!(pager.page_count(), 0, "a new file has no pages");
}

#[test]
fn two_pagers_on_two_files_do_not_share_a_cache() {
    // The cache is per open file. If it were keyed by page id alone,
    // page 3 of one segment would answer a read of page 3 of another —
    // the kind of bug that only appears once a database has more than
    // one segment, which no fixture here has ever had.
    let dir = scratch("two-files");

    let a = Pager::open(&dir.join("a.fqp")).expect("open a");
    let b = Pager::open(&dir.join("b.fqp")).expect("open b");

    for id in 0..300 {
        a.allocate();
        b.allocate();

        a.write(id, stamped(id, "from-a")).expect("write a");
        b.write(id, stamped(id, "from-b")).expect("write b");
    }

    for id in 0..300 {
        expect_stamp(&a.read(id).expect("read a"), id, "from-a");
        expect_stamp(&b.read(id).expect("read b"), id, "from-b");
    }
}

#[test]
fn a_full_page_survives_the_round_trip_through_eviction() {
    // Eviction writes exactly PAGE_SIZE bytes. A page filled to its slot
    // limit is the case where an off-by-one in the envelope arithmetic
    // would truncate the last cell.
    let dir = scratch("full-pages");
    let pager = Pager::open(&dir.join("f.fqp")).expect("open");

    let chunk = vec![b'f'; 256];
    let mut cells_per_page = 0usize;

    for id in 0..400u32 {
        pager.allocate();

        let mut page = Page::new(PageKind::Heap);
        page.set_extra(id);

        let mut n = 0usize;
        while page.push_cell(&chunk).is_some() {
            n += 1;
        }

        if id == 0 {
            cells_per_page = n;
            assert!(cells_per_page > 1, "the page held more than one cell");
        }

        assert_eq!(n, cells_per_page, "every page filled to the same capacity");
        pager.write(id, page).expect("write");
    }

    for id in 0..400u32 {
        let page = pager.read(id).expect("read");

        assert_eq!(page.extra(), id);
        assert_eq!(page.slot_count(), cells_per_page, "no cell lost in transit");

        for i in 0..cells_per_page {
            assert_eq!(page.cell(i), Some(chunk.as_slice()), "cell {i} of page {id}");
        }
    }
}
