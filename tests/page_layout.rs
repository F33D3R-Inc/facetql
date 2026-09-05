//! Direct tests for the slotted page.
//!
//! The page is the smallest unit the whole engine is built out of: every
//! B+tree node, every heap record and every overflow link is cells in one
//! of these. It had no direct tests. The cases below are the ones whose
//! failure modes are silent — a cell that reads back short, a compaction
//! that moves bytes without moving the slot that points at them, a
//! corrupted page that decodes into plausible garbage instead of an
//! error.

use facetql::storage::page::{Page, PageKind, MAX_CELL_LEN, PAGE_BODY_LEN, PAGE_SIZE};

#[test]
fn a_fresh_page_is_empty_and_knows_its_kind() {
    for kind in [
        PageKind::Meta,
        PageKind::Leaf,
        PageKind::Branch,
        PageKind::Heap,
        PageKind::Overflow,
    ] {
        let page = Page::new(kind);

        assert_eq!(page.kind(), kind);
        assert_eq!(page.slot_count(), 0);
        assert!(page.cell(0).is_none());

        // `reclaimable_space` is what an insert could use *after* a
        // compaction, not the size of the holes. With no cells there are
        // no holes, so it equals the plain free space.
        assert_eq!(
            page.reclaimable_space(),
            page.free_space(),
            "an empty page has no holes to reclaim",
        );
    }
}

#[test]
fn cells_read_back_exactly_as_written() {
    let mut page = Page::new(PageKind::Heap);

    let payloads: Vec<Vec<u8>> = (0..64)
        .map(|n| format!("cell-{n}-{}", "x".repeat(n * 3)).into_bytes())
        .collect();

    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(page.push_cell(p), Some(i), "push returns the new slot index");
    }

    assert_eq!(page.slot_count(), payloads.len());

    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(page.cell(i), Some(p.as_slice()), "cell {i} byte-identical");
    }
}

#[test]
fn an_empty_cell_is_distinct_from_an_absent_one() {
    let mut page = Page::new(PageKind::Heap);

    page.push_cell(b"").expect("zero-length cell");

    assert_eq!(page.cell(0), Some(&b""[..]), "present and empty");
    assert_eq!(page.cell(1), None, "absent");
}

#[test]
fn free_space_falls_as_cells_are_added_and_bounds_what_fits() {
    let mut page = Page::new(PageKind::Heap);

    let mut last = page.free_space();
    assert!(last < PAGE_BODY_LEN, "the header is already accounted for");

    for n in 0..32 {
        page.push_cell(format!("{n:040}").as_bytes()).expect("push");

        let now = page.free_space();
        assert!(now < last, "free space strictly decreased on push {n}");
        last = now;
    }
}

#[test]
fn a_page_refuses_a_cell_it_cannot_hold_rather_than_truncating_it() {
    let mut page = Page::new(PageKind::Heap);

    // Fill until it says no.
    let chunk = vec![b'z'; 512];
    let mut pushed = 0usize;

    while page.push_cell(&chunk).is_some() {
        pushed += 1;
        assert!(pushed < 1_000, "page should have filled long before this");
    }

    assert!(pushed > 0, "at least one cell fit");

    // Everything that was accepted is still intact and unmodified.
    assert_eq!(page.slot_count(), pushed);

    for i in 0..pushed {
        assert_eq!(page.cell(i), Some(chunk.as_slice()), "cell {i} undamaged by the refusal");
    }
}

#[test]
fn a_cell_at_the_documented_maximum_fits_an_empty_page() {
    let mut page = Page::new(PageKind::Heap);
    let biggest = vec![b'm'; MAX_CELL_LEN];

    assert_eq!(page.push_cell(&biggest), Some(0), "MAX_CELL_LEN fits exactly");
    assert_eq!(page.cell(0), Some(biggest.as_slice()));

    // And one byte more does not.
    let mut fresh = Page::new(PageKind::Heap);
    let too_big = vec![b'm'; MAX_CELL_LEN + 1];

    assert_eq!(fresh.push_cell(&too_big), None, "one byte over is refused");
    assert_eq!(fresh.slot_count(), 0, "the refusal left no slot behind");
}

#[test]
fn removing_a_cell_makes_space_reclaimable_and_compaction_reclaims_it() {
    let mut page = Page::new(PageKind::Heap);

    for n in 0..16 {
        page.push_cell(format!("payload-{n:03}-{}", "y".repeat(200)).as_bytes())
            .expect("push");
    }

    let before_free = page.free_space();

    assert_eq!(
        page.reclaimable_space(),
        before_free,
        "a densely packed page has no holes",
    );

    page.remove_cell(4);
    page.remove_cell(9);

    // The removed cells' bytes are now holes: still counted as
    // reclaimable, but not yet usable by an insert.
    assert!(
        page.reclaimable_space() > page.free_space(),
        "the removed cells are holes — reclaimable but not yet free",
    );

    page.compact();

    assert_eq!(
        page.reclaimable_space(),
        page.free_space(),
        "compaction turned every hole into contiguous free space",
    );
    assert!(page.free_space() > before_free, "the space came back as free");
}

#[test]
fn compaction_moves_bytes_without_renumbering_slots() {
    // This is the invariant `RecordLocation.slot` depends on: a heap
    // record's address survives compaction of the page it lives in. If
    // compaction renumbered, every index entry pointing into the page
    // would silently address the wrong record.
    let mut page = Page::new(PageKind::Heap);

    let payloads: Vec<Vec<u8>> = (0..12)
        .map(|n| format!("record-{n}-{}", "q".repeat(100)).into_bytes())
        .collect();

    for p in &payloads {
        page.push_cell(p).expect("push");
    }

    // Holes are made the way a heap makes them — by replacing a record
    // with a shorter one. A heap page must never call `remove_cell`,
    // which renumbers (see the test below), because a `RecordLocation`
    // names a slot.
    assert!(page.replace_cell(2, b"short"));
    assert!(page.replace_cell(7, b"short"));

    let slots_before = page.slot_count();
    page.compact();

    assert_eq!(page.slot_count(), slots_before, "slot numbering unchanged");

    for (i, p) in payloads.iter().enumerate() {
        let expected: &[u8] = if i == 2 || i == 7 { b"short" } else { p.as_slice() };

        assert_eq!(
            page.cell(i),
            Some(expected),
            "slot {i} still addresses its own record after compaction",
        );
    }
}

#[test]
fn remove_cell_renumbers_which_is_why_heap_pages_never_call_it() {
    // Pinning the hazard rather than only documenting it. A heap record's
    // durable address is (segment, page, slot); if a heap page ever
    // removed a cell, every location after it would silently point one
    // record to the left.
    let mut page = Page::new(PageKind::Leaf);

    for n in 0..6 {
        page.push_cell(format!("c{n}").as_bytes()).expect("push");
    }

    page.remove_cell(2);

    assert_eq!(page.slot_count(), 5);
    assert_eq!(page.cell(1), Some(&b"c1"[..]), "slots before the hole are fixed");
    assert_eq!(
        page.cell(2),
        Some(&b"c3"[..]),
        "slots after the hole shift down by one — the renumbering that makes \
         this unsafe for a heap page",
    );
}

#[test]
fn replacing_a_cell_in_place_keeps_the_others_addressable() {
    let mut page = Page::new(PageKind::Heap);

    for n in 0..8 {
        page.push_cell(format!("original-{n}").as_bytes()).expect("push");
    }

    assert!(page.replace_cell(3, b"replaced-with-a-longer-payload"));

    assert_eq!(page.cell(3), Some(&b"replaced-with-a-longer-payload"[..]));
    assert_eq!(page.cell(0), Some(&b"original-0"[..]));
    assert_eq!(page.cell(7), Some(&b"original-7"[..]));
    assert_eq!(page.slot_count(), 8);
}

#[test]
fn insert_shifts_later_slots_by_one() {
    let mut page = Page::new(PageKind::Leaf);

    for n in 0..6 {
        page.push_cell(format!("c{n}").as_bytes()).expect("push");
    }

    assert!(page.insert_cell(2, b"inserted"));

    assert_eq!(page.slot_count(), 7);
    assert_eq!(page.cell(1), Some(&b"c1"[..]));
    assert_eq!(page.cell(2), Some(&b"inserted"[..]));
    assert_eq!(page.cell(3), Some(&b"c2"[..]), "the old slot 2 moved to 3");
    assert_eq!(page.cell(6), Some(&b"c5"[..]));
}

#[test]
fn the_extra_word_survives_a_round_trip() {
    // Each page kind uses `extra` for its own purpose — an overflow link,
    // a record count. It is part of the page and has to encode with it.
    let mut page = Page::new(PageKind::Overflow);

    page.set_extra(0xDEAD_BEEF);
    assert_eq!(page.extra(), 0xDEAD_BEEF);

    let bytes = page.encode().to_vec();
    let decoded = Page::decode(&bytes).expect("decode");

    assert_eq!(decoded.extra(), 0xDEAD_BEEF);
    assert_eq!(decoded.kind(), PageKind::Overflow);
}

#[test]
fn encode_then_decode_reproduces_the_page() {
    let mut page = Page::new(PageKind::Leaf);

    let payloads: Vec<Vec<u8>> = (0..40)
        .map(|n| format!("k{n:04}={}", "v".repeat(n * 7)).into_bytes())
        .collect();

    for p in &payloads {
        page.push_cell(p).expect("push");
    }

    let bytes = page.encode().to_vec();
    assert_eq!(bytes.len(), PAGE_BODY_LEN, "a page encodes to exactly one body");

    let decoded = Page::decode(&bytes).expect("decode");

    assert_eq!(decoded.kind(), PageKind::Leaf);
    assert_eq!(decoded.slot_count(), payloads.len());

    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(decoded.cell(i), Some(p.as_slice()), "cell {i} round-tripped");
    }
}

#[test]
fn an_empty_page_round_trips() {
    let mut page = Page::new(PageKind::Heap);
    let bytes = page.encode().to_vec();

    let decoded = Page::decode(&bytes).expect("decode empty");

    assert_eq!(decoded.slot_count(), 0);
    assert_eq!(decoded.kind(), PageKind::Heap);
}

#[test]
fn a_corrupted_page_fails_to_decode_rather_than_returning_garbage() {
    // The CRC is the only thing standing between bit-rot and an answer
    // that looks real. A page that decoded a flipped bit into a plausible
    // cell would hand corruption to the caller as data.
    let mut page = Page::new(PageKind::Heap);

    for n in 0..20 {
        page.push_cell(format!("payload-{n}").as_bytes()).expect("push");
    }

    let clean = page.encode().to_vec();
    assert!(Page::decode(&clean).is_ok(), "the clean page decodes");

    // Flip a bit in the cell region, well past the header.
    let mut torn = clean.clone();
    let target = torn.len() - 32;
    torn[target] ^= 0b0001_0000;

    assert!(
        Page::decode(&torn).is_err(),
        "a single flipped bit in the body is caught",
    );
}

#[test]
fn a_page_with_a_wrong_magic_is_refused() {
    let mut page = Page::new(PageKind::Heap);
    page.push_cell(b"x").expect("push");

    let mut bytes = page.encode().to_vec();
    bytes[0] ^= 0xFF;

    assert!(Page::decode(&bytes).is_err(), "the magic is checked");
}

#[test]
fn a_short_buffer_is_refused_rather_than_read_out_of_bounds() {
    let mut page = Page::new(PageKind::Heap);
    page.push_cell(b"x").expect("push");

    let bytes = page.encode().to_vec();

    for len in [0usize, 1, 8, 23, PAGE_BODY_LEN - 1] {
        assert!(
            Page::decode(&bytes[..len.min(bytes.len())]).is_err(),
            "a {len}-byte buffer is not a page",
        );
    }
}

#[test]
fn the_size_constants_agree_with_each_other() {
    // These four numbers are load-bearing across the pager, the heap and
    // the B+tree, and a change to one without the others is a silent
    // on-disk format change.
    // `const` blocks so these fail the build rather than a test run: an
    // on-disk format constant that has drifted should not be something
    // you discover by running something.
    const { assert!(PAGE_SIZE == 16 * 1024) };
    const { assert!(PAGE_BODY_LEN < PAGE_SIZE, "the envelope costs something") };
    const { assert!(MAX_CELL_LEN < PAGE_BODY_LEN, "a cell cannot be a whole body") };
}
