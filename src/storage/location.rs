//! Where a record physically is.
//!
//! FacetQL's logical identity for a node is its `address`, and for an
//! edge it is `(from, to, kind)`. Neither says anything about where the
//! bytes live, and that separation is deliberate: storage is
//! append-oriented, so updating a node writes a *new* record and leaves
//! the old one in place until compaction reclaims it. A logical identity
//! that doubled as a physical offset would be invalidated by every
//! write.
//!
//! So identity is stable and location is not, and an index is the thing
//! that maps one to the other:
//!
//! ```text
//!   address ──primary index──► RecordLocation ──heap──► record bytes
//! ```
//!
//! A location is explicit about all four coordinates it needs — which
//! segment file, which page in it, which slot in that page, and how long
//! the record is. It is deliberately not a bare byte offset: with the
//! heap split into segments that compaction creates and retires, "offset
//! 91422" does not identify anything on its own.

use std::io::{Error, ErrorKind, Result};

/// Encoded width of a [`RecordLocation`]: segment(4) + page(4) +
/// slot(2) + length(4).
pub const LOCATION_LEN: usize = 14;

/// The physical address of one record in the heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordLocation {
    /// Which heap segment file holds it.
    pub segment: u32,
    /// Which page within that segment.
    pub page: u32,
    /// Which slot within that page. Slot numbering in a heap page is
    /// stable for the life of the page — heap pages never remove or
    /// renumber cells — which is what lets a location stay valid until
    /// the record is superseded.
    pub slot: u16,
    /// On-disk length of the record frame, carried so compaction can
    /// account for reclaimable bytes without reading the record.
    pub length: u32,
}

impl RecordLocation {
    pub fn encode(&self) -> [u8; LOCATION_LEN] {
        let mut out = [0u8; LOCATION_LEN];

        out[0..4].copy_from_slice(&self.segment.to_le_bytes());
        out[4..8].copy_from_slice(&self.page.to_le_bytes());
        out[8..10].copy_from_slice(&self.slot.to_le_bytes());
        out[10..14].copy_from_slice(&self.length.to_le_bytes());

        out
    }

    pub fn decode(bytes: &[u8]) -> Result<RecordLocation> {
        if bytes.len() != LOCATION_LEN {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "record location is {} bytes, expected {LOCATION_LEN}",
                    bytes.len()
                ),
            ));
        }

        Ok(RecordLocation {
            segment: u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")),
            page: u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
            slot: u16::from_le_bytes(bytes[8..10].try_into().expect("2 bytes")),
            length: u32::from_le_bytes(bytes[10..14].try_into().expect("4 bytes")),
        })
    }
}
