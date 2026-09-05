//! The durable change scan behind `GET /changes`.
//!
//! `GET /events` is the live push feed: an in-memory ring of
//! [`crate::database::EVENT_REPLAY_CAPACITY`] events, a position on
//! every frame, and a 410 for a resume it can no longer honour. That
//! 410 is honest, and it is also the end of the road for a consumer
//! that fell behind — which is exactly what a large cell copy under
//! load does.
//!
//! This module is the other half: a **pull** endpoint that reconstructs
//! committed node mutations from the write-ahead log itself, so a
//! consumer that outran the ring can still catch up without re-reading
//! the whole database. It is deliberately not "a bigger ring". It reads
//! different bytes, answers a different question, and has a different
//! (and separately stated) retention horizon.
//!
//! # What a change record is, and why it is this thin
//!
//! ```text
//!     { "seq": 41, "change": "created", "address": "Post:1", "kind": "Post" }
//! ```
//!
//! Address, kind, and the position — nothing else. The consumer this
//! exists for is Fabric's cell mover, and what it does with a change is
//! put the address in a dirty set and re-read it from the source with
//! `POST /nodes/multiget`; it never reads a payload out of the feed.
//! Carrying the node body here would be a second copy of the data path
//! with its own visibility rules to get wrong, for a field nobody
//! consumes. `kind` *is* carried, including on a delete, because the
//! mover cannot otherwise decide whether an address belongs to the cell
//! it is moving (`CellScope::may_contain_address` exists purely because
//! `/events` withholds the kind on updates and deletes).
//!
//! # Only committed work is visible
//!
//! The WAL contains the records of transactions that never committed —
//! a crash between `BEGIN` and `COMMIT` leaves a frame recovery
//! discards. Surfacing one of its mutations would report a write that
//! never happened, which is worse than reporting nothing.
//!
//! So this does not re-derive "committed". It calls the one function
//! that already decides it —
//! [`crate::storage::recovery::classify_transactions`], the same pass
//! recovery runs before replaying anything — and emits a framed
//! mutation only when that pass says its frame committed. Standalone
//! records (`transaction_id == 0`) are whole transactions by
//! definition and are always committed.
//!
//! The **live tail** is the case that matters at runtime rather than
//! after a crash: a frame whose `COMMIT` has not been appended yet at
//! the instant the log was read. That frame is in-flight, not dead, so
//! the scan stops in front of it rather than skipping past it — see
//! [`FrameState`]. An incomplete frame with records *after* it cannot
//! be in-flight (a frame is written under the engine's writer mutex, so
//! frames never interleave), which means its writer is gone; that one
//! is terminal and is skipped.
//!
//! # Authorization
//!
//! A change feed is a read channel. If it did not filter, any valid
//! token could enumerate the address, kind and timing of every private
//! node in the database — data the same token is refused on
//! `GET /node/:address`.
//!
//! The rule is [`Audience::admits`], the same one `/events` applies,
//! reached through the same [`Audience::for_node`] so there is one
//! visibility model rather than two that drift:
//!
//!   * an `Insert` carries the whole node, so the audience is read
//!     straight off it;
//!   * a `Delete` carries only an address — but every delete of a node
//!     that *existed* is preceded, in the same durable unit, by the
//!     `Archive` of the state it removed (see
//!     `StorageEngine::lower_delete_closure`), and that archive carries
//!     the full previous node. The audience comes from there.
//!   * a `Delete` with no such archive removed nothing. It is omitted
//!     entirely — both because it is not a change and because an
//!     unattributable one must fail closed rather than be guessed at.
//!
//! The filter is applied *inside* [`scan`], before the page is
//! assembled, and [`ChangeRecord`] carries no audience field. There is
//! deliberately no way to obtain an unfiltered page: a caller cannot
//! forget to filter what it was never handed.
//!
//! Page positions leak nothing either. `next` is the position of the
//! last change **this caller was actually shown**, never the newest
//! record in the log, so the reply carries no signal about another
//! tenant's write volume. That is also why a unit is always walked in
//! full even when part of it is invisible: an `Archive` the caller may
//! not see is still what attributes the `Delete` that follows it.

use std::collections::HashMap;
use std::io;

use serde::Serialize;

use crate::core::node::Node;
use crate::database::Audience;
use crate::storage::recovery;
use crate::storage::wal::{self, WalOperation, WalRecord, STANDALONE_TRANSACTION_ID};

/// Changes one call will return before it asks the caller to come back.
///
/// Whole durable units are never split across pages (see [`scan`]), so a
/// page may overrun this by the tail of one transaction frame. That is
/// the deliberate trade: an exact page size is worth less than the
/// guarantee that a `Delete` is never separated from the `Archive` that
/// says who owned it.
pub const DEFAULT_CHANGE_LIMIT: usize = 500;

/// The largest page a caller may ask for.
pub const MAX_CHANGE_LIMIT: usize = 2000;

/// What happened to a node.
///
/// The same vocabulary `/events` uses for its node frames, so a consumer
/// can share one match arm between the two channels.
///
/// `created` vs `updated` is exact rather than a guess: the WAL's
/// `Insert` is an upsert, and the engine archives the previous state
/// immediately ahead of an insert that replaces one. An `Insert`
/// preceded by an `Archive` of the same address in the same durable unit
/// therefore replaced something; one without did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeKind {
    Created,
    Updated,
    Deleted,
}

/// One committed node mutation, as a consumer sees it.
///
/// Carries no audience: [`scan`] has already applied it. See the module
/// docs for why that is a shape rather than a convention.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeRecord {
    /// This change's position, drawn from the same
    /// [`wal::next_operation_id`] counter that stamps `/events` frames
    /// with their `seq`. That shared counter is the whole reason the two
    /// channels can be bridged: a position from one is comparable with a
    /// position from the other.
    ///
    /// **Positions are not contiguous**, here for the same reason they
    /// are not contiguous on `/events`: the counter is burned by every
    /// WAL record and every history version, so a consumer can never
    /// infer a gap by arithmetic. `after=` means strictly greater than.
    pub seq: u64,

    pub change: ChangeKind,

    pub address: String,

    /// The node's kind — from the inserted node for a create or update,
    /// and from the archived previous state for a delete.
    pub kind: String,
}

/// One page of the durable change scan.
#[derive(Debug, Serialize)]
pub struct ChangePage {
    pub changes: Vec<ChangeRecord>,

    /// Where to resume: the position of the last change in `changes`,
    /// or the `after` that was asked for when the page is empty.
    ///
    /// Deliberately *not* the newest record the scan examined. See the
    /// module docs — the last visible position is enough to resume
    /// without a gap, and it is the only one that discloses nothing
    /// about writes this caller may not read.
    pub next: u64,

    /// True when the scan reached the end of the settled log: there is
    /// nothing more for this caller right now, and `next` may be handed
    /// to `GET /events?after=` to continue live.
    ///
    /// False means only that the page filled. Call again from `next`.
    pub complete: bool,
}

/// Why a scan was refused: the position asked for is older than
/// anything the log still holds.
///
/// The same discipline as [`crate::database::ResumeTooOld`], and for the
/// same reason: a scan that silently started from the oldest record it
/// happened to have looks *exactly* like a complete one, and the caller
/// would carry on believing it had seen everything.
#[derive(Debug)]
pub struct ScanTooOld {
    /// The `after` the caller asked for.
    pub requested: u64,

    /// The oldest position this log can still serve. Every change after
    /// this one is still in the log.
    pub earliest: u64,

    /// How many WAL records are retained right now.
    pub retained: usize,
}

impl std::fmt::Display for ScanTooOld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot scan changes from {}: the oldest position the \
             write-ahead log still holds is {} ({} records retained). The \
             log is checkpointed and rotated, so it is not infinite \
             either — the changes between those two positions are gone \
             from it and this server will not pretend otherwise by \
             starting from the oldest record it happens to have. \
             Reconcile from a full read instead.",
            self.requested, self.earliest, self.retained,
        )
    }
}

impl std::error::Error for ScanTooOld {}

/// Why a scan could not be served.
#[derive(Debug)]
pub enum ScanError {
    /// The requested position is behind the log's retention horizon.
    TooOld(ScanTooOld),

    /// The log could not be read, or describes a history that cannot be
    /// classified. Not the caller's fault and not survivable by
    /// retrying with a different `after`.
    Log(io::Error),
}

impl From<io::Error> for ScanError {
    fn from(error: io::Error) -> Self {
        ScanError::Log(error)
    }
}

/// Scan the durable log for committed node changes after `after`.
///
/// Returns at most `limit` changes visible to the identity
/// `(owner, is_admin)`, in position order, plus where to resume.
///
/// # Cost, and why a long scan cannot stall a writer
///
/// This reads the log, not the heap. [`wal::read_all`] takes **no lock
/// at all** — it is a plain `fs::read` of the WAL file — so it holds
/// nothing an appender wants: not the engine's writer mutex, not the
/// WAL's cached-handle mutex (which `append` holds only for the
/// duration of one `write_all`), and not the group-commit condvar. A
/// scan of a full log and a burst of writes proceed concurrently.
///
/// What it *does* cost is a whole-file read, hex decode, AEAD decrypt
/// and bincode of every retained record, per call. There is no index
/// over the log — a record's position is inside its encrypted payload —
/// so the scan cannot seek to `after`. That cost is bounded by
/// [`wal::rotate_threshold`] (64 MiB by default), which is why
/// `/changes` is classified as a bulk endpoint and rate-limited as one.
///
/// A concurrent append is safe to read across: the writer emits one
/// hex line per record in a single `write_all`, so the worst a reader
/// can catch is a partial final line, which the frame envelope
/// classifies as a torn tail and this ignores. Nothing here truncates
/// or repairs the log — that is recovery's job, at startup, and doing
/// it from a read path would let a reader delete a writer's bytes.
pub fn scan(
    after: u64,
    limit: usize,
    owner: &str,
    is_admin: bool,
) -> Result<ChangePage, ScanError> {
    // The tail is deliberately dropped rather than repaired: see above.
    let (records, _tail) = wal::read_all()?;

    let earliest = horizon(&records);

    if after < earliest {
        return Err(ScanError::TooOld(ScanTooOld {
            requested: after,
            earliest,
            retained: records.len(),
        }));
    }

    validate_positions(&records)?;

    // The one definition of "committed", borrowed from recovery rather
    // than restated here.
    let committed = recovery::classify_transactions(&records)?;

    let lens = Lens { after, owner, is_admin };

    let mut page = ChangePage {
        changes: Vec::new(),
        next: after,
        complete: true,
    };

    let mut index = 0;

    while index < records.len() {
        let unit = durable_unit(&records, index);
        let next_index = index + unit.len();

        match frame_state(unit, &committed, next_index == records.len()) {
            // In flight: its COMMIT may still be on its way, so the scan
            // ends in front of it rather than deciding for it. The page
            // is still complete — there is nothing settled after this.
            FrameState::InFlight => break,

            FrameState::Terminal => {
                index = next_index;
                continue;
            }

            FrameState::Committed => {}
        }

        index = next_index;

        // Whole units are skipped only when *every* record in them is at
        // or below `after`. A unit that straddles `after` is walked in
        // full so its archives still attribute its deletes.
        let last = unit.last().map(|record| record.operation_id).unwrap_or(0);

        if last <= after {
            continue;
        }

        emit_unit(unit, &lens, &mut page.changes);

        if let Some(change) = page.changes.last() {
            page.next = change.seq;
        }

        if page.changes.len() >= limit && index < records.len() {
            page.complete = false;
            break;
        }
    }

    Ok(page)
}

/// The oldest position this log can serve `after=` from.
///
/// Two cases, and the distinction is what keeps the refusal from firing
/// on a database that has lost nothing:
///
///   * the very first record ever written is still present
///     (`sequence == 1`, and sequences start at 1 and are only ever
///     advanced past durable ones), so the log is complete from the
///     beginning of this database's life and **any** position can be
///     served;
///   * otherwise a prefix may have been checkpointed away by
///     [`wal::rotate`], and the oldest record still here is the oldest
///     thing that can be vouched for.
///
/// An empty log falls back to [`wal::scan_horizon`], which recovery
/// stamps when it opens on a log whose records were already removed.
fn horizon(records: &[WalRecord]) -> u64 {
    match records.first() {
        Some(first) if first.sequence == 1 => 0,
        Some(first) => first.operation_id,
        None => wal::scan_horizon(),
    }
}

/// Refuse a log whose operation IDs do not ascend in file order.
///
/// The scan pages by position and resumes with "strictly greater than",
/// which is only a prefix of the log while file order and position order
/// agree. They do agree — every identifier is allocated under the
/// engine's writer mutex, in the order the records are appended — but
/// this is the assumption a wrong page would silently skip records over,
/// so it is checked rather than assumed. `recovery::validate_sequence`
/// does the same thing for `sequence`, for the same reason.
fn validate_positions(records: &[WalRecord]) -> io::Result<()> {
    for pair in records.windows(2) {
        let (previous, record) = (&pair[0], &pair[1]);

        if record.operation_id <= previous.operation_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL operation ID {} follows {} at sequence {}; the \
                     change scan pages by position and requires file order \
                     and position order to be the same order",
                    record.operation_id, previous.operation_id, record.sequence,
                ),
            ));
        }
    }

    Ok(())
}

/// One durable unit starting at `start`: a standalone record on its own,
/// or the whole run of records belonging to one transaction frame.
///
/// A frame is written under the engine's writer mutex from `BEGIN` to
/// `COMMIT`, so frames never interleave and a frame is exactly the
/// contiguous run carrying its transaction ID.
fn durable_unit(records: &[WalRecord], start: usize) -> &[WalRecord] {
    let transaction_id = records[start].transaction_id;

    if transaction_id == STANDALONE_TRANSACTION_ID {
        return &records[start..start + 1];
    }

    let mut end = start;

    while end < records.len() && records[end].transaction_id == transaction_id {
        end += 1;
    }

    &records[start..end]
}

/// What a durable unit is, for the purpose of a scan.
enum FrameState {
    /// Committed, and its mutations may be reported.
    Committed,

    /// Never committed and never will be — aborted, or abandoned by a
    /// writer that is gone. Skipped.
    Terminal,

    /// Not committed *yet*: it is the last thing in the log, so its
    /// COMMIT may simply not have been appended at the instant the log
    /// was read. The scan stops in front of it.
    InFlight,
}

fn frame_state(
    unit: &[WalRecord],
    committed: &recovery::CommittedTransactions,
    is_tail: bool,
) -> FrameState {
    let Some(first) = unit.first() else {
        return FrameState::Terminal;
    };

    // A standalone record is a whole transaction on its own.
    if first.transaction_id == STANDALONE_TRANSACTION_ID {
        return FrameState::Committed;
    }

    if committed.contains_key(&first.transaction_id) {
        return FrameState::Committed;
    }

    let aborted = unit
        .iter()
        .any(|record| matches!(record.operation, WalOperation::Abort));

    if aborted || !is_tail {
        // Explicitly aborted, or incomplete with records written after
        // it — which a live frame cannot be, because its writer holds
        // the writer mutex until COMMIT.
        return FrameState::Terminal;
    }

    FrameState::InFlight
}

/// Who is looking, and from where.
///
/// Bundled rather than passed as four loose arguments because the two
/// halves are one decision — a change is emitted when it is *both* after
/// the caller's position and admitted to the caller's audience — and
/// splitting them across a long parameter list is how one of them gets
/// dropped at a new call site.
struct Lens<'a> {
    after: u64,
    owner: &'a str,
    is_admin: bool,
}

impl Lens<'_> {
    /// One change, if this caller may be shown it.
    ///
    /// `subject` is the node the audience is derived from: the inserted
    /// node for a create or update, and the *archived previous state*
    /// for a delete, which is the only record of who owned what a delete
    /// removed.
    fn admit(
        &self,
        seq: u64,
        change: ChangeKind,
        subject: &Node,
        address: &str,
    ) -> Option<ChangeRecord> {
        if seq <= self.after {
            return None;
        }

        if !Audience::for_node(subject).admits(self.owner, self.is_admin) {
            return None;
        }

        Some(ChangeRecord {
            seq,
            change,
            address: address.to_string(),
            kind: subject.kind.clone(),
        })
    }
}

/// Turn one committed unit into the changes this caller may see.
///
/// The unit is walked in full regardless of `after` and regardless of
/// visibility, because an `Archive` that is itself filtered out is still
/// what attributes the `Delete` behind it. Only the *emit* decision
/// consults the lens.
fn emit_unit(unit: &[WalRecord], lens: &Lens<'_>, out: &mut Vec<ChangeRecord>) {
    // The previous state of each address this unit has archived so far,
    // which is what makes a delete attributable and an insert
    // distinguishable from a create.
    let mut archived: HashMap<&str, &Node> = HashMap::new();

    for record in unit {
        let admitted = match &record.operation {
            WalOperation::Archive(entry) => {
                archived.insert(entry.address.as_str(), &entry.node);
                None
            }

            WalOperation::Insert(node) => {
                let change = if archived.contains_key(node.address.as_str()) {
                    ChangeKind::Updated
                } else {
                    ChangeKind::Created
                };

                lens.admit(record.operation_id, change, node, &node.address)
            }

            WalOperation::Delete(address) => {
                // Removing the entry matters: a batch that deletes an
                // address and re-creates it must report the second write
                // as a create, not as an update of a node that is gone.
                //
                // No archive means nothing was there to remove: not a
                // change, and — since the archive is also the only thing
                // that says who owned it — not something to guess an
                // audience for. Withheld from everyone, admins included.
                archived.remove(address.as_str()).and_then(|previous| {
                    lens.admit(
                        record.operation_id,
                        ChangeKind::Deleted,
                        previous,
                        address,
                    )
                })
            }

            // Edges, users, indexes, references and text indexes are
            // deliberately not reported here; `/changes` is a node
            // change feed. See the endpoint's own documentation for why
            // that is the honest subset rather than an omission.
            _ => None,
        };

        out.extend(admitted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::history::HistoryEntry;
    use crate::core::node::Visibility;

    fn node(address: &str, owner: &str, visibility: Visibility) -> Node {
        let mut node = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            "Thing".to_string(),
            owner.to_string(),
        );

        node.visibility = visibility;
        node
    }

    fn record(sequence: u64, transaction_id: u64, operation: WalOperation) -> WalRecord {
        WalRecord::new(sequence, transaction_id, sequence + 1000, operation)
    }

    fn emitted(unit: &[WalRecord], owner: &str, is_admin: bool) -> Vec<ChangeRecord> {
        let mut out = Vec::new();
        emit_unit(unit, &Lens { after: 0, owner, is_admin }, &mut out);
        out
    }

    /// A delete carries only an address, so the archive staged
    /// immediately ahead of it is the *only* thing that says whose node
    /// it was. If that attribution were lost, the delete would either
    /// leak (shown to everyone) or vanish (shown to nobody).
    #[test]
    fn a_delete_is_authorized_by_the_state_it_removed() {
        let removed = node("secret:1", "alice", Visibility::Private);

        let unit = vec![
            record(1, 7, WalOperation::Begin),
            record(2, 7, WalOperation::Archive(HistoryEntry::now(removed))),
            record(3, 7, WalOperation::Delete("secret:1".to_string())),
            record(4, 7, WalOperation::Commit),
        ];

        let owner_view = emitted(&unit, "alice", false);

        assert_eq!(owner_view.len(), 1);
        assert_eq!(owner_view[0].change, ChangeKind::Deleted);
        assert_eq!(owner_view[0].address, "secret:1");
        assert_eq!(owner_view[0].kind, "Thing");

        assert!(
            emitted(&unit, "bob", false).is_empty(),
            "a stranger must not learn that a private node was deleted"
        );

        assert_eq!(
            emitted(&unit, "root", true).len(),
            1,
            "an admin reads everything already"
        );
    }

    /// A delete of an address that held nothing archives nothing. It
    /// removed no data, and there is no state to derive an audience
    /// from, so it must not be reported at all rather than reported to
    /// whoever happens to be asking.
    #[test]
    fn an_unattributable_delete_is_omitted() {
        let unit = vec![record(
            1,
            STANDALONE_TRANSACTION_ID,
            WalOperation::Delete("never-existed".to_string()),
        )];

        assert!(emitted(&unit, "alice", false).is_empty());
        assert!(emitted(&unit, "root", true).is_empty());
    }

    /// An insert over an archived value is an update; one without is a
    /// create. Both are derived, not guessed — the WAL has one upsert
    /// record for both.
    #[test]
    fn an_archive_before_an_insert_makes_it_an_update() {
        let previous = node("post:1", "alice", Visibility::Public);
        let next = node("post:1", "alice", Visibility::Public);

        let overwrite = vec![
            record(1, 7, WalOperation::Begin),
            record(2, 7, WalOperation::Archive(HistoryEntry::now(previous))),
            record(3, 7, WalOperation::Insert(next.clone())),
            record(4, 7, WalOperation::Commit),
        ];

        assert_eq!(emitted(&overwrite, "bob", false)[0].change, ChangeKind::Updated);

        let fresh = vec![record(
            1,
            STANDALONE_TRANSACTION_ID,
            WalOperation::Insert(next),
        )];

        assert_eq!(emitted(&fresh, "bob", false)[0].change, ChangeKind::Created);
    }

    /// A frame with no COMMIT at the end of the log may still be in
    /// flight, so the scan stops in front of it. The same frame with
    /// records after it cannot be — frames do not interleave — so that
    /// one is terminal and is skipped.
    #[test]
    fn an_uncommitted_frame_is_in_flight_only_at_the_tail() {
        let unit = vec![
            record(1, 7, WalOperation::Begin),
            record(2, 7, WalOperation::Insert(node("a", "alice", Visibility::Public))),
        ];

        let committed = recovery::CommittedTransactions::new();

        assert!(matches!(
            frame_state(&unit, &committed, true),
            FrameState::InFlight
        ));

        assert!(matches!(
            frame_state(&unit, &committed, false),
            FrameState::Terminal
        ));
    }

    /// The log is complete from the first record ever written, so a scan
    /// from position 0 is answerable. Once that record is gone, the
    /// oldest one still present is the furthest back anything can be
    /// vouched for.
    #[test]
    fn the_horizon_moves_only_once_the_first_record_is_gone() {
        let complete = vec![record(
            1,
            STANDALONE_TRANSACTION_ID,
            WalOperation::Delete("x".to_string()),
        )];

        assert_eq!(horizon(&complete), 0);

        let rotated = vec![record(
            94,
            STANDALONE_TRANSACTION_ID,
            WalOperation::Delete("x".to_string()),
        )];

        assert_eq!(horizon(&rotated), rotated[0].operation_id);
    }

    /// File order and position order have to be the same order, or a
    /// resume silently skips whatever sits out of place.
    #[test]
    fn positions_that_do_not_ascend_are_refused() {
        let mut records = vec![
            record(1, STANDALONE_TRANSACTION_ID, WalOperation::Delete("a".to_string())),
            record(2, STANDALONE_TRANSACTION_ID, WalOperation::Delete("b".to_string())),
        ];

        records[1].operation_id = records[0].operation_id;

        assert!(validate_positions(&records).is_err());
        assert!(validate_positions(&records[..1]).is_ok());
    }
}
