//! The single-writer lock on a data directory.
//!
//! # What this is protecting against
//!
//! Nothing in this engine coordinates between processes. The buffer
//! cache in `storage::pager`, the B+tree's page allocator and its two
//! alternating meta slots, the catalog's segment table and the WAL's
//! sequence counters are all *process-local* state describing files that
//! live on a shared disk. Two `facetql` processes opened on one data
//! directory are therefore two independent writers each convinced it
//! owns those files:
//!
//! ```text
//!   process A allocates page 41 for a B-tree split
//!   process B allocates page 41 for an unrelated split
//!   both write page 41
//!   both publish a meta naming page 41
//!   → one tree now has a node belonging to the other
//! ```
//!
//! There is no checksum, generation or authentication tag that catches
//! that: each page is individually well-formed and correctly signed. The
//! damage is *structural*, and it is silent until a query walks into it.
//! The same applies to the WAL — two processes drawing sequence numbers
//! from separate counters produce a log that
//! `recovery::validate_sequence` will refuse to replay — and to the
//! checkpoint, where the later writer's value can hide the earlier
//! writer's un-flushed records.
//!
//! So the supported deployment model is stated in code rather than in a
//! document nobody reads:
//!
//! > **One data directory is owned by exactly one process.**
//!
//! # Why `flock` and not a PID file
//!
//! A lock file holding a PID has to answer "is that process still
//! alive?", and every answer to that is wrong somewhere: the PID may
//! have been recycled, the check is racy, and a crash leaves a stale
//! file that a human has to delete before the database will start —
//! which turns a crash-restart into an outage requiring an operator.
//!
//! An advisory `flock` has none of those problems because the kernel
//! owns it: the lock lives on the open file description and is released
//! automatically when the process exits, however it exits. A crashed
//! process leaves the file behind and the lock gone, so the next start
//! simply works. Nothing has to be cleaned up, and there is no state
//! that can be stale.
//!
//! # Scope
//!
//! The lock is acquired once per process and held for the life of the
//! process — deliberately not once per `StorageEngine`. Several engines
//! over one directory *inside* one process are serialized by that
//! process (the server holds a single `RwLock<StorageEngine>`; the test
//! suite holds a global mutex), and taking a fresh lock per open would
//! turn that supported case into a failure while doing nothing about the
//! cross-process case this exists for.
//!
//! This is an advisory lock. It stops another `facetql`, not `cat`, and
//! it does not extend across NFS or other network filesystems where
//! `flock` semantics are unreliable — a data directory on such a mount
//! is outside what this engine supports.

use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::sync::OnceLock;

use crate::config;

/// The lock file's name inside the data directory. It carries no
/// contents: everything meaningful about it is the kernel-held lock on
/// the open descriptor, and a file with nothing in it cannot go stale or
/// disagree with reality.
const LOCK_FILE: &str = "facetql.lock";

/// The held lock, kept alive for the process's lifetime.
///
/// Dropping this `File` would release the lock, so it is parked in a
/// `static` rather than returned to a caller who might let it fall out
/// of scope.
static HELD: OnceLock<File> = OnceLock::new();

/// Take exclusive ownership of the configured data directory for this
/// process, or fail explaining who already has it.
///
/// Idempotent: the second and later calls in one process observe the
/// lock this process already holds and return `Ok`.
pub fn acquire() -> Result<()> {
    if HELD.get().is_some() {
        return Ok(());
    }

    config::ensure_data_dir()?;

    let path = config::data_file(LOCK_FILE);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;

    lock_exclusive(&file).map_err(|e| {
        if e.kind() == ErrorKind::WouldBlock {
            return Error::new(
                ErrorKind::WouldBlock,
                format!(
                    "another FacetQL process already has {} open. A data \
                     directory supports exactly one writer process: the \
                     page allocator, the index metadata and the WAL \
                     sequence counters are process-local, so a second \
                     writer would corrupt the files structurally rather \
                     than fail loudly. Stop the other process, or point \
                     this one at a different --data-dir.",
                    config::data_dir().display()
                ),
            );
        }

        Error::new(
            e.kind(),
            format!(
                "could not lock {}: {e}. FacetQL will not open a data \
                 directory it cannot claim exclusively.",
                path.display()
            ),
        )
    })?;

    // Racing callers inside this process: whoever loses simply drops
    // their descriptor, which releases *their* lock request and not the
    // one that was stored — the kernel tracks the lock per open file
    // description, and the stored description keeps it.
    let _ = HELD.set(file);

    Ok(())
}

/// `flock(fd, LOCK_EX | LOCK_NB)`.
///
/// Non-blocking on purpose: waiting would turn "someone else is already
/// running" into a start that hangs indefinitely with no output, which
/// is the least diagnosable failure available.
#[cfg(unix)]
fn lock_exclusive(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    // SAFETY: `file` is an open descriptor for the duration of the call,
    // and `flock` only reads the descriptor number.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };

    if rc != 0 {
        return Err(Error::last_os_error());
    }

    Ok(())
}

/// Windows has no `flock`. `LockFileEx` is the equivalent and would need
/// the `windows-sys` crate; until this engine targets Windows, the honest
/// thing is to leave the platform unlocked rather than to pretend
/// otherwise — the doc comment above and the operator-facing error are
/// the only guarantees a caller gets here.
#[cfg(not(unix))]
fn lock_exclusive(_file: &File) -> Result<()> {
    eprintln!(
        "warning: this platform has no advisory file locking, so FacetQL \
         cannot detect a second process opening the same data directory. \
         Two writers on one directory corrupt it silently — make sure only \
         one is running."
    );

    Ok(())
}
