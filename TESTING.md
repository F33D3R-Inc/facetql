# Testing FacetQL

```sh
cargo test --release              # everything, ~3 s
cargo test --release -- --ignored # the heavy ones (writes 64 MiB)
cargo run --release --example scale_bench            # 100k-row baseline
cargo run --release --example scale_bench -- 1000000 # 1M
```

## Use `--release`

Not a preference. Every page is AES-256-GCM encrypted, and a debug build
of that is roughly two orders of magnitude slower than a release one: the
segment-roll test takes **894 s** in debug and **3.2 s** in release. This
is the direct reason nothing in this engine had ever been tested above
about sixty rows — at debug speed, scale tests look impossible rather
than merely slow.

## What is covered where

| File | Subsystem | Why it exists |
|---|---|---|
| `tests/page_layout.rs` | slotted page | cell round-trips, compaction preserving slot numbers, CRC catching a flipped bit |
| `tests/pager_eviction.rs` | buffer pool | 1 000 pages through a 256-page cache — the dirty-writeback-on-eviction path |
| `tests/btree_scale.rs` | on-disk B+tree | 10 000 keys: splits, range and reverse scans, cursors, free-list churn across generations |
| `tests/heap_records.rs` | record heap | overflow chains, segment roll, compaction accounting |
| `tests/crash_recovery.rs` | durability | spawns the real binary and `SIGKILL`s it |
| `src/**` `#[cfg(test)]` | engine, API, auth, limits | the pre-existing suite, entered through `StorageEngine` |

Before these files, `wal.rs`, `recovery.rs`, `btree.rs`, `pager.rs`,
`page.rs`, `heap.rs`, `commit.rs` and `checkpoint.rs` had **no direct
tests at all**.

## The crash tests

`tests/crash_recovery.rs` starts the actual `facetql` binary, writes to
it over HTTP, `SIGKILL`s it, restarts it and checks two things:

* an acknowledged write survives;
* a transaction is all-or-nothing — every batch comes back whole or
  absent, never half.

It runs with `FACETQL_RATE_*=off`. Rate limiting is a control against a
hostile caller, and this harness *is* one identity issuing thousands of
requests as fast as it can. The first version of the file left the limits
on and counted a `429` as "the record is gone" — which reported a
half-applied transaction that had never happened. A durability test whose
"absent" branch also catches rate limits, timeouts and 500s is not
measuring durability, so `Server::exists` now treats 200 as present, 404
as absent, and **panics on anything else**.

## `tests/reactor_liveness.rs` needs real storage

It measures `GET /` latency under concurrent write load, which only means
something when `fsync` is slow enough to block. It therefore uses
`target/` rather than the system temp directory, and **skips with a
printed reason** when the filesystem it lands on has a sub-200 µs fsync,
instead of passing vacuously.

The same caveat applies to `examples/scale_bench`: it prints the fsync
cost of its data directory on every run, and flags a RAM-backed one, so a
write result can never be read as a durability result by mistake.

## `tests/read_write_concurrency.rs` also needs real storage

It measures whether reads and writes exclude each other, by running each
side alone and then again under load from the other. On tmpfs a write
costs microseconds, so exclusion is not observable — the test skips with
a printed reason rather than passing vacuously, the same way
`reactor_liveness` does.

It asserts on the *ratio* of loaded to solo throughput, not on absolute
numbers, because the absolute numbers are set by how fast this machine's
`fsync` is and that is not what is under test.
