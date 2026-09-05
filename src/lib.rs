//! FacetQL — a single-binary, page-based graph database.
//!
//! This crate is both the server binary and a library. The library half
//! exists for two reasons, and the second one is the load-bearing one:
//!
//! * **Embedding.** A database that can only be reached over HTTP is a
//!   service, not a database. Exposing the engine as a library is what
//!   lets a Rust program link it directly, and it costs nothing —
//!   `main.rs` becomes a consumer of the same surface everyone else gets.
//! * **Testing the physical layer directly.** Until this file existed the
//!   crate had no lib target, so `tests/` could not name `storage::btree`,
//!   `storage::pager` or `storage::heap` at all. Every test had to enter
//!   through `StorageEngine`, which is why the page/B+tree/WAL/recovery
//!   stack — the part that must not have bugs — had no direct tests and
//!   nothing had ever run above ~60 rows.
//!
//! The module surface is deliberately wide rather than a curated façade.
//! A storage engine's invariants live in its subsystems, and a test that
//! cannot reach a subsystem cannot pin its invariant.

pub mod api;
pub mod auth;
pub mod config;
pub mod core;
pub mod crypto;
pub mod database;
pub mod metrics;
pub mod storage;
pub mod tls_server;

pub use database::{Database, DatabaseError};
