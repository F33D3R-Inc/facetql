# FacetQL — CURRENT_ARCHITECTURE.md

**Phase 0 architecture audit — evidence-based, read-only.**
Audit date: 2026-09-03 · Crate version: `0.13.0` (`Cargo.toml:3`) · Rust edition 2024.
Source: `src/` (~35 files, 8,517 LOC incl. tests). `cargo build` = **clean (exit 0, 20 dead-code warnings)**. `cargo test` = **50 passed / 0 failed**.

> Maturity legend: **SOLID** = implemented, tested, behaves as documented · **PARTIAL** = works but with real gaps/caveats · **STUB** = type/scaffold exists, not wired · **ASPIRATIONAL** = described in docs/README but not in code.

---

## 0. Headline findings (read first)

1. **The README is largely aspirational fiction relative to the code.** `README.md:22-52` describes a "156-Cell Block", "Dissonance Score (0–15)", "FacetQL Governance", and "Dimensional Morphing (3D/4D)". **None of it exists in code.** The coordinate is 4 plain `u8` fields (`core/coordinate.rs`), the 12×13 genesis grid generator is **dead code never called** (`core/generator.rs`, see §5), and the Lo-Shu / magic-square validator is **dead code never called** (`rules/magic.rs`). `SECURITY_NOTES.md:10-13` even claims `Database::new()` seeds the 156-node grid — **it does not** (`database.rs:17-39` has no genesis seeding). Treat README + parts of SECURITY_NOTES as stale marketing, not spec.
2. **Transactions are validation-atomic, not crash-atomic — and the WAL's BEGIN/COMMIT machinery is dead.** `execute_transaction` (`storage/engine.rs:1091`) applies through `insert()`/`delete()`, each of which writes a **standalone** WAL record (`transaction_id = 0`). The full `Begin`/`Commit`/`Abort` WAL vocabulary (`storage/wal.rs:38-65`, `358-421`) and the multi-op recovery replay (`storage/recovery.rs:190-401`) are **never exercised by any live path** — no code ever calls `wal::begin`/`commit`/`abort`. A crash mid-batch can leave partial writes on disk. This is the single biggest correctness gap.
3. **`delete()` does not archive to history.** Only `insert()` archives the previous state (`engine.rs:379-399`). Deleted/cleared/`delete_where`d nodes leave **no** history entry (`engine.rs:504-528`). Soft-delete-with-history is an engine-wide decision still open.
4. **`/events` SSE has no visibility filtering.** Every write publishes to every subscriber regardless of node ownership/visibility (`routes.rs:687-698`, `database.rs:46-55`). Any valid token sees every private node's change events. Documented gap (`SECURITY_NOTES.md:261-269`), still open.
5. **The `GET /stats` / observability work (Phase 29) has already landed in the tree.** `reads_total`/`writes_total` atomics (`engine.rs:123-124`), `EngineStats`/`stats()` (`engine.rs:1040-1060, 1363-1372`), and the admin-gated `GET /stats` route + tests (`routes.rs:165, 714-965`) are present and passing. **Downstream Observability phases must not re-implement counters.**
6. **Postgres is correctly confined.** `tokio-postgres` appears **only** in `src/importer.rs`; `main.rs` merely carries `--pg-url` CLI strings into it. It is a one-way, HTTP-mediated import tool (`POST /node`), never in the read/write path. Hard-rule 4 satisfied.
7. **The `facetql-predicate-pushdown.patch` is a dangling/stale artifact.** Its content (the `QueryWhereRequest`, `core/predicate.rs`, `query_where`) is **already present in the working tree** and passing. The `.patch` file adds nothing and should be deleted by the human (never by an agent — git is human-only).

---

## 1. Data model

### Node — SOLID (as a schemaless entity store)
`core/node.rs:15-72`. Fields: `address: String` (client-supplied primary key), `coordinate: Coordinate`, `value: u64` (**unused — always 0**, `node.rs:19`), `kind: String` (free-text entity type), `data: String` (opaque JSON payload), `owner: String`, `claimed_by: Option<String>` (job-queue lease), `visibility: Visibility{Private,Public}`. Serialized with **bincode** (fixed layout → every field addition is a breaking on-disk change, noted `node.rs:37-41`). `can_read` (`node.rs:62`) = public OR owner; `can_write` (`node.rs:69`) = owner only.

### Edge — PARTIAL
`core/edge.rs:17-46`. `{from, to, kind, owner}`, directional, free-text `kind`. **No deletion path exists** — `Edge::can_write` is dead code (`edge.rs:42-45`), there is no `DELETE /edge` route, and edges have no tombstone identity. Both endpoints must exist at insert time (`engine.rs:620-632`).

### Addressing + Base62 — PARTIAL / ASPIRATIONAL
Addresses are **arbitrary client-supplied strings** (`routes.rs:32`, contract `<entity>:<id>`). `core/base62.rs` only provides `encode(u8) -> char` — **no decode, no full address codec**. The only Base62 use is the dead genesis generator. The "addresses are Base62 encodings of coordinates" idea (`SECURITY_NOTES.md:14-18`) is not how live addresses work.

### The 12×13 / 156-cell coordinate design — ASPIRATIONAL
`Coordinate{x,y,z,q: u8}` (`core/coordinate.rs`) with **no grid-size enforcement** (each axis is a full `u8`, 0–255). `generate_genesis()` builds the 12×13=156 grid (`core/generator.rs:7-20`) but is **never called** (grep-confirmed; only referenced by `mod generator`). No dissonance scoring, no dimensional morphing, no governance engine anywhere in code. The coordinate is stored and echoed but drives **no** behavior.

---

## 2. Storage engine — PARTIAL (functional, single-node, in-memory-authoritative)

`storage/engine.rs`, `StorageEngine` (`engine.rs:80-125`). All live state is **in-memory HashMaps**, rebuilt from disk at boot:
- `nodes: HashMap<address, Node>`, `index: Index` (address→file offset), `edges_out`/`edges_in` adjacency, `users: HashMap<token_hash, UserRecord>`, `history: HashMap<address, Vec<HistoryEntry>>`.
- `reads_total`/`writes_total: AtomicU64` — process-lifetime op counters (not persisted), incremented at the mutation/read primitives (`engine.rs:436, 458, 525, 652, 732, 802`).

Reads are served entirely from the in-memory HashMap; the on-disk **`Index` (address→offset) is populated but never read** (`storage/index.rs:22-25`, `get` is `#[allow(dead_code)]`). No mmap, no buffer pool, no on-disk B-tree. Whole dataset must fit in RAM.

`load()` (`engine.rs:279-346`) replays each physical file in append order (later record wins), then removes tombstoned keys. Genesis seeding is **not** done here.

---

## 3. Append / binary storage format — SOLID

`storage/binary.rs`. Every physical record is `[u32 LE length][encrypted blob]`, append-only (`append_record`, `binary.rs:21-37`). The blob = AES-256-GCM `nonce(12) || ciphertext || tag` (see §17). Files (all under the data dir, `binary.rs:123-141`):

| File | Contents | Format |
|------|----------|--------|
| `facetql.data` | Nodes | len-prefixed encrypted bincode records, append-only |
| `facetql.edges` | Edges | same |
| `facetql.users` | UserRecords | same |
| `facetql.history` | HistoryEntries | same |
| `facetql.wal` | WAL | hex(encrypted bincode) + `\n` per line (§4) |
| `facetql.tombstones` | Deleted addrs (+ `user:` prefix) | hex(encrypted) + `\n` per line (§7) |
| `facetql.checkpoint` | Durability high-water seq | single ASCII u64 (§6) |

`read_all_records` (`binary.rs:66-102`) replays sequentially; a decrypt failure **panics** (`binary.rs:87`) — fail-closed but a hard crash, not a graceful error. No compaction/vacuum: append-only files grow forever (overwrites and deletes never reclaim bytes).

---

## 4. WAL — PARTIAL (durable log; transaction framing unused)

`storage/wal.rs`. `WalRecord{format_version=2, sequence, transaction_id, operation_id, operation}` (`wal.rs:67-88`). `append()` (`wal.rs:288-329`): bincode → encrypt → hex → append line → **`sync_data()`** before returning (real durability boundary). WAL is **write-ahead**: `insert`/`delete`/`insert_edge`/`insert_user`/`revoke_user` each append their WAL record *before* the physical write (`engine.rs:405-417, 508-518, 634-647`).

**Two independent sequence counters — a latent hazard.** The engine's live path uses `NEXT_WAL_SEQUENCE` (`engine.rs:17, 25-27`) via `append_wal`. `wal.rs` has its *own* `NEXT_SEQUENCE`/`NEXT_TRANSACTION_ID`/`NEXT_OPERATION_ID` (`wal.rs:29-35`) used only by `standalone`/`begin`/`commit`/`abort` — **which the engine never calls**. Only `advance_wal_sequence` is reconciled at recovery (`recovery.rs:60-64`); `wal::initialize_counters` (`wal.rs:225`) is **never called**, so `operation_id` in every live record is just the sequence number reused (`engine.rs:150-155`). Harmless today only because the transaction-framing path is dead.

**Dead WAL vocabulary:** `Begin`/`Commit`/`Abort` (`wal.rs:38-65`) and their append helpers (`wal.rs:358-421`) are never emitted by any live code path.

---

## 5. Tombstones — SOLID

`storage/tombstone.rs`. `append_tombstone(address)` writes a hex-encoded encrypted line (`tombstone.rs:21-28`); user revocations are namespaced `user:<token_hash>` (`engine.rs:994-996`). `load()` applies them last, removing keys (`engine.rs:333-343`). Permanent (no un-delete). Underlying `facetql.data` bytes are retained (operator-recoverable).

---

## 6. Checkpoint & recovery — PARTIAL

**Checkpoint** (`storage/checkpoint.rs`) — SOLID: highest WAL sequence already reflected in physical files. `advance()` is crash-safe (tmp-file + `sync_data` + atomic rename, `checkpoint.rs:60-79`). Advanced only *after* the physical write completes (`engine.rs:417, 518, 647`). Makes replay idempotent for `Archive`/`InsertEdge` (which are otherwise not idempotent).

**Recovery** (`storage/recovery.rs:41-163`): if `facetql.wal` exists → decrypt+deserialize every line (corruption/auth failure = hard `Err`, `recovery.rs:407-486`), enforce **strictly increasing** sequence (`validate_sequence`, `recovery.rs:489-516`), advance the in-process counter, then **filter to `sequence > checkpoint`** and replay. Standalone (`tid=0`) ops replay immediately via `replay_*` (which don't re-WAL). Any recovery failure **prevents startup** (`database.rs:30`, `main.rs:390-400`) — correct fail-closed posture.

**Dead branch:** the entire multi-op transaction reconstruction (`replay_committed_transaction`, BEGIN…COMMIT lifecycle validation, `recovery.rs:150-401`) can never fire because no live path writes `transaction_id != 0`. It is well-written but unreachable. **Crash-mid-batch recovery does not exist.**

---

## 7. Transactions — PARTIAL (the "major gap" the directive already flags)

`execute_transaction` (`engine.rs:1091-1298`), 3 passes: (1) build the post-batch address set (staging), (2) validate every op against staged state (no write), (3) apply via `insert`/`delete`/`insert_edge`. **If validation fails, nothing is written; if the process crashes during pass 3, already-applied ops persist** (`engine.rs:1068-1090`). `TxOperation` (`engine.rs:1491-1539`): `InsertNode`, `DeleteNode`, `InsertEdge`, `ClearKind{kind,owner,is_admin}`, `DeleteWhere{kind,where_,owner,is_admin}`. `ClearKind`/`DeleteWhere` fan out to N `delete()` calls (N writes, N tombstones, no history). Ownership for deletes is resolved in the handler under the same write lock (`routes.rs:553-634`). `InsertNode` is upsert; cross-owner overwrite is rejected (`engine.rs:1176-1187`). The `storage/transaction.rs` + `storage/commit.rs` `Transaction`/`Operation`/`commit()` types are **STUBs** (`#[allow(dead_code)]`, never constructed).

Wire contract match: `TxOpRequest` (`routes.rs:467-520`) is serde `tag="type"`, snake_case — matches AGENT_LOG §4b exactly (`insert_node`/`delete_node`/`insert_edge`/`clear_kind`/`delete_where`). **`set_if`/CAS op requested by fct (ReserveCron, AGENT_LOG §28) is NOT implemented.**

---

## 8. History / versioning — PARTIAL

`core/history.rs`: `HistoryEntry{address, archived_at_unix, node}` — the full prior node. `insert()` archives the previous value **before** overwriting (WAL `Archive` → durable history file → memory, `engine.rs:379-399`). `GET /node/:address/history` (`routes.rs:248-262`) returns oldest-first, gated by **current** owner/visibility (`routes.rs:240-247`). Gaps: **delete does not archive** (§0.3); no revert-to-version; no retention/pruning (unbounded growth).

---

## 9. Authentication — PARTIAL

`auth.rs`. Credential = `x-api-key` header, with `?key=` query fallback **only** when the header is absent (for browser `EventSource`, `auth.rs:118-132`). **No `Authorization: Bearer` path** (grep-confirmed; matches §4b). Two identity sources, checked in order (`auth.rs:134-142`):
1. **Static bootstrap map** `ENOCHIAN_TOKENS` = `token:owner[:admin]` (`auth.rs:56-88`). If unset → single dev token `dev-local-key-change-me` → owner `dev`, role **Admin**, with a loud warning.
2. **Persistent users** by SHA-256 hash lookup (`auth.rs:38-42, 137-141`).

Gaps: token comparison is **plain HashMap lookup / string equality, not constant-time** (`auth.rs:134`, no `subtle`/`ct_eq`) — low-severity timing side-channel. No expiry, no rotation, no per-session tokens, no rate limiting / lockout / attempt logging. Static-map tokens live in an env var in plaintext.

---

## 10. Authorization — PARTIAL

Model = "owner or superuser" (`core/user.rs:9-13`, two roles `User`/`Admin`). Enforced in handlers: read = `is_admin() || can_read(owner)` (`routes.rs:232, 255, 457, 657`); write/delete = `is_admin() || can_write(owner)` (`routes.rs:277, 309, 554-556`). `query`/`query_where` pass `None` for admin (superuser bypass of the visibility filter) vs `Some(owner)` (`routes.rs:367-371, 405-409`; engine `engine.rs:724-745, 789-809`). `/admin/*` and `/stats` are hard admin-gated (`routes.rs:718, 764, 796, 818`). No per-field/per-kind grants, no ACL lists, no row-level policies beyond owner/public.

---

## 11. API surface — SOLID (see full table)

Router `create_router` (`routes.rs:145-170`). Every route except `GET /` is behind `auth_middleware`. CORS is **permissive `Any`** (`routes.rs:133-143`) — dev-only, flagged.

| Method | Path | Handler (routes.rs) | Auth | Owner/role gating |
|--------|------|--------------------|------|-------------------|
| GET | `/` | `home` :172 | none | public liveness string |
| POST | `/node` | `create_node` :176 | x-api-key | owner = token; `if_absent` → 409; upsert otherwise |
| GET | `/node/:address` | `get_node` :223 | x-api-key | `is_admin \|\| can_read` |
| GET | `/node/:address/history` | `get_node_history` :248 | x-api-key | `is_admin \|\| can_read` (current owner) |
| PUT | `/node/:address` | `update_node` :264 | x-api-key | `is_admin \|\| can_write` |
| DELETE | `/node/:address` | `delete_node` :297 | x-api-key | `is_admin \|\| can_write` |
| GET | `/node/:address/owned` | `list_owned` :345 | x-api-key | own nodes only (by token) |
| POST | `/node/:address/claim` | `claim_node` :323 | x-api-key | atomic lease; 409 if claimed |
| GET | `/nodes` | `query_nodes` :354 | x-api-key | admin bypass / `can_read` filter; array response |
| POST | `/nodes/query` | `query_nodes_where` :391 | x-api-key | predicate + keyset cursor; `{nodes,next}` |
| POST | `/edge` | `create_edge` :430 | x-api-key | owner = token; both endpoints must exist |
| GET | `/node/:address/edges/out` | `get_edges_out` :450 | x-api-key | `is_admin \|\| can_read` |
| GET | `/node/:address/edges/in` | `get_edges_in` :650 | x-api-key | `is_admin \|\| can_read` |
| POST | `/transaction` | `execute_transaction` :528 | x-api-key | per-op owner/admin checks |
| GET | `/events` | `subscribe_events` :687 | x-api-key (or `?key=`) | **NO per-event filtering** |
| POST | `/publish` | `publish_event` :678 | x-api-key | any valid token can broadcast |
| POST | `/admin/users` | `create_user` :759 | x-api-key | **admin only** |
| GET | `/admin/users` | `list_users` :792 | x-api-key | **admin only** |
| DELETE | `/admin/users/:owner` | `revoke_user` :813 | x-api-key | **admin only**; revokes all tokens for owner |
| GET | `/stats` | `stats` :714 | x-api-key | **admin only** (Phase 29 — already landed) |

Response contracts match §4/§4b: `GET /nodes` → bare array; `POST /nodes/query` → `{nodes,next}` opaque base64url keyset cursor (`engine.rs:1379-1482`).

---

## 12. CLI — SOLID

`main.rs` (clap) + `src/cli/`. Server-side subcommands: `init`, `start [--port --tls-identity --tls-identity-password]`, `backup <dir>`, `restore <dir>`, `import postgres …`. Operator client subcommands (HTTP against a *running* server, never touch files directly): `user create/list/delete`, `get`, `put`, `delete`, `query`, `stats` (`cli/mod.rs`). Client sends `x-api-key` (`cli/client.rs:162`), validates path segments (`cli/mod.rs:154-163`), structured exit codes 1/2/3 (`cli/error.rs:41-47`). `stats` here tallies kinds client-side by paging `/nodes` (`cli/mod.rs:323`) — distinct from the server `GET /stats`. Backup/restore = plain file copy, **not** a live-consistent snapshot (`main.rs:239-317`).

---

## 13. Configuration — SOLID

`config.rs`. Data dir via `--data-dir` / `ENOCHIAN_DATA_DIR`, default `~/.facetql` (`config.rs:20-53`). `OnceLock` set once from `main` (`main.rs:145-147`) — a second `set` silently no-ops (`config.rs:23-25`, flagged). Other env: `ENOCHIAN_PORT`, `ENOCHIAN_TOKENS`, `ENOCHIAN_MASTER_KEY`, `ENOCHIAN_TLS_IDENTITY[_PASSWORD]`. **Process-wide `OnceLock`s for data-dir + crypto key make durable-path unit tests hard to isolate** (flagged in AGENT_LOG; tests avoid it by building engines in memory).

---

## 14. Concurrency & locking — PARTIAL (the "Single Gatekeeper")

One `Arc<RwLock<StorageEngine>>` (`database.rs:11-13`). Reads take `.read()`, writes `.write()` (`routes.rs` throughout). All mutation is fully serialized → no lost-update / TOCTOU windows (claim, `if_absent`, and per-op tx checks are all correct-by-construction under the write lock). Cost: **no MVCC, no read concurrency during a write, no row/range locking, single-writer throughput ceiling**. `reads_total`/`writes_total` are atomics so they can be bumped under a read lock (`engine.rs:116-124`). Lock poisoning → `.expect(...)` panic in handlers (e.g. `routes.rs:193`). Only one process may safely own the data dir (no cross-process file locking — importer/CLI go through HTTP for exactly this reason, `importer.rs:9-17`).

---

## 15. Event broadcasting (SSE) — PARTIAL

`tokio::sync::broadcast` channel, capacity 1024 (`database.rs:32`). `Database::publish` is best-effort fire-and-forget (`database.rs:46-55`). Handlers publish JSON events on every successful mutation (`node_created/updated/deleted`, `edge_created`, `node_claimed`, `transaction_committed`, `user_*`). `/events` streams to any authenticated subscriber (`routes.rs:687-698`); `/publish` lets any token inject a payload. Gaps: **no visibility filtering** (§0.4), no durable/replayable event log (offline subscribers miss events), slow subscribers silently drop past the 1024 buffer.

---

## 16. Crypto / encryption at rest — PARTIAL

`crypto.rs`. **AES-256-GCM**, fresh random 12-byte nonce per record, `nonce||ciphertext||tag` (`crypto.rs:49-62`); GCM tag authenticates → tamper detected on decrypt (`crypto.rs:68-78`). **Every data file is encrypted**: data, edges, users, history (via `binary.rs`), WAL (`wal.rs:299-303`), tombstones (`tombstone.rs:27`). Key from `ENOCHIAN_MASTER_KEY` (64 hex → 32 bytes), single process-wide `OnceLock` (`crypto.rs:13-40`); if unset → **all-zero dev key** with a loud warning (insecure, known value). Wrong key → decrypt panic on load (fail-closed, `binary.rs:87-93`). Gaps: **no key rotation / re-encrypt utility, no KMS/Vault, no per-record or per-tenant keys, no secure memory zeroization, single key for the whole DB.** Authentication of records = GCM only (there is no separate MAC/HMAC subsystem; the WAL's "authenticated" wording refers to GCM).

---

## 17. Network / TLS — PARTIAL

Plain HTTP by default (`main.rs:482-499`). Optional native HTTPS via PKCS#12 (`--tls-identity`, `main.rs:427-480`) served by a hand-rolled accept loop replicating `axum::serve` with a TLS handshake per connection (`tls_server.rs:30-75`, uses `native-tls`/system OpenSSL). Gaps: PKCS#12 only (no direct PEM), no ACME/auto-renew, no HTTP→HTTPS redirect, `tls_server` doesn't set `TCP_NODELAY`, no connection limits / timeouts / body-size caps, no rate limiting. `reqwest` (importer + CLI) uses `rustls-tls`.

---

## 18. Query system & predicate pushdown — PARTIAL (single evaluator, no index)

- `query()` (`engine.rs:724-745`): linear scan, `kind`/`owner`/visibility filters, offset/limit.
- `query_where()` (`engine.rs:789-886`): the same filters **plus** a pushable `Expr` predicate over decoded `data`, in-engine ordering, and an **opaque keyset cursor** `(order_value, address)` base64url of `{"o":…,"a":…}` (`engine.rs:1379-1482`) — matches §4b. `next:""` = last page. Falls back to offset when no cursor.
- `core/predicate.rs`: `Expr` mirrors FCT `ir.Expr` field-for-field (`predicate.rs:16-58`); `eval` (`predicate.rs:70-161`) supports `lit`/`get`/`un`/`bin` with the exact pushable subset FCT's `exprSQL` allows; unpushable expr → **error, never a wrong answer** (surfaced as 400, and in `delete_where` aborts the whole tx before any write, `engine.rs:573-607, 1135-1152`).

**Gaps: no secondary indexes — every query is an O(n) full scan** of the kind/owner-filtered set (explicitly noted `engine.rs:711-713, 753-761`). No query planner, no cost model, no joins (edges are the only relationship traversal and only via the dedicated `edges/{out,in}` routes, not the query language). No aggregation, no projection, no LIKE/regex, no full-text.

---

## 19. Dead / stub / unwired code inventory (all `#[allow(dead_code)]` or grep-confirmed uncalled)

| Item | File | Status |
|------|------|--------|
| `generate_genesis()` 12×13 grid | `core/generator.rs` | ASPIRATIONAL — never called |
| `rules::magic::validate_sum` (Lo-Shu) | `rules/magic.rs` | ASPIRATIONAL — never called |
| `facet::FacetStore` | `facet/mod.rs` | STUB — module declared, never constructed (build warns) |
| `storage::transaction::{Transaction,Operation}` + `commit::commit` | `storage/transaction.rs`, `commit.rs` | STUB — never used |
| `Index::get` (offset point-read) | `storage/index.rs:22-25` | unwired — reads use in-memory HashMap |
| `binary::read_record_at` / `read_node_at` | `binary.rs:42-59, 111-114` | unwired |
| `Edge::can_write` | `edge.rs:42-45` | unwired — no edge deletion |
| `Node.value: u64` | `node.rs:19` | unused field, always 0 |
| WAL `Begin/Commit/Abort` + `wal::{begin,commit,abort,standalone,initialize_counters}` | `wal.rs` | unreachable — no live caller |
| `recovery::replay_committed_transaction` (multi-op path) | `recovery.rs:190-401` | unreachable — no `tid!=0` records written |
| `facetql-predicate-pushdown.patch` | repo root | stale — content already in tree |

---

## 20. Test coverage

50 tests, all passing, all unit/in-process (no integration/property/fuzz/bench). Concentrated in: cursor/keyset + admin-bypass + clear_kind + delete_where (`engine.rs` test mods), `GET /stats` via real router+auth (`routes.rs:844-965`), CLI parse/validate/render (`main.rs`, `cli/*`). **Untested:** crash/recovery paths, WAL corruption handling, crypto round-trip under wrong key, concurrency races, the durable disk path end-to-end (tests build engines in memory to dodge the `OnceLock` data-dir). No `tests/`, `benches/`, or `fuzz/` directories exist.

---

## 21. Top correctness/security risks (prioritized)

1. **Non-crash-atomic transactions + dead BEGIN/COMMIT path** (§7, §4) — a crash mid-`execute_transaction` (worst case a large `clear_kind`/`delete_where`) leaves the DB partially mutated; recovery cannot roll it back. **Highest.**
2. **`/events` broadcasts every write to every subscriber** (§15) — cross-tenant metadata leak of private nodes to any valid token.
3. **`delete()` loses history** (§8) — deleted/cleared nodes have no archived state; audit/recovery cannot see what was removed.
4. **All-zero dev crypto key + dev admin token as silent defaults** (§9, §16) — a misconfigured deploy that forgets `ENOCHIAN_MASTER_KEY`/`ENOCHIAN_TOKENS` runs with a public key and a known admin token; only a log warning guards it (no fail-closed on missing prod secrets).
5. **No resource guards** (§14, §17) — no request-body size cap, no connection/rate limits, no query result cap beyond `limit.min(500)`, unbounded WAL/history/append-file growth with no compaction; a single client can OOM (whole dataset is in-memory) or fill disk.

---

## 22. Alignment with the mission (AGENT_LOG §0/§2)

FacetQL is a **native, self-contained Rust engine** — no SQL, no ORM, Postgres confined to a one-way importer (§0.6). The fct↔FacetQL wire contract (§4/§4b) is implemented verbatim on this side (`x-api-key`, tagged tx ops, keyset cursor). The engine is the right foundation to harden; the work is closing the durability/observability/security gaps above, **not** replacing it. The chief risk to the mission is the **README's aspirational framing** (governance/morphing/156-cell) being mistaken for implemented behavior — it is not (§0.1).
