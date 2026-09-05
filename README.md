# FacetQL

A single-binary, page-based graph database with a WAL, crash recovery,
durable B+tree indexes, per-identity token auth, and an HTTP/JSON API.

FacetQL stores **nodes** (typed entities with an opaque JSON payload) and
**edges** (directed, typed relationships between them). It is written in
Rust, has no external database dependency, and is operated the way you
would operate Postgres or Redis: initialize a data directory, start a
server, talk to it over HTTP.

It exists to be the storage backend for the FCT (`.fct`) language and the
F33D3R stack, but it has no dependency on either and runs standalone.

Version `0.13.0` · Rust edition 2024 · `cargo test` = 92 passing.

---

## What it actually is

The database is on disk, not in the process. The engine holds no map of
nodes, no adjacency lists and no history — it holds the machinery for
reaching them (`StorageEngine`, `src/storage/engine.rs`):

```
  StorageEngine
    ├── catalog    which heap segments exist, how long they are
    ├── store      the record heap: segments → 16 KiB pages → records
    ├── indexes    durable B+trees (six built-in + declared ones)
    └── cache      a bounded LRU of recently decoded nodes
```

* **Records live in a slotted-page heap.** A page is 16 KiB
  (`src/storage/page.rs:63`), segments cap at 4096 pages
  (`src/storage/heap.rs:66`), and a record too large for one page spills
  into a chain of overflow pages. Records are appended, never updated in
  place; an overwrite writes a new record and repoints the index.
* **Every index is a real on-disk B+tree** (`src/storage/btree.rs`),
  copy-on-write with two alternating meta pages, so a commit either
  publishes a whole new generation or leaves the previous one intact.
  Opening the database reads metadata and index roots — not records — so
  a large database opens as fast as a small one
  (`StorageEngine::open`).
* **Pages are encrypted at rest.** Every page is stored as
  AES-256-GCM(body) under `ENOCHIAN_MASTER_KEY`, so index keys —
  addresses, kinds, owners — are encrypted exactly like record payloads
  (`src/storage/pager.rs:26-38`, `src/crypto.rs`).
* **Memory is bounded by caches, not by data.** The page cache holds 256
  pages per open file, at most 64 segments stay open, and the record
  cache holds 4096 decoded nodes — all configurable
  (`src/storage/pager.rs:58`, `src/storage/heap.rs:114`,
  `src/storage/cache.rs:26`).
* **One process owns a data directory.** Startup takes an advisory
  `flock`; a second process on the same directory is refused rather than
  allowed to corrupt it (`src/storage/lock.rs`).

---

## Data model

### Node

```json
{
  "address": "post:1",
  "coordinate": { "x": 0, "y": 0, "z": 0, "q": 0 },
  "value": 0,
  "kind": "Post",
  "data": "{\"title\":\"hello\",\"created_at\":1730000000}",
  "owner": "alice",
  "claimed_by": null,
  "visibility": "Private"
}
```

* `address` — the node's identity, supplied by the client. Any string;
  it is the primary index key, so it is bounded at 1024 bytes
  (`src/storage/btree.rs:69`).
* `kind` — free-text entity type (`"Post"`, `"Person"`, `"__session"`).
  Not a schema: it is the label the `kind` index groups by, which is
  what makes "every node of this kind" a prefix range rather than a
  scan.
* `coordinate` — four `u8` axes (`x`, `y`, `z`, `q`). Stored, returned,
  and **not interpreted by any read or write path**. Nothing sorts,
  filters, or routes on it today.
* `data` — an opaque string. The engine only ever parses it as JSON for
  two purposes: evaluating a `where` predicate, and building a declared
  index key. Nothing validates its shape.
* `owner` — the authenticated identity that wrote it. Never taken from a
  request body.
* `visibility` — `"Private"` (owner + admins only) or `"Public"` (any
  authenticated identity) (`src/core/node.rs:62-71`).
* `claimed_by` — set by `POST /node/:address/claim`, the atomic
  claim-once primitive. There is no route that clears it.
* `value` — a `u64` that is always `0`. Nothing writes it. It is a
  vestigial field kept because changing `Node`'s shape is a breaking
  on-disk format change (`src/core/node.rs:37-41`).

### Edge

```json
{ "from": "person:1", "to": "post:1", "kind": "AUTHORED", "owner": "alice" }
```

A directed, typed relationship. Its **identity is the triple
`(from, to, kind)`** — `owner` is deliberately excluded
(`src/core/edge.rs:97-128`), so "A follows B" is one fact in the graph
rather than one per asserter, and re-asserting it lands on the same
entry. `owner` decides who may retract it. Both endpoints must already
exist. Edges are indexed in both directions, so traversal in either
direction is a prefix scan of one node's range.

### History

Every overwrite and every delete archives the previous state first — the
whole node, not just `data` — keyed `(address, version)` with a monotonic
version drawn from the WAL's operation-id counter. A node's history is
therefore a prefix scan returned oldest-first, and reading one node's
history never touches another's (`src/core/history.rs`,
`StorageEngine::history_for`).

---

## Access paths

Six built-in B+trees answer the six fixed questions
(`src/storage/index.rs:56-99`):

| index      | key                          | answers |
|------------|------------------------------|---------|
| `primary`  | `address → location`         | where is the node at this address? |
| `kind`     | `kind + address`             | which nodes have this kind? |
| `owner`    | `owner + address`            | which nodes does this owner own? |
| `edge_out` | `from + kind + to → location`| what does this node point at? |
| `edge_in`  | `to + kind + from → location`| what points at this node? |
| `history`  | `address + version → location`| what did this node used to be? |

They are **authoritative** — a query answers from them without consulting
the heap first — which is why every mutation passes through a single
apply step that maintains all of them
(`StorageEngine::apply_committed`).

### Declared indexes over `data`

An operator can declare an index over one top-level field of one kind
(`POST /admin/indexes`, or `facetql index create`). Each is a real B+tree
file, `facetql.idx.data.<name>`, and each definition is persisted in an
append-only log, `facetql.indexes`, replayed last-write-wins at startup —
exactly like the user log (`StorageEngine::load`).

* **Creating one is a logged, crash-atomic mutation.** It goes through
  the WAL as `CreateIndex`, which writes the definition, opens the tree,
  and backfills from every existing node of that kind in one operation
  (`Operation::CreateIndex`, applied in `StorageEngine::apply_operation`). It therefore **costs the size of
  the kind** — this is the read Postgres does when it builds an index.
  Recovery replays it idempotently: every backfilled entry is keyed by
  `(value, address)`, so a second pass lands on the keys the first wrote
  (`src/storage/recovery.rs:879-892`).
* **The whole kind is read before anything is logged.** A key the tree
  would refuse cannot be discovered after the create is durable —
  recovery would replay it, hit the same refusal, and fail startup
  forever. So an index over a kind containing an oversized value is
  refused outright, while it is still just a failed request
  (`StorageEngine::check_backfill_admissible`).
* **Ordering is defined once.** The byte encoding an index key is built
  from and the comparator the in-memory sort uses are both driven by one
  type ranking — `null < bool < number < string < composite < absent`
  (`src/storage/index.rs:549-594`). Numbers use the standard
  order-preserving `f64` transform (flip all bits of a negative, flip the
  sign bit of a non-negative), and values are `0x00`-escaped and
  terminated with `0x00 0x00` so a prefix sorts before a longer value.
  A sort and an index scan cannot disagree about what "ordered" means.
* **What it buys — ordering.** `POST /nodes/query` with `order` on an
  indexed field is served by a range scan over that index: one page read,
  resumed by the keyset cursor, with no materialize-and-sort and no
  `FACETQL_MAX_SCAN_ROWS` ceiling (`StorageEngine::query_by_data_index`).
  Unindexed orderings keep the old sort path, which holds the whole
  matching set in memory and refuses past that bound.
* **What it buys — equality.** A `where` predicate that pins an indexed
  field to a literal (`item.status == "open"`, at the top level or under
  `&&`) is served by a *prefix* scan of that index: only the entries
  holding that value are read, so a kind with a million rows and fifty
  matches costs the fifty (`StorageEngine::query_by_data_prefix`). Only
  `==` against a non-null literal qualifies, and only through `&&` —
  under `||` or `!` a conjunct is not a requirement, and ordering
  comparisons can *fail* a query rather than skip a row, so narrowing on
  them would make the indexed and unindexed paths disagree about what the
  request means (`predicate::equality_literal`). The predicate is still
  evaluated in full on every candidate; the prefix answers one conjunct,
  not the whole thing.
* **Bounds.** Index name ≤ 64 bytes, `[A-Za-z0-9_-]` only (it becomes a
  filename). Encoded indexed value ≤ 512 bytes, checked before anything
  is logged — so a write that would produce an oversized index entry is
  rejected rather than committed (`src/storage/index.rs:144-166`,
  `706-741`).
* Dropping an index never breaks a query: one it was serving falls back
  to the sort path.

---

## Durability

The rule the whole write path is built around
(`StorageEngine::apply_atomic`; `src/storage/transaction.rs:326-352`):

> **Two or more durable records ⇒ one `BEGIN … COMMIT` frame.
> Exactly one ⇒ standalone.**

Decided in one place, so no mutation can re-decide it. An overwrite
(archive + insert), a delete (archive + delete), a claim, a create-with-
edges and every `POST /transaction` batch are framed; a lone insert is
not.

**Write path:** WAL record encrypted, checksummed, framed and fsync'd →
heap and index writes into the buffer pool → checkpoint later. A
mutation is durable the instant its WAL record is fsync'd; the
checkpoint only decides how much of the log a restart replays.

**Transactions** (`StorageEngine::execute_transaction`) are
validate-all-then-apply *and* crash-atomic: the batch is resolved in full
against a staged view of the data — expanding `clear_kind`/`delete_where`
into concrete deletes and pairing each overwrite with its archive —
before a byte is written; then every record is staged under one
transaction id; then the `COMMIT` marker is written; then the batch is
applied. A crash before the `COMMIT` is durable makes recovery discard
the whole frame. A crash after it makes recovery replay the whole frame.
There is no in-between.

**Checkpointing** (`StorageEngine::checkpoint`) runs every 256 mutations
and moves in a fixed order: compact a dead heap segment → fsync heap and
catalog → fsync index metas → advance the WAL checkpoint → retire drained
segments. A crash anywhere before the checkpoint advances simply means
recovery replays those operations, and every replay is idempotent by key.

**Compaction** drains a heap segment once more than half of it is dead,
one segment per checkpoint, and decides liveness by asking the indexes
rather than by trusting the obsolete-byte counter
(`StorageEngine::compact`). **WAL rotation** reclaims the log
below the durable checkpoint once it passes 64 MiB
(`StorageEngine::rotate_wal`).

**Recovery** (`src/storage/recovery.rs`) reads the WAL through its own
framed reader, repairs a torn *trailing* frame (the signature of a crash
mid-append, never acknowledged to anyone) and refuses to start on
anything else — a mid-file integrity failure, a bad format version, a
sequence that goes backwards, a COMMIT after an ABORT. Replay is in
strict WAL sequence order, with a transaction's mutations applied at its
COMMIT position. **There is no flag that starts the server anyway**
(`src/main.rs:307-437`); startup failure is classified as storage (exit
1), integrity (exit 4) or WAL recovery (exit 5) and prints the file, the
offset and the next steps.

---

## Running it

### Build

```bash
cargo build --release          # target/release/facetql
```

### Start

```bash
facetql init                                    # create ~/.facetql

ENOCHIAN_TOKENS="$(openssl rand -hex 32):root:admin" \
ENOCHIAN_MASTER_KEY="$(openssl rand -hex 32)" \
facetql start --port 8080
```

### It will not start without those

The default posture is **production**, and in production the server
refuses to start rather than fall back to a development credential. Three
things are checked before anything is opened:

* `ENOCHIAN_TOKENS` — unset, unparseable, or containing the published
  `dev-local-key-change-me` token means the only credential accepted
  would be a public one;
* `ENOCHIAN_MASTER_KEY` — unset, malformed, or all-zero means the entire
  database is encrypted under a value anyone can type;
* TLS — no identity configured and no explicit acknowledgement that TLS
  is terminated in front of this process means every `x-api-key` crosses
  the network in the clear.

A refusal exits **7**, distinct from every other exit code this binary
uses, and prints exactly which variable to set. Set
`FACETQL_ALLOW_PLAINTEXT=1` when a reverse proxy terminates TLS.

For local work, declare it:

```bash
FACETQL_ENV=development facetql start
```

That restores the old behaviour — the token `dev-local-key-change-me`
(owner `dev`, role Admin) and an **all-zero** encryption key, both public
known values — and prints them as findings on the way up. Do not put real
data behind them.

`ENOCHIAN_MASTER_KEY` must be 64 hex characters (32 bytes). It is not
recoverable and not derived from anything — lose it and the data is
unreadable; change it and startup fails with an authentication error.

### TLS

```bash
facetql start --tls-identity identity.p12 --tls-identity-password <pw>
```

Plain HTTP otherwise.

---

## Auth

Every route except `GET /` requires a bearer token in the `x-api-key`
header (`src/auth.rs:297`). `GET /events` additionally accepts `?key=`,
because browser `EventSource` cannot set headers — a documented
downgrade, since a token in a URL reaches access logs.

Two credential stores, both looked up **by SHA-256 digest** so a
presented secret is never compared byte-for-byte against a stored one:

* **Bootstrap**, from `ENOCHIAN_TOKENS`: `token:owner` or
  `token:owner:admin`, comma-separated. This solves the same problem
  `POSTGRES_PASSWORD` solves — something must authenticate before there
  is a user store to authenticate against. These identities cannot be
  revoked over the API; edit the variable and restart.
* **Persistent**, created through `POST /admin/users`. Only the token
  hash is stored; the plaintext is shown exactly once, at creation, and
  cannot be retrieved again.

Two roles. A `User` reads public nodes plus its own, and writes only its
own. An `Admin` bypasses ownership and visibility on every path, the way
a Postgres superuser bypasses row-level security. Nothing in a request
body can influence the identity a write is attributed to.

The per-endpoint matrix — every route, who may call it, and the
per-object rule its handler applies — is compiled into the binary and
printed by:

```bash
facetql routes          # add --json for the machine-readable form
```

It is the same table `api::routes::ROUTES` that the authorization tests
drive through the real router, so what it prints is what this build
enforces.

`GET /events` is audience-filtered: an event about a private node reaches
its owner and admins only, and `POST /publish` broadcasts to everyone
only for an admin — anyone else's publish is scoped to their own
subscribers (`src/database.rs:286-327`, `src/api/routes.rs:1203-1273`).

---

## Configuration

| Variable | Default | What it bounds |
|---|---|---|
| `ENOCHIAN_DATA_DIR` (`--data-dir`) | `~/.facetql` | where every file lives |
| `ENOCHIAN_PORT` (`--port`) | `8080` | listen port |
| `ENOCHIAN_MASTER_KEY` | all-zero dev key | AES-256-GCM key for pages, WAL, logs |
| `ENOCHIAN_TOKENS` | one dev admin token | bootstrap identities |
| `ENOCHIAN_TLS_IDENTITY` / `_PASSWORD` | — | PKCS#12 identity for HTTPS |
| `FACETQL_ENV` | `production` | `development`/`dev`/`test` permits dev credentials and plaintext; anything else (including unset) refuses them |
| `FACETQL_ALLOW_PLAINTEXT` | unset | set to any value to acknowledge that TLS is terminated in front of this process |
| `FACETQL_ALLOWED_ORIGINS` | none in production, any in development | comma-separated browser origins, or `*` |
| `FACETQL_MAX_BODY_BYTES` | 4 MiB | largest request body |
| `FACETQL_REQUEST_TIMEOUT_SECS` | 30 | per-request deadline (never applied to `GET /events`) |
| `FACETQL_MAX_CONCURRENT_REQUESTS` | 512 | in-flight requests before 503 |
| `FACETQL_MAX_CONNECTIONS` | 2048 | accepted TLS connections before the socket is dropped |
| `FACETQL_MAX_SUBSCRIBERS` | 256 | concurrent `GET /events` streams |
| `FACETQL_RATE_READ` | `600:300` | per-identity `burst[:per_second]`, or `off` |
| `FACETQL_RATE_WRITE` | `300:150` | as above, for durable mutations |
| `FACETQL_RATE_BULK` | `120:60` | as above, for `POST /nodes/query` and `POST /transaction` |
| `FACETQL_RATE_ADMIN` | `60:30` | as above, for `/admin/*` and `/stats` |
| `FACETQL_RATE_SUBSCRIBE` | `30:5` | as above, for opening `GET /events` |
| `FACETQL_MAX_SCAN_ROWS` | 100 000 | rows one request may materialize |
| `FACETQL_MAX_TRANSACTION_OPS` | 50 000 | mutations one lowered batch may stage |
| `FACETQL_MAX_QUERY_OFFSET` | 10 000 | deepest `offset` (use the cursor instead) |
| `FACETQL_CHECKPOINT_INTERVAL` | 256 | mutations between checkpoints |
| `FACETQL_WAL_ROTATE_BYTES` | 64 MiB | WAL size that triggers rotation |
| `FACETQL_RECORD_CACHE` | 4096 | decoded nodes held resident |
| `FACETQL_PAGE_CACHE_PAGES` | 256 | resident pages per open file |
| `FACETQL_OPEN_SEGMENTS` | 64 | heap segments kept open |

Client-side: `FACETQL_URL` and `FACETQL_TOKEN` for the CLI.

---

## CLI

`init`, `start`, `backup` and `restore` act on the data directory
directly. Everything else is an HTTP client of a **running** server —
because one process owns a data directory, a second tool that opened
those files would corrupt them (`src/cli/client.rs:1-15`).

```bash
facetql init                                  # create the data directory
facetql start [--port N] [--tls-identity …]   # run the server
facetql backup <dir>                          # copy every data file out
facetql restore <dir>                         # copy them back (refuses to clobber)

facetql user create <owner> [--admin]         # prints the token once
facetql user list
facetql user delete <owner> [--yes]

facetql index create <name> --kind K --field F   # declare + backfill
facetql index list
facetql index drop <name> [--yes]

facetql get <address>
facetql put <address> --kind K --data '<json>' [--public]
facetql delete <address> [--yes]
facetql query --kind K [--order F] [--desc] [--limit N]
facetql stats
```

Add `--json` to any read for raw server output. Exit codes: 0 success,
1 runtime failure, 2 usage error, 3 declined a confirmation.

`backup` is a plain file copy and is **not** a consistent snapshot of a
live server — run it against a directory nothing is writing to.

---

## API

Full request/response shapes are in
[`API_REFERENCE.md`](./API_REFERENCE.md). The surface, from
`create_router` (`src/api/routes.rs:291-324`):

```
GET    /                          liveness, unauthenticated
POST   /node                      create/overwrite, optionally with edges
GET    /node/:address
PUT    /node/:address
DELETE /node/:address
GET    /node/:address/history
GET    /node/:address/owned
POST   /node/:address/claim
GET    /node/:address/edges/out
GET    /node/:address/edges/in
GET    /nodes                     kind/owner/limit/offset → array
POST   /nodes/query               predicate + order + keyset cursor
POST   /edge
DELETE /edge                      addressed by body: from/to/kind
POST   /transaction               insert_node, delete_node, insert_edge,
                                  delete_edge, clear_kind, delete_where, set_if
GET    /events                    SSE, audience-filtered
POST   /publish
GET    /stats                     admin
POST   /admin/users               admin
GET    /admin/users               admin
DELETE /admin/users/:owner        admin
POST   /admin/indexes             admin
GET    /admin/indexes             admin
DELETE /admin/indexes/:name       admin
```

Example:

```bash
curl -X POST localhost:8080/node \
  -H "x-api-key: $TOKEN" -H 'content-type: application/json' \
  -d '{"address":"post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,
       "data":"{\"title\":\"hello\",\"created_at\":1730000000}","public":true}'

curl -X POST localhost:8080/admin/indexes \
  -H "x-api-key: $ADMIN" -H 'content-type: application/json' \
  -d '{"name":"post_created","kind":"Post","field":"created_at"}'

curl -X POST localhost:8080/nodes/query \
  -H "x-api-key: $TOKEN" -H 'content-type: application/json' \
  -d '{"kind":"Post","order":"created_at","desc":true,"limit":20}'
```

---

## What this does not do yet

Honest list. Each item is absent from the code, not merely unpolished.

**Concurrency and scale**
* **One writer, one process.** Every mutation serializes on a single
  `RwLock<StorageEngine>`. There is no MVCC, no row locking, and no
  reader/writer concurrency beyond "many readers or one writer".
* **No replication, failover, or clustering.** One node, one directory.
* **No interactive transactions.** `POST /transaction` is a batch
  submitted whole; there is no `BEGIN`/`COMMIT` a client holds open, and
  no isolation level to choose.

**Query**
* **No composite or multi-field indexes**, no indexes on nested paths
  (declared indexes cover exactly one top-level `data` field of one
  kind), and none on `coordinate`.
* **Only equality predicates reach an index.** A `where` that pins a
  declared-index field to a literal with `==` is served as a prefix scan
  (`equality_prefix_plan`); every other comparison — `<`, `>`, `!=`, or
  anything under an `||` — is evaluated row by row over the candidate
  set. The restriction is deliberate: narrowing on `<` would make the
  indexed and unindexed paths disagree, because an unindexed `<` against
  a missing or non-numeric field errors the query rather than skipping
  the row.
* **Index selection is by name, not by selectivity.** With two
  applicable indexes the engine takes the alphabetically first, so the
  choice is deterministic rather than good. There are no statistics.
* **No joins, no aggregation, no graph traversal beyond one hop.**
  `edges/out` and `edges/in` return one node's adjacency; multi-hop is
  the client's loop.
* **No query planner.** The access path is chosen by three explicit
  rules: kind beats owner, a declared index beats a sort, and everything
  else walks the primary index.

**Data model**
* **`coordinate` is inert.** It is stored and returned; nothing reads it.
* **No schema, no constraints, no uniqueness beyond `address`, no
  foreign keys and no cascading delete.** Deleting a node leaves edges
  that pointed at it, and leaves any id that another node's `data`
  happens to hold.
* **`data` is an opaque string.** It is never validated.

**Authorization**
* **Two roles and one owner per record.** No ACLs, no groups, no
  per-field permissions.
* **No shared or delegated write access.** A record has exactly one
  owner, and only that owner (or an admin) may overwrite or delete it —
  enforced in the engine, on every path including `POST /node`
  (`insert_with_edges`). There is no way to grant another identity write
  access to a record you own.
* **No token expiry or rotation policy.** A token is valid until it is
  explicitly revoked, and bootstrap tokens cannot be revoked at all
  without a restart.
* **CORS is closed by default in production.** Browser origins must be
  listed in `FACETQL_ALLOWED_ORIGINS`; the permissive
  `Access-Control-Allow-Origin: *` applies only in a Development
  deployment or when that variable is explicitly set to `*`
  (`cors_layer` in `src/api/routes.rs`). A server-to-server client sends
  no `Origin`, so CORS never applies to it.

**Operations**
* **Rate limits are per identity and per process, not per cluster.**
  A token bucket over five endpoint classes (read, write, bulk, admin,
  subscribe), plus caps on body size, request timeout, in-flight
  requests, open connections and SSE subscribers (`src/api/limits.rs`).
  The bucket map is capped at 10,000 identities, and none of it is
  shared between processes — two instances behind a load balancer each
  enforce their own budget.
* **No structured logging, metrics export, or tracing.** `GET /stats` is
  the whole observability surface: counts, per-kind breakdown, process
  lifetime read/write counters, and physical heap statistics.
* **`backup` is not an online backup.** It copies files; it does not
  quiesce a running server.
* **No point-in-time restore.** History is per-node and read-only —
  there is no "restore this node to version N" call, let alone a
  database-wide one.
* **Deletes are not erasures.** Removing the index entry is what makes a
  node gone; its bytes stay in the heap until compaction reclaims them,
  and its previous states remain readable through
  `GET /node/:address/history`.

The engine's own doc comments are the architecture reference — every
bound carries a paragraph on the failure it closes and why the number is
the number it is. `CURRENT_ARCHITECTURE.md` and
`facetql-status-checklist.md` used to sit here; both had fallen several
versions behind the code and were removed rather than half-corrected. A
checklist that lists shipped features as missing is worse than no
checklist: someone plans a rewrite of something that already works.

`SECURITY_NOTES.md` predates this engine and is **not** currently
accurate. Among other things it states that `Database::new()` seeds a
156-node "genesis grid". No code does, and none ever did on a live path:
`generate_genesis`, the base62 coordinate-address codec and the
magic-square validator were unreachable scaffolding and have since been
deleted from the tree entirely (`src/core/coordinate.rs` documents the
removal). Read `SECURITY_NOTES.md` as history until it is rewritten.
