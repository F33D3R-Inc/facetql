# Security state — v0.1 checkpoint

## Fixed in this pass (persistence)

- **`facetql.data` was write-only.** Nothing ever read it back —
  every restart lost all data even though writes were being
  persisted correctly after the truncation fix. `StorageEngine::load()`
  now replays the file on boot via `binary::read_all()` and rebuilds
  the in-memory map + index from it.
- **`generate_genesis()` was dead code** — written but never called.
  `Database::new()` now seeds the 156-node genesis grid on a genuinely
  fresh install (empty/missing data file) and skips seeding if data
  already exists on disk.
- **Genesis addresses weren't actually Base62.** The old generator
  built addresses like `"0100"` (decimal digits), not real Base62 —
  contradicting the spec's own `A9k2`-style examples and leaving
  `base62::encode()` unused. Fixed to encode each coordinate component
  properly.
- **`recovery::recover()` was dead code**, never called on boot. Now
  runs at startup and prints the WAL contents. Worth being precise
  about what it does *not* yet do: it doesn't replay uncommitted WAL
  entries into storage — durability instead comes from every insert
  being written to disk (`binary::append_node`) before `insert()`
  returns. If you want real WAL-replay-based crash recovery (recovering
  writes that hit the WAL but never made it to `facetql.data`), that's
  still a gap — right now WAL is a log, not yet a recovery mechanism.

## Fixed earlier (security)


1. **Data-loss bug in binary.rs**: `write_node` used `File::create()`,
   which truncates `facetql.data` on every call. Every insert was
   silently destroying all previously stored nodes. Replaced with an
   append-only, length-prefixed record format (`append_node` /
   `read_node_at`) so writes are additive and each node is findable at
   its own offset.

2. **No auth on the API**: `/node` and `/node/:address` were fully
   open — any request from anywhere could create or read data. Added
   `auth_middleware` requiring a matching `x-api-key` header on both
   routes. Key comes from `ENOCHIAN_API_KEY`; a dev-only default is
   used (and a boot-time warning printed) if that env var isn't set.

3. **No permissions on nodes**: `Node` had no concept of who owns it
   or who can see it. Added `owner: String` and `visibility: Visibility`
   (`Private` / `Public`), with `can_read()` / `can_write()` checks.
   `get_node` now enforces `can_read()` — knowing a node's Base62
   address is no longer sufficient to read it.

## Explicitly NOT done yet — don't mistake this for finished

- **Single shared API key, not per-client scoped/rotating tokens.**
  This is the "one master password" version, not the JWT + refresh +
  revocation model discussed earlier. Anyone with the key can act as
  any owner (the `owner` field on write is client-supplied, not
  derived from an authenticated identity). That's the next real gap
  to close before this is internet-facing.
- **`x-requester` header is self-reported**, not verified — it's a
  placeholder for real authenticated identity, not a security boundary
  by itself yet.
- **No encryption at rest** for `facetql.data` or `facetql.wal` —
  both are plaintext bincode/text on disk.
- **No TLS enforced** at the server level — that's a deployment
  concern (reverse proxy / axum TLS config) not yet wired in.
- **`can_write` exists but isn't enforced anywhere yet** — there's no
  update/delete route in this checkpoint to enforce it on.
- **No audit log** separate from the WAL.

## Suggested next slice

Replace the shared API key with per-client tokens (even a static
token-to-owner-name map to start) so `owner` on write is derived from
who's authenticated, not self-reported in the request body. That one
change closes the biggest remaining gap.

## v0.2 checkpoint — relationships, per-client auth, update/delete

### Added

- **Edges (relationships between nodes).** New `Edge { from, to, kind, owner }`
  type, its own append-only log (`facetql.edges`), and an in-memory adjacency
  index in both directions. `POST /edge`, `GET /node/:address/edges/out`,
  `GET /node/:address/edges/in`. Both endpoints of an edge must already exist.
  This was the biggest functional gap for any real data model (people, orgs,
  goals, steps, resources all need to reference each other) — v0.1 had no way
  to connect two nodes at all.
- **Per-client tokens, replacing the single shared API key.** `ENOCHIAN_TOKENS`
  env var (`token1:alice,token2:bob`) maps each token to an owner identity.
  `owner` is no longer read from the request body on `create_node` — it comes
  entirely from the authenticated token via request extensions
  (`auth::AuthOwner`). A client can no longer claim to write data as anyone
  else; verified directly (see test below). This was flagged in the v0.1 notes
  as "the one change that closes the biggest remaining gap" — done, though it's
  still a static map, not JWT/refresh/revocation.
- **Update route.** `PUT /node/:address` — enforces `can_write` (owner-only,
  same as before), then re-inserts under the same address (the storage engine
  already treated same-address inserts as overwrites; this was just missing a
  route and an authorization check in front of it).
- **Delete route.** `DELETE /node/:address` — enforces `can_write`, then
  appends the address to a new `facetql.tombstones` log rather than mutating
  the append-only data file. `StorageEngine::load()` now filters tombstoned
  addresses out of the rebuilt in-memory view, so a delete survives a restart.
  The underlying record in `facetql.data` is untouched (still recoverable by
  an operator who needs to investigate), it's just no longer live.
- **`GET /node/:address/owned`** — every live node the authenticated caller
  owns. Linear scan; fine at this scale, flagged as the first thing to index
  properly once it isn't.

### Verified with a live smoke test (not just "should work")

Ran the built binary and exercised it with curl: created two nodes and an
edge as one identity, confirmed a second identity gets 403 on reading and
writing the first identity's private node, confirmed a client-supplied
`owner` field in the request body is silently overridden by the authenticated
token (i.e. ownership spoofing does not work), updated and deleted a node,
and confirmed both the edge and the delete survive a full process restart
(tombstone + both logs replay correctly together).

### Still NOT done — same honesty policy as the v0.1 notes

- **No edge deletion yet.** `Edge::can_write` exists but nothing calls it —
  same situation Node::can_write was in in the v0.1 notes. Needs a tombstone
  identity scheme for edges (no single address the way nodes have one; likely
  `(from, to, kind)`) before it's wired in.
- **Tokens are still a static map**, not real per-session JWT/refresh/
  revocation — anyone with a leaked token has that owner's full access until
  the token is rotated by changing the env var and restarting.
- **No encryption at rest, no TLS**, same as v0.1.
- **No query language beyond point-get, owner-scan, and edge traversal.** No
  filtering, no pagination, no full-text search. Anything resembling "find all
  Goals with status=incomplete" needs to be built client-side today by pulling
  everything and filtering, which won't scale past a small dataset.
- **No real transactions.** Creating a node and an edge to it are two separate
  operations, each individually durable — if the process dies between them
  you can be left with a node and no edge, not a clean rollback. The
  `Transaction`/`Operation` scaffolding in `storage/transaction.rs` still isn't
  wired to anything.
- **WAL is still a log, not a replay-based recovery mechanism** — durability
  continues to come from every write completing before the API call returns,
  not from replaying the WAL on boot. Real crash-mid-write recovery is still
  open.

### Suggested next slice

Multi-op transactions (create node + create edge as one atomic unit) is
probably the next highest-value piece — case data is inherently relational
(a goal doesn't mean much without being linked to a person), and right now
there's a window where that link can silently fail to happen.

## v0.3 checkpoint — typed nodes, querying, atomic create-with-edges

### Added

- **`Node.kind`** — every node now declares its entity type (`"Person"`,
  `"Goal"`, `"Resource"`, etc.). This is a breaking format change: a v0.1/v0.2
  `facetql.data` file will fail to deserialize under v0.3, since the byte
  layout gained a field. There's no data in production yet, so this wasn't
  written as a migration — if that's no longer true by the time this lands,
  say so before merging and a migration path needs to be written first.
- **`GET /nodes?kind=&owner=&limit=&offset=`** — filtered, paginated listing.
  This is what turns the API from "point lookups only" into something an
  application can actually build list views against. Same `can_read` rule as
  a single GET is applied per-node, so filters can't be used to enumerate
  private data.
- **Atomic create-with-edges** — `POST /node` now accepts an optional `edges`
  array and creates them right after the node in the same call
  (`StorageEngine::insert_with_edges`). Verified live: the success path
  (node + edge both created), and the failure path (edge target missing →
  node is tombstoned, confirmed gone via a follow-up GET). This is
  best-effort atomicity for the single most common write pattern, not a
  general transaction system — see the caveat in `API_REFERENCE.md` about
  what happens with more than one edge in the list.
- **`API_REFERENCE.md`** — full endpoint documentation for client teams
  (Swift/Kotlin/web) building against this without reading the Rust source.

### Still NOT done

Same list as the v0.2 notes, minus general node/edge querying (now done):
edge deletion, real multi-node-and-edge transactions beyond the
create-with-edges special case, TLS, encryption at rest, WAL-replay-based
crash recovery, and any kind of schema validation on `data` itself.

## v0.4 checkpoint — installable as a real binary, not just `cargo run` from the repo

### Added

- **`facetql init` / `facetql start [--port N]` CLI**, via `clap`. Running
  the binary with no subcommand still just starts the server (backward
  compatible with every prior checkpoint's behavior).
- **Configurable data directory** (`config.rs`), defaulting to `~/.facetql`
  instead of the current working directory. Every storage module
  (`binary.rs`, `wal.rs`, `tombstone.rs`, `recovery.rs`) now reads/writes
  through this instead of hardcoded `"facetql.data"`-style literals in the
  repo root — the thing that made this feel like a dev script instead of an
  installed service. Overridable via `--data-dir` or `ENOCHIAN_DATA_DIR`.
  Verified live: `--data-dir /tmp/facetql-test-data start --port 9090`
  correctly isolates its data files and serves on the requested port.
- **`.github/workflows/release.yml`** — on any `v*` tag push, builds native
  binaries on GitHub's own macOS, Linux, and Windows runners (no
  cross-compilation — each OS builds itself) and attaches them to a GitHub
  Release. **I was not able to actually trigger or watch this workflow run**
  — that requires a real push to GitHub, which I don't have access to do.
  It follows the standard, widely-used pattern for this
  (`dtolnay/rust-toolchain` + a build matrix + `softprops/action-gh-release`),
  and the Rust code itself is confirmed to build with `cargo build --release`
  in this checkpoint, but the workflow YAML itself is unverified until
  someone pushes a tag and watches it actually run. Check the Actions tab
  after the first tag push before assuming it's correct.
- **`install.sh`** — one-line `curl | sh` installer for macOS/Linux that
  detects OS/arch and pulls the matching release asset. Depends on the
  release workflow above actually publishing binaries with the exact asset
  names it expects (`facetql-macos-arm64`, `facetql-linux-x86_64`, etc.) —
  those two files need to stay in sync if either changes.
- **README rewritten** with real install instructions per platform, CLI
  reference, and a pointer to this file and `API_REFERENCE.md`.

### Still NOT done

Everything from the v0.3 notes, plus:

- **No LICENSE file.** Flagged in the README. This is a decision for you to
  make (MIT/Apache-2.0 are the usual choices for something meant to be
  broadly downloaded and built on), not something to pick by default.
- **No Homebrew formula / winget / apt package** — the install script and
  raw `.exe` download are real but one step below what "brew install
  facetql" would be. Worth doing once the release workflow is confirmed
  working and there's an actual version people are installing.
- **No Docker image** — mentioned early in planning ("docker run
  enochiandb") but not built. Given a working release binary now exists,
  a minimal Dockerfile (copy the linux-x86_64 binary, expose 8080, set
  ENOCHIAN_DATA_DIR to a volume mount) is a small follow-up, not a big one.
- **The release workflow is genuinely unverified** — see above. Don't
  advertise "download and install" publicly until a real tag has been
  pushed and the resulting release actually has working binaries attached.

## v0.5 checkpoint — atomic job claiming and a live change feed

### Added

- **`Node.claimed_by: Option<String>`** and **`POST /node/:address/claim`**.
  Whoever is authenticated (from their token, never the request body) atomically
  claims a node if nobody has yet — `409` with who already holds it otherwise.
  Correct by construction, not by anything clever: every write already goes
  through the single `RwLock::write()` guard every other mutation uses, so the
  check and the set happen with nothing able to run in between. **Verified
  live**, not just argued for: fired 4 genuinely concurrent claim requests at
  the same node from 4 different identities — exactly one won, the other 3
  each got a clean 409 naming the winner.
- **`GET /events`** — Server-Sent Events change feed. Every successful write
  (node created/updated/deleted, edge created, node claimed) publishes a
  message; anyone connected to `/events` gets it in real time. **Verified
  live**: connected a listener, performed a claim from another identity, the
  listener received the event immediately.
- Both features are built directly against this repo's actual types and
  functions (`StorageEngine`, `Node`, `Database.engine.write()`) — every line
  here was written by reading the real code first, then compiled, then
  exercised with curl before being called done.

### Known gap, stated plainly

**`/events` broadcasts every write to every connected subscriber, regardless
of node visibility or ownership.** A private node's changes are currently
visible on the live feed to anyone holding any valid token, even one that
couldn't read that node directly. Fine for single-tenant local dev, not fine
once this holds real multi-owner data (case records, PIAL identities, etc.).
The fix is per-subscriber filtering — only publish an event to a subscriber
who could actually read the node it's about — and it's a real piece of work,
not a toggle. Do not expose `/events` on anything with sensitive multi-owner
data until this is addressed.

### Still NOT done

Same list as v0.4, plus: `/events` has no visibility filtering (above), and
there's still no durable/guaranteed-delivery event log — `/events` is
fire-and-forget; a subscriber that isn't connected when something happens
simply never sees it, and a slow subscriber can silently drop messages past
the 1024-message buffer. If Project Interstate needs "catch me up on
everything I missed while I was offline," that's a different, not-yet-built
feature (an actual persisted event log with an offset/cursor), not something
this broadcast channel does.

## v0.6 checkpoint — roles and a real admin/user system

Prompted by: "doesn't Postgres/MySQL/Redis already do this?" — yes, and this
checkpoint brings FacetQL to roughly the same place: a bootstrap
superuser plus a persistent, admin-manageable user store, instead of every
authenticated identity being equal.

### Added

- **`Role` (User/Admin)** on every identity. Admin bypasses ownership checks
  on reads/writes/queries — same rationale as a Postgres superuser bypassing
  row-level security by default. Verified live: an admin token read a
  private node it didn't own; a non-admin, non-owner identity correctly got
  403 on the same node.
- **Persistent, hashed user store** (`facetql.users`, same append-only
  format as nodes/edges). `POST /admin/users` (admin-only) generates a real
  random 32-byte token, returns it exactly once, and stores only its
  SHA-256 hash — the plaintext is never persisted or logged anywhere.
  `GET /admin/users` lists owner+role (never tokens or hashes).
  `DELETE /admin/users/:owner` revokes — verified live, including that the
  revoked token immediately stops authenticating.
- **`ENOCHIAN_TOKENS` now accepts a third field for role**:
  `token:owner:admin`. This static map remains the bootstrap layer only —
  the same job `POSTGRES_USER`/`POSTGRES_PASSWORD` do for a fresh Postgres
  container, or root's initial password in MySQL. Everything created after
  bootstrap should go through `POST /admin/users`, not grow this env var.
- If `ENOCHIAN_TOKENS` is unset, the dev fallback token is now Admin
  (previously undefined role) specifically so local dev has a way to call
  `POST /admin/users` and bootstrap a real first admin without hand-editing
  environment variables.

### Known gaps, stated plainly

- **No password/token rotation policy, no expiry.** A generated token is
  valid forever until explicitly revoked. Real systems generally support
  expiring credentials; this doesn't yet.
- **Only two roles.** Nothing like Postgres's per-table/per-column GRANT
  system — it's "regular user" or "bypasses everything," not fine-grained
  privileges. Fine for now, a real limitation if Project Interstate needs
  e.g. "this identity can verify Goal completions but not read Person
  records."
- **`revoke_user` revokes every persistent record for that owner name**,
  not a specific token. If one owner somehow has multiple tokens (not
  currently possible through `POST /admin/users`, which always creates one),
  revoking by owner takes all of them. Worth revisiting if multi-token-per-
  owner ever becomes a real use case.
- **Admin actions aren't yet written to a dedicated audit trail** beyond the
  existing WAL text log — "who created/revoked which user, when" is
  reconstructable from the WAL but there's no queryable audit log the way a
  compliance-sensitive deployment (e.g. anything touching PIAL identities)
  would eventually want.

## v0.7 checkpoint — more CLI, and a bridge from an existing database

### Added, verified live

- **`facetql backup <dir>` / `facetql restore <dir>`** — copies every
  data file out to a directory, and back in. Verified end-to-end: created
  a node, backed it up, restored into a brand-new empty data directory,
  booted a second server instance against the restored copy on a
  different port, and confirmed the node was actually there. Also
  verified the safety refusal: restoring a second time into a directory
  that already has data is refused rather than silently overwritten.
  Explicitly a plain file copy, not a consistent-snapshot-of-a-live-server
  backup — run it against a data directory that isn't being actively
  written to by a running `facetql start`, same caveat `pg_basebackup`
  has for a similarly simple approach.

### Added, NOT verified in this environment — read this before relying on it

- **`facetql import postgres`** — pulls rows from an existing Postgres
  table into FacetQL, one node per row, through the same `POST /node`
  API any other client uses (deliberately not direct file access — see
  the comment in `importer.rs` for why: two processes writing straight to
  `facetql.data` with no shared lock is a real corruption risk this
  avoids entirely by going through the running server instead).
  Type-dispatches Postgres columns (bool, int2/4/8, float4/8, text/
  varchar, date, timestamp, timestamptz) into JSON; unrecognized types
  fall back to a text representation rather than being silently dropped.
  **I could not get `tokio-postgres` to build in this sandbox** — its
  dependency tree pulls in a crate that requires a newer Cargo edition
  than the Rust toolchain available here (1.75) supports, regardless of
  which tokio-postgres version I pinned. This is an environment
  limitation, not a known issue with the code itself — the code follows
  tokio-postgres's documented API directly and every other module in this
  checkpoint (backup/restore, the full CLI, everything from v0.1–v0.6)
  builds and passes its tests in this same environment. **Run
  `cargo build` on a normal, up-to-date Rust toolchain before trusting
  this — if it doesn't compile cleanly there, that's a real bug and I
  want to know about it, not something to paper over.**

## v0.8 checkpoint — real multi-op transactions

### Added, verified live

- **`POST /transaction`** — batches `insert_node`, `insert_edge`, and
  `delete_node` operations and validates the *entire batch* before
  applying any of it. Verified live, both directions: a 3-operation batch
  (create Person, create Goal, link them) committed and all three were
  confirmed present; a batch referencing a nonexistent edge target was
  rejected with `400` and the node in that same batch — which would have
  succeeded entirely on its own — was confirmed NOT created. That's the
  actual guarantee: an invalid batch touches nothing, not even the valid
  parts of it.
- Edge operations can reference a node created earlier in the *same*
  batch, not just nodes that already existed — the whole batch's final
  state is computed before anything is validated.
- Delete permission (ownership/admin) is checked for every delete target
  before the batch reaches the engine, using the same lock the whole
  request holds — no window for another request to change ownership
  between the check and the batch applying.

### Stated plainly — what this is NOT

This is batch validation with atomic apply-if-valid, not crash-safe
multi-write commit. If the process dies **after** some operations in an
already-validated batch have durably written to disk but **before** the
rest have, those partial writes are not rolled back on restart — there's
no separate pending-transaction staging log yet, just the same
append-only files every other write already uses. Real crash-mid-commit
protection needs that staging log (write the whole batch to a pending
file, only append to the live files once the entire batch is confirmed
good, delete the pending file last) — that's real, separate work, and I
don't want "transactions" in the name to imply a stronger guarantee than
what's actually built and tested here.

## v0.8.1 — fix: importer.rs referenced reqwest without it being declared

Real bug, caught by your build, not mine: `reqwest` got dropped from
`Cargo.toml` while I was isolating a toolchain issue in my own sandbox
(see the v0.7 notes above) and never got restored correctly before that
checkpoint shipped. Fixed:

- `reqwest` is back in `Cargo.toml`, with `rustls-tls` (not the default
  `native-tls`, to avoid pulling a different problematic dependency chain).
- `importer.rs` no longer calls `.json(&body)` — it builds the JSON string
  itself (already had `serde_json::Value` in hand) and sends it via
  `.body(...)` with an explicit `content-type` header instead. This
  removes the dependency on reqwest's `json` cargo feature being enabled
  correctly at all, so this specific class of mismatch can't recur even
  if the dependency list changes again later.

**Still unverified in my sandbox** — same as v0.7: this environment's old
Rust toolchain can't get past dependency resolution for `reqwest` OR
`tokio-postgres` at all (confirmed separately, same edition2024 wall).
Your build getting a real Rust compiler error rather than a dependency
resolution error is actually a good sign — it means your toolchain
handles this dependency graph fine; this was a real missing-dependency
bug on my end, not an environment issue on yours. Please run
`cargo build` again and let me know if anything else surfaces.

## v0.9 checkpoint — CORS, browser-compatible SSE auth, and a working starter console

Prompted by wanting something to actually start building a website with,
not another backend-only pillar.

### Added, verified live via real fetch() calls (not curl this time — an
actual Node process making the same requests a browser would, including
a CORS preflight)

- **CORS**, permissive (`Access-Control-Allow-Origin: *`) so a page served
  from a different origin/port can call this API at all. **Explicitly
  dev-only** — narrow `cors_layer()` in `api/routes.rs` to your real
  frontend's actual origin before this is public.
- **`GET /events` now accepts `?key=<token>` as a fallback when the
  `x-api-key` header is absent.** This exists for one specific reason:
  browser `EventSource` cannot set custom headers at all — there's no
  way to open a live SSE connection from a plain browser page otherwise.
  Real tradeoff, stated plainly: a token in a URL can land in server
  access logs or proxy logs in a way a header wouldn't. Verified live
  both directions — a valid `?key=` connects and receives real-time
  events, no key at all is still a clean 401.
- **`facetql-console.html`** (shipped alongside, not inside, the repo
  zip) — a small single-file starter site: connect with a server URL +
  token, create nodes, create edges, query by kind/owner, and watch the
  live event feed. Every request it makes was run for real against a
  live server before this was called done, using Node's built-in
  `fetch` to exactly replicate what a browser sends (headers, body,
  and the SSE query-param path). This is meant to be the seed you
  extend into Project Interstate's actual frontend, not a finished
  product.

## v0.10 checkpoint — Pillar 2: encryption at rest

### Added, verified live

- **AES-256-GCM encryption for every data file**: `facetql.data`,
  `facetql.edges`, `facetql.users`, `facetql.wal`, and
  `facetql.tombstones` — not just the two files originally named, since
  addresses and operation logs are readable data too, not just the `data`
  field. A fresh random 12-byte nonce per record; GCM's authentication tag
  means tampering is detected on decrypt, not silently accepted.
- **`ENOCHIAN_MASTER_KEY`** — a 64-hex-character (32-byte) key. If unset,
  falls back to an obviously-fake all-zero dev key with a loud warning on
  every boot, same pattern as the dev API token. Generate a real one with
  `openssl rand -hex 32`.
- **Verified live, all three real scenarios**:
  1. No key set → dev key warning printed, data still round-trips correctly through the API (encrypted on disk, transparently decrypted on read)
  2. Real key set, server restarted with the *same* key → data persists and decrypts correctly
  3. Real key set, server restarted with the *wrong* key → **fails loudly and exits** (a panic with a clear "wrong key or corrupted data" message), rather than silently returning garbage. This is the correct fail-closed behavior for a security feature — verified the process actually exits, not just logs a warning and limps on.
- Confirmed directly: grepped the raw bytes of `facetql.data` and
  `facetql.wal` for a known plaintext secret written through the API —
  not found in either file. A hex dump of `facetql.data` looks like
  random noise, not readable JSON/bincode.

### Breaking change, stated plainly

Every data file's on-disk format changed — this is not backward
compatible with any prior checkpoint's unencrypted files. A pre-v0.10
`facetql.data`/`facetql.wal`/etc. will fail to decrypt (loudly) if
loaded under this version. There's no migration path built for this yet;
if there's real data in a pre-encryption deployment, it needs to be
exported and re-imported, not just upgraded in place.

### Known gaps, stated plainly

- **No key rotation.** Changing `ENOCHIAN_MASTER_KEY` makes every existing
  record unreadable — there's no re-encrypt-under-a-new-key utility.
- **No external KMS integration** — the key is a single env var, not
  pulled from AWS KMS/HashiCorp Vault/etc. Fine for a single-node
  deployment, a real gap for anything wanting managed key rotation or
  hardware-backed key storage.
- **No secure memory wiping** — the key and decrypted plaintext sit in
  regular process memory like anything else; nothing scrubs it on
  drop. A memory dump of a running process would expose both.
- **Noisy boot output** — if an old/incompatible WAL exists, recovery
  prints one warning line per undecryptable entry, which got extremely
  verbose in testing (156 lines for the genesis grid alone). Cosmetic,
  not a correctness issue, but worth summarizing ("skipped N
  undecryptable WAL lines") rather than printing each one individually.

## v0.11 checkpoint — Pillar 3: TLS/HTTPS

### Added, verified live

- **Native HTTPS**: `facetql start --tls-identity <path.p12> --tls-identity-password <pw>`
  serves HTTPS instead of HTTP. Verified end-to-end: generated a real
  self-signed cert with `openssl req` + `openssl pkcs12`, started the
  server with it, confirmed a plain HTTP request to that port fails,
  confirmed a full authenticated API call (create a node) succeeds over
  HTTPS, and independently confirmed the handshake itself with
  `openssl s_client -connect ... -brief` — a real TLS 1.2 connection,
  correct certificate CN, not just "the port answered."
- Without `--tls-identity`, behavior is unchanged from every prior
  checkpoint — plain HTTP, with a printed note that production traffic
  should either use this flag or terminate TLS at a reverse proxy (nginx/
  Caddy) in front of FacetQL — both are legitimate, this just makes
  native termination possible without forcing it.
- **How this was actually built, because it matters for the next person
  touching this code**: the "normal" approach (the `axum-server` crate)
  turned out to have a real trait-bound incompatibility with this
  project's exact resolved dependency versions — not a toolchain issue
  this time, a genuine version conflict I could not resolve through
  pinning. Rather than fight it further, `src/tls_server.rs` replicates
  axum's own internal connection-serving code directly (the
  `Builder::new(TokioExecutor::new()).serve_connection_with_upgrades(...)`
  call is copied from axum's own `src/serve.rs`, not invented), with a
  TLS handshake inserted before each connection is handed to it. This
  reuses the exact machinery already proven working for every plaintext
  request this whole project has served, instead of a wrapper crate with
  its own separate version constraints.
- Uses `native-tls` (backed by the system's OpenSSL), not `rustls` — the
  pure-Rust `rustls`/`webpki`/`ring` dependency chain hit the same
  edition2024 toolchain wall documented in the v0.7/v0.8 notes for
  `reqwest`/`tokio-postgres`, in *this* sandbox specifically.
  `native-tls` sidesteps it entirely by linking the system OpenSSL that
  was already installed for other reasons.

### Known gaps, stated plainly

- **PKCS#12 only.** If your certs come as separate PEM cert + key files
  (the more common format from Let's Encrypt/certbot), they need
  converting first: `openssl pkcs12 -export -out identity.p12 -inkey key.pem -in cert.pem`.
  Direct PEM support would be a reasonable follow-up.
- **No automatic certificate provisioning/renewal** (no ACME/Let's
  Encrypt integration) — bring your own certificate, and rotate it
  yourself before it expires.
- **No HTTP→HTTPS redirect** — if both are somehow reachable, plain HTTP
  isn't automatically upgraded. In practice only one mode runs per
  process today (whichever `--tls-identity` selects), so this is mostly
  theoretical, but worth knowing if that assumption ever changes.
- **`tls_server.rs`'s accept loop doesn't set `TCP_NODELAY`** the way
  `axum::serve` does by default — a minor latency detail, not a
  correctness issue.

## v0.12 checkpoint — two small primitives for the Facet bridge

Added specifically to unblock a real `Store` implementation for Facet
(see the separate Facet-FacetQL bridge writeup) — both genuinely useful
outside that context too.

### Added, verified live

- **`POST /node` gains `if_absent: true`.** Plain `POST /node` has always
  been upsert (an existing address is silently overwritten) — fine for
  most writes, wrong for anything needing "only I create this, and I
  need to know if I lost that race." Verified under real concurrency: 5
  simultaneous `if_absent` requests for the same address — exactly one
  `201`, the other four cleanly `409`, not a sequential-luck result.
  Atomic because the existence check and the insert happen inside the
  same write lock every mutation already goes through — same mechanism
  as the atomic claim, applied one level earlier (at creation instead of
  at claiming an existing node).
- **`POST /publish`** — broadcasts an arbitrary payload to every
  `/events` subscriber, the same way FacetQL's own internal writes
  already do. This is the piece that was missing for anything OTHER
  than FacetQL itself to put a message on the live feed. Verified live:
  connected a subscriber, published a custom string via `/publish`,
  confirmed it arrived verbatim.
