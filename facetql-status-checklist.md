# FacetQL — Status Checklist (as of v0.9)

Everything marked ✅ below was written, compiled, and exercised against a
live running server before being called done — not just designed. Everything
marked ⬜ is a real, open gap, not a minor detail.

## Core storage & data model

- ✅ Nodes with a typed `kind` (Person, Goal, Resource, etc.) — usable like a real application database, not just a key-value store
- ✅ Relationships (edges) between nodes, typed and directional, indexed both ways
- ✅ Create, read, update, delete — full CRUD, with tombstone-based deletes (append-only log, nothing is silently overwritten)
- ✅ Filtered, paginated queries (`?kind=&owner=&limit=&offset=`)
- ✅ Atomic create-node-with-edges (one call, one node, its edges, together)
- ✅ Multi-operation transactions (`POST /transaction`) — a whole batch validates before any of it applies; verified an invalid batch applies nothing, not even the parts that would've succeeded alone
- ⬜ Transactions are not crash-mid-commit-safe yet — no separate staging log; a process crash partway through an already-valid batch isn't rolled back on restart
- ⬜ No secondary indexes — queries are a linear scan; fine at small scale, not fine once a `kind` has thousands of rows
- ⬜ No edge deletion yet (creating and reading edges works; there's no route to remove one)
- ⬜ No mmap / zero-copy storage — everything is in-memory plus an append-only log, not memory-mapped files

## Auth & access control

- ✅ Per-identity tokens (not one shared key) — `ENOCHIAN_TOKENS` for bootstrap, persistent admin-managed users for everything after
- ✅ Roles: User and Admin — Admin bypasses ownership checks the way a Postgres superuser bypasses row-level security
- ✅ `POST/GET/DELETE /admin/users` — create, list, and revoke users at runtime; tokens are hashed at rest, shown once at creation
- ✅ Ownership enforcement on every read/write (ownership or public visibility required, unless Admin)
- ⬜ No token expiry or rotation policy — a token is valid forever until explicitly revoked
- ⬜ Only two roles — no fine-grained per-table/per-field permissions

## Live updates & jobs

- ✅ Atomic job claiming (`POST /node/:address/claim`) — verified under real concurrent race conditions (4 simultaneous claims, exactly one winner)
- ✅ Live change feed (`GET /events`, Server-Sent Events) — every write anywhere shows up in real time
- ✅ Browser-compatible SSE auth (`?key=` fallback, since `EventSource` can't send headers) — verified live both with and without a valid key
- ⬜ `/events` is not visibility-filtered — every connected subscriber currently sees every event, regardless of who owns the node. Not safe for sensitive multi-owner data yet.

## Networking & security

- ✅ CORS enabled, so a browser page can call the API at all
- ⬜ CORS is currently permissive (any origin) — dev-only, needs narrowing before anything public
- ⬜ No TLS/HTTPS — traffic (including API keys) is unencrypted on the wire
- ⬜ No encryption at rest — data files on disk are plaintext
- ⬜ No rate limiting, no connection limits

## CLI & packaging

- ✅ Real CLI: `init`, `start`, `backup`, `restore`, `import postgres` (see the separate CLI reference)
- ✅ Configurable data directory (`~/.facetql` by default, not wherever you happened to run the command from)
- ✅ `backup`/`restore` — verified live end-to-end, including the safety refusal against overwriting existing data
- ✅ `.github/workflows/release.yml` written (builds native macOS/Linux/Windows binaries on a version tag) — **NOT verified working**, since pushing a tag and watching Actions run needs to happen from your side
- ✅ `install.sh` one-line installer script (depends on the release workflow above actually working)
- ⬜ No LICENSE file chosen yet — needed before public distribution
- ⬜ No actual GitHub Release has been cut yet — the CLI/install story is real, but nobody can `curl | sh` install this until a tag is pushed and confirmed working

## Bridging existing data in

- ✅ `facetql import postgres` — pulls rows from an existing Postgres table in, one node per row, through the same API any other client uses
- ⬜ Could not be verified in my build sandbox (old Rust toolchain there, unrelated to the code) — needs a real `cargo build` on a normal machine to confirm; one real bug was already found and fixed this way (a missing `reqwest` dependency), so treat this specifically as "written, not yet proven"

## A starter to build on

- ✅ `facetql-console.html` — single-file browser starter (create nodes/edges, query, live feed), every request verified against a live server
- ⬜ This is a seed, not a product — styling and structure are meant to be extended into Project Interstate's real frontend

## Explicitly not started

- ⬜ Facet's data layer doesn't talk to FacetQL — Facet currently only persists to Postgres; nothing connects the two yet
- ⬜ No observability (structured logging, metrics, tracing)
- ⬜ No replication / high availability / clustering
- ⬜ No automated test suite, chaos testing, or fuzzing
