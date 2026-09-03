# FacetQL — Production-Hardening Roadmap

> **One integrated plan.** This folds the 33-phase FacetQL production-hardening
> directive into the plans we already have — the F33D3R coordination log
> (`../AGENT_LOG.md`), the fabric seam (`../fabric/FABRIC_INTEGRATION_PLAN.md`),
> and the honest v0.9 gap list (`facetql-status-checklist.md`). It is a
> **sequencing artifact, not code.** Nothing here modifies the engine; it decides
> *what order* the work happens in, *which agents can run in parallel without
> colliding on the same files*, and *what gates the human owns.*

Status of inputs at time of writing:
- **Phase 0 audit — IN FLIGHT** (read-only agent producing `CURRENT_ARCHITECTURE.md`).
  Every code wave below is **gated on that audit landing** — the directive itself
  forbids touching core architecture before the audit is done.
- **`GET /stats` — IN FLIGHT** (Phase 29 observability; also the fabric telemetry
  seam). Treated as already-owned so no other wave re-implements counters.

---

## 0. How this reconciles with the existing plan

- **Priority is unchanged.** `AGENT_LOG.md §33` already ranks *FacetQL core* as
  priority #1. This roadmap is the concretization of that #1 — not a new
  direction. Fabric stays #12; fct/facets consume FacetQL through the stable wire
  contract (`§4b`) and must not be destabilized by this work.
- **The one rule holds.** Native engine only. No Postgres/SQL/ORM in the path.
  `tokio-postgres` stays confined to `src/importer.rs` (one-way import tool); the
  audit confirms this and any leak is a Wave-1 blocker.
- **Root solutions, never patch, no rework, never touch git.** Every wave audits
  before writing, grows the correct layer, and leaves changes in the working tree
  for the human to commit.
- **The fabric seam already fits.** `GET /stats` (Phase 29) is simultaneously the
  observability primitive *and* the telemetry producer fabric's poller needs —
  one primitive, two consumers. No conflict; it's the first thing standing.

---

## 1. The core orchestration problem, and the rule that solves it

Almost every hardening phase wants to edit the **same six hot files**:
`storage/engine.rs`, `storage/wal.rs`, `storage/recovery.rs`,
`storage/transaction.rs`, `api/routes.rs`, `auth.rs`. Firing 30 agents at them in
parallel guarantees merge chaos = rework = forbidden.

**Rule: hot files get a single owner at a time. Independent work goes in NEW
files/dirs and runs in parallel.** So the program splits into two lanes:

- **Lane P (Parallel-safe):** phases that create *new* top-level dirs or docs and
  touch **zero** hot files — they can all run at once, anytime after Gate 0.
- **Lane S (Serialized):** phases that mutate hot files — they run **one wave at a
  time**, in dependency order, each on a clean tree handed off by the prior wave.

---

## 2. Gate 0 — Architecture audit (IN FLIGHT, blocks Lane S)

`FACETQL-AUDIT-ARCHITECTURE` → `CURRENT_ARCHITECTURE.md` + a phase/file-ownership
map + top blockers. **No Lane-S wave starts until this lands and I've read the
file-ownership map.** Lane-P phases that are pure-new-files may start as soon as
the audit confirms they collide with nothing (threat model, supply-chain, license
audit have no dependency on it at all — see §4).

---

## 3. Lane S — serialized core-code waves (hot files, one at a time)

Dependency-ordered. Each wave = one focused agent (or a tight pair on disjoint
files), verified green (`build`/`test`/`clippy`/`fmt --check`) before the next
starts. Ordering follows correctness dependencies: durability substrate → the
things that rely on it → the surfaces on top.

| Wave | Phase(s) | Hot files owned | Depends on | Why here |
|------|----------|-----------------|-----------|----------|
| **S1** | 1 Storage engine (framing, checksums, atomic/partial-write, fsync, corruption detection, format versioning) | `storage/binary.rs`, `storage/engine.rs` | Gate 0 | Everything durable sits on the record format. Fix the substrate first. |
| **S2** | 2 WAL & crash recovery (real recovery states; interrupted-op tests) | `storage/wal.rs`, `storage/recovery.rs` | S1 | Recovery semantics depend on the record framing S1 defines. |
| **S3** | 3 Transactions (true atomic commit/rollback, crash-mid-commit, staging log) | `storage/transaction.rs`, `storage/commit.rs`, `storage/engine.rs` | S2 | The known v0.9 gap (§checklist): valid batch not rolled back on crash. Needs S2's recovery. |
| **S4** | 4 Query engine + 5 Indexing (finish predicate pushdown, keyset cursor already exists; secondary indexes to kill linear scan) | `core/predicate.rs`, `storage/index.rs`, `storage/engine.rs`, `api/routes.rs` | S1 | Reads over a now-trustworthy store; indexes measured vs scan (Lane-P benches). |
| **S5** | 8 Auth + 9 Authz + 10 Admin-plane (identity vs permission split; rotation/revocation/expiry; role model; data-plane vs control-plane isolation) | `auth.rs`, `core/user.rs`, `api/routes.rs` | Gate 0 | Security core. Independent of storage internals, so could interleave with S1–S4 on a *separate* agent IF routes.rs ownership is coordinated (see §5). |
| **S6** | 11 Crypto + 12 Encryption-at-rest (key lifecycle, nonce, rotation, fail-closed dev-vs-prod keys; encrypted data/wal/edges/tombstones with framing+auth) | `crypto.rs`, `config.rs`, `storage/binary.rs` | S1, S5 | At-rest encryption wraps the S1 record format and uses S5/config key policy. |
| **S7** | 13 Network + 14 TLS + 15 API-security (limits, timeouts, connection caps, no plaintext prod start, per-endpoint authz matrix) | `tls_server.rs`, `api/routes.rs`, `api/mod.rs`, `config.rs` | S5 | Transport + endpoint hardening on top of the finished authz model. |
| **S8** | 7 Resource-guard + 17 Rate-limit (body/record/result/traversal caps; per-identity/token/endpoint limits; fail-safe) | `api/routes.rs`, `api/mod.rs`, new `api/limits.rs` | S7 | Governs the surface S7 finalizes; fail-closed before exposure. |
| **S9** | 6 Concurrency (audit shared-state boundaries; reduce lock contention if measured) | `storage/engine.rs`, `database.rs` | S1–S3 | Only meaningful once storage/tx semantics are final; guided by Lane-P stress tests. |
| **S10** | 16 Audit-log + 29 Observability finish (security audit trail distinct from WAL; health/ready/live/metrics beyond `/stats`) | new `src/audit.rs`, `api/routes.rs` | S5 | Audit trail records the auth/authz events S5 defines. Builds on in-flight `/stats`. |
| **S11** | 18 Backup + 19 Integrity-check + 27 CLI (`facetql check`, consistent snapshot/restore, `init/start/status/query/...`) | `cli/*`, `storage/checkpoint.rs`, new `src/integrity.rs` | S1–S3 | Backup/restore/check must reflect the final storage+tx format. CLI surfaces real engine ops only. |

**Fast-track note:** S5 (security core) has no data-dependency on S1–S4. If the
audit confirms `auth.rs`/`core/user.rs` are disjoint from the storage waves and
`routes.rs` edits can be windowed, S5 may run *concurrently* with S1–S4 as a
second serialized lane. That's the only sanctioned parallelism inside Lane S, and
only if the audit's file-map says it's clean.

---

## 4. Lane P — parallel-safe from the start (new files/dirs, zero hot-file edits)

These need no hot-file access and can run concurrently the moment their inputs
exist. They are also how we *measure* the Lane-S work rather than asserting it.

| Track | Phase(s) | Output (new, isolated) | Can start |
|-------|----------|------------------------|-----------|
| **P-Bench** | 22 Performance + 23 Load/overload | `benches/`, load harness | After S3 (needs real tx) for meaningful numbers; scaffolding anytime |
| **P-Fuzz** | 20 Fuzzing | `fuzz/` targets (parsers, predicates, WAL records, addresses, payloads) | After Gate 0 |
| **P-Prop** | 21 Property testing | `tests/` invariants (insert→read, commit-survives-restart, owner-spoof-impossible…) | Grows per Lane-S wave that lands the property |
| **P-Threat** | 25 Threat model | `docs/THREAT_MODEL.md` | Immediately (independent of code) |
| **P-Red** | 24 Red-team | `REDTEAM_REPORT.md` (finding/severity/repro/fix/verify) | After S5+S7 exist (attacks the real surface); design anytime |
| **P-Supply** | 26 Supply chain | dependency/lockfile/CI audit note | Immediately |
| **P-Fail** | 30 Failure semantics | `docs/FAILURE_SEMANTICS.md` | Tracks each Lane-S wave |
| **P-Docs** | 31 Documentation + 28 Compatibility | `docs/*` describing *actual* guarantees | Trails Lane-S; never document aspirational as implemented |

---

## 5. Hot-file ownership ledger (the anti-conflict contract)

Only one wave holds a hot file at a time. `api/routes.rs` is the busiest — it is
touched by S4, S5, S7, S8, S10. **It is the critical section of the whole
program**; those waves must serialize on it even if nothing else forces them to.

```
storage/engine.rs   → S1 → S3 → S4 → S9   (durability core; heaviest)
storage/binary.rs   → S1 → S6
storage/wal.rs      → S2
storage/recovery.rs → S2
storage/transaction.rs / commit.rs → S3
core/predicate.rs / storage/index.rs → S4
auth.rs / core/user.rs → S5
api/routes.rs       → S4 → S5 → S7 → S8 → S10   ← strict single-owner queue
crypto.rs / config.rs → S6 → S7
tls_server.rs / api/mod.rs → S7 → S8
cli/* / storage/checkpoint.rs → S11
```

New files (no contention): `api/limits.rs`, `src/audit.rs`, `src/integrity.rs`,
`benches/`, `fuzz/`, `tests/`, `docs/`.

---

## 6. Human-owned decisions (gates I will NOT invent)

The directive is explicit that some choices are the owner's, not an agent's:

- **Phase 32 — License.** Public repo ≠ licensed. Do **not** invent a license.
  Needs your explicit choice before any LICENSE file is added; the agents will
  only *audit* current files for notices and dependency-license compatibility.
- **Phase 14 — TLS/cert policy for production.** Dev mode may be plaintext;
  production must fail closed. The *cert/key provisioning* decision (self-managed
  vs. terminating proxy expectation) is yours to set as policy before S7 finalizes.
- **Phase 33 — Security gate & "production-ready" sign-off.** The final call that
  FacetQL is production-ready is a human judgment against the gate, informed by
  `FACETQL_PRODUCTION_READINESS.md` — agents produce evidence, not the verdict.

---

## 7. Definition of done (this phase) — carried verbatim as the exit test

`cargo check` · `cargo test` · `cargo clippy` · `cargo fmt --check` all pass, **and**
crash-recovery tested · transactions tested · authorization tested · storage-
corruption tested · resource-limits tested · concurrency tested · load tests exist
· fuzz/property tests where appropriate · backups restore successfully · security
audit complete · red-team findings addressed or explicitly accepted · benchmarks
reproducible · CLI functional · docs match implementation · release process
verified · license decision explicit.

Final deliverable at program end: **`FACETQL_PRODUCTION_READINESS.md`** with the
15 required sections and explicit statuses (`IMPLEMENTED / TESTED / PARTIALLY
IMPLEMENTED / UNTESTED / KNOWN LIMITATION / BLOCKER`). Nothing marked "secure"
because code exists; nothing "production-ready" because tests pass.

---

## 8. Cross-repo ties (so hardening doesn't break the stack)

- **Wire contract stability (§4b).** Auth/authz (S5) and API-security (S7) must not
  change existing endpoint shapes fct's `fqStore` depends on. New endpoints are
  additive; behavior changes to existing ones require a coordination entry in
  `AGENT_LOG.md` and a matching fct-side note. **No silent contract drift.**
- **Fabric telemetry seam.** `GET /stats` (in flight) is the producer fabric's
  poller consumes. Fabric stays fabric-unaware-of-nothing: FacetQL emits, fabric
  polls. No coupling added to FacetQL.
- **The §28 CAS / `set_if` request** (needed by fct's `ReserveCron`) is a native
  tx-op decision that lands naturally inside S3 (transactions) — fold it there
  rather than tracking it separately.

---

## 9. Immediate next actions (in order)

1. **Land Gate 0** (audit, in flight) — read its file-ownership map & blockers.
2. **Land `GET /stats`** (in flight) — unblocks the fabric poller and Phase 29.
3. Only then, on your go-ahead, begin **Wave S1** (storage substrate) as a single
   owner, with Lane-P threat-model / supply-chain / property-test scaffolding
   running alongside.

This document is the plan. No engine code has been changed to produce it.
