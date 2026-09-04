# FacetQL API Reference

Every route below was verified against `create_router` and its handler in
`src/api/routes.rs` for FacetQL `0.13.0`. Nothing here is carried forward
from an earlier version of this document.

Base URL: `http://<host>:8080` (HTTPS if the server was started with
`--tls-identity`).

---

## Authentication

Every route except `GET /` sits behind the auth middleware
(`src/api/routes.rs:316`, `src/auth.rs:297`).

```
x-api-key: <token>
```

`GET /events` additionally accepts `?key=<token>` in the query string,
because browser `EventSource` cannot set headers. There is no
`Authorization: Bearer` support.

* Missing credential → **401** `missing x-api-key header (or ?key= for SSE)`
* Unrecognized credential → **401** `invalid x-api-key`

The token resolves to an identity of `{owner, role}`. **No request body
anywhere carries an `owner`** — the owner stamped on a node or edge is
always the authenticated caller's, which is what makes ownership
spoofing impossible from the client side.

Roles are `User` and `Admin`. An `Admin` bypasses the per-node
visibility and ownership checks on every path (same idea as a Postgres
superuser bypassing row-level security). Routes marked **admin** below
return **403** `admin only` to anyone else.

### Request body limit

4 MiB by default, applied before any deserialization
(`FACETQL_MAX_BODY_BYTES`). Over it → **413**.

### CORS

Permissive: any origin, any header, `GET`/`POST`/`PUT`/`DELETE`
(`src/api/routes.rs:244`). Development-appropriate; narrow before
exposing publicly.

---

## Object shapes

### Node

```json
{
  "address": "post:1",
  "coordinate": { "x": 0, "y": 0, "z": 0, "q": 0 },
  "value": 0,
  "kind": "Post",
  "data": "{\"title\":\"hello\"}",
  "owner": "alice",
  "claimed_by": null,
  "visibility": "Private"
}
```

`visibility` is `"Private"` or `"Public"`. `data` is an opaque string —
the server never validates its shape, and only parses it as JSON to
evaluate a `where` predicate or build a declared index key. `value` is
always `0`; nothing writes it. `coordinate` is stored and returned but
not interpreted by any read or write path.

### Edge

```json
{ "from": "person:1", "to": "post:1", "kind": "AUTHORED", "owner": "alice" }
```

Identity is `(from, to, kind)`. `owner` is not part of it; it decides who
may delete the edge.

### HistoryEntry

```json
{
  "address": "post:1",
  "archived_at_unix": 1730000000,
  "node": { "...": "the full node as it was" },
  "version": 41
}
```

### Error bodies

Plain text with the status, except three JSON-bodied cases: `POST /node`
(both outcomes), `POST /nodes/query` success, and the admin/stats reads.

---

## Nodes

### `POST /node`

Create or overwrite a node, optionally with outgoing edges, as one
crash-atomic mutation.

```json
{
  "address": "post:1",
  "kind": "Post",
  "x": 0, "y": 0, "z": 0, "q": 0,
  "data": "{\"title\":\"hello\"}",
  "public": true,
  "edges": [ { "to": "person:1", "kind": "AUTHORED" } ],
  "if_absent": false
}
```

| field | required | notes |
|---|---|---|
| `address` | yes | client-supplied identity |
| `kind` | yes | free text |
| `x`,`y`,`z`,`q` | yes | `u8` each; stored, not interpreted |
| `data` | yes | opaque string |
| `public` | no | `true` → `Public`, absent/`false` → `Private` |
| `edges` | no | each `{to, kind}`; `from` is the new node |
| `if_absent` | no | `true` → fail with 409 instead of overwriting |

Every edge target must already exist, or be the node being created. The
node, its history entry (if it replaced something) and all N edges are
staged in one `BEGIN…COMMIT` frame — they become visible together or not
at all.

**201 Created**

```json
{ "address": "post:1",
  "edges_created": [ { "from": "post:1", "to": "person:1", "kind": "AUTHORED", "owner": "alice" } ] }
```

**400 Bad Request**

```json
{ "error": "edge 'to' address not found: nope", "edges_created_before_failure": [] }
```

`edges_created_before_failure` is now **always empty** — the batch is
atomic, so there is no such thing as an edge created before the failure.
The field is retained for wire compatibility.

**409 Conflict** — `if_absent: true` and the address exists:
`node already exists: <address>`.

> **Known gap:** this route does *not* check ownership on overwrite.
> Writing to an address another identity owns replaces their node and
> transfers ownership to the caller. `PUT`, `DELETE` and every
> transaction op do check.

### `GET /node/:address`

**200** the Node · **403** `not authorized to read this node` · **404**
`node not found` · **500** on a storage failure.

Readable if it is `Public`, or you own it, or you are an admin.

### `PUT /node/:address`

```json
{ "data": "{\"title\":\"updated\"}", "public": false }
```

Full replacement of `data`; `public` is optional and only changes
visibility when present. There is no partial patch. The previous state
is archived to history automatically.

**200** `Node updated` · **403** `not authorized to modify this node` ·
**404** `node not found` · **500**.

### `DELETE /node/:address`

**204 No Content** · **403** `not authorized to delete this node` ·
**404** `node not found` · **500**.

The removed state is archived to history first. Removing the index entry
is what makes the node gone; the record's bytes remain in the heap until
compaction reclaims them.

### `GET /node/:address/history`

Every archived previous state, oldest first, as an array of
`HistoryEntry`. Does **not** include the current live value. Empty array
if the node was never overwritten.

Authorized against the node's **current** owner and visibility — a node
that changed hands shows its full history to whoever owns it now.

**200** `[HistoryEntry, …]` · **403** · **404** · **500**.

### `GET /node/:address/owned`

Every live node owned by the **authenticated caller**.

> The `:address` path segment is accepted and **entirely ignored** —
> the handler takes no path parameter (`src/api/routes.rs:537`). Pass
> anything; you get your own nodes.

**200** `[Node, …]` · **500**.

Not paginated. Refuses with **400** past `FACETQL_MAX_SCAN_ROWS`
(100 000) rather than truncating.

### `POST /node/:address/claim`

Atomically claim a node for the caller if nobody holds it. No request
body. This is the `FOR UPDATE SKIP LOCKED` primitive for a durable job
queue: the check and the write happen inside one mutation, so exactly
one concurrent caller wins.

**200** `claimed` · **404** `node not found` · **409**
`already claimed by <owner>` · **500**.

There is no unclaim route; release a claim by overwriting the node.

---

## Listing and querying

### `GET /nodes`

```
GET /nodes?kind=Post&owner=alice&limit=20&offset=0
```

All parameters optional. `limit` defaults to 50, capped at 500. `offset`
defaults to 0 and is refused past `FACETQL_MAX_QUERY_OFFSET` (10 000)
with **400**.

The access path is chosen from the filters: `kind` given → kind index;
`owner` only → owner index; neither → primary index scan.

Visibility: a `User` sees public nodes plus its own; an `Admin` sees
everything matching the filter.

**200** — a **bare JSON array**, not an object:

```json
[ { "address": "post:1", "...": "..." } ]
```

### `POST /nodes/query`

Predicate pushdown, in-engine ordering, and keyset pagination. The field
names mirror FCT's `runtime.Query`.

```json
{
  "kind": "Post",
  "owner": "alice",
  "where": { "kind": "bin", "op": ">", "l": {...}, "r": {...} },
  "item_var": "item",
  "order": "created_at",
  "desc": true,
  "after": "eyJvIjoxNzMwMDAwMDAwLCJhIjoicG9zdDoxIn0",
  "limit": 20,
  "offset": 0
}
```

| field | default | notes |
|---|---|---|
| `kind` | — | optional exact match |
| `owner` | — | optional exact match |
| `where` | — | optional `Expr` (below); omitted = no predicate |
| `item_var` | `"item"` | loop variable the predicate's field accesses name |
| `order` | — | top-level `data` field; `"id"` or absent = order by `address` |
| `desc` | `false` | reverse the ordering |
| `after` | — | opaque cursor from the previous page's `next` |
| `limit` | `50` | capped at 500 |
| `offset` | `0` | ignored when `after` is present; else capped at 10 000 |

**200**

```json
{ "nodes": [ { "...": "Node" } ], "next": "eyJvIjoxNzI5OTk5OTk5LCJhIjoicG9zdDo3In0" }
```

`next` is `""` on the last page. Otherwise feed it back as `after`.
Anything else — a bad cursor, an unevaluable predicate, an over-limit
offset, a scan that would exceed `FACETQL_MAX_SCAN_ROWS` — is **400**
with the reason in the body. A predicate the engine cannot evaluate is
always an error, never a partial or wrong result set.

#### The cursor

Opaque to the client: base64url of compact JSON `{"o":<order value or
omitted>,"a":"<address>"}`, capped at 4 KiB
(`Cursor`, `src/storage/engine.rs`). The ordering is the composite
`(order_field, address)` — `address` is the stable tiebreak that keeps
the total order deterministic when order values collide — and the next
page selects rows *strictly past* the cursor in the requested direction.
Paging is therefore stable under concurrent inserts and deletes in a way
`offset` is not.

The same cursor works whether the query is served by a declared index or
by the sort path, so declaring or dropping an index never invalidates an
outstanding cursor.

#### Which access path serves it

Chosen in this order (`StorageEngine::query_where`):

1. **Equality prefix.** `kind` is given and `where` pins a field covered
   by a declared index to a literal — `item.f == <lit>`, at the top level
   or under `&&`, with a non-null literal — and either no `order` or an
   `order` on that same field. Only the entries holding that value are
   read.
2. **Address range scan.** `order` absent or `"id"`. One page read.
3. **Declared-index range scan.** `order` names a field with a declared
   index over `(kind, field)`. One page read, no scan-row ceiling.
4. **Materialize and sort.** Everything else: every matching candidate is
   held in memory, sorted by `(order_field, address)`, and the page is
   cut out. Bounded by `FACETQL_MAX_SCAN_ROWS`; over it, **400** rather
   than a truncated answer.

The result is identical whichever path runs — the predicate is evaluated
in full on every candidate in all four. Only the cost differs.

#### `Expr` — the predicate

Wire-compatible with FCT's `ir.Expr` (`src/core/predicate.rs`). Fields:
`kind`, `val`, `vtype`, `name`, `field`, `obj`, `key`, `op`, `args`,
`l`, `r`, `x`, `var`, `where`. The evaluated subset is:

* `{"kind":"lit","val":…,"vtype":"int"|"text"|"bool"}` — a literal.
  With no `vtype`, the literal is whatever JSON says it is.
* `{"kind":"get","obj":{"kind":"ref","name":"<item_var>"},"field":"f"}`
  — read `data.f`. A `get` whose object is not a reference to
  `item_var` is rejected.
* `{"kind":"un","op":"!"|"-","x":…}`
* `{"kind":"bin","op":…,"l":…,"r":…}` with
  `== != < <= > >= + - * / % && ||`. `&&`/`||` short-circuit. `+`
  concatenates when both sides are strings, otherwise adds. Ordering
  comparisons require numeric operands.

Anything else — a `call`, a `filter`, a nested object access — is
rejected with a message naming what could not be evaluated, so a client
can fall back to filtering its own rows. Nesting deeper than 64 levels
is refused.

Example — `item.status == "open" && item.score > 10`:

```json
{"kind":"bin","op":"&&",
 "l":{"kind":"bin","op":"==",
      "l":{"kind":"get","obj":{"kind":"ref","name":"item"},"field":"status"},
      "r":{"kind":"lit","val":"open","vtype":"text"}},
 "r":{"kind":"bin","op":">",
      "l":{"kind":"get","obj":{"kind":"ref","name":"item"},"field":"score"},
      "r":{"kind":"lit","val":10,"vtype":"int"}}}
```

---

## Edges

### `POST /edge`

```json
{ "from": "person:1", "to": "post:1", "kind": "AUTHORED" }
```

Both endpoints must already exist. **Upsert on identity**: re-asserting
the same `(from, to, kind)` lands on the same entry rather than beside
it — but replacing an edge owned by someone else is refused, because the
owner is who may retract it.

**201** `Edge created` · **400** with the reason (`edge 'from' address
not found: …`, `edge … is owned by …`).

### `DELETE /edge`

Addressed by **request body**, not by path — `from`, `to` and `kind` are
arbitrary strings that may contain `/`, and a path segment cannot carry
one without an escaping convention both sides must get right
(`src/api/routes.rs:124-147`).

```json
{ "from": "person:1", "to": "post:1", "kind": "AUTHORED" }
```

The edge's owner, or an admin. **204** · **403** `not authorized to
delete this edge` · **404** `edge not found` · **500**.

### `GET /node/:address/edges/out` · `GET /node/:address/edges/in`

Outgoing and incoming edges for one node, as `[Edge, …]`. Gated by
whether you can read the node itself, not by who owns each edge.

**200** · **403** · **404** `node not found` · **500**.

---

## Transactions

### `POST /transaction`

```json
{ "operations": [ { "type": "insert_node", "...": "..." } ] }
```

Each operation is tagged by `"type"` with snake_case values. `owner` and
`is_admin` are stamped onto every op from the authenticated identity —
a batch cannot ask to act as somebody else.

The batch is resolved and validated in full against a staged view of the
data (live state overlaid with what the batch has done so far) before a
byte is written, then staged into one durable `BEGIN … COMMIT` frame,
then applied. A crash before the `COMMIT` is durable discards the whole
batch; a crash after it replays the whole batch.

#### `insert_node`

```json
{ "type": "insert_node", "address": "post:1", "kind": "Post",
  "x": 0, "y": 0, "z": 0, "q": 0, "data": "{}", "public": false }
```

Upsert. Unlike `POST /node`, overwriting an address owned by a different
identity **aborts the batch** (`address X is owned by Y`). An overwrite
archives the previous state.

#### `delete_node`

```json
{ "type": "delete_node", "address": "post:1" }
```

Targeted, so it is strict: the handler pre-checks under the same write
lock the engine will use and returns **403** `not authorized to delete
<address>` or **404** `delete target not found: <address>` before
anything is written. Deleting an address this same batch already removed
is a no-op, not an error. The removed state is archived.

#### `insert_edge`

```json
{ "type": "insert_edge", "from": "person:1", "to": "post:1", "kind": "AUTHORED" }
```

Endpoints are checked against the staged view: an edge may reference a
node this batch inserted **earlier**, but not one it deleted, and not one
inserted later.

#### `delete_edge`

```json
{ "type": "delete_edge", "from": "person:1", "to": "post:1", "kind": "AUTHORED" }
```

Targeted like `delete_node`: **403** or **404** rather than a silent
skip. `owner` is never taken from the body — it is not part of an edge's
identity.

#### `clear_kind`

```json
{ "type": "clear_kind", "kind": "Post" }
```

Remove every node of `kind` the caller may write, as one all-or-nothing
step. Never rejects on authorization: a non-admin clears only its own
nodes of that kind and an admin clears all of them — non-writable nodes
are skipped, not an error. Each removed node is archived. Driven by the
kind index, so the cost is the kind, not the database.

#### `delete_where`

```json
{ "type": "delete_where", "kind": "__session",
  "where": { "kind": "bin", "op": "<", "...": "..." } }
```

`clear_kind`'s predicated superset. Selection is: kind matches **and**
the caller may write it **and** (when `where` is present) the predicate
holds against the decoded `data`. Omitting `where` is exactly
`clear_kind`. The predicate runs through the same `predicate::eval` the
query path uses, so a bulk delete selects the rows the equivalent query
would return. An unevaluable predicate is a **400** that aborts the whole
batch before anything is written — never a wrong or partial delete. The
loop variable is `"item"` (this op carries no `item_var`).

#### `set_if`

Compare-and-set on one field of one node.

```json
{ "type": "set_if", "address": "cron:nightly", "field": "next_run",
  "expect_le": 1730000000,
  "set": { "next_run": 1730086400, "owner_worker": "w1" } }
```

Exactly one expectation must be given, or **400**:

| expectation | holds when |
|---|---|
| `expect_le: <number>` | the field is a number and is ≤ this (lease / deadline) |
| `expect_eq: <value>` | the field equals this exactly (version CAS) |
| `expect_absent: true` | the field is unset or `null` (create-once) |

`expect_absent: false` states no condition and counts as "not given".
`set` is **merged** into the node's `data`, so unrelated fields are not
clobbered. The target must exist and be writable by the caller. A won CAS
archives the previous state.

**Responses**

* **200** `transaction committed`
* **400** the batch was invalid — nothing was applied
* **403** / **404** from the `delete_node` / `delete_edge` pre-checks
* **412 Precondition Failed** a `set_if` condition did not hold. Nothing
  in the batch was applied: you lost the race.
* **500** the batch was fine; the storage was not

On success the batch publishes one `transaction_committed` event naming
the node addresses it touched, to the caller's own audience.

**Size:** the *lowered* batch is capped at
`FACETQL_MAX_TRANSACTION_OPS` (50 000). One wire op can lower into many
— `clear_kind` over a large kind is one delete plus one archive per node
— so the check is on the resolved list, not on the request.

---

## Events

### `GET /events` — Server-Sent Events

Open and hold the connection (`curl -N`, or `EventSource`). Auth via
`x-api-key` or `?key=<token>`.

Every event carries an audience stamped by the handler that wrote it, and
a subscriber receives only the events that admit it
(`src/database.rs:286-327`):

* an event about a **public** node → everyone
* an event about a **private** node → its owner, plus admins
* an edge event → everyone only when **both** endpoints are public,
  otherwise the edge owner plus admins
* a `transaction_committed` → the caller, plus admins
* a `user_created` / `user_revoked` → the acting admin, plus admins

Payloads are JSON strings:

```json
{"event":"node_created","address":"post:1","kind":"Post"}
{"event":"node_updated","address":"post:1"}
{"event":"node_deleted","address":"post:1"}
{"event":"node_claimed","address":"job:1","worker":"w1"}
{"event":"edge_created","from":"a","to":"b","kind":"AUTHORED"}
{"event":"edge_deleted","from":"a","to":"b","kind":"AUTHORED"}
{"event":"transaction_committed","addresses":["post:1","post:2"]}
{"event":"user_created","owner":"bob","created_by":"root"}
{"event":"user_revoked","owner":"bob"}
```

Events are best-effort notifications, never the source of truth. The
channel retains 1024 events; a subscriber that falls behind silently
misses some and the stream continues.

### `POST /publish`

```json
{ "payload": "anything you like" }
```

Puts an arbitrary application message on the same feed — the
LISTEN/NOTIFY replacement. The audience comes from the caller's identity,
never from the body: an **admin** reaches everyone; anyone else reaches
their own subscribers and admins.

**200** `published` · **413** if the payload exceeds 64 KiB.

---

## Administration

### `GET /stats` — admin

```json
{
  "node_count": 1024,
  "edge_count": 96,
  "user_count": 3,
  "history_entries": 512,
  "kinds": [ { "kind": "Post", "count": 900 }, { "kind": "Person", "count": 124 } ],
  "reads_total": 88123,
  "writes_total": 4021,
  "storage": { "page_size": 16384, "segments": 3, "pages": 812, "obsolete_bytes": 190233 }
}
```

Structural counts come off the indexes' own entry counters and are free.
`kinds` walks the kind index (index-only, but proportional to the number
of nodes) and is sorted by kind. `reads_total`/`writes_total` are
process-lifetime counters that reset on restart — difference two samples
for a rate.

**200** · **403** `admin only`.

### `POST /admin/users` — admin

```json
{ "owner": "bob", "role": "user" }
```

`role` is `"admin"`, `"user"`, or omitted (defaults to `"user"`). An
unknown value is **400**.

**201**

```json
{ "owner": "bob", "role": "User", "token": "f949ad35…" }
```

The token is shown **exactly once**. Only its SHA-256 hash is stored;
there is no way to retrieve it again. If it is lost, revoke and recreate.

### `GET /admin/users` — admin

**200** `[ { "owner": "bob", "role": "User" }, … ]`. Never tokens or
hashes. Does **not** include `ENOCHIAN_TOKENS` bootstrap identities.

### `DELETE /admin/users/:owner` — admin

Revokes every persistent record for that owner.

**204** · **404** `no persistent user with that owner`. A bootstrap
identity cannot be revoked this way — edit `ENOCHIAN_TOKENS` and
restart.

### `POST /admin/indexes` — admin

Declare an index over one top-level `data` field of one kind.

```json
{ "name": "post_created", "kind": "Post", "field": "created_at" }
```

* `name` — 1–64 bytes, `[A-Za-z0-9_-]` only (it becomes the filename
  `facetql.idx.data.<name>`).
* `kind` — the node kind the index covers. Per-kind because `data` has
  no schema across kinds.
* `field` — the top-level field, the same name you would pass as
  `order` on `POST /nodes/query`.

**This call reads every existing node of the kind**, twice over: once to
prove every existing value produces an admissible key, and once to
backfill. It therefore costs the size of the kind. It is a logged,
crash-atomic mutation (WAL `CreateIndex`) whose replay is idempotent.

**201** — the stored definition:

```json
{ "name": "post_created", "kind": "Post", "field": "created_at" }
```

**409 Conflict** — a *different* index already holds that name, or
another index already covers that `(kind, field)`. Re-declaring the
**identical** index is a successful 201, so a setup script may run twice.

**400 Bad Request** — a name/kind/field the storage layer cannot honour,
or a kind that already contains a value whose encoding exceeds 512 bytes
(`cannot index Post.body: node 'post:9' field 'body' is 900 bytes
encoded, over the 512-byte maximum for index 'post_body'`). Nothing is
logged in that case: the index is simply not created.

Once declared, a write producing an oversized encoded value for that
index is rejected too — again before anything reaches the WAL, because a
logged-but-unapplicable mutation would fail every subsequent startup.

### `GET /admin/indexes` — admin

**200** `[ { "name": …, "kind": …, "field": … }, … ]`, in name order.

### `DELETE /admin/indexes/:name` — admin

**204** · **404** `no index named '<name>'`.

Queries the index was serving keep working — they fall back to the
materialize-and-sort path, which is slower and bounded by
`FACETQL_MAX_SCAN_ROWS`, not wrong.

---

## `GET /` — liveness

The one unauthenticated route. **200**, body `FacetQL Online`.

---

## Status codes, at a glance

| code | meaning |
|---|---|
| 200 | OK |
| 201 | node / user / index created, edge created |
| 204 | deleted (node, edge, user, index) |
| 400 | malformed request, invalid batch, unevaluable predicate, bad cursor, over a scan/offset bound |
| 401 | missing or invalid `x-api-key` |
| 403 | authenticated but not permitted (ownership, or `admin only`) |
| 404 | no such node / edge / user / index |
| 409 | `if_absent` collision, already-claimed node, contradictory index declaration |
| 412 | a `set_if` precondition did not hold; nothing in the batch applied |
| 413 | request body over `FACETQL_MAX_BODY_BYTES`, or `/publish` payload over 64 KiB |
| 500 | storage failure — the request was fine, the disk was not |
