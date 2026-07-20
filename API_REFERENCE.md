# FacetQL API Reference — v0.2

This is the surface a client (Swift/iOS, Kotlin/Android, web, Facet, or
anything else that speaks HTTP+JSON) needs to build against. It documents
what's actually implemented and tested, not the long-term vision — see
`SECURITY_NOTES.md` for what's deliberately not built yet.

Base URL: `http://<host>:8080` (no TLS in this checkpoint — see Known Gaps).

## Authentication

Every route except `GET /` requires an `x-api-key` header. The server maps
that key to an owner identity via the `FACETQL_TOKENS` environment variable
it was started with (`token1:alice,token2:bob`). **There is no per-request
identity field anywhere else in the API** — whatever `owner` ends up on a
node or edge you create is determined entirely by which token you sent, not
by anything in the request body. This is intentional: it's what makes
ownership spoofing impossible from the client side.

```
x-api-key: <your token>
```

Missing or unrecognized key → `401 Unauthorized`.

## Data model

- **Node** — the basic entity. Every node has an `address` (unique ID),
  a `kind` (free-text entity type — `"Person"`, `"Goal"`, `"Resource"`,
  whatever your application needs), `data` (an opaque string — put your
  JSON payload here, the DB doesn't validate its shape), an `owner`, and
  a `visibility` (`"Private"` or `"Public"`).
- **Edge** — a directed, typed relationship between two nodes: `from`,
  `to`, `kind` (free-text, e.g. `"BELONGS_TO"`, `"VERIFIED_BY"`), `owner`.

## Endpoints

### `POST /node` — create a node (optionally with edges, atomically)

```json
{
  "address": "Goal1",
  "kind": "Goal",
  "x": 2, "y": 1, "z": 0, "q": 0,
  "data": "{\"title\":\"Get off the streets\"}",
  "public": true,
  "edges": [
    { "to": "Pers1", "kind": "BELONGS_TO" }
  ]
}
```

- `x/y/z/q` are the coordinate fields — pick any values for now; nothing
  currently depends on their meaning beyond identifying the node's position
  in the grid. `address` must be unique.
- `edges` is optional. If present, each edge is created from the new node
  to an **existing** node right after the node itself. See "Atomicity" below
  for exactly what happens if one fails.
- `owner` is NOT a field here — see Authentication.

**Success — `201 Created`:**
```json
{ "address": "Goal1", "edges_created": [ { "from": "Goal1", "to": "Pers1", "kind": "BELONGS_TO", "owner": "alice" } ] }
```

**Failure (e.g. an edge target doesn't exist) — `400 Bad Request`:**
```json
{ "error": "edge 'to' address not found: DoesNotExist", "edges_created_before_failure": [] }
```

#### Atomicity — read this before you rely on it

If any edge in the `edges` list fails, the server tombstones (deletes) the
node it just created, so you never end up with a live node that's missing
an expected relationship — verified with a live test. **What it does NOT
do:** roll back edges that succeeded *before* the failing one. If you send
three edges and the second one fails, the first edge still exists pointing
at a now-deleted node. For the common case — one node, one edge — this is
fully safe. For multi-edge creates, check `edges_created`/
`edges_created_before_failure` in the response and clean up if needed.

### `GET /node/:address` — read one node

Returns the node if it's public, or if you're the owner. Otherwise `403`.
`404` if it doesn't exist (including if it was deleted).

### `PUT /node/:address` — update a node

```json
{ "data": "{\"title\":\"updated\"}", "public": false }
```
Owner-only (`403` otherwise). Full replace of `data`/`visibility` — there's
no partial-field patch.

### `DELETE /node/:address` — delete a node

Owner-only. `204 No Content` on success. This is a tombstone, not a byte
erasure — the historical record isn't recoverable through the API, but the
raw log isn't scrubbed either (see `SECURITY_NOTES.md` if that distinction
matters for your compliance requirements).

### `GET /node/:address/owned` — list everything you own

Returns every live node whose owner matches your authenticated identity.

### `POST /transaction` — multiple operations, validated and applied together

```json
{
  "operations": [
    {"type":"insert_node","address":"Goal1","kind":"Goal","x":2,"y":1,"z":0,"q":0,"data":"{}","public":true},
    {"type":"insert_edge","from":"Goal1","to":"Pers1","kind":"BELONGS_TO"},
    {"type":"delete_node","address":"OldGoal"}
  ]
}
```

An edge can reference a node created earlier in the same batch. If ANY
operation is invalid (bad edge target, delete target not found/not
yours), **nothing in the batch is applied** — verified live, including
that a node which would have succeeded on its own doesn't get created if
a later operation in the same batch fails.

**Not** crash-mid-commit-safe yet — see `SECURITY_NOTES.md` for exactly
what guarantee this does and doesn't provide.

### `POST /node/:address/claim` — atomic job claim

No body. Claims the node for the authenticated identity if nobody has yet.

**Success — `200 OK`**, body `claimed`.
**Already claimed — `409 Conflict`**, body `already claimed by <owner>`.

Safe under real concurrency — verified with genuinely simultaneous requests
from multiple identities; exactly one wins.

### `GET /events` — live change feed (Server-Sent Events)

Connect and keep the connection open (`curl -N`, or `EventSource` in a
browser/native client). Every successful write anywhere in the database — 
node created/updated/deleted, edge created, node claimed, user
created/revoked, transaction committed — is pushed here as it happens.

**Auth for this endpoint specifically:** browser `EventSource` can't set
custom headers, so if `x-api-key` is absent this endpoint also accepts
`?key=<token>` in the URL. Real security tradeoff: a token in a URL can
end up in logs in a way a header wouldn't — use the header everywhere
else, this fallback exists only because SSE from a browser has no other
option today.

**Known gap:** this is currently unfiltered — every connected subscriber
sees every event regardless of node ownership/visibility. Do not point this
at anything with sensitive multi-owner data yet; see `SECURITY_NOTES.md`.

## CORS

Enabled and permissive (`Access-Control-Allow-Origin: *`) so a browser
page on any origin can call this API — needed for `facetql-console.html`
(or any other browser-based client) to work at all during development.
**Narrow this before running anything public** — see `cors_layer()` in
`src/api/routes.rs`.

### `GET /nodes?kind=&owner=&limit=&offset=` — the query endpoint

This is the one to build list views against. All params optional:

- `kind` — exact match on the `kind` field (`?kind=Goal`)
- `owner` — exact match on owner (`?owner=alice`)
- `limit` — default 50, capped at 500
- `offset` — for pagination

Applies the same visibility rule as a single GET — you only see nodes that
are public or that you own, regardless of filters.

```
GET /nodes?kind=Goal&owner=alice&limit=20
```

### `POST /edge` — create a relationship directly

```json
{ "from": "Pers1", "to": "Goal1", "kind": "HAS_GOAL" }
```
Both `from` and `to` must already exist, or `400`. Owner is your
authenticated identity, same rule as nodes.

### `GET /node/:address/edges/out` / `GET /node/:address/edges/in`

Outgoing and incoming edges for a node. Gated by whether you can read the
node itself — private node, private edge list, even to someone who could
otherwise see the edge's other endpoint.

## Error shape

Most errors are plain-text bodies with the relevant HTTP status
(`401`/`403`/`404`/`400`/`500`). The two JSON-shaped exceptions are the
`POST /node` success/failure bodies documented above, since those need to
carry structured edge-creation results.

## Roles and admin

Every identity is `User` or `Admin`. An Admin bypasses ownership checks
entirely on reads/writes/queries — the same idea as a Postgres superuser.
Bootstrap your first admin via `FACETQL_TOKENS=token:owner:admin`; create
every subsequent user through the API below instead of growing that env var.

### `POST /admin/users` — create a user (admin only)

```json
{ "owner": "bob", "role": "user" }
```
`role` is `"admin"` or `"user"` (defaults to `"user"` if omitted).

**Success — `201 Created`:**
```json
{ "owner": "bob", "role": "User", "token": "f949ad35..." }
```
The token is shown **exactly once**, here. It's stored only as a hash —
there's no way to retrieve it again. If it's lost, revoke and recreate.

### `GET /admin/users` — list persistent users (admin only)

Returns `[{ "owner": "bob", "role": "User" }, ...]` — never tokens or
hashes. Does **not** include static `FACETQL_TOKENS` bootstrap identities,
only ones created through this API.

### `DELETE /admin/users/:owner` — revoke a user (admin only)

Immediately invalidates that owner's token(s). `204` on success, `404` if
no persistent user has that owner name (a static bootstrap identity isn't
revocable this way — remove it from `FACETQL_TOKENS` and restart instead).

## Known gaps a client team should plan around

- **No TLS.** Put this behind a reverse proxy (nginx/Caddy) that terminates
  TLS before this goes anywhere near production traffic or a mobile client.
- **No pushed/real-time updates.** Everything here is request/response;
  there's no websocket/SSE feed yet if Project Interstate needs live updates
  (e.g. "notify the app the moment a step gets verified").
- **`data` is an opaque string, not validated JSON.** The server will
  happily store garbage in `data` — schema validation is the client's job
  for now (or Facet's, if that's the layer in front of this).
- **No batch/multi-node reads.** One `GET /node/:address` per node; there's
  no "give me these 20 addresses" endpoint yet.
