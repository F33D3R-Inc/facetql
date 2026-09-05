# FacetQL — CLI Command Reference (v0.9)

Every command below is real and built into the `facetql` binary — run
`facetql --help` or `facetql <command> --help` any time to see this
from the tool itself.

## `facetql` (no subcommand)

Starts the server on port 8080. Identical to `facetql start`, kept as the
default so a plain `facetql` does the obvious thing.

## `facetql init`

Creates the data directory and exits. Not required before `start` (start
does the same setup itself) — useful for scripting an install.

```
facetql init
facetql --data-dir /custom/path init
```

## `facetql start [--port N]`

Runs the server.

```
facetql start
facetql start --port 9090
```

| Flag / env var | Default | What it does |
|---|---|---|
| `--port` / `FACETQL_PORT` | 8080 | Port to listen on |
| `--data-dir` / `FACETQL_DATA_DIR` | `~/.facetql` | Where data files live |

## `facetql backup <output_dir>`

Copies every data file to a directory — a plain file copy, meant for a
data directory that isn't actively being written to by a running server
at the same moment.

```
facetql backup ~/facetql-backups/2026-07-20
```

## `facetql restore <input_dir>`

Copies data files from a `backup` directory back into the configured data
directory. Refuses to run if the target already has data in it, so it
can't silently clobber something.

```
facetql restore ~/facetql-backups/2026-07-20
```

## `facetql index <action>`

Declares, lists and drops **secondary indexes over a node's `data`
fields** — admin only, and a client of a *running* server (it goes
through `POST/GET/DELETE /admin/indexes`, so start the server first).

An index is a declaration that one top-level field of one `kind` is worth
keeping in sorted order. It changes what a read *costs*, never what it
returns:

- Without one, `POST /nodes/query` with `--order <field>` materializes
  every matching row and sorts it. That path is bounded by
  `FACETQL_MAX_SCAN_ROWS` and **fails outright** once a kind grows past
  the bound.
- With one, the same query walks an already-ordered access path and stops
  at `--limit` — a range scan, not a sort.

An index covers exactly one `kind` and one top-level `data` field. It is
per-kind because `data` has no schema across kinds: `created_at` on a
`Post` and `created_at` on a `Session` are unrelated fields that happen
to share a name. It is top-level because that is what `query --order` can
name — an index on a path no query can express would be an index no query
could use.

```
facetql index create post_created --kind Post --field created_at --token <admin-token>
facetql index create handle --kind User --field handle --unique
facetql index list
facetql index drop post_created
```

### `facetql index create <name> --kind <Kind> --field <field>`

Declares the index and **backfills it**, which reads every existing node
of that kind once. The command therefore costs the size of the kind — run
it when that's affordable, not in a hot path.

Re-declaring the *identical* index (same name, kind and field) is a
successful no-op, so a provisioning script is safe to run twice. A
*different* index under a name already in use, or a second index over a
field already covered, is a conflict (HTTP 409) — those are
contradictions, not repeats.

`<name>` may contain only letters, digits, `_` and `-`, up to 64 bytes:
the name becomes part of the index's filename on disk, so anything that
could be read as a path is rejected — before the request is sent, as a
usage error (exit 2).

`--unique` makes the index a **constraint**: a write that would give two
nodes of that kind the same value for the field is refused, checked
inside the writer lock ahead of the WAL so two callers racing for one
value cannot both win. Declaring it over data that already contains a
duplicate is refused rather than silently created false. It is also what
`reference create --parent-field` resolves through — a value two nodes
can hold names neither of them.

### `facetql index list`

Prints every declared index as a `NAME` / `KIND` / `FIELD` table, or the
raw server JSON under `--json`.

### `facetql index drop <name> [--yes]`

Removes the index. **This never breaks a query** — one that was being
served by the index simply falls back to the materialize-and-sort path.
It prompts for confirmation anyway (`--yes` skips) because rebuilding
costs another full read of the kind, and because queries that were fast
range scans become sorts bounded by `FACETQL_MAX_SCAN_ROWS`. Dropping a
name that was never declared is an error (HTTP 404), not a silent
success.

| Flag / env var | Required? | Default | What it does |
|---|---|---|---|
| `--kind` | `create` only | — | Node `kind` the index covers |
| `--field` | `create` only | — | Top-level `data` field to keep ordered |
| `--unique` | no | off | `create` only — refuse duplicate values for the field |
| `--yes` | no | off | `drop` only — skip the confirmation prompt |
| `--url` / `FACETQL_URL` | no | `http://localhost:8080` | Server to talk to |
| `--token` / `FACETQL_TOKEN` | yes | — | Admin token, sent as `x-api-key` |
| `--json` | no | off | Emit the server's raw JSON instead of a table |

Exit codes match every other client command: `2` for a usage mistake (bad
name, missing token), `1` for a transport failure or a non-2xx response,
`3` when a confirmation prompt is declined.

## `facetql reference <action>`

Declares, lists and drops **references between kinds** — admin only, and
a client of a *running* server (`POST/GET/DELETE /admin/references`).

An index changes what a read costs. A reference changes what a **delete
means**. It is the durable statement that one `data` field of one kind
points at another kind, plus what deleting the referenced node does to
the nodes referencing it:

- `cascade` — delete them too, and whatever references *them*, in the
  same frame.
- `restrict` — refuse the delete while any remain.
- `set-null` — clear the field and keep the rows.

The whole closure is expanded before anything is written and committed
as one transaction, which is the part an application cannot do for
itself: parent in one request and children in the next is two
transactions, and a crash between them leaves rows pointing at a node
that is gone.

```
facetql reference create post_comments --kind Comment --field post \
    --parent-kind Post --on-delete cascade --token <admin-token>

facetql reference create post_author --kind Post --field author_id \
    --parent-kind User --parent-field id --on-delete restrict

facetql reference list
facetql reference drop post_comments
```

### `facetql reference create <name> --kind <Kind> --field <field> --parent-kind <Kind> --on-delete <action>`

Refused unless the access paths that make the rule cheap already exist:

- an index over `<kind>.<field>`, because otherwise every delete of a
  referenced node is a scan of the whole referencing kind;
- with `--parent-field`, a **unique** index over
  `<parent-kind>.<parent-field>`, because a reference has to name exactly
  one node. Without `--parent-field` the reference is by address, which
  is unique by construction and needs nothing declared.

Also refused when the existing data already breaks the rule (HTTP 400,
naming the row) — a constraint that is false the moment it is created is
worse than no constraint. Declaring the identical reference again is a
successful no-op; a different rule under the same name is a 409.

### `facetql reference list`

Prints every declared reference as a `NAME` / `REFERENCE` / `ON DELETE`
table — the rule as one arrow, `Comment.post -> Post` — or the raw
server JSON under `--json`.

### `facetql reference drop <name> [--yes]`

Stops the enforcement. The rows it governed are untouched, which is
exactly why it prompts: from that moment a delete of a referenced node
leaves whatever pointed at it behind, with nothing to point those rows
out again.

| Flag / env var | Required? | Default | What it does |
|---|---|---|---|
| `--kind` | `create` only | — | The kind that holds the reference |
| `--field` | `create` only | — | The `data` field carrying the referenced key |
| `--parent-kind` | `create` only | — | The kind being referenced |
| `--parent-field` | no | the parent's address | Parent `data` field the value matches (needs a unique index) |
| `--on-delete` | `create` only | — | `cascade`, `restrict` or `set-null` |
| `--yes` | no | off | `drop` only — skip the confirmation prompt |
| `--url` / `FACETQL_URL` | no | `http://localhost:8080` | Server to talk to |
| `--token` / `FACETQL_TOKEN` | yes | — | Admin token, sent as `x-api-key` |
| `--json` | no | off | Emit the server's raw JSON instead of a table |

## `facetql import postgres`

Pulls rows from an existing Postgres table in, one row = one FacetQL
node, through the same API any other client uses. Needs a *running*
`facetql start` to import into — it's a client of the API, not a
direct file-level import.

```
facetql import postgres \
  --pg-url postgres://user:pass@host/dbname \
  --table clients \
  --kind Client \
  --token <admin-or-user-token> \
  --id-column id \
  --server http://localhost:8080
```

| Flag / env var | Required? | Default | What it does |
|---|---|---|---|
| `--pg-url` | yes | — | Postgres connection string |
| `--table` | yes | — | Table to import |
| `--kind` | yes | — | Node `kind` assigned to every imported row |
| `--token` / `FACETQL_TOKEN` | yes | — | FacetQL token to authenticate the import as |
| `--id-column` | no | `id` | Column used to build a stable node address |
| `--server` | no | `http://localhost:8080` | FacetQL server to import into |

## Global flag (works with every command)

| Flag / env var | Default | What it does |
|---|---|---|
| `--data-dir` / `FACETQL_DATA_DIR` | `~/.facetql` | Where all data files live |

## Environment variables (not tied to a specific command)

| Variable | What it does |
|---|---|
| `FACETQL_TOKENS` | Bootstrap identities, e.g. `token1:alice,token2:bob:admin` (the `:admin` suffix marks that identity as Admin) |
| `FACETQL_DATA_DIR` | Same as `--data-dir` |
| `FACETQL_PORT` | Same as `--port` |
| `FACETQL_TOKEN` | Default `--token` for every client command (`index`, `user`, `get`, `put`, `delete`, `query`, `stats`). Prefer it over `--token` so the token never lands in shell history |
| `FACETQL_URL` | Default `--url` for every client command |
| `FACETQL_MAX_SCAN_ROWS` | Ceiling on rows a sort-path query may materialize. A declared index turns that query into a range scan, so it is no longer subject to this bound |

## What's NOT a CLI command (common asks that don't exist yet)

- No `facetql stop` — it's a foreground process; stop it the normal way (Ctrl+C, or however your process manager/systemd handles it)
- No interactive shell — there's no `psql`-style REPL. `facetql query --kind <Kind>` runs one native predicate query and exits; for anything more, use the HTTP API or `facetql-console.html`
- No index over a nested `data` path, and no multi-field index — an index covers exactly one top-level field of one kind, which is what `query --order` can name
