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
| `FACETQL_TOKEN` | Default `--token` for `import postgres` |

## What's NOT a CLI command (common asks that don't exist yet)

- No `facetql stop` — it's a foreground process; stop it the normal way (Ctrl+C, or however your process manager/systemd handles it)
- No `facetql users` — user management is via the HTTP API (`POST/GET/DELETE /admin/users`), not the CLI, since it needs a running server anyway
- No `facetql query` / interactive shell — there's no `psql`-style REPL yet; use the HTTP API or `facetql-console.html`
