# FacetQL (formerly EnochianDB)

## The Multi-Dimensional Semantic Coordinate Database Engine

A standalone Rust database management system designed around entities,
symbolic 4D coordinates (`x`, `y`, `z`, `q`), and relationships.
It serves as the native storage backend for the Facet (`.fct`) compiler-first language.


## Install

### macOS / Linux

```bash
curl -fsSL https://raw.githubusercontent.com/FACETQL-LLC/facetql/main/install.sh | sh
```

Installs to `/usr/local/bin/facetql`. Set `FACETQL_INSTALL_DIR` first to
install elsewhere.

### Windows

Download `facetql-windows-x86_64.exe` from the
[latest release](https://github.com/FACETQL-LLC/facetql/releases/latest),
rename it to `facetql.exe`, and put it somewhere on your `PATH`.

### From source (any platform)

```bash
git clone https://github.com/FACETQL-LLC/facetql.git
cd facetql
cargo build --release
# binary is at target/release/facetql
```

## Quick start

```bash
facetql init          # creates ~/.facetql
FACETQL_TOKENS="mytoken:myself:admin" FACETQL_MASTER_KEY="$(openssl rand -hex 32)" facetql start
```

The server listens on port 8080 by default. Every request needs an
`x-api-key` header matching a token from `FACETQL_TOKENS`
(`token1:alice,token2:bob:admin` — one token per identity, `:admin`
marks a bootstrap identity as an admin). All data on disk is encrypted
with `FACETQL_MASTER_KEY` (AES-256-GCM) — generate a real one with
`openssl rand -hex 32` and keep it safe; losing it means losing access
to everything encrypted with it, and running without one falls back to
an insecure, publicly-known dev key (a loud warning prints every time
that happens). See [`API_REFERENCE.md`](./API_REFERENCE.md) for the
full HTTP API.

```bash
curl -X POST http://localhost:8080/node \
  -H "x-api-key: mytoken" -H "Content-Type: application/json" \
  -d '{"address":"n1","kind":"Person","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false}'
```

### CLI reference

```
facetql [--data-dir PATH] init                          # create the data directory
facetql [--data-dir PATH] start [--port N]               # run the server (default port 8080, plain HTTP)
facetql [--data-dir PATH] start [--port N] --tls-identity <file.p12> --tls-identity-password <pw>
                                                             # run over HTTPS instead
facetql                                                   # same as `start`, for convenience
facetql [--data-dir PATH] backup <output_dir>             # copy data files out
facetql [--data-dir PATH] restore <input_dir>             # copy data files back in (refuses to clobber existing data)
facetql import postgres --pg-url <url> --table <t> --kind <k> --token <token> [--id-column id] [--server http://localhost:8080]
                                                             # bring rows from an existing Postgres table in, one node per row
```

`--data-dir` defaults to `~/.facetql` and is also settable via
`FACETQL_DATA_DIR`. `--port` is also settable via `FACETQL_PORT`.
`import postgres` needs a *running* `facetql start` to import into — it
talks to the API, not the data files directly.

For TLS, generate a dev certificate and package it as PKCS#12:
```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
openssl pkcs12 -export -out identity.p12 -inkey key.pem -in cert.pem -passout pass:<pick-a-password>
facetql start --tls-identity identity.p12 --tls-identity-password <that-password>
```
Without `--tls-identity`, the server runs plain HTTP — fine behind a
reverse proxy that terminates TLS itself, not fine exposed directly.

## What this actually is right now

This is an early, functional checkpoint, not a finished DBMS — see
[`SECURITY_NOTES.md`](./SECURITY_NOTES.md) for an honest, versioned account
of what's implemented, what's tested, and what's explicitly not built yet
(TLS, encryption at rest, a real transaction system, and more). Read it
before depending on this for anything with real data in it.

## License

Not yet chosen — treat this as all-rights-reserved until a `LICENSE` file is
added. If you intend for people to actually download and build on this,
picking a license (MIT/Apache-2.0 are the common permissive choices for
something like this) is one of the few remaining non-technical blockers.
