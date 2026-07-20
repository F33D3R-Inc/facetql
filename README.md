# FacetQL

**The Semantic Coordinate Database Engine**

FacetQL is a standalone, high-performance database management system designed around entities, symbolic 4D coordinates (`x`, `y`, `z`, `q`), and the relationships between them.

It is built to serve as the native, hyper-optimized storage backend for the Facet (`.fct`) compiler-first language, but it operates as a completely independent DBMS. Run it the way you'd run PostgreSQL or Redis: install a single binary, start a server, and talk to it over HTTP from any environment (native mobile, web, or backend microservices).

---

## 🌌 The Paradigm Shift

Traditional databases handle complexity by adding more tables, indexes, and `JOIN`s, which eventually leads to massive, slow, and tangled schemas.

FacetQL handles complexity by **folding inward through dimensions**. Instead of rigid rows, data lives in a self-organizing topological grid. When a piece of data becomes too complex, FacetQL doesn't create a "join table"; it natively extrudes that data into a higher dimension at the exact coordinate it belongs to.

---

## 🏗️ Core Architecture

### 1. The 156-Cell Block (The 2D Plane)
The fundamental unit of storage in FacetQL is a strict 12×13 grid containing exactly 156 cells. This is the "hot" cache. It is designed to be cache-local, pristine, and lightning-fast for direct reads and writes.

### 2. FacetQL Governance (The Law)
How does the database know when a cell is getting too complex? **FacetQL Governance**. Inspired by the mathematical harmony of the Magic Square, the Governance Engine continuously measures the "Dissonance Score" (0–15) of every cell based on three axes:
* **Volume:** Raw data size.
* **Temporal:** Frequency of updates (version history).
* **Relational:** Number of connections to other nodes.

When the local stress of a neighborhood hits the sacred threshold of **15**, FacetQL Governance declares the 2D plane out of balance and triggers an automatic dimensional morph to restore harmony.

### 3. Dimensional Morphing (3D & 4D)
When Governance triggers a morph, the database seamlessly shifts data to keep the 2D plane fast:
* **The 3D Morph (Z-Axis / Time Machine):** If a cell is updated constantly, historical versions are pushed *down* into the Z-axis. The 2D plane retains only the current state, guaranteeing microsecond read latency, while full lineage remains queryable.
* **The 4D Morph (Q-Axis / The Web):** If a cell becomes a highly connected hub, its tangled web of relationships is lifted *up* into the Q-axis. The core entity stays clean on the 2D plane, while complex graph topology is stored natively in 4D.

### 4. The Single Gatekeeper
Unlike traditional databases that rely on complex row-locks, MVCC, and distributed consensus to handle concurrent writes, FacetQL uses a **Single Gatekeeper**. Every write operation passes through one serialized lock.
* **Zero Race Conditions:** There is no gap between "checking" a state and "changing" it.
* **Native Atomic Claims:** A worker can say, *"Claim this job, but only if no one else has."* The Gatekeeper checks and takes it in the exact same breath. Double-pickups are physically impossible.
* **The Live Megaphone (SSE):** The millisecond a write is committed, the Gatekeeper shouts the update to every connected client via Server-Sent Events, enabling true reactive synchronization without polling.

---

## 🚀 Quick Start

### Install

**macOS / Linux**
```bash
curl -fsSL https://raw.githubusercontent.com/F33D3R-Inc/facetql/main/install.sh | sh
```
*Installs to `/usr/local/bin/facetql`. Set `FACETQL_INSTALL_DIR` first to install elsewhere.*

**From Source (Any Platform)**
```bash
git clone https://github.com/F33D3R-Inc/facetql.git
cd facetql
cargo build --release
# Binary is located at target/release/facetql
```

### Run

```bash
# 1. Initialize the data directory (creates ~/.facetql)
facetql init

# 2. Start the server with authentication and encryption
FACETQL_TOKENS="mytoken:myself:admin" FACETQL_MASTER_KEY="$(openssl rand -hex 32)" facetql start
```
*The server listens on port `8080` by default. Every request requires an `x-api-key` header matching a token from `FACETQL_TOKENS`.*

---

## 🔌 API Examples

FacetQL speaks standard HTTP/JSON. See `API_REFERENCE.md` for the complete specification.

**1. Create / Write a Node**
```bash
curl -X POST http://localhost:8080/node \
  -H "x-api-key: mytoken" \
  -H "Content-Type: application/json" \
  -d '{"address":"n1","kind":"Person","x":0,"y":0,"z":0,"q":0,"data":"{\"name\":\"Alice\"}","public":false}'
```

**2. Read a Node (with Dimensional Resolution)**
```bash
# Get just the 2D head state
curl -H "x-api-key: mytoken" http://localhost:8080/node/n1

# Get the 2D head + 4D relationships (edges)
curl -H "x-api-key: mytoken" "http://localhost:8080/node/n1?mode=entity"

# Get the 2D head + 3D history (lineage)
curl -H "x-api-key: mytoken" "http://localhost:8080/node/n1?mode=lineage"
```

**3. Atomically Claim a Job (Zero Double-Pickups)**
```bash
curl -X POST http://localhost:8080/node/job_123/claim \
  -H "x-api-key: mytoken" \
  -H "Content-Type: application/json" \
  -d '{"worker_id": "worker-node-1"}'
```
*Returns `200 OK` if successfully claimed. Returns `409 Conflict` if another worker already claimed it.*

**4. Subscribe to the Live Megaphone (SSE)**
```bash
curl -H "x-api-key: mytoken" -H "Accept: text/event-stream" http://localhost:8080/events
```

---

## 🛠️ CLI Reference

```bash
facetql init                                            # Create the data directory
facetql start [--port N]                                # Run the server (default: 8080, plain HTTP)
facetql start --tls-identity <file.p12> --tls-identity-password <pw> # Run over HTTPS
facetql backup <output_dir>                             # Safely copy data files out
facetql restore <input_dir>                             # Copy data files back in (refuses to clobber)
facetql import postgres --pg-url <url> --table <t> ...  # Migrate rows from Postgres into FacetQL nodes
```
*All CLI flags can also be set via environment variables (e.g., `FACETQL_DATA_DIR`, `FACETQL_PORT`).*

---

## ⚠️ Current State & Security

FacetQL is an early, functional checkpoint. It features a working Write-Ahead Log (WAL), crash recovery, and the core FacetQL Governance morphing engine.

However, it is **not yet a finished, hardened DBMS**.
* TLS and Encryption at Rest are implemented but require careful key management.
* A full multi-statement ACID Transaction Coordinator (`BEGIN`/`COMMIT`/`ROLLBACK`) is on the roadmap.

Please read [`SECURITY_NOTES.md`](./SECURITY_NOTES.md) for an honest, versioned account of what is implemented, what is tested, and what is explicitly not built yet. Do not depend on this for production data without reviewing this document.

---

## 📜 License

Not yet chosen — treat this as all-rights-reserved until a `LICENSE` file is added. If you intend to build on this, picking a permissive license (MIT/Apache-2.0) is one of the few remaining non-technical blockers.

---

**FacetQL: Perfectly balanced, as all data should be.**


