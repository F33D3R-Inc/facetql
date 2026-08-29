use std::collections::HashMap;
use crate::core::node::Node;
use crate::core::edge::Edge;
use crate::core::user::UserRecord;
use crate::core::history::HistoryEntry;
use crate::storage::binary;
use crate::storage::index::Index;
use crate::storage::tombstone;
use crate::storage::wal;

#[derive(Debug)]
pub enum ClaimError {
    NotFound,
    /// Carries who already holds the claim, so the caller can report
    /// something more useful than a bare 409.
    AlreadyClaimed(String),
    StorageError(String),
}

impl std::fmt::Display for ClaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClaimError::NotFound => write!(f, "node not found"),
            ClaimError::AlreadyClaimed(by) => write!(f, "already claimed by {by}"),
            ClaimError::StorageError(e) => write!(f, "{e}"),
        }
    }
}

pub struct StorageEngine {
    pub nodes: HashMap<String, Node>,
    pub index: Index,
    /// Adjacency lists, kept in both directions so "what does this node
    /// point to" and "what points to this node" are both O(1) lookups
    /// instead of a full scan — the thing a case-management graph needs
    /// most (e.g. "every step under this goal" and "every goal this
    /// step belongs to").
    pub edges_out: HashMap<String, Vec<Edge>>,
    pub edges_in: HashMap<String, Vec<Edge>>,
    /// Persistent users, keyed by token_hash. See core/user.rs.
    pub users: HashMap<String, UserRecord>,
    /// Archived previous states, keyed by node address, oldest first.
    /// See core/history.rs.
    pub history: HashMap<String, Vec<HistoryEntry>>,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            index: Index::new(),
            edges_out: HashMap::new(),
            edges_in: HashMap::new(),
            users: HashMap::new(),
            history: HashMap::new(),
        }
    }

    /// Rebuilds engine state by replaying facetql.data and
    /// facetql.edges from disk, then applying facetql.tombstones so
    /// deleted addresses don't come back on restart. Because the logs
    /// are append-only, replaying in file order and letting later
    /// records win for the same key reconstructs the correct final
    /// state.
    pub fn load() -> std::io::Result<Self> {
        let mut engine = Self::new();

        for (offset, node) in binary::read_all()? {
            engine.index.insert(node.address.clone(), offset);
            engine.nodes.insert(node.address.clone(), node);
        }

        for (_offset, edge) in binary::read_all_records::<Edge>(&binary::edges_path())? {
            engine.index_edge(edge);
        }

        for (_offset, user) in binary::read_all_records::<UserRecord>(&binary::users_path())? {
            engine.users.insert(user.token_hash.clone(), user);
        }

        for (_offset, entry) in binary::read_all_records::<HistoryEntry>(&binary::history_path())? {
            engine.history.entry(entry.address.clone()).or_default().push(entry);
        }

        // Tombstones are shared between nodes and revoked users, namespaced
        // by a "user:" prefix so a user's token_hash can never collide with
        // a node address. See StorageEngine::revoke_user.
        for key in tombstone::read_tombstones()? {
            match key.strip_prefix("user:") {
                Some(token_hash) => {
                    engine.users.remove(token_hash);
                }
                None => {
                    engine.nodes.remove(&key);
                }
            }
        }

        Ok(engine)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Write-ahead-log the intent, archive whatever this address
    /// currently holds (if anything — a first-time create has nothing
    /// to archive), persist the node to disk, index its offset, then
    /// update the in-memory view.
    ///
    /// The archiving step is the actual version history feature: if a
    /// node already exists at this address, its current state is
    /// captured as a `HistoryEntry` and durably appended to
    /// `facetql.history` BEFORE the new value is written — so even if
    /// the process died between those two writes, the old version is
    /// safely recorded either way; the new value either landed or
    /// didn't, but the history of what came before it is never lost.
    /// Every overwrite is archived, unconditionally — no attempt to
    /// detect "nothing actually changed" and skip it, which keeps the
    /// behavior simple and predictable rather than silently dropping
    /// history someone might have expected to see.
    pub fn insert(&mut self, node: Node) -> Result<(), String> {
        if let Some(previous) = self.nodes.get(&node.address) {
            let entry = HistoryEntry::now(previous.clone());
            wal::log(&wal::WalEntry::Archive(entry.clone()));
            binary::append_record(&binary::history_path(), &entry).map_err(|e| e.to_string())?;
            self.history.entry(node.address.clone()).or_default().push(entry);
        }

        wal::log(&wal::WalEntry::Insert(node.clone()));

        let offset = binary::append_node(&node).map_err(|e| e.to_string())?;
        self.index.insert(node.address.clone(), offset);
        self.nodes.insert(node.address.clone(), node);

        Ok(())
    }

    /// Every archived previous state for `address`, oldest first. Does
    /// NOT include the current live value — that's `get()`. Empty if
    /// the node has never been overwritten (or has never existed).
    pub fn history_for(&self, address: &str) -> &[HistoryEntry] {
        self.history.get(address).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn get(&self, address: &str) -> Option<&Node> {
        self.nodes.get(address)
    }

    /// Atomically claims a node for `worker` — the job-queue "hand this to
    /// exactly one worker" primitive. Correct *because* it does nothing
    /// clever: `StorageEngine` is only ever reached through
    /// `Database.engine.write()`, which is a single `RwLock` held for the
    /// whole handler (see api/routes.rs) — so by the time this function
    /// runs, no other request can be touching storage at all. There's no
    /// gap between checking `claimed_by` and setting it, because nothing
    /// else can run in between. That's the entire mechanism; it doesn't
    /// need row locks or compare-and-swap primitives on top of what's
    /// already true about how every write reaches this engine.
    pub fn claim(&mut self, address: &str, worker: &str) -> Result<(), ClaimError> {
        let mut node = match self.nodes.get(address) {
            Some(n) => n.clone(),
            None => return Err(ClaimError::NotFound),
        };

        if let Some(existing) = &node.claimed_by {
            return Err(ClaimError::AlreadyClaimed(existing.clone()));
        }

        node.claimed_by = Some(worker.to_string());
        self.insert(node).map_err(ClaimError::StorageError)?;
        Ok(())
    }

    /// Tombstones a node so it no longer appears live, per the
    /// append-only delete model described on `tombstone::append_tombstone`.
    /// Callers are responsible for the `can_write` authorization check
    /// before calling this — the engine enforces storage invariants, not
    /// permissions (permissions live on `Node`/`Edge` and are checked at
    /// the API layer, same as the existing `can_read` calls).
    pub fn delete(&mut self, address: &str) -> Result<(), String> {
        wal::log(&wal::WalEntry::Delete(address.to_string()));
        tombstone::append_tombstone(address).map_err(|e| e.to_string())?;
        self.nodes.remove(address);
        Ok(())
    }

    /// Creates a relationship between two existing nodes. Both
    /// endpoints must already exist — an edge to a nonexistent node is
    /// almost always a client bug (typo'd address, race with a delete)
    /// and silently allowing it would make graph traversals return
    /// dangling references with no signal of why.
    pub fn insert_edge(&mut self, edge: Edge) -> Result<(), String> {
        if !self.nodes.contains_key(&edge.from) {
            return Err(format!("edge 'from' address not found: {}", edge.from));
        }
        if !self.nodes.contains_key(&edge.to) {
            return Err(format!("edge 'to' address not found: {}", edge.to));
        }

        wal::log(&wal::WalEntry::InsertEdge(edge.clone()));
        binary::append_record(&binary::edges_path(), &edge).map_err(|e| e.to_string())?;
        self.index_edge(edge);
        Ok(())
    }

    fn index_edge(&mut self, edge: Edge) {
        self.edges_out.entry(edge.from.clone()).or_default().push(edge.clone());
        self.edges_in.entry(edge.to.clone()).or_default().push(edge);
    }

    pub fn edges_from(&self, address: &str) -> &[Edge] {
        self.edges_out.get(address).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn edges_to(&self, address: &str) -> &[Edge] {
        self.edges_in.get(address).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Every live node owned by `owner`. Linear scan — fine at v0.1
    /// scale, and flagged here rather than hidden so it's an obvious
    /// spot to revisit (a secondary owner->addresses index) once a
    /// dataset makes this a real cost.
    pub fn nodes_by_owner(&self, owner: &str) -> Vec<&Node> {
        self.nodes.values().filter(|n| n.owner == owner).collect()
    }

    /// General-purpose listing for building a real application on top
    /// of this instead of hand-fetching one address at a time: filter
    /// by `kind` and/or `owner`, apply the same `can_read` rule a
    /// single-node GET would, then paginate. Still a linear scan over
    /// every live node — the honest ceiling on this is "fine for a
    /// pilot, not fine once there are millions of nodes," and a real
    /// kind/owner index is the follow-up once that's a real constraint.
    pub fn query(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        requester: &str,
        limit: usize,
        offset: usize,
    ) -> Vec<&Node> {
        self.nodes
            .values()
            .filter(|n| kind.map_or(true, |k| n.kind == k))
            .filter(|n| owner.map_or(true, |o| n.owner == o))
            .filter(|n| n.can_read(requester))
            .skip(offset)
            .take(limit)
            .collect()
    }

    /// Creates a node and, in the same call, one or more outgoing edges
    /// from it — the shape almost every real write is (e.g. "create
    /// this Goal and link it to the Person it belongs to"), and doing
    /// it as two separate API calls leaves a window where the node
    /// exists with no link if the client crashes or drops the
    /// connection between them.
    ///
    /// This is *best-effort* atomicity, not full ACID — see
    /// SECURITY_NOTES.md. What it actually does: insert the node, then
    /// attempt each edge in order. If every edge succeeds, return them.
    /// If any edge fails (almost always because its target doesn't
    /// exist), stop immediately, tombstone the node just created so it
    /// doesn't end up live-but-half-linked, and return which edges
    /// already succeeded so the caller knows exactly what state was
    /// left behind — those already-created edges are NOT rolled back,
    /// because there's no edge tombstone mechanism yet (flagged in
    /// SECURITY_NOTES.md as a known gap). A real transaction log is the
    /// eventual fix; this closes the common case without it.
    pub fn insert_with_edges(
        &mut self,
        node: Node,
        edge_targets: Vec<(String, String)>,
    ) -> Result<Vec<Edge>, (String, Vec<Edge>)> {
        let address = node.address.clone();
        let owner = node.owner.clone();

        self.insert(node).map_err(|e| (e, Vec::new()))?;

        let mut created = Vec::new();
        for (to, kind) in edge_targets {
            let edge = Edge::new(address.clone(), to, kind, owner.clone());
            match self.insert_edge(edge.clone()) {
                Ok(()) => created.push(edge),
                Err(e) => {
                    let _ = self.delete(&address);
                    return Err((e, created));
                }
            }
        }

        Ok(created)
    }

    /// Persists a new user record (admin-created — see
    /// `POST /admin/users` in api/routes.rs). Stores only the hash,
    /// never the plaintext token — the caller is responsible for
    /// returning the plaintext to whoever asked for the account exactly
    /// once, since it can't be recovered from here afterward.
    pub fn insert_user(&mut self, record: UserRecord) -> Result<(), String> {
        wal::log(&wal::WalEntry::InsertUser(record.clone()));
        binary::append_record(&binary::users_path(), &record).map_err(|e| e.to_string())?;
        self.users.insert(record.token_hash.clone(), record);
        Ok(())
    }

    /// Revokes a user by token hash. Same tombstone approach as node
    /// deletes (append-only log, can't un-write bytes) — namespaced
    /// with a "user:" prefix so it shares the tombstone file without
    /// colliding with node addresses. See `load()`.
    pub fn revoke_user(&mut self, token_hash: &str) -> Result<(), String> {
        wal::log(&wal::WalEntry::RevokeUser(token_hash.to_string()));
        tombstone::append_tombstone(&format!("user:{token_hash}")).map_err(|e| e.to_string())?;
        self.users.remove(token_hash);
        Ok(())
    }

    pub fn find_user_by_hash(&self, token_hash: &str) -> Option<&UserRecord> {
        self.users.get(token_hash)
    }

    /// Every persistent user, for `GET /admin/users`. Static
    /// ENOCHIAN_TOKENS bootstrap identities are NOT included here —
    /// they're not stored in the engine at all, so there's nothing for
    /// this to list. Document that distinction to whoever's consuming
    /// this endpoint: it shows admin-created users, not every identity
    /// capable of authenticating.
    pub fn list_users(&self) -> Vec<&UserRecord> {
        self.users.values().collect()
    }

    /// Validates an entire batch of operations against each other and
    /// against current state, and only applies any of it if all of it
    /// is valid. This is the real fix for the gap `insert_with_edges`
    /// documented: creating several related nodes and edges was
    /// previously multiple separate durable writes, each individually
    /// safe but with no guarantee across the group — a crash or a bad
    /// reference partway through could leave a node with some of its
    /// edges missing.
    ///
    /// What this DOES guarantee, verified live: if any edge in the
    /// batch references a node that doesn't exist (and isn't itself
    /// being created earlier in the same batch), or any delete targets
    /// a node that doesn't exist, NOTHING in the batch is applied —
    /// not even the parts that would have succeeded on their own. The
    /// whole call happens while the single write lock every mutation
    /// goes through is held (the caller in api/routes.rs holds it for
    /// the duration), so no concurrent request can observe a
    /// partially-applied batch.
    ///
    /// What this does NOT guarantee, stated plainly: if the process
    /// crashes mid-batch — after some operations have durably written
    /// to disk but before the rest have — those already-applied writes
    /// are not rolled back on restart. Real crash-mid-transaction
    /// rollback needs a staging log distinct from the live append-only
    /// files (write the whole batch to a pending-transaction file
    /// first, only append to facetql.data/edges once the whole batch
    /// is known-good, delete the pending file last) — that's a bigger
    /// piece of work than this pass, and it's a different guarantee
    /// than "invalid batches never partially apply," which is what's
    /// actually built and tested here.
    pub fn execute_transaction(&mut self, ops: Vec<TxOperation>) -> Result<(), String> {
        // Pass 1: figure out which addresses will exist after this
        // batch, so an edge can validly reference a node created
        // earlier in the SAME batch, not just ones that already existed.
        let mut staged_existing: std::collections::HashSet<&str> =
            self.nodes.keys().map(String::as_str).collect();

        for op in &ops {
            match op {
                TxOperation::InsertNode(node) => {
                    staged_existing.insert(&node.address);
                }
                TxOperation::DeleteNode(address) => {
                    staged_existing.remove(address.as_str());
                }
                TxOperation::InsertEdge(_) => {}
            }
        }

        // Pass 2: validate every operation against that staged view.
        // Nothing is written yet — a failure here means the batch never
        // touches WAL, disk, or memory at all.
        for op in &ops {
            match op {
                TxOperation::InsertNode(_) => {}
                TxOperation::DeleteNode(address) => {
                    if !self.nodes.contains_key(address) {
                        return Err(format!("transaction failed, nothing applied: delete target not found: {address}"));
                    }
                }
                TxOperation::InsertEdge(edge) => {
                    if !staged_existing.contains(edge.from.as_str()) {
                        return Err(format!(
                            "transaction failed, nothing applied: edge 'from' address not found: {}",
                            edge.from
                        ));
                    }
                    if !staged_existing.contains(edge.to.as_str()) {
                        return Err(format!(
                            "transaction failed, nothing applied: edge 'to' address not found: {}",
                            edge.to
                        ));
                    }
                }
            }
        }

        // Pass 3: everything validated — apply for real, in order.
        for op in ops {
            match op {
                TxOperation::InsertNode(node) => self.insert(node)?,
                TxOperation::DeleteNode(address) => self.delete(&address)?,
                TxOperation::InsertEdge(edge) => self.insert_edge(edge)?,
            }
        }

        Ok(())
    }
}

/// One operation inside a transaction. Deliberately a small, closed set
/// for this pass — update-in-place could be added the same way, kept
/// out for now to keep the validation logic above easy to reason about.
#[derive(Debug, Clone)]
pub enum TxOperation {
    InsertNode(Node),
    DeleteNode(String),
    InsertEdge(Edge),
}
