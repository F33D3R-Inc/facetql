use axum::{
    Router,
    routing::{get, post, put, delete},
    extract::{State, Json, Path, Extension, Query},
    http::StatusCode,
    middleware,
    response::{IntoResponse, sse::{Event, Sse, KeepAlive}},
};
use std::sync::Arc;
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::database::Database;
use crate::core::node::{Node, Visibility};
use crate::core::edge::Edge;
use crate::core::coordinate::Coordinate;
use crate::core::predicate::Expr;
use crate::core::user::{Role, UserRecord};
use crate::core::history::HistoryEntry;
use crate::auth::{auth_middleware, hash_token, AuthIdentity};
use crate::storage::engine::{ClaimError, TxOperation};

#[derive(Deserialize)]
pub struct EdgeSpec {
    pub to: String,
    pub kind: String,
}

#[derive(Deserialize)]
pub struct CreateNodeRequest {
    pub address: String,
    pub kind: String,
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
    pub data: String,
    pub public: Option<bool>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    /// If true, fail with 409 instead of overwriting when `address`
    /// already exists. Plain `POST /node` is upsert-by-default (an
    /// existing address is silently overwritten) — fine for most
    /// writes, actively wrong for anything that needs "only I create
    /// this, and I need to know if I lost that race" (e.g. a cron tick
    /// reservation across multiple app instances hitting the same
    /// FacetQL server).
    #[serde(default)]
    pub if_absent: bool,
}

#[derive(Deserialize)]
pub struct UpdateNodeRequest {
    pub data: String,
    pub public: Option<bool>,
}

#[derive(Deserialize)]
pub struct CreateEdgeRequest {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub kind: Option<String>,
    pub owner: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Body for `POST /nodes/query` — a pushed-down predicate query.
///
/// Field names mirror FCT's `runtime.Query` (`Entity`/`Where`/
/// `ItemVar`/`Order`/`Desc`/`Limit`/`After` in runtime/sql.go) so a
/// client can translate its own `Query` value into this body with a
/// field-for-field rename, not a restructure.
///
/// `where_` (JSON key `where`) is optional: omitting it just runs the
/// same `kind`/`owner`/visibility filter `GET /nodes` does, ordered and
/// paginated. `item_var` defaults to `"item"` — FCT's compiler always
/// sets one, but a hand-written request can rely on the default when
/// there's only one plausible loop variable in the predicate.
#[derive(Deserialize)]
pub struct QueryWhereRequest {
    pub kind: Option<String>,
    pub owner: Option<String>,
    #[serde(rename = "where")]
    pub where_: Option<Expr>,
    #[serde(default = "default_item_var")]
    pub item_var: String,
    pub order: Option<String>,
    #[serde(default)]
    pub desc: bool,
    /// Opaque keyset cursor from the previous page's `next` field.
    /// Omitted/empty for the first page. When present it takes
    /// precedence over `offset` and selects the rows strictly past the
    /// cursor in the requested `(order, address)` ordering — stable
    /// under concurrent writes in a way offset is not. Mirrors FCT's
    /// `Query.After`.
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

fn default_item_var() -> String {
    "item".to_string()
}

#[derive(Serialize)]
struct CreateNodeResponse {
    address: String,
    edges_created: Vec<Edge>,
}

#[derive(Serialize)]
struct CreateNodeError {
    error: String,
    edges_created_before_failure: Vec<Edge>,
}

use tower_http::cors::CorsLayer;

/// Permissive by design for this checkpoint: any origin, any header,
/// any of the methods this API actually uses. That's fine for local
/// development against a browser page on a different origin/port — it
/// is NOT fine for a production deployment, where this should be
/// narrowed to the specific origin(s) your real frontend is served
/// from. Flagged in SECURITY_NOTES.md; don't ship this permissive
/// version to anything public.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::DELETE,
        ])
        .allow_headers(tower_http::cors::Any)
}

pub fn create_router(db: Arc<Database>) -> Router {
    let protected = Router::new()
        .route("/node", post(create_node))
        .route("/node/:address", get(get_node))
        .route("/node/:address/history", get(get_node_history))
        .route("/node/:address", put(update_node))
        .route("/node/:address", delete(delete_node))
        .route("/node/:address/owned", get(list_owned))
        .route("/node/:address/claim", post(claim_node))
        .route("/nodes", get(query_nodes))
        .route("/nodes/query", post(query_nodes_where))
        .route("/edge", post(create_edge))
        .route("/transaction", post(execute_transaction))
        .route("/node/:address/edges/out", get(get_edges_out))
        .route("/node/:address/edges/in", get(get_edges_in))
        .route("/events", get(subscribe_events))
        .route("/publish", post(publish_event))
        .route("/admin/users", post(create_user))
        .route("/admin/users", get(list_users))
        .route("/admin/users/:owner", delete(revoke_user))
        .route_layer(middleware::from_fn_with_state(db.clone(), auth_middleware))
        .with_state(db);

    Router::new().route("/", get(home)).merge(protected).layer(cors_layer())
}

async fn home() -> String {
    "FacetQL Online".to_string()
}

async fn create_node(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CreateNodeRequest>,
) -> impl IntoResponse {
    let coordinate = Coordinate::new(payload.x, payload.y, payload.z, payload.q);
    let mut node = Node::new(coordinate, payload.address.clone(), payload.kind.clone(), identity.owner);
    node.data = payload.data;
    if payload.public.unwrap_or(false) {
        node.visibility = Visibility::Public;
    }

    let address = node.address.clone();
    let kind = payload.kind;
    let edge_targets: Vec<(String, String)> =
        payload.edges.into_iter().map(|e| (e.to, e.kind)).collect();

    let mut engine = db.engine.write().expect("storage engine lock poisoned");

    if payload.if_absent && engine.get(&address).is_some() {
        return (
            StatusCode::CONFLICT,
            format!("node already exists: {address}"),
        )
            .into_response();
    }

    match engine.insert_with_edges(node, edge_targets) {
        Ok(edges_created) => {
            drop(engine);
            db.publish(
                serde_json::json!({"event": "node_created", "address": address, "kind": kind}).to_string(),
            );
            (
                StatusCode::CREATED,
                Json(CreateNodeResponse { address, edges_created }),
            )
                .into_response()
        }
        Err((e, edges_created_before_failure)) => (
            StatusCode::BAD_REQUEST,
            Json(CreateNodeError { error: e, edges_created_before_failure }),
        )
            .into_response(),
    }
}

async fn get_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine.read().expect("storage engine lock poisoned");
    match engine.get(&address) {
        // Admin bypasses can_read the same way a Postgres superuser
        // bypasses row-level security — deliberate, not a bug.
        Some(node) if identity.is_admin() || node.can_read(&identity.owner) => {
            (StatusCode::OK, Json(node.clone())).into_response()
        }
        Some(_) => (StatusCode::FORBIDDEN, "not authorized to read this node").into_response(),
        None => (StatusCode::NOT_FOUND, "node not found").into_response(),
    }
}

/// `GET /node/:address/history` — every archived previous state of this
/// node, oldest first. Does not include the current live value (that's
/// the plain GET). Same visibility rule as reading the node itself:
/// permission is checked against the node's CURRENT owner/visibility,
/// since ownership isn't itself versioned in this pass — a node that
/// changed hands would show its full history to whoever owns it now,
/// not to whoever owned it at each past point in time. Worth knowing if
/// that distinction ever matters for a real use case.
async fn get_node_history(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine.read().expect("storage engine lock poisoned");
    match engine.get(&address) {
        Some(node) if identity.is_admin() || node.can_read(&identity.owner) => {
            let history: Vec<HistoryEntry> = engine.history_for(&address).to_vec();
            (StatusCode::OK, Json(history)).into_response()
        }
        Some(_) => (StatusCode::FORBIDDEN, "not authorized to read this node's history").into_response(),
        None => (StatusCode::NOT_FOUND, "node not found").into_response(),
    }
}

async fn update_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<UpdateNodeRequest>,
) -> impl IntoResponse {
    let mut engine = db.engine.write().expect("storage engine lock poisoned");

    let existing = match engine.get(&address) {
        Some(n) => n.clone(),
        None => return (StatusCode::NOT_FOUND, "node not found").into_response(),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to modify this node").into_response();
    }

    let mut updated = existing;
    updated.data = payload.data;
    if let Some(public) = payload.public {
        updated.visibility = if public { Visibility::Public } else { Visibility::Private };
    }

    match engine.insert(updated) {
        Ok(()) => {
            drop(engine);
            db.publish(serde_json::json!({"event": "node_updated", "address": address}).to_string());
            (StatusCode::OK, "Node updated").into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn delete_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let mut engine = db.engine.write().expect("storage engine lock poisoned");

    let existing = match engine.get(&address) {
        Some(n) => n.clone(),
        None => return (StatusCode::NOT_FOUND, "node not found").into_response(),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to delete this node").into_response();
    }

    match engine.delete(&address) {
        Ok(()) => {
            drop(engine);
            db.publish(serde_json::json!({"event": "node_deleted", "address": address}).to_string());
            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn claim_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let mut engine = db.engine.write().expect("storage engine lock poisoned");
    match engine.claim(&address, &identity.owner) {
        Ok(()) => {
            drop(engine);
            db.publish(
                serde_json::json!({"event": "node_claimed", "address": address, "worker": identity.owner}).to_string(),
            );
            (StatusCode::OK, "claimed").into_response()
        }
        Err(ClaimError::NotFound) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(ClaimError::AlreadyClaimed(by)) => {
            (StatusCode::CONFLICT, format!("already claimed by {by}")).into_response()
        }
        Err(ClaimError::StorageError(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn list_owned(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine.read().expect("storage engine lock poisoned");
    let owned: Vec<Node> = engine.nodes_by_owner(&identity.owner).into_iter().cloned().collect();
    (StatusCode::OK, Json(owned)).into_response()
}

async fn query_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    let offset = params.offset.unwrap_or(0);

    let engine = db.engine.read().expect("storage engine lock poisoned");
    // Admins see everything matching the filter, ignoring visibility —
    // same bypass rationale as get_node. Everyone else gets the normal
    // can_read-filtered view.
    let results: Vec<Node> = if identity.is_admin() {
        engine
            .query(params.kind.as_deref(), params.owner.as_deref(), "", limit, offset)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
    } else {
        engine
            .query(params.kind.as_deref(), params.owner.as_deref(), &identity.owner, limit, offset)
            .into_iter()
            .cloned()
            .collect()
    };

    (StatusCode::OK, Json(results)).into_response()
}

/// `POST /nodes/query` — predicate-pushdown query.
///
/// Unlike `query_nodes` (`GET /nodes`), this evaluates a caller-supplied
/// `where` predicate against each candidate's `data` inside the engine
/// (`StorageEngine::query_where`) rather than requiring the caller to
/// pull every row back and filter client-side. See that function's doc
/// comment for exactly what "pushdown" does and doesn't buy yet (no
/// secondary index, so still a full scan of the kind/owner-filtered
/// set — but one evaluator, run once, in the engine).
async fn query_nodes_where(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<QueryWhereRequest>,
) -> impl IntoResponse {
    let limit = payload.limit.unwrap_or(50).min(500);
    let offset = payload.offset.unwrap_or(0);

    let engine = db.engine.read().expect("storage engine lock poisoned");

    // Admins bypass visibility the same way query_nodes/get_node do —
    // query_where's can_read filter treats "" as "no requester", which
    // for a Private node only matches its owner. Passing "" here mirrors
    // the same bypass query_nodes already does for admins.
    let requester = if identity.is_admin() {
        ""
    } else {
        identity.owner.as_str()
    };

    let result = engine.query_where(
        payload.kind.as_deref(),
        payload.owner.as_deref(),
        requester,
        payload.where_.as_ref(),
        &payload.item_var,
        payload.order.as_deref(),
        payload.desc,
        payload.after.as_deref(),
        limit,
        offset,
    );

    match result {
        Ok(page) => (StatusCode::OK, Json(page)).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn create_edge(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CreateEdgeRequest>,
) -> impl IntoResponse {
    let edge = Edge::new(payload.from.clone(), payload.to.clone(), payload.kind.clone(), identity.owner);

    let mut engine = db.engine.write().expect("storage engine lock poisoned");
    match engine.insert_edge(edge) {
        Ok(()) => {
            drop(engine);
            db.publish(
                serde_json::json!({"event": "edge_created", "from": payload.from, "to": payload.to, "kind": payload.kind}).to_string(),
            );
            (StatusCode::CREATED, "Edge created").into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn get_edges_out(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine.read().expect("storage engine lock poisoned");
    match engine.get(&address) {
        Some(node) if identity.is_admin() || node.can_read(&identity.owner) => {
            (StatusCode::OK, Json(engine.edges_from(&address).to_vec())).into_response()
        }
        Some(_) => (StatusCode::FORBIDDEN, "not authorized to read this node's edges").into_response(),
        None => (StatusCode::NOT_FOUND, "node not found").into_response(),
    }
}

// ── transactions ────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TxOpRequest {
    InsertNode {
        address: String,
        kind: String,
        x: u8,
        y: u8,
        z: u8,
        q: u8,
        data: String,
        public: Option<bool>,
    },
    InsertEdge {
        from: String,
        to: String,
        kind: String,
    },
    DeleteNode {
        address: String,
    },
    /// Native bulk clear: remove every node of `kind` the caller may
    /// write, as one all-or-nothing step in the transaction (WAL +
    /// tombstones per node, exactly like `delete_node`). Wire tag is
    /// `clear_kind` (snake_case of the variant). Non-admin clears only
    /// its own nodes of that kind; admin clears all of that kind —
    /// authorization is resolved here and enforced in the engine.
    ClearKind {
        kind: String,
    },
}

#[derive(Deserialize)]
pub struct TransactionRequest {
    pub operations: Vec<TxOpRequest>,
}

/// `POST /transaction` — see StorageEngine::execute_transaction for
/// exactly what guarantee this does and doesn't provide. Ownership
/// checks for deletes happen here, before the batch reaches the engine,
/// using the same lock the whole handler holds — no window for another
/// request to change a node's ownership between this check and the
/// engine applying the batch.
async fn execute_transaction(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    let mut engine = db.engine.write().expect("storage engine lock poisoned");

    let mut ops = Vec::with_capacity(payload.operations.len());
    let mut touched_addresses = Vec::new();

    for op in payload.operations {
        match op {
            TxOpRequest::InsertNode { address, kind, x, y, z, q, data, public } => {
                let coordinate = Coordinate::new(x, y, z, q);
                let mut node = Node::new(coordinate, address.clone(), kind, identity.owner.clone());
                node.data = data;
                if public.unwrap_or(false) {
                    node.visibility = Visibility::Public;
                }
                touched_addresses.push(address);
                ops.push(TxOperation::InsertNode(node));
            }
            TxOpRequest::InsertEdge { from, to, kind } => {
                ops.push(TxOperation::InsertEdge(Edge::new(from, to, kind, identity.owner.clone())));
            }
            TxOpRequest::DeleteNode { address } => {
                if !identity.is_admin() {
                    match engine.get(&address) {
                        Some(n) if !n.can_write(&identity.owner) => {
                            return (
                                StatusCode::FORBIDDEN,
                                format!("not authorized to delete {address}"),
                            )
                                .into_response();
                        }
                        None => {
                            return (
                                StatusCode::NOT_FOUND,
                                format!("delete target not found: {address}"),
                            )
                                .into_response();
                        }
                        _ => {}
                    }
                }
                touched_addresses.push(address.clone());
                ops.push(TxOperation::DeleteNode(address));
            }
            TxOpRequest::ClearKind { kind } => {
                // Unlike delete_node, a clear never rejects: it's
                // defined as "remove what I'm allowed to remove", so
                // non-writable nodes are skipped, not an error. We
                // resolve the caller's authorization (owner + admin)
                // here — under the same write lock the engine will use
                // — and let the engine enforce it when selecting rows.
                let is_admin = identity.is_admin();
                // Record the exact addresses this clear will tombstone
                // for the committed event, using the same rule the
                // engine applies. The write lock is held throughout, so
                // this snapshot matches what execute_transaction removes.
                for node in engine.nodes.values() {
                    if node.kind == kind
                        && (is_admin || node.can_write(&identity.owner))
                    {
                        touched_addresses.push(node.address.clone());
                    }
                }
                ops.push(TxOperation::ClearKind {
                    kind,
                    owner: identity.owner.clone(),
                    is_admin,
                });
            }
        }
    }

    match engine.execute_transaction(ops) {
        Ok(()) => {
            drop(engine);
            db.publish(
                serde_json::json!({"event": "transaction_committed", "addresses": touched_addresses}).to_string(),
            );
            (StatusCode::OK, "transaction committed").into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn get_edges_in(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine.read().expect("storage engine lock poisoned");
    match engine.get(&address) {
        Some(node) if identity.is_admin() || node.can_read(&identity.owner) => {
            (StatusCode::OK, Json(engine.edges_to(&address).to_vec())).into_response()
        }
        Some(_) => (StatusCode::FORBIDDEN, "not authorized to read this node's edges").into_response(),
        None => (StatusCode::NOT_FOUND, "node not found").into_response(),
    }
}

#[derive(Deserialize)]
pub struct PublishRequest {
    pub payload: String,
}

/// Publishes an arbitrary application-level message to every `/events`
/// subscriber, exactly like the messages FacetQL already sends itself
/// for node/edge/user changes — this is the one FacetQL didn't have
/// until now: a way for something OTHER than FacetQL's own internal
/// writes to put a message on the same live feed. Specifically what
/// Facet's `Store.Notify(payload string) error` needs: Postgres's
/// LISTEN/NOTIFY lets any connection publish an arbitrary string that
/// every listener receives; this is that, over the FacetQL API instead.
async fn publish_event(
    State(db): State<Arc<Database>>,
    Extension(_identity): Extension<AuthIdentity>,
    Json(payload): Json<PublishRequest>,
) -> impl IntoResponse {
    db.publish(payload.payload);
    (StatusCode::OK, "published").into_response()
}

async fn subscribe_events(
    State(db): State<Arc<Database>>,
    Extension(_identity): Extension<AuthIdentity>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = db.broadcaster.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(msg) => Some(Ok(Event::default().data(msg))),
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── admin: user management ─────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateUserRequest {
    pub owner: String,
    /// "admin" or "user" (or omitted — defaults to "user"). Only an
    /// existing admin can create another admin; enforced below, not by
    /// trusting this field alone.
    pub role: Option<String>,
}

#[derive(Serialize)]
struct CreateUserResponse {
    owner: String,
    role: Role,
    /// Shown exactly once. Not retrievable again — see core/user.rs.
    token: String,
}

#[derive(Serialize)]
struct UserSummary {
    owner: String,
    role: Role,
}

/// A simple 32-byte random token, hex-encoded. Not derived from
/// anything guessable (time, pid, counters) — uses the OS RNG via
/// `rand`'s thread-local generator.
fn generate_token() -> String {
    use rand::Rng;
    let bytes: [u8; 32] = rand::thread_rng().r#gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn create_user(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    let role = match payload.role.as_deref() {
        Some("admin") => Role::Admin,
        Some("user") | None => Role::User,
        Some(other) => {
            return (StatusCode::BAD_REQUEST, format!("unknown role {other:?} — use \"admin\" or \"user\"")).into_response();
        }
    };

    let token = generate_token();
    let record = UserRecord { token_hash: hash_token(&token), owner: payload.owner.clone(), role };

    let mut engine = db.engine.write().expect("storage engine lock poisoned");
    match engine.insert_user(record) {
        Ok(()) => {
            drop(engine);
            db.publish(
                serde_json::json!({"event": "user_created", "owner": payload.owner, "created_by": identity.owner}).to_string(),
            );
            (StatusCode::CREATED, Json(CreateUserResponse { owner: payload.owner, role, token })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn list_users(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }
    let engine = db.engine.read().expect("storage engine lock poisoned");
    let users: Vec<UserSummary> = engine
        .list_users()
        .into_iter()
        .map(|u| UserSummary { owner: u.owner.clone(), role: u.role })
        .collect();
    (StatusCode::OK, Json(users)).into_response()
}

/// Revokes every persistent user record owned by `owner`. Note this
/// only reaches persistent (admin-created) users — it cannot revoke a
/// static ENOCHIAN_TOKENS bootstrap identity, since those aren't stored
/// here at all. Removing one of those means editing the env var and
/// restarting.
async fn revoke_user(
    State(db): State<Arc<Database>>,
    Path(owner): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    let mut engine = db.engine.write().expect("storage engine lock poisoned");
    let hashes: Vec<String> = engine
        .list_users()
        .into_iter()
        .filter(|u| u.owner == owner)
        .map(|u| u.token_hash.clone())
        .collect();

    if hashes.is_empty() {
        return (StatusCode::NOT_FOUND, "no persistent user with that owner").into_response();
    }

    for hash in &hashes {
        if let Err(e) = engine.revoke_user(hash) {
            return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
        }
    }
    drop(engine);
    db.publish(serde_json::json!({"event": "user_revoked", "owner": owner}).to_string());
    (StatusCode::NO_CONTENT, "").into_response()
}
