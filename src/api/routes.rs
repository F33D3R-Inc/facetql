use axum::{
    Router,
    routing::{get, post, put, delete},
    extract::{State, Json, Path, Extension, Query},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response, sse::{Event, Sse, KeepAlive}},
};
use std::sync::Arc;
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::database::{Audience, Database};
use crate::core::node::{Node, Visibility};
use crate::core::edge::{Edge, EdgeId};
use crate::core::coordinate::Coordinate;
use crate::core::predicate::Expr;
use crate::core::user::{Role, UserRecord};
use crate::core::history::HistoryEntry;
use crate::auth::{auth_middleware, hash_token, AuthIdentity};
use crate::storage::engine::{ClaimError, Expectation, TransactionError, TxOperation};

/// A read that could not reach the storage it needed.
///
/// Reads are I/O now. A node lives on disk and getting to it goes
/// through the primary index, a page read, a decryption and a checksum
/// check, any of which can fail for reasons that have nothing to do with
/// the request. Those are 500s and have to be reported as such:
/// collapsing them into "not found" would tell a client its data is gone
/// when the truth is that this server could not read it, which is the
/// difference between "delete your local copy" and "retry, and page
/// somebody".
fn storage_failure(error: std::io::Error) -> Response {
    // `InvalidInput` from the storage layer means the request asked for
    // something this engine will not do — today, a read whose result set
    // exceeds the per-request row bound (see `max_scan_rows`). That is
    // the caller's to fix by narrowing or paging, so it must not be
    // reported as a server fault: a 500 tells an operator to go looking
    // for a broken database that is working exactly as designed.
    if error.kind() == std::io::ErrorKind::InvalidInput {
        return (StatusCode::BAD_REQUEST, format!("{error}")).into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("storage error: {error}"),
    )
        .into_response()
}

/// Are both endpoints of an edge publicly readable?
///
/// An edge is only as public as the pair it connects: announcing
/// "a → b" to everyone reveals that both nodes exist and are related, so
/// a single private endpoint keeps the whole event owner-scoped. Shared
/// by `create_edge` and `delete_edge` so the creation and the retraction
/// of one fact can never reach different subscribers — a listener that
/// saw the follow but not the unfollow would hold a stale graph forever.
///
/// A node that does not exist is not public, which is also the safe
/// answer: the event stays owner-scoped.
fn public_endpoints(
    engine: &crate::storage::engine::StorageEngine,
    from: &str,
    to: &str,
) -> std::io::Result<bool> {
    for address in [from, to] {
        let public = engine
            .get(address)?
            .is_some_and(|node| matches!(node.visibility, Visibility::Public));

        if !public {
            return Ok(false);
        }
    }

    Ok(true)
}

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

/// Body for `DELETE /edge` — the edge to remove, named by the same
/// three fields `POST /edge` creates it with.
///
/// Deliberately a body rather than a path like `/edge/:from/:to/:kind`.
/// `from`, `to` and `kind` are arbitrary client-supplied strings: an
/// address or a relationship label may contain a `/` (F33D3R's own
/// addresses are `kind:id`-shaped today, but nothing enforces that and
/// a label like "HAS/OWNS" is legal), and a path segment cannot carry
/// one without an escaping convention both sides must agree on and
/// never get wrong. A JSON body has no such question. `DELETE` with a
/// body is unusual but permitted, and it buys wire symmetry: the same
/// three field names create and remove an edge, so a client that can
/// name an edge to create it can name it to delete it.
///
/// Note the caller does NOT supply `owner` — it isn't part of an edge's
/// identity (see [`EdgeId`]), and the owner that matters here is the
/// one stored on the edge, which is what authorization is checked
/// against.
#[derive(Deserialize)]
pub struct DeleteEdgeRequest {
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

/// Largest request body any endpoint will buffer, in bytes.
///
/// Axum applies a 2 MiB default of its own, which is a fine number and a
/// bad place to leave it: it is invisible at the call site, it is not
/// something an operator can raise for a legitimately large batch, and a
/// future extractor added without `DefaultBodyLimit` in mind would
/// silently inherit whatever the framework's default happens to be that
/// version. Stating it here makes the bound part of this router's
/// contract rather than a property of a dependency.
///
/// It is the first bound a request meets, and it is the cheapest: it
/// rejects on the length header or as the body streams, before any
/// deserialization, so an oversized `POST /transaction` never becomes
/// allocated JSON. The bounds behind it — transaction size, scan rows,
/// record and key limits — are the ones that matter for what a
/// well-formed request can *ask for*; this one only stops the bytes.
const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

const MAX_BODY_BYTES_ENV: &str = "FACETQL_MAX_BODY_BYTES";

fn max_body_bytes() -> usize {
    std::env::var(MAX_BODY_BYTES_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_MAX_BODY_BYTES)
}

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

/// The full HTTP surface, in one place.
///
/// Everything except `GET /` sits behind [`auth_middleware`], so every
/// handler below can rely on an `AuthIdentity` extension being present
/// and on an unauthenticated request having been refused before it got
/// here.
///
/// | Method + path | Handler |
/// |---|---|
/// | `GET /` | `home` (unauthenticated liveness ping) |
/// | `POST /node` | `create_node` |
/// | `GET /node/:address` | `get_node` |
/// | `GET /node/:address/history` | `get_node_history` |
/// | `PUT /node/:address` | `update_node` |
/// | `DELETE /node/:address` | `delete_node` |
/// | `GET /node/:address/owned` | `list_owned` |
/// | `POST /node/:address/claim` | `claim_node` |
/// | `GET /nodes` | `query_nodes` |
/// | `POST /nodes/query` | `query_nodes_where` |
/// | `POST /edge` | `create_edge` |
/// | `DELETE /edge` | `delete_edge` (body-addressed, see [`DeleteEdgeRequest`]) |
/// | `POST /transaction` | `execute_transaction` |
/// | `GET /node/:address/edges/out` | `get_edges_out` |
/// | `GET /node/:address/edges/in` | `get_edges_in` |
/// | `GET /events` | `subscribe_events` (SSE) |
/// | `POST /publish` | `publish_event` |
/// | `POST /admin/users` | `create_user` |
/// | `GET /admin/users` | `list_users` |
/// | `DELETE /admin/users/:owner` | `revoke_user` |
/// | `GET /stats` | `stats` |
///
/// `/edge` is the one path that takes its target in a `DELETE` body
/// rather than in the path — an edge's identity is three arbitrary
/// strings, not one path-safe address; [`DeleteEdgeRequest`] explains
/// why that beats escaping them into a URL.
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
        .route("/edge", delete(delete_edge))
        .route("/transaction", post(execute_transaction))
        .route("/node/:address/edges/out", get(get_edges_out))
        .route("/node/:address/edges/in", get(get_edges_in))
        .route("/events", get(subscribe_events))
        .route("/publish", post(publish_event))
        .route("/admin/users", post(create_user))
        .route("/admin/users", get(list_users))
        .route("/admin/users/:owner", delete(revoke_user))
        .route("/stats", get(stats))
        .route_layer(middleware::from_fn_with_state(db.clone(), auth_middleware))
        .with_state(db);

    Router::new()
        .route("/", get(home))
        .merge(protected)
        .layer(cors_layer())
        .layer(axum::extract::DefaultBodyLimit::max(max_body_bytes()))
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
    // Taken before the node moves into the engine: a private node's
    // creation is announced only to its owner.
    let audience = Audience::for_node(&node);
    let edge_targets: Vec<(String, String)> =
        payload.edges.into_iter().map(|e| (e.to, e.kind)).collect();

    let mut engine = db.engine_mut();

    let existing = match engine.get(&address) {
        Ok(existing) => existing,
        Err(e) => return storage_failure(e),
    };

    if payload.if_absent && existing.is_some() {
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
                audience,
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
    let engine = db.engine();
    match engine.get(&address) {
        // Admin bypasses can_read the same way a Postgres superuser
        // bypasses row-level security — deliberate, not a bug.
        Ok(Some(node)) if identity.is_admin() || node.can_read(&identity.owner) => {
            (StatusCode::OK, Json(node)).into_response()
        }
        Ok(Some(_)) => (StatusCode::FORBIDDEN, "not authorized to read this node").into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => storage_failure(e),
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
    let engine = db.engine();
    match engine.get(&address) {
        Ok(Some(node)) if identity.is_admin() || node.can_read(&identity.owner) => {
            match engine.history_for(&address) {
                Ok(history) => {
                    let history: Vec<HistoryEntry> = history;
                    (StatusCode::OK, Json(history)).into_response()
                }
                Err(e) => storage_failure(e),
            }
        }
        Ok(Some(_)) => (StatusCode::FORBIDDEN, "not authorized to read this node's history").into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => storage_failure(e),
    }
}

async fn update_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<UpdateNodeRequest>,
) -> impl IntoResponse {
    let mut engine = db.engine_mut();

    let existing = match engine.get(&address) {
        Ok(Some(n)) => n,
        Ok(None) => return (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => return storage_failure(e),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to modify this node").into_response();
    }

    let mut updated = existing;
    updated.data = payload.data;
    if let Some(public) = payload.public {
        updated.visibility = if public { Visibility::Public } else { Visibility::Private };
    }

    let audience = Audience::for_node(&updated);

    match engine.insert(updated) {
        Ok(()) => {
            drop(engine);
            db.publish(
                audience,
                serde_json::json!({"event": "node_updated", "address": address}).to_string(),
            );
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
    let mut engine = db.engine_mut();

    let existing = match engine.get(&address) {
        Ok(Some(n)) => n,
        Ok(None) => return (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => return storage_failure(e),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to delete this node").into_response();
    }

    let audience = Audience::for_node(&existing);

    match engine.delete(&address) {
        Ok(()) => {
            drop(engine);
            db.publish(
                audience,
                serde_json::json!({"event": "node_deleted", "address": address}).to_string(),
            );
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
    let mut engine = db.engine_mut();

    // Resolved under the same lock as the claim itself, so the audience
    // matches the node the claim actually applied to.
    let audience = match engine.get(&address) {
        Ok(Some(node)) => Audience::for_node(&node),
        Ok(None) => Audience::Owner(identity.owner.clone()),
        Err(e) => return storage_failure(e),
    };

    match engine.claim(&address, &identity.owner) {
        Ok(()) => {
            drop(engine);
            db.publish(
                audience,
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
    let engine = db.engine();
    match engine.nodes_by_owner(&identity.owner) {
        Ok(owned) => {
            let owned: Vec<Node> = owned;
            (StatusCode::OK, Json(owned)).into_response()
        }
        Err(e) => storage_failure(e),
    }
}

async fn query_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    let offset = params.offset.unwrap_or(0);

    let engine = db.engine();
    // Admins see everything matching the filter, ignoring visibility —
    // same bypass rationale as get_node. `None` is the engine's
    // superuser bypass (skip the per-node can_read filter entirely).
    // Everyone else passes `Some(owner)` for the normal can_read view.
    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    match engine.query(
        params.kind.as_deref(),
        params.owner.as_deref(),
        requester,
        limit,
        offset,
    ) {
        Ok(results) => {
            let results: Vec<Node> = results;
            (StatusCode::OK, Json(results)).into_response()
        }
        Err(e) => storage_failure(e),
    }
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

    let engine = db.engine();

    // Admins bypass visibility the same way query_nodes/get_node do —
    // `None` is query_where's superuser bypass (skip the per-node
    // can_read filter entirely), so an admin lists Private nodes it does
    // not own. Everyone else passes `Some(owner)` for the can_read view.
    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
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
    let owner = identity.owner.clone();
    let edge = Edge::new(payload.from.clone(), payload.to.clone(), payload.kind.clone(), identity.owner);

    let mut engine = db.engine_mut();

    // An edge is only as public as the pair it connects: announcing
    // "a → b" to everyone reveals that both nodes exist and are related,
    // so a single private endpoint keeps the whole event owner-scoped.
    let endpoints_public = match public_endpoints(&engine, &payload.from, &payload.to) {
        Ok(public) => public,
        Err(e) => return storage_failure(e),
    };

    let audience = if endpoints_public {
        Audience::Everyone
    } else {
        Audience::Owner(owner.clone())
    };

    match engine.insert_edge(edge) {
        Ok(()) => {
            drop(engine);
            db.publish(
                audience,
                serde_json::json!({"event": "edge_created", "from": payload.from, "to": payload.to, "kind": payload.kind}).to_string(),
            );
            (StatusCode::CREATED, "Edge created").into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `DELETE /edge` — retract a relationship.
///
/// The counterpart to `create_edge`, and the piece that turns every
/// relationship in the graph from a one-way door into something a
/// client can undo: follow/unfollow, like/unlike, block/unblock,
/// mute/unmute are all "insert this edge" / "remove this edge".
///
/// The edge is named in the request body ([`DeleteEdgeRequest`]) by the
/// same `from`/`to`/`kind` triple that created it — see that struct for
/// why the target isn't in the path.
///
/// # Authorization
///
/// `identity.is_admin() || edge.can_write(&identity.owner)`, i.e. the
/// edge's own owner or an admin, exactly mirroring `delete_node`'s rule
/// for nodes. **403** when the edge exists but this caller may not
/// remove it, **404** when no such edge exists. Note the 403/404 split
/// does leak the existence of an edge between two nodes to a caller who
/// cannot delete it — the same trade `delete_node` already makes, kept
/// identical here rather than made subtly different for edges.
///
/// # One lock, three steps
///
/// The existence check, the authorization check and the delete all
/// happen inside a single write lock, as in `delete_node`. Taking a
/// read lock to resolve the owner and then re-acquiring a write lock to
/// delete would open a TOCTOU window: between the two, another request
/// could remove that edge and a third insert a *different* owner's edge
/// with the same identity (the owner is not part of [`EdgeId`]), and
/// this handler would then delete an edge it never authorized against.
async fn delete_edge(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<DeleteEdgeRequest>,
) -> impl IntoResponse {
    let id = EdgeId::new(payload.from.clone(), payload.to.clone(), payload.kind.clone());

    let mut engine = db.engine_mut();

    let existing = match engine.find_edge(&id) {
        Ok(Some(edge)) => edge,
        Ok(None) => return (StatusCode::NOT_FOUND, "edge not found").into_response(),
        Err(e) => return storage_failure(e),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to delete this edge").into_response();
    }

    // Same audience rule `create_edge` uses for `edge_created`, resolved
    // under the same lock as the delete: an edge is only as public as
    // the pair it connects, so "a → b was removed" goes to everyone only
    // when both endpoints are public — a single private endpoint would
    // otherwise reveal that the node exists and was related to the
    // other. Creation and retraction of the same fact must reach the
    // same subscribers, or a listener that saw the follow would never
    // see the unfollow and would hold a stale graph forever.
    //
    // The owner-scoped fallback names the EDGE's owner, not the caller:
    // an admin may delete someone else's edge, and the subscribers who
    // were told about the edge_created are that owner's. Admins receive
    // Audience::Owner events regardless (see Audience::admits), so
    // nothing is hidden from the caller either way.
    let endpoints_public = match public_endpoints(&engine, &payload.from, &payload.to) {
        Ok(public) => public,
        Err(e) => return storage_failure(e),
    };

    let audience = if endpoints_public {
        Audience::Everyone
    } else {
        Audience::Owner(existing.owner.clone())
    };

    match engine.delete_edge(&id) {
        Ok(()) => {
            drop(engine);
            db.publish(
                audience,
                serde_json::json!({"event": "edge_deleted", "from": payload.from, "to": payload.to, "kind": payload.kind}).to_string(),
            );
            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn get_edges_out(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine();
    match engine.get(&address) {
        Ok(Some(node)) if identity.is_admin() || node.can_read(&identity.owner) => {
            match engine.edges_from(&address) {
                Ok(edges) => (StatusCode::OK, Json(edges)).into_response(),
                Err(e) => storage_failure(e),
            }
        }
        Ok(Some(_)) => (StatusCode::FORBIDDEN, "not authorized to read this node's edges").into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => storage_failure(e),
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
    /// Retract one edge as part of the batch — the transactional form of
    /// `DELETE /edge`, and the op that lets an unfollow/unblock happen
    /// atomically alongside the node writes that accompany it. Wire tag
    /// is `delete_edge` (snake_case of the variant), and the edge is
    /// named by the same `from`/`to`/`kind` triple `insert_edge` uses —
    /// that triple IS the edge's identity (`EdgeId`); `owner` is not
    /// part of it and is never taken from the body. Authorization (the
    /// edge's owner, or admin) is resolved here, under the same write
    /// lock the engine will use, and stamped onto the op.
    DeleteEdge {
        from: String,
        to: String,
        kind: String,
    },
    DeleteNode {
        address: String,
    },
    /// Native bulk clear: remove every node of `kind` the caller may
    /// write, as one all-or-nothing step in the transaction (WAL +
    /// removals per node, exactly like `delete_node`). Wire tag is
    /// `clear_kind` (snake_case of the variant). Non-admin clears only
    /// its own nodes of that kind; admin clears all of that kind —
    /// authorization is resolved here and enforced in the engine.
    ClearKind {
        kind: String,
    },
    /// Native predicated bulk delete — `clear_kind`'s superset. Removes
    /// every node of `kind` the caller may write AND (when `where` is
    /// present) whose `data` satisfies the predicate, as one
    /// all-or-nothing step (WAL + removals per node, exactly like
    /// `delete_node`). Wire tag is `delete_where` (snake_case of the
    /// variant); the predicate field is JSON key `where` (`Expr`, the
    /// same type `POST /nodes/query` takes) and is optional — omitting
    /// it makes this behave exactly like `clear_kind`. Non-admin deletes
    /// only its own nodes of that kind; admin deletes all matching —
    /// authorization is resolved here and enforced in the engine, and
    /// the predicate is evaluated by the same `predicate::eval` the
    /// query path uses. An unpushable/erroring predicate aborts the
    /// whole transaction (never a wrong or partial delete).
    DeleteWhere {
        kind: String,
        #[serde(rename = "where")]
        where_: Option<Expr>,
    },
    /// Native compare-and-set: rewrite `address` only if `field` inside
    /// its `data` still satisfies the stated expectation, as one
    /// all-or-nothing step in the transaction. Wire tag is `set_if`.
    ///
    /// Exactly one expectation must be given:
    ///
    /// * `expect_le: <number>` — the field is a number and is at most
    ///   this. The lease/deadline form (`"next_run" <= now`).
    /// * `expect_eq: <value>` — the field equals this exactly. The
    ///   version form (compare-and-swap on a revision counter).
    /// * `expect_absent: true` — the field is unset or null. The
    ///   create-once form.
    ///
    /// `set` is merged into the node's `data` (not a replacement), so an
    /// unrelated field is never clobbered. The caller learns the outcome
    /// from the status: **200** means the condition held and the batch
    /// committed — you won; **412 Precondition Failed** means it did not
    /// and *nothing* in the batch was applied — someone else won. This
    /// is the primitive behind a durable scheduler's "reserve this tick"
    /// and any conditional update; emulating it with a read followed by
    /// a write is a race, which is exactly why it lives in the engine.
    SetIf {
        address: String,
        field: String,
        expect_le: Option<f64>,
        expect_eq: Option<serde_json::Value>,
        expect_absent: Option<bool>,
        #[serde(default)]
        set: serde_json::Map<String, serde_json::Value>,
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
/// request to change a node's or edge's ownership between this check
/// and the engine applying the batch.
///
/// The body is `{"operations": [...]}`, each element tagged by `type`
/// (see [`TxOpRequest`] for each one's fields and semantics):
///
/// * `insert_node` — create or overwrite a node.
/// * `insert_edge` — create an edge.
/// * `delete_edge` — retract one edge, named by `from`/`to`/`kind`.
/// * `delete_node` — remove one node by address.
/// * `clear_kind` — remove every node of a kind the caller may write.
/// * `delete_where` — `clear_kind` plus a `where` predicate.
/// * `set_if` — compare-and-set on one field of one node.
///
/// `owner` and `is_admin` are stamped onto every op from the
/// authenticated identity and its role, never read from the request
/// body: a batch cannot ask to act as somebody else.
async fn execute_transaction(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    let mut engine = db.engine_mut();

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
            TxOpRequest::DeleteEdge { from, to, kind } => {
                // Targeted like delete_node, not best-effort like
                // clear_kind: naming one edge that doesn't exist, or one
                // this caller may not retract, is a mistake worth
                // reporting rather than silently skipping. Resolved
                // under the write lock this handler already holds, so
                // nothing can change the edge's ownership between the
                // check and the engine applying the batch.
                let id = EdgeId::new(from, to, kind);
                if !identity.is_admin() {
                    match engine.find_edge(&id) {
                        Err(e) => return storage_failure(e),
                        Ok(Some(e)) if !e.can_write(&identity.owner) => {
                            return (
                                StatusCode::FORBIDDEN,
                                format!(
                                    "not authorized to delete edge {} -{}-> {}",
                                    id.from, id.kind, id.to
                                ),
                            )
                                .into_response();
                        }
                        Ok(None) => {
                            return (
                                StatusCode::NOT_FOUND,
                                format!(
                                    "delete target not found: edge {} -{}-> {}",
                                    id.from, id.kind, id.to
                                ),
                            )
                                .into_response();
                        }
                        _ => {}
                    }
                }
                // An edge is not a node: it has no address, so it adds
                // nothing to `touched_addresses` — the committed event
                // lists the node addresses a batch wrote, exactly as
                // insert_edge leaves it alone too.
                ops.push(TxOperation::DeleteEdge {
                    id,
                    owner: identity.owner.clone(),
                    is_admin: identity.is_admin(),
                });
            }
            TxOpRequest::DeleteNode { address } => {
                if !identity.is_admin() {
                    match engine.get(&address) {
                        Err(e) => return storage_failure(e),
                        Ok(Some(n)) if !n.can_write(&identity.owner) => {
                            return (
                                StatusCode::FORBIDDEN,
                                format!("not authorized to delete {address}"),
                            )
                                .into_response();
                        }
                        Ok(None) => {
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
                // Record the exact addresses this clear will remove
                // for the committed event, using the same rule the
                // engine applies. The write lock is held throughout, so
                // this snapshot matches what execute_transaction removes.
                // The engine's own selector, driven by the kind index,
                // rather than a walk of every node in the database —
                // and the same rule `execute_transaction` applies, so
                // the reported addresses cannot drift from the removed
                // ones. A clear is a `delete_where` with no predicate.
                match engine.delete_where_targets(
                    &kind,
                    None,
                    &identity.owner,
                    is_admin,
                ) {
                    Ok(addresses) => touched_addresses.extend(addresses),
                    Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
                }
                ops.push(TxOperation::ClearKind {
                    kind,
                    owner: identity.owner.clone(),
                    is_admin,
                });
            }
            TxOpRequest::DeleteWhere { kind, where_ } => {
                // Predicated superset of clear_kind: same "remove what
                // I'm allowed to remove" rule, additionally filtered by
                // the same `where` predicate the /nodes/query path
                // evaluates. Like a clear, a delete_where never rejects
                // on authorization — non-writable nodes are simply not
                // selected. Authorization (owner + admin) is resolved
                // here, under the same write lock the engine uses, and
                // enforced in the engine when it selects rows.
                let is_admin = identity.is_admin();
                // Record the exact addresses this delete_where will
                // remove for the committed event, using the same
                // selection (kind + auth + predicate) the engine
                // applies. This reuses the engine's single
                // `predicate::eval`-backed selector, so an
                // unpushable/erroring predicate surfaces here exactly as
                // the query path surfaces it — a BAD_REQUEST that aborts
                // before anything is written.
                match engine.delete_where_targets(
                    &kind,
                    where_.as_ref(),
                    &identity.owner,
                    is_admin,
                ) {
                    Ok(addresses) => touched_addresses.extend(addresses),
                    Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
                }
                ops.push(TxOperation::DeleteWhere {
                    kind,
                    where_,
                    owner: identity.owner.clone(),
                    is_admin,
                });
            }
            TxOpRequest::SetIf {
                address,
                field,
                expect_le,
                expect_eq,
                expect_absent,
                set,
            } => {
                // Exactly one expectation, resolved here so a malformed
                // condition is a plain 400 rather than something the
                // engine has to guess at. `expect_absent: false` counts
                // as "not given" — it states no condition.
                let mut stated = [
                    expect_le.map(Expectation::AtMost),
                    expect_eq.map(Expectation::Equals),
                    expect_absent.filter(|set| *set).map(|_| Expectation::Absent),
                ]
                .into_iter()
                .flatten();

                let expect = match (stated.next(), stated.next()) {
                    (Some(expect), None) => expect,
                    _ => {
                        return (
                            StatusCode::BAD_REQUEST,
                            "set_if requires exactly one of expect_le, expect_eq, \
                             expect_absent",
                        )
                            .into_response();
                    }
                };

                touched_addresses.push(address.clone());

                ops.push(TxOperation::SetIf {
                    address,
                    field,
                    expect,
                    set,
                    owner: identity.owner.clone(),
                    is_admin: identity.is_admin(),
                });
            }
        }
    }

    match engine.execute_transaction(ops) {
        Ok(()) => {
            drop(engine);
            // A batch is one identity's writes, and its address list can
            // name private nodes, so it is announced to that identity
            // (and admins) rather than broadcast. Public fan-out is what
            // POST /publish is for — an explicit choice, not a side
            // effect of writing.
            db.publish(
                Audience::Owner(identity.owner.clone()),
                serde_json::json!({"event": "transaction_committed", "addresses": touched_addresses}).to_string(),
            );
            (StatusCode::OK, "transaction committed").into_response()
        }
        // A failed precondition is not a malformed request: the batch
        // was well-formed, the caller simply lost the race. 412 keeps
        // that distinguishable from a 400 without parsing error prose.
        Err(TransactionError::Precondition(e)) => {
            (StatusCode::PRECONDITION_FAILED, e).into_response()
        }
        // The batch was fine; the disk was not. Telling a caller its
        // request was bad would send it off to fix something that isn't
        // broken.
        Err(TransactionError::Storage(e)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
        Err(TransactionError::Invalid(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

async fn get_edges_in(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let engine = db.engine();
    match engine.get(&address) {
        Ok(Some(node)) if identity.is_admin() || node.can_read(&identity.owner) => {
            match engine.edges_to(&address) {
                Ok(edges) => (StatusCode::OK, Json(edges)).into_response(),
                Err(e) => storage_failure(e),
            }
        }
        Ok(Some(_)) => (StatusCode::FORBIDDEN, "not authorized to read this node's edges").into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => storage_failure(e),
    }
}

/// Largest `POST /publish` payload, in bytes.
///
/// Paired with `database::BROADCAST_CAPACITY`: the two multiply to the
/// most memory the event channel can hold, which is the number that
/// actually matters. 64 KiB × 1024 events is a 64 MiB ceiling.
///
/// It is also well above what the channel is for. Every event FacetQL
/// publishes itself is a short JSON notice naming an address; a
/// subscriber that needs the record reads it. A payload near this bound
/// is a sign the channel is being used as a transport, which it is not.
const MAX_EVENT_PAYLOAD: usize = 64 * 1024;

#[derive(Deserialize)]
pub struct PublishRequest {
    pub payload: String,
}

/// Publishes an arbitrary application-level message onto the `/events`
/// feed, alongside the messages FacetQL already sends itself for
/// node/edge/user changes — a way for something OTHER than FacetQL's own
/// internal writes to put a message on the live feed. Specifically what
/// Facet's `Store.Notify(payload string) error` needs: Postgres's
/// LISTEN/NOTIFY lets a connection publish an arbitrary string that
/// listeners receive; this is that, over the FacetQL API instead.
///
/// # Who the message reaches
///
/// The audience is decided from the caller's identity, never from the
/// request body:
///
/// * **admin** → [`Audience::Everyone`]. A superuser can already read
///   every node, so letting it address every subscriber grants nothing
///   it did not already have.
/// * **anyone else** → [`Audience::Owner`] of the caller. The message
///   reaches that owner's own subscribers (every connection holding one
///   of its tokens) and admins, and nobody else.
///
/// This is deliberately narrower than a plain LISTEN/NOTIFY, and the
/// narrowing is the point. `/events` is a read path shared by every
/// tenant, so an unrestricted broadcast would hand any valid token a
/// channel into every other subscriber's stream — a spam and phishing
/// surface, and an odd one to leave open in the same handler set that
/// filters node events by visibility (see [`subscribe_events`]). An
/// owner-scoped notification still satisfies the `Notify` contract it
/// exists for: a service publishes under its own identity and its own
/// listeners — which is every instance of that service — receive it.
///
/// Nothing here is read out of the database, so there is no stored
/// visibility to honour; the caller still must not put another tenant's
/// private data in a payload it chose the contents of.
async fn publish_event(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<PublishRequest>,
) -> impl IntoResponse {
    // The body limit alone does not bound this one. A published payload
    // does not pass through and get dropped — it is retained in the
    // broadcast ring until every subscriber has read past it, and the
    // ring holds `BROADCAST_CAPACITY` events. So the memory a caller can
    // pin is capacity × payload size, and with only the 4 MiB body limit
    // in front of it that product is gigabytes, reachable by one
    // authenticated identity in a loop with no subscribers at all.
    //
    // Bounding the payload is what makes the ring's own bound mean
    // something: the two constants multiply to a fixed ceiling.
    if payload.payload.len() > MAX_EVENT_PAYLOAD {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "event payload is {} bytes; the maximum is \
                 {MAX_EVENT_PAYLOAD}. `/publish` is a notification \
                 channel, not a transport — publish an address and let \
                 subscribers read the node.",
                payload.payload.len()
            ),
        )
            .into_response();
    }

    let audience = if identity.is_admin() {
        Audience::Everyone
    } else {
        Audience::Owner(identity.owner.clone())
    };

    db.publish(audience, payload.payload);
    (StatusCode::OK, "published").into_response()
}

/// `GET /events` (SSE) — the live notification stream, filtered to what
/// this subscriber is allowed to see.
///
/// Every event carries the audience the writing handler stamped on it
/// (see [`Audience`]), and a subscriber receives only the ones that
/// admit it. Without this filter the stream is a read path with no
/// authorization at all: any valid token would learn the address, kind
/// and timing of every private node in the database — data the same
/// token would be refused on `GET /node/:address`.
///
/// The identity is resolved once, when the stream is opened, and the
/// filter closure owns it for the life of the connection.
async fn subscribe_events(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = db.broadcaster.subscribe();

    let owner = identity.owner.clone();
    let is_admin = identity.is_admin();

    let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(event) if event.audience.admits(&owner, is_admin) => {
            Some(Ok(Event::default().data(event.payload)))
        }
        // Either the event is not for this subscriber, or the receiver
        // lagged and dropped messages. Neither ends the stream.
        _ => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── stats / observability ──────────────────────────────────────────────

/// `GET /stats` — the engine's own storage/operation statistics, the
/// native observability surface a Fabric telemetry poller differences
/// over time into `WorkloadMetrics` (and a real health/capacity read for
/// operators; NOTES EPIC 08). Additive to the §4/§4b wire contract: a new
/// endpoint under the same `x-api-key` auth, no change to any existing op.
///
/// Admin-gated (returns 403 for a non-admin), mirroring `list_users` /
/// the other `/admin` handlers: it exposes fleet-wide counts, so it's a
/// superuser read. The response body is `StorageEngine::stats()`'s
/// [`EngineStats`](crate::storage::engine::EngineStats) serialized
/// directly — that struct IS the wire shape, so there's one source of
/// truth for the JSON, not a second response struct to keep in sync.
async fn stats(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }
    let engine = db.engine();
    match engine.stats() {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => storage_failure(e),
    }
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

    let mut engine = db.engine_mut();
    match engine.insert_user(record) {
        Ok(()) => {
            drop(engine);
            // Account lifecycle is admin business: only an admin can
            // reach this route, and the event names a new identity, so
            // it stays inside the admin audience.
            db.publish(
                Audience::Owner(identity.owner.clone()),
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
    let engine = db.engine();
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

    let mut engine = db.engine_mut();
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
    // Account lifecycle is admin business; only admins created or
    // revoked it, and only admins should hear about it.
    db.publish(
        Audience::Owner(identity.owner.clone()),
        serde_json::json!({"event": "user_revoked", "owner": owner}).to_string(),
    );
    (StatusCode::NO_CONTENT, "").into_response()
}

#[cfg(test)]
mod stats_route_tests {
    //! HTTP-level tests for `GET /stats` driven through the real router
    //! (`create_router` + the `x-api-key` auth middleware) via
    //! `tower::ServiceExt::oneshot`, so they exercise auth-gating and JSON
    //! shape end-to-end, not just the handler in isolation.
    //!
    //! State is built through the engine's public API against a real
    //! data directory, because there is no longer an in-memory map to
    //! place nodes into — the database is the heap and the indexes on
    //! disk. The data directory is process-wide (`config` resolves it
    //! through a `OnceLock`), so every test module in this binary shares
    //! one and these assertions are written to be true regardless of
    //! what else is in it: unique kinds and addresses, and `>=` on the
    //! global counters. Auth uses *persistent* user tokens (resolved via
    //! `find_user_by_hash`), not the env `ENOCHIAN_TOKENS` bootstrap, so
    //! the tests don't depend on process-wide env state.
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::RwLock;
    use tower::ServiceExt; // for `oneshot`

    const ADMIN_TOKEN: &str = "stats-admin-token";
    const USER_TOKEN: &str = "stats-user-token";

    const POST_KIND: &str = "StatsRoutePost";
    const PROFILE_KIND: &str = "StatsRouteProfile";

    use crate::storage::engine::test_support::disk_guard;

    /// A router over an engine holding 2 `StatsRoutePost` nodes and 1
    /// `StatsRouteProfile`, plus an admin and a non-admin persistent
    /// user.
    fn router() -> axum::Router {
        let mut engine = StorageEngine::open().expect("open storage engine");

        for (addr, kind) in [
            ("stats:p1", POST_KIND),
            ("stats:p2", POST_KIND),
            ("stats:pr1", PROFILE_KIND),
        ] {
            engine
                .insert(Node::new(
                    Coordinate::new(0, 0, 0, 0),
                    addr.to_string(),
                    kind.to_string(),
                    "admin".to_string(),
                ))
                .expect("insert node");
        }

        engine.users.insert(
            hash_token(ADMIN_TOKEN),
            UserRecord { token_hash: hash_token(ADMIN_TOKEN), owner: "admin".to_string(), role: Role::Admin },
        );
        engine.users.insert(
            hash_token(USER_TOKEN),
            UserRecord { token_hash: hash_token(USER_TOKEN), owner: "bob".to_string(), role: Role::User },
        );

        let (broadcaster, _) = tokio::sync::broadcast::channel(16);
        let db = Arc::new(Database { engine: Arc::new(RwLock::new(engine)), broadcaster });
        create_router(db)
    }

    /// The count this response reports for one kind.
    fn kind_count(json: &serde_json::Value, kind: &str) -> u64 {
        json["kinds"]
            .as_array()
            .expect("kinds is an array")
            .iter()
            .find(|entry| entry["kind"] == kind)
            .and_then(|entry| entry["count"].as_u64())
            .unwrap_or(0)
    }

    fn stats_request(token: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri("/stats")
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    /// Admin token → 200 with the exact counts and sorted per-kind
    /// breakdown the wire contract specifies.
    #[tokio::test]
    async fn admin_gets_stats_with_expected_counts() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(stats_request(ADMIN_TOKEN))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");

        // This module's own kinds are counted exactly; the global
        // totals are bounds, because the data directory is shared with
        // every other test in this binary.
        assert_eq!(kind_count(&json, POST_KIND), 2);
        assert_eq!(kind_count(&json, PROFILE_KIND), 1);

        assert!(json["node_count"].as_u64().expect("node_count") >= 3);
        assert_eq!(json["user_count"], 2);

        // The whole wire shape is present, including the physical
        // storage block.
        for field in [
            "node_count",
            "edge_count",
            "user_count",
            "history_entries",
            "kinds",
            "reads_total",
            "writes_total",
            "storage",
        ] {
            assert!(!json[field].is_null(), "{field} missing from /stats");
        }

        assert!(json["storage"]["page_size"].as_u64().expect("page_size") > 0);
    }

    /// Non-admin token → 403, and no stats body is leaked.
    #[tokio::test]
    async fn non_admin_is_forbidden() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(stats_request(USER_TOKEN))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// No credential at all → 401 from the auth middleware (the route is
    /// on the protected router), never reaching the handler.
    #[tokio::test]
    async fn missing_token_is_unauthorized() {
        let _guard = disk_guard();

        let req = Request::builder()
            .method("GET")
            .uri("/stats")
            .body(Body::empty())
            .expect("build request");

        let resp = router().oneshot(req).await.expect("router response");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
