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
use tokio_stream::{
    wrappers::BroadcastStream, wrappers::errors::BroadcastStreamRecvError, StreamExt,
};

use crate::database::{Audience, Database, LiveEvent};
use crate::core::aggregate::{AggFunc, AggSpec};
use crate::core::node::{Node, Visibility};
use crate::core::edge::{Edge, EdgeId};
use crate::core::coordinate::Coordinate;
use crate::core::predicate::Expr;
use crate::core::user::{Role, UserRecord};
use crate::core::history::HistoryEntry;
use crate::auth::{auth_middleware, hash_token, AuthIdentity};
use crate::storage::changes::{
    ScanError, DEFAULT_CHANGE_LIMIT, MAX_CHANGE_LIMIT,
};
use crate::storage::engine::{ClaimError, Expectation, TransactionError, TxOperation};
use crate::storage::index::{IndexDef, IndexInfo};
use crate::storage::text::TextIndexDef;
use crate::storage::reference::{ReferenceDef, ReferentialAction};

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

/// Body for `POST /sequence/:name/next` — take a block of ids.
#[derive(Deserialize)]
pub struct SequenceRequest {
    /// How many consecutive values to allocate. One if omitted.
    #[serde(default = "one")]
    pub count: u64,
}

fn one() -> u64 {
    1
}

#[derive(Serialize)]
struct SequenceResponse {
    /// First value allocated. The caller owns `[first, first + count)`.
    first: u64,
    count: u64,
}

/// Body for `POST /nodes/multiget` — many point reads in one request.
#[derive(Deserialize)]
pub struct MultiGetRequest {
    pub addresses: Vec<String>,
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

/// `POST /nodes/count` — the selection half of [`QueryWhereRequest`] with
/// nothing from its paging half.
///
/// Deliberately not the same struct with the ordering fields ignored: a
/// request that accepts `order`, `limit` and `after` and silently drops
/// them invites a caller to believe it counted a page. There is one thing
/// this endpoint can be asked, and this is its shape.
#[derive(Deserialize)]
pub struct CountRequest {
    pub kind: Option<String>,
    pub owner: Option<String>,
    #[serde(rename = "where")]
    pub where_: Option<Expr>,
    #[serde(default = "default_item_var")]
    pub item_var: String,
}

#[derive(Serialize)]
struct CountResponse {
    count: u64,
}

/// `POST /nodes/count_by` — a count per distinct value of one field.
///
/// The selection half of a query plus the field to group on. No paging,
/// for the same reason [`CountRequest`] has none.
#[derive(Deserialize)]
pub struct CountByRequest {
    pub kind: Option<String>,
    pub owner: Option<String>,
    #[serde(rename = "where")]
    pub where_: Option<Expr>,
    #[serde(default = "default_item_var")]
    pub item_var: String,
    pub group_by: String,
    /// The values to answer about, when the caller knows them — the rows
    /// a page is rendering, typically. Omitted means "every distinct
    /// value", which is a different and much larger question: grouping a
    /// 20 000-value field to fill in 20 numbers costs a thousand times
    /// what asking for those 20 does.
    pub values: Option<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
struct CountByResponse {
    counts: Vec<crate::storage::engine::GroupCount>,
}

/// `POST /nodes/aggregate` — one aggregate over the rows a filter selects.
///
/// The selection half of [`QueryWhereRequest`] plus which aggregate to
/// compute. `func` is `count`, `sum`, `avg`, `min` or `max`; `field`
/// names the `data` field being aggregated, and is required by every
/// function except `count` — which is refused *with* one, because a
/// `count` that quietly ignored the field the caller named would be
/// answering a different question than the one asked.
///
/// No paging, for the reason [`CountRequest`] has none: the answer is one
/// value.
#[derive(Deserialize)]
pub struct AggregateRequest {
    pub kind: Option<String>,
    pub owner: Option<String>,
    #[serde(rename = "where")]
    pub where_: Option<Expr>,
    #[serde(default = "default_item_var")]
    pub item_var: String,
    pub func: String,
    pub field: Option<String>,
}

#[derive(Serialize)]
struct AggregateResponse {
    result: serde_json::Value,
}

/// `POST /nodes/aggregate_by` — one aggregate per distinct value of a
/// field.
///
/// [`AggregateRequest`] grouped, exactly as [`CountByRequest`] is
/// [`CountRequest`] grouped, and with the same `values` shortcut: a page
/// rendering twenty rows asks about those twenty values rather than
/// grouping the whole kind.
#[derive(Deserialize)]
pub struct AggregateByRequest {
    pub kind: Option<String>,
    pub owner: Option<String>,
    #[serde(rename = "where")]
    pub where_: Option<Expr>,
    #[serde(default = "default_item_var")]
    pub item_var: String,
    pub group_by: String,
    pub values: Option<Vec<serde_json::Value>>,
    pub func: String,
    pub field: Option<String>,
}

#[derive(Serialize)]
struct AggregateByResponse {
    groups: Vec<crate::storage::engine::GroupAggregate>,
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

use crate::api::limits::{self, EndpointClass};
use crate::config::Deployment;

/// Which browsers may make cross-origin requests to this server.
///
/// # Why this is not simply `Any`
///
/// It was, and the comment above it said not to ship it. The reasoning
/// for why that is survivable is worth stating rather than assuming,
/// because it is the reason this is a hardening step and not an
/// emergency: authentication here is an `x-api-key` header, never a
/// cookie, so a browser does not attach a credential to a cross-origin
/// request on its own. A hostile page therefore cannot act as a visitor
/// merely by being open — it would have to already hold the token, and
/// if it holds the token the origin policy was never what was stopping
/// it.
///
/// What `Any` does cost is everything downstream of that: it invites a
/// front-end to hold a long-lived database token in a browser at all, it
/// makes every future cookie- or session-shaped feature a same-site
/// question nobody re-asks, and it means a token leaked into a page's
/// JavaScript is usable from any other page the user has open. None of
/// those are exploits today and all of them are the ordinary way this
/// becomes one.
///
/// So the policy follows the deployment posture, and in production it
/// fails closed:
///
/// * **Development** — any origin, as before. Nothing here is real.
/// * **Production with `FACETQL_ALLOWED_ORIGINS` set** — exactly those
///   origins, comma-separated (`https://app.example.com,https://admin.example.com`).
/// * **Production with `FACETQL_ALLOWED_ORIGINS=*`** — any origin,
///   because an operator said so explicitly. The escape hatch exists;
///   it just is not the default.
/// * **Production with nothing set** — no cross-origin access at all.
///
/// The last case is the one that matters and it breaks nothing that
/// exists: a server-to-server client sends no `Origin` header, so CORS
/// never applies to it. The live client (`fct`'s `fqStore`) is exactly
/// that — a Go process, not a browser — which is why the safe default is
/// affordable here.
const ALLOWED_ORIGINS_ENV: &str = "FACETQL_ALLOWED_ORIGINS";

fn cors_layer() -> CorsLayer {
    let methods = [
        axum::http::Method::GET,
        axum::http::Method::POST,
        axum::http::Method::PUT,
        axum::http::Method::DELETE,
    ];

    let configured = std::env::var(ALLOWED_ORIGINS_ENV).unwrap_or_default();
    let configured = configured.trim();

    let permissive = crate::config::deployment() == Deployment::Development
        || configured == "*";

    if permissive {
        return CorsLayer::new()
            .allow_origin(tower_http::cors::Any)
            .allow_methods(methods)
            .allow_headers(tower_http::cors::Any);
    }

    let origins: Vec<axum::http::HeaderValue> = configured
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .filter_map(|origin| origin.parse().ok())
        .collect();

    // An empty list is a real answer, not a missing one: `CorsLayer`
    // with no allowed origin emits no `Access-Control-Allow-Origin`, so
    // a browser refuses the response and a non-browser client — which
    // sends no `Origin` and is what actually talks to this server — is
    // unaffected.
    CorsLayer::new()
        .allow_origin(origins)
        .allow_methods(methods)
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderName::from_static("x-api-key"),
        ])
}

/// Who may call one route, stated rather than implied.
///
/// The authorization for an endpoint used to exist only as whatever its
/// handler happened to do, which is how the two holes fixed in the pass
/// before this one survived: `POST /node` skipped an ownership check
/// that three sibling paths performed, and `GET /node/:address/owned`
/// declared a path parameter it ignored. Neither is visible by reading
/// `create_router`, and neither contradicts anything written down —
/// because nothing was written down.
///
/// So the rule for every route is written down here, next to the router,
/// and [`route_authorization_tests`] drives every entry of it through
/// the real router. A statement nobody checks is a comment; a statement
/// a test checks is a control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No credential. Exactly one route, and it answers a constant.
    Anonymous,

    /// Any authenticated identity may call it. What it may then *see* or
    /// *change* is decided per object by the handler — see `objects`.
    Authenticated,

    /// `Role::Admin` required; every other identity gets 403 before the
    /// handler touches anything.
    AdminOnly,
}

/// One row of the authorization matrix.
pub struct RouteSpec {
    pub method: &'static str,

    /// The axum route pattern, verbatim, so this can be read against
    /// `create_router` line by line.
    pub path: &'static str,

    pub access: Access,

    /// The per-object rule the handler applies *on top of* `access`,
    /// in the vocabulary of `Node::can_read` / `Node::can_write` /
    /// `Edge::can_write`, with `admin` meaning the superuser bypass.
    pub objects: &'static str,

    /// The rate-limit bucket this path draws from. Paired with the
    /// route so a new endpoint cannot be added without deciding what it
    /// costs; `EndpointClass::of` is checked against this column.
    pub class: EndpointClass,
}

/// Every route, its caller requirement, and its cost class.
pub const ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "GET",
        path: "/",
        access: Access::Anonymous,
        objects: "none — a constant liveness string, no state read",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "POST",
        path: "/node",
        access: Access::Authenticated,
        objects: "can_write on the node being replaced, if one exists; \
                  a fresh address is unrestricted; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "GET",
        path: "/node/:address",
        access: Access::Authenticated,
        objects: "can_read on the node; admin bypasses",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "GET",
        path: "/node/:address/history",
        access: Access::Authenticated,
        objects: "can_read on the node's CURRENT value; admin bypasses",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "PUT",
        path: "/node/:address",
        access: Access::Authenticated,
        objects: "can_write on the node; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "DELETE",
        path: "/node/:address",
        access: Access::Authenticated,
        objects: "can_write on the node; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "GET",
        path: "/node/:address/owned",
        access: Access::Authenticated,
        objects: "can_read on the subject node, and the listing is \
                  filtered to what the caller may read; admin bypasses",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "POST",
        path: "/node/:address/claim",
        access: Access::Authenticated,
        objects: "can_write on the node — a claim writes claimed_by and \
                  archives the previous value; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "GET",
        path: "/nodes",
        access: Access::Authenticated,
        objects: "results filtered by can_read; admin sees every match",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "POST",
        path: "/sequence/:name/next",
        access: Access::Authenticated,
        objects: "can_write on the sequence node; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/multiget",
        access: Access::Authenticated,
        objects: "results filtered by can_read; admin sees every named node",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/query",
        access: Access::Authenticated,
        objects: "results filtered by can_read; admin sees every match",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/count",
        access: Access::Authenticated,
        objects: "counts only rows can_read admits; admin counts every match",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/count_by",
        access: Access::Authenticated,
        objects: "counts only rows can_read admits; admin counts every match",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/aggregate",
        access: Access::Authenticated,
        objects: "aggregates only rows can_read admits; admin sees every match",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/nodes/aggregate_by",
        access: Access::Authenticated,
        objects: "aggregates only rows can_read admits; admin sees every match",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/edge",
        access: Access::Authenticated,
        objects: "can_write on `from` and can_read on `to`; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "DELETE",
        path: "/edge",
        access: Access::Authenticated,
        objects: "Edge::can_write on the stored edge; admin bypasses",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "POST",
        path: "/transaction",
        access: Access::Authenticated,
        objects: "per op: insert_node/set_if need can_write on what they \
                  replace and insert_node's owner/claimed_by are admin-only \
                  (403 for anyone else, whole batch refused), delete_node \
                  needs can_write, delete_edge needs \
                  Edge::can_write, insert_edge needs can_write on `from` \
                  and can_read on `to`, clear_kind/delete_where select \
                  only writable nodes; admin bypasses each",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "GET",
        path: "/node/:address/edges/out",
        access: Access::Authenticated,
        objects: "can_read on the node; admin bypasses",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "GET",
        path: "/node/:address/edges/in",
        access: Access::Authenticated,
        objects: "can_read on the node; admin bypasses",
        class: EndpointClass::Read,
    },
    RouteSpec {
        method: "GET",
        path: "/events",
        access: Access::Authenticated,
        objects: "the stream is filtered by Audience::admits, so a \
                  subscriber sees only its own owner's events; admin \
                  sees every event. `?after=` replays under the same \
                  filter — a resume is never a way to read events a \
                  live subscription would have withheld",
        class: EndpointClass::Subscribe,
    },
    RouteSpec {
        method: "GET",
        path: "/changes",
        access: Access::Authenticated,
        objects: "every change is filtered by Audience::admits — derived \
                  from the inserted node, or for a delete from the \
                  archived state it removed — so a caller sees only \
                  changes to nodes can_read would have shown it; a \
                  delete with no archive to attribute it is withheld \
                  from everyone; admin sees every change",
        class: EndpointClass::Bulk,
    },
    RouteSpec {
        method: "POST",
        path: "/publish",
        access: Access::Authenticated,
        objects: "audience is the caller's own owner; admin publishes to \
                  everyone",
        class: EndpointClass::Write,
    },
    RouteSpec {
        method: "POST",
        path: "/admin/users",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "GET",
        path: "/admin/users",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "DELETE",
        path: "/admin/users/:owner",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "POST",
        path: "/admin/indexes",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "GET",
        path: "/admin/indexes",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "DELETE",
        path: "/admin/indexes/:name",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "POST",
        path: "/admin/references",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "GET",
        path: "/admin/references",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "DELETE",
        path: "/admin/references/:name",
        access: Access::AdminOnly,
        objects: "none beyond the role",
        class: EndpointClass::Admin,
    },
    RouteSpec {
        method: "GET",
        path: "/stats",
        access: Access::AdminOnly,
        objects: "none beyond the role — the counts are fleet-wide",
        class: EndpointClass::Admin,
    },
];

/// The full HTTP surface, in one place. [`ROUTES`] is the authorization
/// matrix for it, and the table below is the same list with handlers.
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
/// | `POST /nodes/count` | `count_nodes` |
/// | `POST /nodes/count_by` | `count_nodes_by` |
/// | `POST /nodes/aggregate` | `aggregate_nodes` |
/// | `POST /nodes/aggregate_by` | `aggregate_nodes_by` |
/// | `POST /edge` | `create_edge` |
/// | `DELETE /edge` | `delete_edge` (body-addressed, see [`DeleteEdgeRequest`]) |
/// | `POST /transaction` | `execute_transaction` |
/// | `GET /node/:address/edges/out` | `get_edges_out` |
/// | `GET /node/:address/edges/in` | `get_edges_in` |
/// | `GET /events` | `subscribe_events` (SSE; `?after=<seq>` resumes) |
/// | `GET /changes` | `scan_changes` (durable WAL-backed pull; `?after=<seq>`) |
/// | `POST /publish` | `publish_event` |
/// | `POST /admin/users` | `create_user` |
/// | `GET /admin/users` | `list_users` |
/// | `DELETE /admin/users/:owner` | `revoke_user` |
/// | `POST /admin/indexes` | `create_index` |
/// | `GET /admin/indexes` | `list_indexes` |
/// | `DELETE /admin/indexes/:name` | `drop_index` |
/// | `POST /admin/references` | `create_reference` |
/// | `GET /admin/references` | `list_references` |
/// | `DELETE /admin/references/:name` | `drop_reference` |
/// | `GET /stats` | `stats` |
///
/// `/edge` is the one path that takes its target in a `DELETE` body
/// rather than in the path — an edge's identity is three arbitrary
/// strings, not one path-safe address; [`DeleteEdgeRequest`] explains
/// why that beats escaping them into a URL.
///
/// # The order the guards are in, and why
///
/// Reading outwards from the handler:
///
///  1. **`limits::request_timeout_layer`** — innermost, and applied to
///     the request routes *only*. `GET /events` is deliberately outside
///     it: an SSE stream that is severed every thirty seconds is not a
///     guarded stream, it is a broken one. That connection is bounded by
///     `limits::subscriber_permit` inside the handler instead.
///  2. **`limits::rate_limit`** — keyed by the authenticated identity,
///     so it must sit *inside* the auth layer. This ordering is the
///     whole reason it is a separate `route_layer` call rather than part
///     of the auth middleware: axum applies the last-added layer
///     outermost, so listing the limiter first is what puts it second in
///     the request's path.
///  3. **`auth_middleware`** — outermost of the three, and a
///     `route_layer` so a request that matched no route is a 404 rather
///     than a 401 that leaks which paths exist.
///  4. **`cors_layer`**, then **`limits::concurrency`**, then
///     **`limits::body_limit_layer`** on the outer router, which also
///     carries the unauthenticated `GET /`. The body limit is outermost
///     because it is the only bound that can refuse a request before its
///     bytes are read.
pub fn create_router(db: Arc<Database>) -> Router {
    // Streams and requests are the same router except for one property:
    // a request has a deadline and a stream must not. Splitting them
    // here is what lets the deadline be a layer rather than a check
    // repeated in every handler that is not `/events`.
    let streaming = Router::new().route("/events", get(subscribe_events));

    let requests = Router::new()
        .route("/node", post(create_node))
        .route("/node/:address", get(get_node))
        .route("/node/:address/history", get(get_node_history))
        .route("/node/:address", put(update_node))
        .route("/node/:address", delete(delete_node))
        .route("/node/:address/owned", get(list_owned))
        .route("/node/:address/claim", post(claim_node))
        .route("/nodes", get(query_nodes))
        .route("/sequence/:name/next", post(sequence_next))
        .route("/nodes/multiget", post(multiget_nodes))
        .route("/nodes/query", post(query_nodes_where))
        .route("/nodes/count", post(count_nodes))
        .route("/nodes/count_by", post(count_nodes_by))
        .route("/nodes/aggregate", post(aggregate_nodes))
        .route("/nodes/aggregate_by", post(aggregate_nodes_by))
        .route("/edge", post(create_edge))
        .route("/edge", delete(delete_edge))
        .route("/transaction", post(execute_transaction))
        .route("/node/:address/edges/out", get(get_edges_out))
        .route("/node/:address/edges/in", get(get_edges_in))
        .route("/changes", get(scan_changes))
        .route("/publish", post(publish_event))
        .route("/admin/users", post(create_user))
        .route("/admin/users", get(list_users))
        .route("/admin/users/:owner", delete(revoke_user))
        .route("/admin/indexes", post(create_index))
        .route("/admin/indexes", get(list_indexes))
        .route("/admin/indexes/:name", delete(drop_index))
        .route("/admin/references", post(create_reference))
        .route("/admin/references", get(list_references))
        .route("/admin/references/:name", delete(drop_reference))
        .route("/stats", get(stats))
        .layer(limits::request_timeout_layer())
        // Throughput, latency and classification for every request that
        // matched a route. A `route_layer` so it never observes a 404,
        // and applied here rather than to `streaming` because an SSE
        // subscription is a connection that is *supposed* to last for
        // hours — timing it would put a multi-hour sample in a latency
        // histogram that exists to describe request service time.
        .route_layer(middleware::from_fn(crate::metrics::observe));

    let protected = streaming
        .merge(requests)
        .route_layer(middleware::from_fn(limits::rate_limit))
        .route_layer(middleware::from_fn_with_state(db.clone(), auth_middleware))
        .with_state(db);

    Router::new()
        .route("/", get(home))
        .merge(protected)
        .layer(cors_layer())
        .layer(middleware::from_fn(limits::concurrency))
        .layer(limits::body_limit_layer())
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
    let is_admin = identity.is_admin();
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

    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

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

    match engine.insert_with_edges(node, edge_targets, is_admin) {
        Ok(edges_created) => {
            events.publish(
                audience,
                serde_json::json!({"event": "node_created", "address": address, "kind": kind}),
            );
            (
                StatusCode::CREATED,
                Json(CreateNodeResponse { address, edges_created }),
            )
                .into_response()
        }
        // An ownership refusal is a 403, not a 400: the request is
        // well-formed and the caller simply may not make it. `PUT` and
        // `DELETE` already answer that way for the same refusal, and a
        // client that saw 400 here would retry after "fixing" a body
        // that was never wrong.
        Err((e, _)) if e.starts_with("not authorized") => {
            (StatusCode::FORBIDDEN, e).into_response()
        }

        Err((e, edges_created_before_failure)) => (
            StatusCode::BAD_REQUEST,
            Json(CreateNodeError { error: e, edges_created_before_failure }),
        )
            .into_response(),
    }
    })
    .await
}

async fn get_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    db.with_engine(move |engine| {
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
    })
    .await
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
    db.with_engine(move |engine| {
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
    })
    .await
}

async fn update_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<UpdateNodeRequest>,
) -> impl IntoResponse {
    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

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

    // Taken before the node moves into the engine. `kind` rides on the
    // event for the same reason it does on `node_created`: a subscriber
    // deciding whether an address is any of its business should not
    // have to spend a `GET` per event to find out, and on a delete
    // there is no longer anything to `GET`. A node's kind cannot change
    // through this route — `PUT` rewrites `data` and `visibility` only
    // — so the value announced is the value the node has.
    let kind = updated.kind.clone();

    match engine.insert(updated) {
        Ok(()) => {
            events.publish(
                audience,
                serde_json::json!({"event": "node_updated", "address": address, "kind": kind}),
            );
            (StatusCode::OK, "Node updated").into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
    })
    .await
}

async fn delete_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

    let existing = match engine.get(&address) {
        Ok(Some(n)) => n,
        Ok(None) => return (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(e) => return storage_failure(e),
    };

    if !identity.is_admin() && !existing.can_write(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to delete this node").into_response();
    }

    let audience = Audience::for_node(&existing);

    // Read off the node while it still exists: after the delete there
    // is no way for a subscriber to learn what kind the address was,
    // which is exactly when knowing it matters most.
    let kind = existing.kind.clone();

    match engine.delete(&address) {
        Ok(()) => {
            events.publish(
                audience,
                serde_json::json!({"event": "node_deleted", "address": address, "kind": kind}),
            );
            (StatusCode::NO_CONTENT, "").into_response()
        }

        // A delete can now be refused for reasons that are the caller's
        // to act on rather than the server's to apologise for: something
        // still references this node, or the cascade is too large to
        // stage atomically. Reporting those as 500 would tell a client
        // to retry an operation that will never succeed until it changes
        // something.
        Err(TransactionError::Precondition(e)) => {
            (StatusCode::CONFLICT, e).into_response()
        }

        Err(TransactionError::Invalid(e)) => {
            (StatusCode::BAD_REQUEST, e).into_response()
        }

        Err(TransactionError::Storage(e)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
    })
    .await
}

/// `POST /node/:address/claim` — take the lease on a node, once.
///
/// # Authorization
///
/// `can_write` on the node, or admin — the same rule `PUT` and `DELETE`
/// apply, because this is the same kind of act. A claim is not a read
/// with a flag: it sets `claimed_by`, archives the previous value to
/// history and appends a durable record, which is a mutation of somebody
/// else's row by every definition this engine uses.
///
/// This handler previously performed **no** authorization at all, and
/// the consequences were not subtle. A claim is claim-*once*: the only
/// way to clear `claimed_by` is an overwrite by the node's owner. So any
/// identity holding any token could walk another tenant's job queue and
/// lease every node in it — nodes it could not read, could not write and
/// could not delete — and the owner's workers would then find every job
/// permanently held by a stranger. The 404/409/200 split was also an
/// oracle over private addresses: "not found", "already claimed by X"
/// and "claimed" are three different answers about a node the caller is
/// refused on everywhere else.
///
/// The refusal is 403 and, for a node the caller cannot even read, 404 —
/// so an unreadable address cannot be distinguished from an absent one,
/// which is what closes the oracle. That is a *narrower* answer than the
/// 403/404 split `delete_node` makes, and deliberately so: `delete_node`
/// is reached with an address the caller is asserting it owns, while a
/// claim is the primitive a scanner would use.
async fn claim_node(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

    // Resolved under the same lock as the claim itself, so the audience
    // and the authorization both describe the node the claim actually
    // applies to — no window for another request to change ownership in
    // between.
    //
    // The kind travels with the audience for the same reason it does on
    // every other node event: a subscriber can attribute the address
    // without a lookup. It is `None` only on the path where the node
    // does not exist and the engine is about to say so.
    let (audience, kind) = match engine.get(&address) {
        Ok(Some(node)) => {
            if !identity.is_admin() && !node.can_write(&identity.owner) {
                // Unreadable and absent must look the same, or this
                // endpoint becomes a way to enumerate private
                // addresses.
                let status = if node.can_read(&identity.owner) {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::NOT_FOUND
                };

                return (
                    status,
                    if status == StatusCode::FORBIDDEN {
                        "not authorized to claim this node"
                    } else {
                        "node not found"
                    },
                )
                    .into_response();
            }

            (Audience::for_node(&node), Some(node.kind.clone()))
        }
        Ok(None) => (Audience::Owner(identity.owner.clone()), None),
        Err(e) => return storage_failure(e),
    };

    match engine.claim(&address, &identity.owner) {
        Ok(()) => {
            events.publish(
                audience,
                serde_json::json!({"event": "node_claimed", "address": address, "kind": kind, "worker": identity.owner}),
            );
            (StatusCode::OK, "claimed").into_response()
        }
        Err(ClaimError::NotFound) => (StatusCode::NOT_FOUND, "node not found").into_response(),
        Err(ClaimError::AlreadyClaimed(by)) => {
            (StatusCode::CONFLICT, format!("already claimed by {by}")).into_response()
        }
        Err(ClaimError::StorageError(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
    })
    .await
}

/// `GET /node/:address/owned` — every node held by the same owner as the
/// node at `:address`.
///
/// The route has always declared `:address` and the handler used to
/// ignore it, returning the caller's own nodes whatever address was
/// passed. That is not a stricter reading of the route, it is a
/// different endpoint wearing its name: a caller asking about one node
/// got an answer about themselves, and the parameter documented a
/// capability that did not exist.
///
/// The subject is therefore resolved from `:address`, and because the
/// answer can now be about somebody else, three rules apply that did not
/// have to before:
///
///   * reading the subject node needs read permission on it, so an
///     address the caller cannot see cannot be used to discover who owns
///     it;
///   * the listing is filtered to what the caller may read, so a private
///     node never appears in another identity's result; and
///   * an admin bypasses both, the same way it does everywhere else.
async fn list_owned(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    db.with_engine(move |engine| {

    let subject = match engine.get(&address) {
        Ok(Some(node)) => node,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "node not found").into_response()
        }
        Err(e) => return storage_failure(e),
    };

    if !identity.is_admin() && !subject.can_read(&identity.owner) {
        return (StatusCode::FORBIDDEN, "not authorized to read this node")
            .into_response();
    }

    // An admin filters by nothing, matching `query`'s superuser bypass.
    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    match engine.nodes_by_owner(&subject.owner, requester) {
        Ok(owned) => {
            let owned: Vec<Node> = owned;
            (StatusCode::OK, Json(owned)).into_response()
        }
        Err(e) => storage_failure(e),
    }
    })
    .await
}

async fn query_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    let offset = params.offset.unwrap_or(0);

    db.with_engine(move |engine| {
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
    })
    .await
}

/// The bound on a wire-supplied predicate, applied once at the edge.
///
/// `predicate::validate` is the rule; this is the one place it is
/// enforced, for both of the two ways an `Expr` can arrive from a
/// client — `POST /nodes/query` and the `delete_where` transaction op.
/// It belongs here rather than inside `predicate::eval` for a reason
/// that is the whole point of the bound: `eval` runs once per candidate
/// row, so checking there would re-walk the tree a hundred thousand
/// times to discover something knowable from the request body alone.
///
/// A refusal is a 400 because the request is what is wrong with it —
/// the same status the query path already returns for a predicate it
/// cannot push down — and the message names the limit so a caller can
/// tell "too complex" from "not supported".
fn reject_unbounded_predicate(where_: Option<&Expr>) -> Option<Response> {
    let expr = where_?;

    match crate::core::predicate::validate(expr) {
        Ok(()) => None,

        Err(why) => Some((StatusCode::BAD_REQUEST, why).into_response()),
    }
}

/// `POST /sequence/:name/next` — allocate identifiers.
///
/// The durable, race-free answer to "what id should this row have". An
/// application that instead asks the database for its largest existing id
/// has to read every row to find out, and two callers doing it at the
/// same moment get the same answer.
///
/// `count` above one takes a block in a single round trip, which is what
/// makes a bulk insert cheap: one durable write for a thousand ids rather
/// than a thousand.
async fn sequence_next(
    State(db): State<Arc<Database>>,
    Path(name): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<SequenceRequest>,
) -> impl IntoResponse {
    let count = payload.count;

    db.with_engine_mut(move |engine| {
        match engine.sequence_next(&name, count, &identity.owner, identity.is_admin()) {
            Ok(first) => {
                (StatusCode::OK, Json(SequenceResponse { first, count })).into_response()
            }
            Err(e) if e.starts_with("not authorized") => {
                (StatusCode::FORBIDDEN, e).into_response()
            }
            Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
        }
    })
    .await
}

/// `POST /nodes/multiget` — read many nodes by address in one request.
///
/// The batch form of `GET /node/:address`. A page of a feed needs the
/// viewer's own state for every row on it, and asking one row at a time
/// makes the round trip the cost of the page rather than the work.
///
/// Absent addresses and ones the caller may not read are both simply
/// missing from the reply rather than reported: telling a caller that an
/// address exists but is forbidden is the disclosure the visibility rule
/// exists to prevent. The reply preserves the order asked for, minus the
/// gaps, so a client can walk both lists together.
async fn multiget_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<MultiGetRequest>,
) -> impl IntoResponse {
    db.with_engine(move |engine| {
        let requester = if identity.is_admin() {
            None
        } else {
            Some(identity.owner.as_str())
        };

        match engine.multi_get(&payload.addresses, requester) {
            Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
        }
    })
    .await
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
    // Before the read lock is taken and before a single row is read:
    // an oversized predicate is refused for what it *is*, not for what
    // it would have cost.
    if let Some(refusal) = reject_unbounded_predicate(payload.where_.as_ref()) {
        return refusal;
    }

    let limit = payload.limit.unwrap_or(50).min(500);
    let offset = payload.offset.unwrap_or(0);

    db.with_engine(move |engine| {

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
    })
    .await
}

/// `POST /nodes/count` — how many nodes match, as a number.
///
/// Same selection and the same visibility rules as `POST /nodes/query`,
/// including the admin bypass, so a count and the query it summarises can
/// never disagree about which rows exist for this caller. It exists
/// because the alternative a caller is left with otherwise is asking for
/// one enormous page, or walking the cursor to the end — one round trip
/// per page to learn a single integer.
///
/// Classified as a `bulk` endpoint (see `api::limits`), alongside
/// `/nodes/query` and `/transaction`: its cost is set by how much data
/// the predicate has to be tested against, not by the size of the reply.
async fn count_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CountRequest>,
) -> impl IntoResponse {
    if let Some(refusal) = reject_unbounded_predicate(payload.where_.as_ref()) {
        return refusal;
    }

    db.with_engine(move |engine| {

    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    let result = engine.count_where(
        payload.kind.as_deref(),
        payload.owner.as_deref(),
        requester,
        payload.where_.as_ref(),
        &payload.item_var,
    );

    match result {
        Ok(count) => (StatusCode::OK, Json(CountResponse { count })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
    })
    .await
}

/// `POST /nodes/count_by` — how many nodes carry each distinct value of
/// one `data` field.
///
/// Exists so a caller rendering many rows can ask one question instead of
/// one per row: a feed showing like/reply/repost totals for twenty posts
/// is sixty counts under `/nodes/count` and three under this. Moving an
/// N+1 from SQL to HTTP does not stop it being an N+1.
///
/// Unlike `/nodes/count`, the reply is a result set whose size the data
/// chooses, so it IS bounded by `FACETQL_MAX_SCAN_ROWS` — a group-by over
/// a nearly-unique field is a request for the table with its rows
/// replaced by ones, and that refusal is the same one a query gets.
async fn count_nodes_by(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CountByRequest>,
) -> impl IntoResponse {
    if let Some(refusal) = reject_unbounded_predicate(payload.where_.as_ref()) {
        return refusal;
    }

    if payload.group_by.is_empty() {
        return (StatusCode::BAD_REQUEST, "group_by must name a field").into_response();
    }

    db.with_engine(move |engine| {

    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    let result = engine.count_by(
        payload.kind.as_deref(),
        payload.owner.as_deref(),
        requester,
        payload.where_.as_ref(),
        &payload.item_var,
        &payload.group_by,
        payload.values.as_deref(),
    );

    match result {
        Ok(counts) => (StatusCode::OK, Json(CountByResponse { counts })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
    })
    .await
}

/// `POST /nodes/aggregate` — `sum`, `avg`, `min`, `max` or `count` over
/// the rows a filter selects, as one value.
///
/// Same selection and the same visibility rules as `POST /nodes/query`,
/// including the admin bypass, so an aggregate and the query it
/// summarises can never disagree about which rows exist for this caller.
/// It exists for the reason `/nodes/count` does, one level up: without
/// it, "what do these orders total" is a page of rows crossing the wire
/// to produce a single number, and a wrong number as soon as the rows do
/// not fit in one page.
///
/// The function/field pair is checked before any row is read, so a `sum`
/// with no field is a 400 rather than a reply that aggregated nothing.
///
/// Classified as a `bulk` endpoint (see `api::limits`), alongside
/// `/nodes/count` and `/nodes/query`: its cost is set by how much data
/// the predicate has to be tested against, not by the size of the reply.
async fn aggregate_nodes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<AggregateRequest>,
) -> impl IntoResponse {
    if let Some(refusal) = reject_unbounded_predicate(payload.where_.as_ref()) {
        return refusal;
    }

    let spec = match AggFunc::parse(&payload.func)
        .and_then(|f| AggSpec::new(f, payload.field.clone()))
    {
        Ok(spec) => spec,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    db.with_engine(move |engine| {

    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    let result = engine.aggregate_where(
        payload.kind.as_deref(),
        payload.owner.as_deref(),
        requester,
        payload.where_.as_ref(),
        &payload.item_var,
        &spec,
    );

    match result {
        Ok(result) => {
            (StatusCode::OK, Json(AggregateResponse { result })).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
    })
    .await
}

/// `POST /nodes/aggregate_by` — one aggregate per distinct value of one
/// `data` field.
///
/// The grouped form of `/nodes/aggregate`, and it exists for the reason
/// `/nodes/count_by` does: a page showing a total per row is one grouped
/// request, not one request per row. Moving an N+1 from SQL to HTTP does
/// not stop it being an N+1.
///
/// Bounded by `FACETQL_MAX_SCAN_ROWS`, unlike the ungrouped form: the
/// reply is a result set whose size the data chooses.
async fn aggregate_nodes_by(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<AggregateByRequest>,
) -> impl IntoResponse {
    if let Some(refusal) = reject_unbounded_predicate(payload.where_.as_ref()) {
        return refusal;
    }

    if payload.group_by.is_empty() {
        return (StatusCode::BAD_REQUEST, "group_by must name a field").into_response();
    }

    let spec = match AggFunc::parse(&payload.func)
        .and_then(|f| AggSpec::new(f, payload.field.clone()))
    {
        Ok(spec) => spec,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    db.with_engine(move |engine| {

    let requester = if identity.is_admin() {
        None
    } else {
        Some(identity.owner.as_str())
    };

    let result = engine.aggregate_by(
        payload.kind.as_deref(),
        payload.owner.as_deref(),
        requester,
        payload.where_.as_ref(),
        &payload.item_var,
        &payload.group_by,
        payload.values.as_deref(),
        &spec,
    );

    match result {
        Ok(groups) => {
            (StatusCode::OK, Json(AggregateByResponse { groups })).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
    })
    .await
}

/// May this identity assert the edge `from -[kind]-> to`?
///
/// # The rule, and why it is this one
///
/// **`can_write` on `from`, `can_read` on `to`**, with admin bypassing
/// both. An edge is a claim *made by* one node about another, so the two
/// endpoints are not symmetric and must not be checked as if they were:
///
/// * Writing an edge out of a node changes what that node's adjacency
///   list says. `GET /node/:address/edges/out` reads it back, and an
///   application built on this graph reads it to decide what is true —
///   who follows whom, what belongs to what. Letting any identity append
///   to another owner's outgoing edges is letting it write that owner's
///   record. It is the same act `PUT /node/:address` refuses.
/// * Pointing an edge *at* a node does not modify the target, so
///   requiring write there would forbid the ordinary case this graph
///   exists for — following, liking, referencing somebody else's public
///   node. Read is the right bound: an edge whose target the caller
///   cannot see would otherwise be a way to ask "does this address
///   exist", one address at a time.
///
/// # What this closed
///
/// Neither endpoint was checked at all. Any authenticated identity could
/// create `alice:profile -[FOLLOWS]-> anyone`, and `EdgeId` deliberately
/// excludes the owner, so that edge is *the* copy of that fact: alice
/// could not create her own (`insert_edge` refuses an identity already
/// owned by someone else) and could not delete it (`Edge::can_write`
/// names the creator). One request forged a relationship on somebody
/// else's node and simultaneously made it permanent.
///
/// # Reporting
///
/// A node the caller cannot read is reported exactly as an absent one,
/// with the wording `insert_edge` already uses for a missing endpoint —
/// so this endpoint cannot be used to enumerate private addresses. A
/// node the caller *can* read but not write is a 403, because at that
/// point nothing is being revealed that a plain `GET` would not.
/// Returns the refusal, or `None` when the edge may be asserted — the
/// same shape [`reject_unbounded_predicate`] uses, so every "check, then
/// return the response the check produced" site in this file reads the
/// same way.
fn authorize_edge(
    engine: &crate::storage::engine::StorageEngine,
    identity: &AuthIdentity,
    from: &str,
    to: &str,
) -> Option<Response> {
    if identity.is_admin() {
        return None;
    }

    let missing = |end: &str, address: &str| {
        Some(
            (
                StatusCode::BAD_REQUEST,
                format!("edge '{end}' address not found: {address}"),
            )
                .into_response(),
        )
    };

    // `from`: write permission, because this appends to that node's
    // outgoing edges.
    match engine.get(from) {
        Err(e) => return Some(storage_failure(e)),

        // Absent is `insert_edge`'s own error; let it stand rather than
        // inventing a second wording for the same condition.
        Ok(None) => {}

        Ok(Some(node)) => {
            if !node.can_read(&identity.owner) {
                return missing("from", from);
            }

            if !node.can_write(&identity.owner) {
                return Some(
                    (
                        StatusCode::FORBIDDEN,
                        format!(
                            "not authorized to create an edge from {from}: \
                             an edge out of a node is a write to that node"
                        ),
                    )
                        .into_response(),
                );
            }
        }
    }

    // `to`: read permission only.
    match engine.get(to) {
        Err(e) => Some(storage_failure(e)),

        Ok(None) => None,

        Ok(Some(node)) if node.can_read(&identity.owner) => None,

        Ok(Some(_)) => missing("to", to),
    }
}

async fn create_edge(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(payload): Json<CreateEdgeRequest>,
) -> impl IntoResponse {
    let owner = identity.owner.clone();
    let edge = Edge::new(payload.from.clone(), payload.to.clone(), payload.kind.clone(), owner.clone());

    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

    // Under the same write lock the insert will use, so no other
    // request can change either endpoint's ownership between the check
    // and the write.
    if let Some(refusal) = authorize_edge(engine, &identity, &payload.from, &payload.to) {
        return refusal;
    }

    // An edge is only as public as the pair it connects: announcing
    // "a → b" to everyone reveals that both nodes exist and are related,
    // so a single private endpoint keeps the whole event owner-scoped.
    let endpoints_public = match public_endpoints(engine, &payload.from, &payload.to) {
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
            events.publish(
                audience,
                serde_json::json!({"event": "edge_created", "from": payload.from, "to": payload.to, "kind": payload.kind}),
            );
            (StatusCode::CREATED, "Edge created").into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
    })
    .await
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

    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

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
    let endpoints_public = match public_endpoints(engine, &payload.from, &payload.to) {
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
            events.publish(
                audience,
                serde_json::json!({"event": "edge_deleted", "from": payload.from, "to": payload.to, "kind": payload.kind}),
            );
            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
    })
    .await
}

async fn get_edges_out(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    db.with_engine(move |engine| {
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
    })
    .await
}

// ── transactions ────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TxOpRequest {
    /// Create or overwrite a node.
    ///
    /// # `owner` and `claimed_by` are admin-only, and refused rather
    /// than ignored
    ///
    /// Without them this op stamps the *writing* identity as the owner
    /// and cannot express a claim at all, which makes an operator-run
    /// migration impossible: copying a cell of nodes from one FacetQL
    /// instance to another with a credential that is not already every
    /// node's owner would silently re-own the data and drop every
    /// lease. Carrying the two fields is what makes such a copy
    /// faithful.
    ///
    /// They are exactly as dangerous as they are useful, so they are
    /// **admin-only**: a non-admin that names either one has its whole
    /// transaction refused with 403, and nothing is applied. Ignoring
    /// the fields instead would be the worse failure — the write
    /// succeeds, the caller is told nothing, and the node is owned by
    /// somebody other than the one the request named.
    ///
    /// An admin gains no authority it lacked: it can already read,
    /// overwrite and delete every node, and `POST /node/:address/claim`
    /// already lets it set `claimed_by` (to itself). What is new is
    /// naming *which* owner, which is the whole requirement. A
    /// non-admin's node is still stamped with the non-admin's own
    /// identity, exactly as before, and the engine still refuses an
    /// overwrite whose owner differs from the stored one — so this adds
    /// no path to taking over an address.
    InsertNode {
        address: String,
        kind: String,
        x: u8,
        y: u8,
        z: u8,
        q: u8,
        data: String,
        public: Option<bool>,
        /// Admin-only. Absent means the writer, as always.
        owner: Option<String>,
        /// Admin-only. Absent means unclaimed, as always. `Some` sets
        /// the lease holder; there is deliberately no way to say
        /// "explicitly unclaimed" distinctly from "absent", because a
        /// fresh node is unclaimed either way.
        claimed_by: Option<String>,
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
    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {

    let mut ops = Vec::with_capacity(payload.operations.len());
    let mut touched_addresses = Vec::new();

    for op in payload.operations {
        match op {
            TxOpRequest::InsertNode {
                address, kind, x, y, z, q, data, public, owner, claimed_by,
            } => {
                // Refused, not ignored. A body that asks to write
                // somebody else's node must not be answered with a
                // success that quietly wrote the caller's own — the
                // caller would believe the migration faithful and it
                // would not be. Checked before anything is staged, so
                // the whole batch is refused with nothing applied.
                if !identity.is_admin() && (owner.is_some() || claimed_by.is_some()) {
                    return (
                        StatusCode::FORBIDDEN,
                        format!(
                            "not authorized to set owner or claimed_by on \
                             {address}: those fields are admin-only, \
                             because they create a node owned by or leased \
                             to somebody other than the writer. Omit them \
                             and the node is owned by you."
                        ),
                    )
                        .into_response();
                }

                let coordinate = Coordinate::new(x, y, z, q);

                // `unwrap_or` and not `unwrap_or_else`-with-a-check: by
                // the time control reaches here, `owner` is `Some` only
                // for an admin.
                let node_owner = owner.unwrap_or_else(|| identity.owner.clone());

                let mut node = Node::new(coordinate, address.clone(), kind, node_owner);
                node.data = data;
                node.claimed_by = claimed_by;
                if public.unwrap_or(false) {
                    node.visibility = Visibility::Public;
                }
                touched_addresses.push(address);
                ops.push(TxOperation::InsertNode(node));
            }
            TxOpRequest::InsertEdge { from, to, kind } => {
                // Same rule as `POST /edge` — can_write on `from`,
                // can_read on `to` — resolved through the same helper so
                // a batch cannot assert an edge a single request would
                // have been refused. Checked under the write lock this
                // handler already holds.
                if let Some(refusal) = authorize_edge(engine, &identity, &from, &to) {
                    return refusal;
                }

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
                // The same bound `POST /nodes/query` applies, through
                // the same helper: `delete_where` runs the same
                // evaluator over the same candidate set, so a predicate
                // too expensive to answer is exactly as expensive to
                // delete by — and this one holds the *write* lock while
                // it does it.
                if let Some(refusal) = reject_unbounded_predicate(where_.as_ref()) {
                    return refusal;
                }

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
            // A batch is one identity's writes, and its address list can
            // name private nodes, so it is announced to that identity
            // (and admins) rather than broadcast. Public fan-out is what
            // POST /publish is for — an explicit choice, not a side
            // effect of writing.
            events.publish(
                Audience::Owner(identity.owner.clone()),
                serde_json::json!({"event": "transaction_committed", "addresses": touched_addresses}),
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
    })
    .await
}

async fn get_edges_in(
    State(db): State<Arc<Database>>,
    Path(address): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    db.with_engine(move |engine| {
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
    })
    .await
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

    db.publish_opaque(audience, payload.payload);
    (StatusCode::OK, "published").into_response()
}

/// Query for `GET /events`.
#[derive(Deserialize)]
pub struct EventsQuery {
    /// Resume: deliver every retained event whose `seq` is strictly
    /// greater than this, then continue live. Omitted means "start at
    /// the live edge", which is what this endpoint has always done.
    ///
    /// A value the server can no longer honour is a **410**, never a
    /// silent start at the live edge — see [`Database::feed`] and
    /// [`crate::database::ResumeTooOld`].
    pub after: Option<u64>,
}

/// One SSE frame for a feed event.
///
/// The position goes in the frame's `id:` field as well as in the
/// payload, because `id:` is where SSE puts a resume token: a client
/// using an off-the-shelf EventSource gets `lastEventId` maintained for
/// it, and one parsing the `data` line finds `seq` there too.
fn event_frame(event: LiveEvent) -> Event {
    Event::default().id(event.seq.to_string()).data(event.payload)
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
///
/// # Falling behind is an event, not silence
///
/// The broadcast this reads from drops messages for a receiver that
/// cannot keep up, and reports that as `Lagged(n)`. This handler used
/// to map that to the same `None` as "this event is not for you", so a
/// subscriber that had just lost a hundred writes saw exactly what a
/// subscriber with nothing to receive sees: nothing. A total operation
/// with no way to say "I missed something" is not a feed, and a
/// consumer built on one cannot be correct — it believes it has seen
/// everything, always.
///
/// So a lag is now its own frame, `{"event":"feed_lagged","dropped":n}`,
/// and it deliberately carries **no `id:`**: the subscriber's resume
/// point is the last event it actually received, and advancing it over
/// the gap is precisely the lie this frame exists to prevent. The
/// stream continues after it — the subscriber decides whether to refill
/// with `?after=<its last seq>` or to reconcile from a full read.
///
/// Note that positions are not contiguous (see [`LiveEvent::seq`]) and
/// an audience filter removes more of them, so a gap is never
/// detectable by arithmetic on the numbers. This frame is the only
/// signal, which is why it is explicit.
async fn subscribe_events(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(query): Query<EventsQuery>,
) -> Response {
    // A subscriber is the one caller that is *supposed* to never
    // finish, which is exactly why none of the request-shaped bounds
    // reach it: it is past the body limit at once, it must not have a
    // deadline, and its concurrency permit is released as soon as this
    // function returns — while the connection it opened lives on inside
    // the stream below, holding a broadcast receiver that pins the
    // ring's messages whenever it falls behind.
    //
    // So the permit is acquired here and *moved into the stream*, where
    // it is dropped when the client goes away. Holding it in this
    // function instead would bound nothing at all.
    let permit = match limits::subscriber_permit() {
        Some(permit) => permit,

        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "too many live subscribers; retry shortly",
            )
                .into_response();
        }
    };

    let owner = identity.owner.clone();
    let is_admin = identity.is_admin();

    // The backlog and the live receiver are taken together, under the
    // feed's lock, so nothing can be published between the snapshot and
    // the subscription — the one gap a resume exists to close, and the
    // one that would be invisible if it opened.
    let (backlog, rx) = match db.feed.subscribe(query.after) {
        Ok(opened) => opened,

        // 410 Gone, and not 400: the request is well-formed and was
        // valid when the caller minted the position — the resource it
        // names has simply aged out. That is the difference between
        // "fix your client" and "you have a hole; go reconcile", and
        // the body says which positions are still available so the
        // caller can tell how big the hole is.
        Err(too_old) => {
            return (StatusCode::GONE, too_old.to_string()).into_response();
        }
    };

    // The same audience rule as the live half, applied to the replayed
    // half. A resume must not become the way to read events a live
    // subscription would have filtered out.
    let replay: Vec<Result<Event, Infallible>> = backlog
        .into_iter()
        .filter(|event| event.audience.admits(&owner, is_admin))
        .map(|event| Ok(event_frame(event)))
        .collect();

    let live = BroadcastStream::new(rx).filter_map(move |result| {
        // Named rather than `_`: the closure owns the permit for the
        // life of the stream, and that ownership is the whole bound.
        let _subscriber_slot = &permit;

        match result {
            Ok(event) if event.audience.admits(&owner, is_admin) => {
                Some(Ok::<Event, Infallible>(event_frame(event)))
            }

            // Not for this subscriber. Nothing was lost.
            Ok(_) => None,

            // Something *was* lost. Say so, in band, and keep going.
            Err(BroadcastStreamRecvError::Lagged(dropped)) => {
                Some(Ok(Event::default().data(
                    serde_json::json!({
                        "event": "feed_lagged",
                        "dropped": dropped,
                    })
                    .to_string(),
                )))
            }
        }
    });

    Sse::new(tokio_stream::iter(replay).chain(live))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Query for `GET /changes`.
#[derive(Deserialize)]
pub struct ChangesQuery {
    /// Return committed changes whose position is strictly greater than
    /// this. Omitted means "from the beginning of the log".
    ///
    /// A position the log can no longer serve is a **410**, never a page
    /// that quietly begins at the oldest record still present — see
    /// [`crate::storage::changes::ScanTooOld`].
    pub after: Option<u64>,

    /// Changes per page. Clamped to
    /// [`MAX_CHANGE_LIMIT`]; a page may still overrun it by the tail of
    /// one transaction frame, because a durable unit is never split.
    pub limit: Option<usize>,
}

/// `GET /changes` — the durable, WAL-backed change scan.
///
/// # What this is, and what it is not
///
/// `GET /events` is the live push feed and stays exactly that: an
/// in-memory ring, a position on every frame, and a 410 for a resume it
/// can no longer honour. This is the pull half. It reconstructs
/// committed node mutations out of the write-ahead log, so a consumer
/// that outran the ring can catch up without re-reading the whole
/// database — which is the difference between "migrating a busy cell
/// fails cleanly" and "migrating a busy cell works".
///
/// It is not a bigger ring, and it is deliberately not the same feed:
/// different bytes, different retention, its own refusal. Nothing about
/// `/events` changes.
///
/// # The bridge to `/events`, and the one race in it
///
/// Both channels number their positions from the same
/// [`crate::storage::wal::next_operation_id`] counter, so a position
/// from one is comparable with a position from the other. That is what
/// makes a hand-off possible, and the direction it works in is
/// **subscribe first, then scan**:
///
/// ```text
///     GET /events                 open the live subscription
///     GET /changes?after=<seq>    scan durably up to `next`
///     ... paging until complete   the two overlap; duplicates only
/// ```
///
/// *Scanning first and subscribing afterwards is the direction that has
/// a hole in it*, and it is worth being exact about where. A scan
/// returns `next`; `GET /events?after=<next>` then replays from the live
/// ring — but the ring retains only
/// [`crate::database::EVENT_REPLAY_CAPACITY`] events, so if enough was
/// published between the scan and the subscribe, `next` has already
/// aged out and the subscribe answers **410**. The refusal is honest —
/// no writes are lost silently — but the hand-off has failed, and on a
/// busy instance it can fail repeatedly. Subscribing first removes the
/// window entirely, because the subscription is already live while the
/// scan runs.
///
/// In that order there is **no gap**, and the argument is short: an
/// event's `seq` is minted when the handler publishes, which is always
/// *after* the WAL records of the mutation it describes were stamped. So
/// for any mutation the scan did not return, its WAL positions are above
/// `next`, and therefore its event position is above `next` too — the
/// live subscription delivers it. The reverse overlap does happen: a
/// mutation whose records the scan returned may also arrive live,
/// because its event position can be minted after another writer's. That
/// is a **duplicate, never a gap**, and a consumer that reconciles by
/// address (`POST /nodes/multiget`) absorbs it for free.
///
/// # What is reported, and what deliberately is not
///
/// Node changes only: `created`, `updated`, `deleted`, each carrying the
/// address and the kind — enough to attribute the address to a cell and
/// re-fetch it with `POST /nodes/multiget`, which is precisely what the
/// consumer does with it. The node body is *not* carried: it would be a
/// second copy of the data path, with its own visibility rules to get
/// wrong, for a field nobody reads out of a change feed.
///
/// Edge, user, index, reference and text-index mutations are in the log
/// and are **not** reported here. That is the honest subset rather than
/// an oversight, and the reason is per-kind:
///
///   * an **edge** is only as readable as the pair it connects, so its
///     audience is a function of two nodes, and a delete record carries
///     an `EdgeId` with no archived state to recover either of them
///     from — there is nothing in the log to authorize it against. It
///     is also moot for the one consumer: a cell copy refuses outright
///     when the cell has edges, because a cross-cell edge cannot be
///     rebuilt on the destination at all.
///   * **users** are credentials. A feed that announced them would be a
///     way for any authenticated token to enumerate identities that
///     `GET /admin/users` refuses it.
///   * **indexes, references and text indexes** are schema, not data.
///     They are declared by admins through `/admin/*` and a mover does
///     not reconcile them row by row.
///
/// A consumer that needs any of those still has `/events`, live.
///
/// # Response
///
/// ```text
///     { "changes": [ {"seq":41,"change":"created","address":"a","kind":"K"} ],
///       "next": 41,
///       "complete": true }
/// ```
///
/// `complete` is true when the scan reached the end of the settled log:
/// there is nothing more for this caller right now, and `next` is the
/// position to continue from. False means the page filled — call again
/// from `next`. `next` is the position of the last change **this caller
/// was shown**, never the newest record in the log, so the reply
/// discloses nothing about writes it may not read.
///
/// # Cost
///
/// A bulk endpoint. It reads the log rather than the heap, takes no
/// lock, and cannot stall a writer — see
/// [`crate::storage::changes::scan`] for exactly which locks the WAL
/// read path does and does not need. On the blocking pool, because the
/// work is I/O plus one AES-GCM decryption per record.
async fn scan_changes(
    Extension(identity): Extension<AuthIdentity>,
    Query(query): Query<ChangesQuery>,
) -> Response {
    let after = query.after.unwrap_or(0);

    let limit = query
        .limit
        .unwrap_or(DEFAULT_CHANGE_LIMIT)
        .clamp(1, MAX_CHANGE_LIMIT);

    let owner = identity.owner.clone();
    let is_admin = identity.is_admin();

    let scanned = tokio::task::spawn_blocking(move || {
        crate::storage::changes::scan(after, limit, &owner, is_admin)
    })
    .await;

    let scanned = match scanned {
        Ok(scanned) => scanned,

        // The scan touches no engine state and holds no lock, so a panic
        // here is not the poisoned-engine case `Database::with_engine`
        // exits on: nothing is left inconsistent, and one failed request
        // is the whole blast radius.
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "the change scan failed",
            )
                .into_response();
        }
    };

    match scanned {
        Ok(page) => (StatusCode::OK, Json(page)).into_response(),

        // 410 Gone, and not 400, for the same reason `/events` answers a
        // stale resume that way: the request is well formed and was
        // valid when the caller minted the position — the log has simply
        // been checkpointed past it. "You have a hole; go reconcile", not
        // "fix your client".
        Err(ScanError::TooOld(too_old)) => {
            (StatusCode::GONE, too_old.to_string()).into_response()
        }

        Err(ScanError::Log(error)) => storage_failure(error),
    }
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
    db.with_engine(move |engine| {
    match engine.stats() {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => storage_failure(e),
    }
    })
    .await
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

    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {
    match engine.insert_user(record) {
        Ok(()) => {
            // Account lifecycle is admin business: only an admin can
            // reach this route, and the event names a new identity, so
            // it stays inside the admin audience.
            events.publish(
                Audience::Owner(identity.owner.clone()),
                serde_json::json!({"event": "user_created", "owner": payload.owner, "created_by": identity.owner}),
            );
            (StatusCode::CREATED, Json(CreateUserResponse { owner: payload.owner, role, token })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
    })
    .await
}

async fn list_users(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }
    db.with_engine(move |engine| {
    let users: Vec<UserSummary> = engine
        .list_users()
        .into_iter()
        .map(|u| UserSummary { owner: u.owner.clone(), role: u.role })
        .collect();
    (StatusCode::OK, Json(users)).into_response()
    })
    .await
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

    let events = std::sync::Arc::clone(&db);
    db.with_engine_mut(move |engine| {
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
    // Account lifecycle is admin business; only admins created or
    // revoked it, and only admins should hear about it.
    events.publish(
        Audience::Owner(identity.owner.clone()),
        serde_json::json!({"event": "user_revoked", "owner": owner}),
    );
    (StatusCode::NO_CONTENT, "").into_response()
    })
    .await
}

// ---------------------------------------------------------------------
// Index administration
// ---------------------------------------------------------------------
//
// Admin-only, and on the control plane (`/admin/*`) rather than beside
// the data endpoints, because declaring an index is an operational
// decision about how the database is shaped — not something an
// application makes on its own behalf mid-request. It is also the one
// write whose cost is proportional to the data already stored, which is
// not a cost an ordinary caller should be able to impose.

#[derive(Deserialize)]
struct CreateIndexRequest {
    name: String,
    kind: String,
    field: String,

    /// Declare the index unique: a write that would give two nodes of
    /// this kind the same value for this field is refused.
    ///
    /// Optional and false by default, so an existing caller declaring an
    /// ordinary index is unchanged.
    #[serde(default)]
    unique: bool,

    /// Which question the index is being declared to answer.
    ///
    /// * `"ordered"` (the default) — a B+tree over the field's whole
    ///   value: point lookups, prefixes, ranges, `order by`.
    /// * `"text"` — an inverted index over the field's text: `contains`,
    ///   `starts_with` and `ends_with` served from trigram postings
    ///   instead of a scan of the kind.
    ///
    /// One field can carry both, under two names, because they answer
    /// different questions and neither subsumes the other.
    ///
    /// Optional, so a client that predates it declares exactly the index
    /// it always declared. The value is matched case-insensitively and
    /// anything else is a 400 rather than a silent fall-back to
    /// `ordered` — declaring the wrong kind of index is a mistake an
    /// operator wants told, not absorbed.
    #[serde(default)]
    mode: Option<String>,
}

/// Declare an index over one `data` field of one kind.
///
/// Returns 201 with the definition. Re-declaring the identical index
/// succeeds — the operation an operator actually wants is "make sure
/// this exists", and failing the second run of a setup script is not
/// that. A *different* index under an existing name, or a second index
/// over the same field, is a 409: those are contradictions, not repeats.
async fn create_index(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(request): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    let mode = request.mode.unwrap_or_else(|| "ordered".to_string());

    if mode.eq_ignore_ascii_case("text") {
        if request.unique {
            return (
                StatusCode::BAD_REQUEST,
                "a text index cannot be unique: it stores windows of a value, \
                 not the value, so it has nothing to hold unique. Declare an \
                 ordered unique index over the same field if that is what you \
                 want.",
            )
                .into_response();
        }

        let def = TextIndexDef {
            name: request.name,
            kind: request.kind,
            field: request.field,
        };

        return db
            .with_engine_mut(move |engine| {
                match engine.create_text_index(def.clone()) {
                    Ok(()) => {}

                    Err(e) if e.contains("already") => {
                        return (StatusCode::CONFLICT, e).into_response();
                    }

                    Err(e) => {
                        return (StatusCode::BAD_REQUEST, e).into_response();
                    }
                }

                (StatusCode::CREATED, Json(IndexInfo::text(&def))).into_response()
            })
            .await;
    }

    if !mode.eq_ignore_ascii_case("ordered") {
        return (
            StatusCode::BAD_REQUEST,
            format!("unknown index mode {mode:?}; expected \"ordered\" or \"text\""),
        )
            .into_response();
    }

    let def = IndexDef {
        name: request.name,
        kind: request.kind,
        field: request.field,
        unique: request.unique,
    };

    db.with_engine_mut(move |engine| {
    match engine.create_index(def.clone()) {
        Ok(()) => {}

        Err(e) if e.contains("already") => {
            return (StatusCode::CONFLICT, e).into_response();
        }

        Err(e) => {
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    }

    (StatusCode::CREATED, Json(IndexInfo::ordered(&def))).into_response()
    })
    .await
}

/// Every declared index, ordered and inverted alike, in one list with a
/// `mode` column saying which is which.
///
/// One list rather than two endpoints because an operator reading it is
/// asking one question — is the field I care about covered, and covered
/// how — and two lists is the answer to a different one. Additive on the
/// wire: the rows a client already knew about still carry the fields
/// they always carried.
///
/// Admin-only for the same reason the definitions are: the shape of the
/// access paths is operational detail, and an application that had to
/// know about them would be an application coupled to them.
async fn list_indexes(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    db.with_engine(move |engine| {

    (StatusCode::OK, Json(engine.list_all_indexes())).into_response()
    })
    .await
}

/// Drop a declared index.
///
/// Queries that were being served by it keep working — they fall back to
/// the materialize-and-sort path, which is slower and bounded by
/// `FACETQL_MAX_SCAN_ROWS`, not wrong.
async fn drop_index(
    State(db): State<Arc<Database>>,
    Path(name): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    db.with_engine_mut(move |engine| {

    match engine.drop_index(&name) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e).into_response(),
    }
    })
    .await
}

// ---------------------------------------------------------------------
// Reference administration
// ---------------------------------------------------------------------
//
// Admin-only and on the control plane for the same reasons the index
// endpoints are, plus one of its own: a reference decides what a delete
// *does*. An application that could declare one on its own behalf could
// arrange for another owner's nodes to be removed by deleting its own.

#[derive(Deserialize)]
struct CreateReferenceRequest {
    name: String,

    /// The kind holding the reference, and the `data` field on it that
    /// carries the referenced node's key.
    kind: String,
    field: String,

    /// The kind being referenced.
    parent_kind: String,

    /// Which value on the referenced node the field matches. Omitted —
    /// the common case — means its address.
    #[serde(default)]
    parent_field: Option<String>,

    /// What deleting the referenced node does: `cascade`, `restrict` or
    /// `set_null`. No default: this is the decision the declaration
    /// exists to record, and guessing it would guess whether a delete
    /// removes rows.
    on_delete: ReferentialAction,
}

/// Declare a reference between two kinds.
///
/// Returns 201 with the definition. Re-declaring the identical
/// reference succeeds, for the reason re-declaring an index does. A
/// different definition under an existing name is a 409; a definition
/// the access paths or the existing data cannot support is a 400 that
/// names what is missing.
async fn create_reference(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(request): Json<CreateReferenceRequest>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    let def = ReferenceDef {
        name: request.name,
        kind: request.kind,
        field: request.field,
        parent_kind: request.parent_kind,
        parent_field: request.parent_field,
        on_delete: request.on_delete,
    };

    db.with_engine_mut(move |engine| {
    match engine.create_reference(def.clone()) {
        Ok(()) => {}

        Err(e) if e.contains("already exists") => {
            return (StatusCode::CONFLICT, e).into_response();
        }

        Err(e) => {
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    }

    (StatusCode::CREATED, Json(def)).into_response()
    })
    .await
}

/// Every declared reference.
async fn list_references(
    State(db): State<Arc<Database>>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    db.with_engine(move |engine| {

    (StatusCode::OK, Json(engine.list_references())).into_response()
    })
    .await
}

/// Drop a declared reference.
///
/// The nodes it governed are untouched. What stops is the enforcement,
/// so a later delete of a referenced node leaves the nodes that
/// referenced it behind.
async fn drop_reference(
    State(db): State<Arc<Database>>,
    Path(name): Path<String>,
    Extension(identity): Extension<AuthIdentity>,
) -> impl IntoResponse {
    if !identity.is_admin() {
        return (StatusCode::FORBIDDEN, "admin only").into_response();
    }

    db.with_engine_mut(move |engine| {

    match engine.drop_reference(&name) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e).into_response(),
    }
    })
    .await
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

        engine.seed_user(UserRecord { token_hash: hash_token(ADMIN_TOKEN), owner: "admin".to_string(), role: Role::Admin });
        engine.seed_user(UserRecord { token_hash: hash_token(USER_TOKEN), owner: "bob".to_string(), role: Role::User });

        create_router(Arc::new(Database::attach(engine)))
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

    /// Drive `GET /stats` as the admin and parse the body.
    async fn stats_json(router: axum::Router) -> serde_json::Value {
        let resp = router
            .oneshot(stats_request(ADMIN_TOKEN))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");

        serde_json::from_slice(&bytes).expect("valid JSON")
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

    /// The operational half of the response: the fields a control plane
    /// needs to tell a busy instance from an idle one, which the counts
    /// above cannot express at all.
    ///
    /// A real request is driven through the router first, because the
    /// request classifier depends on axum handing the middleware a
    /// `MatchedPath`. If it ever stopped doing so, every request would
    /// fall into `unclassified`, the latency histograms would stay empty
    /// and `/stats` would report a permanently idle server — a silent
    /// failure of the whole measurement, with no error anywhere. So it
    /// is asserted end to end rather than assumed.
    #[tokio::test]
    async fn stats_report_version_and_operational_metrics() {
        let _guard = disk_guard();

        // One router — one engine — throughout: the per-cell table is
        // engine state, so a fresh engine per request would report on a
        // database that never served the request.
        let app = router();

        let reads_before = stats_json(app.clone()).await["runtime"]["requests"]["read"]
            .as_u64()
            .expect("requests.read");

        let request = Request::builder()
            .method("GET")
            .uri("/node/stats:p1")
            .header("x-api-key", ADMIN_TOKEN)
            .body(Body::empty())
            .expect("build request");

        let resp = app.clone().oneshot(request).await.expect("router response");
        assert_eq!(resp.status(), StatusCode::OK);

        let json = stats_json(app).await;

        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));

        for field in ["uptime_seconds", "requests", "window", "process"] {
            assert!(
                !json["runtime"][field].is_null(),
                "runtime.{field} missing from /stats",
            );
        }

        assert!(
            json["runtime"]["requests"]["read"].as_u64().expect("requests.read")
                > reads_before,
            "a GET /node/:address must be counted as a read",
        );

        // Every route this router declares is named in the classifier,
        // so anything landing here means one was added without a line in
        // `metrics::classify` and the read/write split is missing it.
        assert_eq!(
            json["runtime"]["requests"]["unclassified"], 0,
            "a route reached the server that the classifier does not name",
        );

        // The per-cell block always states its own bound and its own
        // overflow, so a consumer can tell a complete attribution from a
        // partial one instead of assuming.
        assert!(json["cells"]["capacity"].as_u64().expect("capacity") > 0);
        assert!(!json["cells"]["overflow_reads"].is_null());
        assert!(!json["cells"]["unattributed_writes"].is_null());

        // Inserting three nodes at the origin attributed three writes to
        // it — per-cell attribution is the whole reason a placeable unit
        // can be smaller than an instance.
        let origin = json["cells"]["cells"]
            .as_array()
            .expect("cells array")
            .iter()
            .find(|cell| {
                cell["x"] == 0 && cell["y"] == 0 && cell["z"] == 0 && cell["q"] == 0
            })
            .expect("the origin cell the fixture writes to");

        assert!(
            origin["writes"].as_u64().expect("writes") >= 3,
            "three fixture inserts at the origin, got {}",
            origin["writes"],
        );

        assert!(
            origin["reads"].as_u64().expect("reads") >= 1,
            "the GET above read a record at the origin",
        );
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

#[cfg(test)]
mod write_authorization_tests {
    //! Regression tests for two holes in the node endpoints, both found
    //! by reading the routes against the handlers rather than by a
    //! failing request:
    //!
    //!   * `POST /node` overwrote a node belonging to another identity
    //!     and reassigned ownership to the caller, while `PUT`, `DELETE`
    //!     and `insert_node` inside a transaction all refused exactly
    //!     that write.
    //!   * `GET /node/:address/owned` declared a path parameter and
    //!     ignored it, answering about the caller instead of about the
    //!     address — so the parameter promised a capability that did not
    //!     exist, and the listing had never needed a visibility filter.
    //!
    //! Driven through the real router so the auth middleware, the status
    //! codes and the JSON shape are all part of what is asserted.
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::RwLock;
    use tower::ServiceExt;

    const ALICE_TOKEN: &str = "authz-alice-token";
    const BOB_TOKEN: &str = "authz-bob-token";
    const ADMIN_TOKEN: &str = "authz-admin-token";

    const KIND: &str = "AuthzNode";

    fn owned_node(address: &str, owner: &str, public: bool) -> Node {
        let mut node = Node::new(
            Coordinate::new(0, 0, 0, 0),
            address.to_string(),
            KIND.to_string(),
            owner.to_string(),
        );

        if public {
            node.visibility = Visibility::Public;
        }

        node
    }

    /// Alice owns a public node, a private node, and nothing else.
    fn router() -> axum::Router {
        let mut engine = StorageEngine::open().expect("open storage engine");

        for (address, owner, public) in [
            ("authz:alice-public", "alice", true),
            ("authz:alice-private", "alice", false),
            ("authz:bob-public", "bob", true),
        ] {
            engine
                .insert(owned_node(address, owner, public))
                .expect("insert node");
        }

        for (token, owner, role) in [
            (ALICE_TOKEN, "alice", Role::User),
            (BOB_TOKEN, "bob", Role::User),
            (ADMIN_TOKEN, "authz-admin", Role::Admin),
        ] {
            engine.seed_user(UserRecord {
                    token_hash: hash_token(token),
                    owner: owner.to_string(),
                    role,
                });
        }

        create_router(Arc::new(Database::attach(engine)))
    }

    fn post_node(token: &str, address: &str) -> Request<Body> {
        let body = serde_json::json!({
            "address": address,
            "kind": KIND,
            "x": 0, "y": 0, "z": 0, "q": 0,
            "data": r#"{"written_by":"the caller"}"#,
        });

        Request::builder()
            .method("POST")
            .uri("/node")
            .header("x-api-key", token)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("build request")
    }

    fn get(token: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    async fn json_body(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");

        serde_json::from_slice(&bytes).expect("valid JSON")
    }

    // -----------------------------------------------------------------
    // POST /node
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn post_node_cannot_overwrite_another_owners_node() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(post_node(BOB_TOKEN, "authz:alice-public"))
            .await
            .expect("router response");

        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "a cross-owner overwrite must be refused, not accepted"
        );
    }

    /// The refusal has to leave the node exactly as it was — owner
    /// included. A check that rejects the response but not the write
    /// would be worse than none, because it would look safe.
    #[tokio::test]
    async fn a_refused_overwrite_changes_nothing() {
        let _guard = disk_guard();

        let app = router();

        app.clone()
            .oneshot(post_node(BOB_TOKEN, "authz:alice-public"))
            .await
            .expect("router response");

        let resp = app
            .oneshot(get(ALICE_TOKEN, "/node/authz:alice-public"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::OK);

        let node = json_body(resp).await;

        assert_eq!(node["owner"], "alice", "ownership was reassigned");
        assert_ne!(
            node["data"].as_str().unwrap_or_default(),
            r#"{"written_by":"the caller"}"#,
            "the refused write landed anyway"
        );
    }

    /// The owner overwriting their own node is the ordinary upsert and
    /// must keep working — the fix must not turn `POST` into
    /// create-only.
    #[tokio::test]
    async fn an_owner_may_still_overwrite_their_own_node() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(post_node(ALICE_TOKEN, "authz:alice-public"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    /// Creating a brand-new address is unaffected: there is nothing to
    /// authorize against.
    #[tokio::test]
    async fn creating_a_fresh_address_is_unaffected() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(post_node(BOB_TOKEN, "authz:brand-new"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    /// An admin overwrites anything, the same way it bypasses
    /// visibility on every read path.
    #[tokio::test]
    async fn an_admin_may_overwrite_any_node() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(post_node(ADMIN_TOKEN, "authz:alice-public"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // -----------------------------------------------------------------
    // GET /node/:address/owned
    // -----------------------------------------------------------------

    /// The listing is now about the address, and filtered to what the
    /// caller may read: Bob asking about Alice's public node sees
    /// Alice's public nodes and none of her private ones.
    #[tokio::test]
    async fn owned_answers_about_the_address_without_leaking_private_nodes() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(get(BOB_TOKEN, "/node/authz:alice-public/owned"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::OK);

        let addresses: Vec<String> = json_body(resp)
            .await
            .as_array()
            .expect("an array")
            .iter()
            .map(|n| n["address"].as_str().unwrap_or_default().to_string())
            .collect();

        assert!(
            addresses.contains(&"authz:alice-public".to_string()),
            "the subject's own public node is missing: {addresses:?}"
        );

        assert!(
            !addresses.contains(&"authz:alice-private".to_string()),
            "a private node leaked to another identity: {addresses:?}"
        );

        assert!(
            !addresses.contains(&"authz:bob-public".to_string()),
            "the caller's own nodes are not what was asked for: {addresses:?}"
        );
    }

    /// The owner asking about their own node still sees everything they
    /// own, private included.
    #[tokio::test]
    async fn an_owner_sees_their_own_private_nodes() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(get(ALICE_TOKEN, "/node/authz:alice-private/owned"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::OK);

        let addresses: Vec<String> = json_body(resp)
            .await
            .as_array()
            .expect("an array")
            .iter()
            .map(|n| n["address"].as_str().unwrap_or_default().to_string())
            .collect();

        assert!(addresses.contains(&"authz:alice-private".to_string()));
        assert!(addresses.contains(&"authz:alice-public".to_string()));
    }

    /// An address the caller cannot read cannot be used to find out who
    /// owns it, or what else they own.
    #[tokio::test]
    async fn an_unreadable_address_cannot_be_used_as_a_subject() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(get(BOB_TOKEN, "/node/authz:alice-private/owned"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn an_unknown_address_is_not_found() {
        let _guard = disk_guard();

        let resp = router()
            .oneshot(get(BOB_TOKEN, "/node/authz:no-such-node/owned"))
            .await
            .expect("router response");

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod route_authorization_tests {
    //! The authorization matrix, driven through the real router.
    //!
    //! [`ROUTES`] states who may call each endpoint. This module is what
    //! makes that statement a control rather than a comment: every row
    //! is turned into actual requests against `create_router`, so a
    //! route added without an entry, an entry whose claim is false, and
    //! an admin gate that was never wired are all failures here instead
    //! of discoveries in production.
    //!
    //! Two properties are asserted for every row, and they are the two
    //! that the holes fixed in the previous pass violated:
    //!
    //!   * an endpoint that requires a credential refuses a request
    //!     without one — which also proves the route is registered at
    //!     all, since an unregistered path answers 404, not 401;
    //!   * an endpoint marked [`Access::AdminOnly`] refuses an ordinary
    //!     identity, and one marked [`Access::Authenticated`] does not.
    //!
    //! What this cannot check is the reverse direction — a route
    //! registered in `create_router` and *absent* from `ROUTES` — because
    //! axum's `Router` does not expose its own table. That gap is real
    //! and is why the table sits immediately above the router rather
    //! than in a separate file.
    use super::*;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::RwLock;
    use tower::ServiceExt;

    const USER_TOKEN: &str = "matrix-user-token";
    const ADMIN_TOKEN: &str = "matrix-admin-token";

    fn router() -> axum::Router {
        let mut engine = StorageEngine::open().expect("open storage engine");

        for (token, owner, role) in [
            (USER_TOKEN, "matrix-user", Role::User),
            (ADMIN_TOKEN, "matrix-admin", Role::Admin),
        ] {
            engine.seed_user(UserRecord {
                    token_hash: hash_token(token),
                    owner: owner.to_string(),
                    role,
                });
        }

        create_router(Arc::new(Database::attach(engine)))
    }

    /// A concrete path for a route pattern.
    fn concrete(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                if segment.starts_with(':') {
                    "matrix:subject"
                } else {
                    segment
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// A minimally-valid body for the routes that take one.
    ///
    /// Required, not cosmetic: axum runs the `Json` extractor before the
    /// handler body, so a route sent no body answers 415 and never
    /// reaches the admin check this module is trying to observe. A test
    /// that accepted 415 as "refused" would pass with the gate deleted.
    fn body_for(method: &str, path: &str) -> Option<serde_json::Value> {
        match (method, path) {
            ("POST", "/node") => Some(serde_json::json!({
                "address": "matrix:written",
                "kind": "MatrixNode",
                "x": 0, "y": 0, "z": 0, "q": 0,
                "data": "{}"
            })),
            ("PUT", "/node/:address") => Some(serde_json::json!({"data": "{}"})),
            ("POST", "/nodes/query") => Some(serde_json::json!({})),
            ("POST", "/edge") | ("DELETE", "/edge") => Some(serde_json::json!({
                "from": "matrix:subject",
                "to": "matrix:subject",
                "kind": "MATRIX"
            })),
            ("POST", "/transaction") => Some(serde_json::json!({"operations": []})),
            ("POST", "/publish") => Some(serde_json::json!({"payload": "x"})),
            ("POST", "/admin/users") => Some(serde_json::json!({"owner": "matrix:new"})),
            ("POST", "/admin/indexes") => Some(serde_json::json!({
                "name": "matrix-index",
                "kind": "MatrixNode",
                "field": "score"
            })),
            ("POST", "/admin/references") => Some(serde_json::json!({
                "name": "matrix-reference",
                "kind": "MatrixNode",
                "field": "parent",
                "parent_kind": "MatrixParent",
                "on_delete": "cascade"
            })),
            _ => None,
        }
    }

    fn request(spec: &RouteSpec, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method(spec.method)
            .uri(concrete(spec.path));

        if let Some(token) = token {
            builder = builder.header("x-api-key", token);
        }

        match body_for(spec.method, spec.path) {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("build request"),

            None => builder.body(Body::empty()).expect("build request"),
        }
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");

        String::from_utf8_lossy(&bytes).to_string()
    }

    /// Every row states its per-object rule.
    ///
    /// The `objects` column is prose, so no test can check that it is
    /// *true*. What this checks is that it exists and says something —
    /// which is the property that was actually missing when
    /// `GET /node/:address/owned` shipped ignoring its own path
    /// parameter: nobody had ever had to write down what the endpoint
    /// answered about. A blank here is a route whose author did not
    /// decide.
    /// Every row is written in the router's own path syntax.
    ///
    /// axum 0.7 (matchit 0.7) captures with `:name`; `{name}` is an
    /// ordinary literal segment there, and only became a capture in
    /// 0.8. A row written in the 0.8 form is not a typo with a cosmetic
    /// cost — `POST /sequence/{name}/next` was registered that way and
    /// answered 404 for every real sequence name, because the only path
    /// it matched was the literal one.
    ///
    /// That went unseen because `concrete()` substitutes a segment only
    /// when it starts with ':', so the authorization matrix sent the
    /// brace path through *unchanged* and hit the one literal path the
    /// broken route did match. The route and the test agreed with each
    /// other and disagreed with every caller. This assertion is what
    /// makes the two forms distinguishable again.
    #[test]
    fn every_route_is_written_in_the_routers_path_syntax() {
        for spec in ROUTES {
            assert!(
                !spec.path.contains('{') && !spec.path.contains('}'),
                "{} {} uses axum 0.8 brace capture syntax; this build is \
                 axum 0.7, where `{{name}}` is a literal segment and `:name` \
                 is the capture",
                spec.method,
                spec.path,
            );
        }
    }

    #[test]
    fn every_route_states_its_object_rule() {
        for spec in ROUTES {
            let objects = spec.objects.trim();

            assert!(
                objects.len() > 10,
                "{} {} does not state what it authorizes per object",
                spec.method,
                spec.path
            );

            if spec.access == Access::Authenticated {
                assert!(
                    objects.contains("can_read")
                        || objects.contains("can_write")
                        || objects.contains("audience")
                        || objects.contains("Audience")
                        || objects.contains("none"),
                    "{} {} is open to any identity, so the rule named here \
                     is the only thing standing between one tenant and \
                     another — name it in the vocabulary the code uses \
                     (can_read / can_write / Audience), or say `none` and \
                     mean it: {objects}",
                    spec.method,
                    spec.path
                );
            }
        }
    }

    /// Every route that needs a credential refuses a request without
    /// one — and, because an unregistered path answers 404 rather than
    /// 401, this simultaneously proves every row of the table is really
    /// wired into `create_router`.
    #[tokio::test]
    async fn every_credentialed_route_refuses_an_anonymous_request() {
        let _guard = disk_guard();

        let app = router();

        for spec in ROUTES {
            let response = app
                .clone()
                .oneshot(request(spec, None))
                .await
                .expect("router response");

            match spec.access {
                Access::Anonymous => assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "{} {} is declared anonymous but did not answer",
                    spec.method,
                    spec.path
                ),

                _ => assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{} {} answered an anonymous request with {} — either it \
                     is not behind the auth layer, or it is not registered \
                     at all",
                    spec.method,
                    spec.path,
                    response.status()
                ),
            }
        }
    }

    /// Every route the table calls admin-only actually refuses an
    /// ordinary identity, with the handler's own refusal rather than
    /// with some incidental 4xx.
    #[tokio::test]
    async fn every_admin_only_route_refuses_an_ordinary_identity() {
        let _guard = disk_guard();

        let app = router();

        for spec in ROUTES {
            if spec.access != Access::AdminOnly {
                continue;
            }

            let response = app
                .clone()
                .oneshot(request(spec, Some(USER_TOKEN)))
                .await
                .expect("router response");

            let status = response.status();
            let body = body_text(response).await;

            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "{} {} admitted a non-admin (body: {body})",
                spec.method,
                spec.path
            );

            assert_eq!(
                body, "admin only",
                "{} {} refused a non-admin for the wrong reason — the \
                 refusal must be the role gate, not an incidental \
                 rejection that would vanish if the request were better \
                 formed",
                spec.method, spec.path
            );
        }
    }

    /// The other half of the same claim: a route the table does *not*
    /// mark admin-only must not be quietly admin-gated. An endpoint that
    /// is stricter than its documented rule is a bug in the same family
    /// as one that is laxer — it is just one that fails visibly.
    #[tokio::test]
    async fn no_authenticated_route_is_secretly_admin_gated() {
        let _guard = disk_guard();

        let app = router();

        for spec in ROUTES {
            if spec.access != Access::Authenticated {
                continue;
            }

            let response = app
                .clone()
                .oneshot(request(spec, Some(USER_TOKEN)))
                .await
                .expect("router response");

            let status = response.status();

            assert_ne!(
                status,
                StatusCode::UNAUTHORIZED,
                "{} {} rejected a valid identity",
                spec.method,
                spec.path
            );

            // `GET /events` answers with an SSE stream that by design
            // never ends, so its body is never read — reading it would
            // hang this test forever rather than fail it. The status is
            // the whole answer for it anyway.
            if spec.class == EndpointClass::Subscribe {
                continue;
            }

            let body = body_text(response).await;

            assert_ne!(
                body, "admin only",
                "{} {} is declared open to any identity but is admin-gated",
                spec.method,
                spec.path
            );
        }
    }
}

#[cfg(test)]
mod object_authorization_tests {
    //! The per-object half of the matrix, for the two endpoints that had
    //! none at all.
    //!
    //! `POST /node/:address/claim` and `POST /edge` both wrote through
    //! another identity's nodes without checking anything, and both are
    //! writes: a claim sets `claimed_by` and archives the previous
    //! value, and an edge appends to a node's outgoing adjacency, which
    //! is what `GET /node/:address/edges/out` reads back as fact. Every
    //! test here fails against the handlers as they were.
    use super::*;
    use crate::core::coordinate::Coordinate;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::RwLock;
    use tower::ServiceExt;

    const ALICE_TOKEN: &str = "obj-alice-token";
    const BOB_TOKEN: &str = "obj-bob-token";
    const ADMIN_TOKEN: &str = "obj-admin-token";

    const KIND: &str = "ObjAuthzNode";

    /// Alice owns one public and one private node; Bob owns one public
    /// node. Nothing is claimed and no edges exist.
    fn router() -> axum::Router {
        let mut engine = StorageEngine::open().expect("open storage engine");

        for (address, owner, public) in [
            ("obj:alice-public", "alice", true),
            ("obj:alice-private", "alice", false),
            ("obj:bob-public", "bob", true),
        ] {
            let mut node = Node::new(
                Coordinate::new(0, 0, 0, 0),
                address.to_string(),
                KIND.to_string(),
                owner.to_string(),
            );

            if public {
                node.visibility = Visibility::Public;
            }

            engine.insert(node).expect("insert node");
        }

        for (token, owner, role) in [
            (ALICE_TOKEN, "alice", Role::User),
            (BOB_TOKEN, "bob", Role::User),
            (ADMIN_TOKEN, "obj-admin", Role::Admin),
        ] {
            engine.seed_user(UserRecord {
                    token_hash: hash_token(token),
                    owner: owner.to_string(),
                    role,
                });
        }

        create_router(Arc::new(Database::attach(engine)))
    }

    fn claim(token: &str, address: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(format!("/node/{address}/claim"))
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    fn edge(token: &str, from: &str, to: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/edge")
            .header("x-api-key", token)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"from": from, "to": to, "kind": "OBJ_REL"})
                    .to_string(),
            ))
            .expect("build request")
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");

        String::from_utf8_lossy(&bytes).to_string()
    }

    // -----------------------------------------------------------------
    // POST /node/:address/claim
    // -----------------------------------------------------------------

    /// The whole hole in one request: any identity could lease any node,
    /// and a claim is claim-once, so the owner's own workers would find
    /// it held by a stranger forever.
    #[tokio::test]
    async fn a_stranger_cannot_claim_a_readable_node_they_do_not_own() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(claim(BOB_TOKEN, "obj:alice-public"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// A node the caller cannot read must be indistinguishable from one
    /// that does not exist — otherwise this endpoint is an oracle over
    /// private addresses, answering "already claimed by X" and "claimed"
    /// about rows a plain `GET` refuses outright.
    #[tokio::test]
    async fn an_unreadable_node_is_reported_as_absent() {
        let _guard = disk_guard();

        let app = router();

        let refused = app
            .clone()
            .oneshot(claim(BOB_TOKEN, "obj:alice-private"))
            .await
            .expect("router response");

        let absent = app
            .oneshot(claim(BOB_TOKEN, "obj:no-such-node"))
            .await
            .expect("router response");

        assert_eq!(refused.status(), StatusCode::NOT_FOUND);
        assert_eq!(absent.status(), StatusCode::NOT_FOUND);

        assert_eq!(
            body_text(refused).await,
            body_text(absent).await,
            "a private node and an absent one must answer identically"
        );
    }

    /// The lease primitive still works for the identity it exists for —
    /// `fqStore`'s job queue claims nodes it wrote itself.
    #[tokio::test]
    async fn an_owner_may_still_claim_their_own_node() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(claim(ALICE_TOKEN, "obj:alice-private"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// And an admin bypasses it, as it does on every other write path.
    #[tokio::test]
    async fn an_admin_may_claim_any_node() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(claim(ADMIN_TOKEN, "obj:alice-public"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------
    // POST /edge
    // -----------------------------------------------------------------

    /// Forging a relationship out of somebody else's node. Worse than a
    /// stray row: `EdgeId` excludes the owner, so the forged edge is
    /// *the* copy of that fact — Alice can neither create her own nor
    /// delete this one.
    #[tokio::test]
    async fn a_stranger_cannot_create_an_edge_out_of_another_owners_node() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(edge(BOB_TOKEN, "obj:alice-public", "obj:bob-public"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// The ordinary case — "my node points at your public node" — is
    /// exactly what this graph is for and must keep working.
    #[tokio::test]
    async fn an_owner_may_point_their_own_node_at_a_readable_one() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(edge(BOB_TOKEN, "obj:bob-public", "obj:alice-public"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    /// Pointing at a node the caller cannot read would be an existence
    /// oracle one address at a time, so it answers the same way a
    /// missing endpoint does.
    #[tokio::test]
    async fn an_unreadable_target_is_reported_as_absent() {
        let _guard = disk_guard();

        let app = router();

        let refused = app
            .clone()
            .oneshot(edge(BOB_TOKEN, "obj:bob-public", "obj:alice-private"))
            .await
            .expect("router response");

        let absent = app
            .oneshot(edge(BOB_TOKEN, "obj:bob-public", "obj:no-such-node"))
            .await
            .expect("router response");

        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert_eq!(absent.status(), StatusCode::BAD_REQUEST);

        assert_eq!(
            body_text(refused).await.replace("obj:alice-private", "X"),
            body_text(absent).await.replace("obj:no-such-node", "X"),
            "a private target and an absent one must answer identically"
        );
    }

    /// The batch path must not be a way around the single-request rule.
    /// It is the same helper, and this is the test that says so.
    #[tokio::test]
    async fn a_transaction_cannot_forge_an_edge_either() {
        let _guard = disk_guard();

        let request = Request::builder()
            .method("POST")
            .uri("/transaction")
            .header("x-api-key", BOB_TOKEN)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "operations": [{
                        "type": "insert_edge",
                        "from": "obj:alice-public",
                        "to": "obj:bob-public",
                        "kind": "OBJ_REL"
                    }]
                })
                .to_string(),
            ))
            .expect("build request");

        let response = router().oneshot(request).await.expect("router response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}

#[cfg(test)]
mod resource_guard_tests {
    //! The bounds from [`crate::api::limits`] and
    //! [`crate::core::predicate`], observed where they have to be true:
    //! in front of the handlers, through the real router.
    //!
    //! `limits`' own tests check the arithmetic. These check the wiring,
    //! which is the half that can be silently absent — a token bucket
    //! that is never consulted still passes every unit test it has.
    use super::*;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::RwLock;
    use tower::ServiceExt;

    const USER_TOKEN: &str = "guard-user-token";

    /// Two admins, one per rate-limit test.
    ///
    /// The buckets are process-wide and keyed by owner, and a flood test
    /// leaves its bucket empty on purpose — so two tests sharing an
    /// identity would have the second one observe the first one's
    /// exhaustion and conclude the burst was never honoured. Distinct
    /// owners here, and owners unique to this module, are what make each
    /// test's arithmetic about its own traffic.
    const FLOOD_ADMIN_TOKEN: &str = "guard-flood-admin-token";
    const ISOLATION_ADMIN_TOKEN: &str = "guard-isolation-admin-token";

    const USER_OWNER: &str = "guard-user";
    const FLOOD_ADMIN_OWNER: &str = "guard-flood-admin";
    const ISOLATION_ADMIN_OWNER: &str = "guard-isolation-admin";

    fn router() -> axum::Router {
        let mut engine = StorageEngine::open().expect("open storage engine");

        for (token, owner, role) in [
            (USER_TOKEN, USER_OWNER, Role::User),
            (FLOOD_ADMIN_TOKEN, FLOOD_ADMIN_OWNER, Role::Admin),
            (ISOLATION_ADMIN_TOKEN, ISOLATION_ADMIN_OWNER, Role::Admin),
        ] {
            engine.seed_user(UserRecord {
                    token_hash: hash_token(token),
                    owner: owner.to_string(),
                    role,
                });
        }

        create_router(Arc::new(Database::attach(engine)))
    }

    fn list_users(token: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri("/admin/users")
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    fn post(token: &str, uri: &str, body: String) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("x-api-key", token)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("build request")
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");

        String::from_utf8_lossy(&bytes).to_string()
    }

    // -----------------------------------------------------------------
    // Body size
    // -----------------------------------------------------------------

    /// The cheapest bound, and the one that has to fire before any
    /// deserialization: an oversized batch must never become allocated
    /// JSON.
    #[tokio::test]
    async fn a_body_past_the_limit_is_refused() {
        let _guard = disk_guard();

        let oversize = "x".repeat(limits::max_body_bytes() + 1024);

        let response = router()
            .oneshot(post(
                USER_TOKEN,
                "/node",
                serde_json::json!({
                    "address": "guard:big",
                    "kind": "GuardNode",
                    "x": 0, "y": 0, "z": 0, "q": 0,
                    "data": oversize
                })
                .to_string(),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // -----------------------------------------------------------------
    // Predicate size
    // -----------------------------------------------------------------

    /// A predicate 4 MiB of JSON wide is inside the body limit and was
    /// therefore accepted, then evaluated once per candidate row. This
    /// is the amplifier the node bound closes, refused before the read
    /// lock is taken.
    #[tokio::test]
    async fn an_oversized_predicate_is_refused_by_the_query_path() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(post(
                USER_TOKEN,
                "/nodes/query",
                serde_json::json!({
                    "kind": "GuardNode",
                    "where": balanced_conjunction(9)
                })
                .to_string(),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        assert!(
            body_text(response).await.contains("expression nodes"),
            "refused, but not by the node bound"
        );
    }

    /// The same predicate through `delete_where`, which runs the same
    /// evaluator over the same candidates while holding the *write*
    /// lock. A bound applied on only one of the two entry points is not
    /// applied.
    #[tokio::test]
    async fn an_oversized_predicate_is_refused_by_the_transaction_path() {
        let _guard = disk_guard();

        let response = router()
            .oneshot(post(
                USER_TOKEN,
                "/transaction",
                serde_json::json!({
                    "operations": [{
                        "type": "delete_where",
                        "kind": "GuardNode",
                        "where": balanced_conjunction(9)
                    }]
                })
                .to_string(),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        assert!(
            body_text(response).await.contains("expression nodes"),
            "refused, but not by the node bound"
        );
    }

    /// 1023 nodes at depth 9 — past the node bound, nowhere near the
    /// depth bound, so only the bound under test can refuse it.
    fn balanced_conjunction(height: u32) -> serde_json::Value {
        if height == 0 {
            return serde_json::json!({"kind": "lit", "val": true});
        }

        serde_json::json!({
            "kind": "bin",
            "op": "&&",
            "l": balanced_conjunction(height - 1),
            "r": balanced_conjunction(height - 1),
        })
    }

    // -----------------------------------------------------------------
    // Rate limiting
    // -----------------------------------------------------------------

    /// One identity spending its whole allowance is refused, and the
    /// refusal carries a usable retry hint.
    ///
    /// Uses the `Admin` class because it has the tightest default
    /// bucket, so the flood is short enough that refill cannot rescue
    /// it: 60 burst refilling at 30/s means the loop below would have to
    /// take seven seconds for the limiter to keep up.
    #[tokio::test]
    async fn a_flood_from_one_identity_is_refused() {
        let _guard = disk_guard();

        if std::env::var("FACETQL_RATE_ADMIN").is_ok() {
            // An operator-configured rate makes the arithmetic below
            // untrue; skip rather than assert something this run cannot
            // know.
            return;
        }

        let app = router();

        let mut refused = None;

        for attempt in 0..300 {
            let response = app
                .clone()
                .oneshot(list_users(FLOOD_ADMIN_TOKEN))
                .await
                .expect("router response");

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                refused = Some((attempt, response));
                break;
            }

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "attempt {attempt} failed for a reason other than the rate limit"
            );
        }

        let (attempt, response) = refused.expect("the flood was never refused");

        assert!(
            attempt >= 60,
            "the limiter refused at request {attempt}, inside the configured burst"
        );

        assert!(
            response.headers().contains_key("retry-after"),
            "a 429 without a Retry-After tells the caller nothing it can act on"
        );
    }

    /// …and it is *that* identity's allowance, not the server's. A
    /// limiter that shut everyone out when one caller misbehaved would
    /// be the outage it exists to prevent.
    #[tokio::test]
    async fn one_identity_exhausting_its_allowance_does_not_affect_another() {
        let _guard = disk_guard();

        if std::env::var("FACETQL_RATE_ADMIN").is_ok() {
            return;
        }

        let app = router();

        // Drain one admin identity's Admin bucket.
        for _ in 0..300 {
            let response = app
                .clone()
                .oneshot(list_users(ISOLATION_ADMIN_TOKEN))
                .await
                .expect("router response");

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                break;
            }
        }

        // A different identity, same endpoint: refused for its role, not
        // for the other caller's traffic.
        let response = app
            .oneshot(list_users(USER_TOKEN))
            .await
            .expect("router response");

        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "a second identity was charged for the first one's flood"
        );
    }
}

#[cfg(test)]
mod migration_field_tests {
    //! The three things a cell migration needs from this API, checked
    //! where they have to hold: through the real router, and against the
    //! feed a subscriber actually reads.
    //!
    //!   * `insert_node` can name an `owner` and a `claimed_by` — but
    //!     only for an admin, and a non-admin that names either has the
    //!     whole batch refused rather than quietly rewritten to itself.
    //!   * `node_updated` and `node_deleted` name the node's `kind`, so
    //!     an address can be attributed without a lookup — and after a
    //!     delete there is nothing left to look up.
    //!   * every frame carries its position.
    use super::*;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    const ADMIN_TOKEN: &str = "migrate-admin-token";
    const USER_TOKEN: &str = "migrate-user-token";

    const ADMIN_OWNER: &str = "migrate-admin";
    const USER_OWNER: &str = "migrate-user";

    /// The identity a migration is *for*: it owns the data and never
    /// makes a request in these tests.
    const TENANT: &str = "migrate-tenant";

    const KIND: &str = "MigratedNode";

    /// The router, plus the database behind it — the feed is read
    /// directly because an SSE body is a stream and what is being
    /// asserted is the frame, not the transport.
    fn app() -> (axum::Router, Arc<Database>) {
        let engine = StorageEngine::open().expect("open storage engine");

        for (token, owner, role) in [
            (ADMIN_TOKEN, ADMIN_OWNER, Role::Admin),
            (USER_TOKEN, USER_OWNER, Role::User),
        ] {
            engine.seed_user(UserRecord {
                token_hash: hash_token(token),
                owner: owner.to_string(),
                role,
            });
        }

        let db = Arc::new(Database::attach(engine));

        (create_router(Arc::clone(&db)), db)
    }

    /// The position everything published from now on comes after.
    fn start(db: &Database) -> u64 {
        db.feed
            .subscribe(Some(0))
            .expect_err("position 0 predates this feed")
            .earliest
    }

    /// Every event published since `start`, decoded.
    fn events_since(db: &Database, start: u64) -> Vec<serde_json::Value> {
        let (backlog, _) = db.feed.subscribe(Some(start)).expect("resume");

        backlog
            .into_iter()
            .map(|event| {
                serde_json::from_str(&event.payload).expect("valid JSON event")
            })
            .collect()
    }

    fn transaction(token: &str, op: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/transaction")
            .header("x-api-key", token)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"operations": [op]}).to_string(),
            ))
            .expect("build request")
    }

    fn insert_op(address: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut op = serde_json::json!({
            "type": "insert_node",
            "address": address,
            "kind": KIND,
            "x": 0, "y": 0, "z": 0, "q": 0,
            "data": "{}",
        });

        for (key, value) in extra.as_object().expect("an object") {
            op[key] = value.clone();
        }

        op
    }

    fn request(method: &str, token: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    async fn json_body(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");

        serde_json::from_slice(&bytes).expect("valid JSON")
    }

    // -----------------------------------------------------------------
    // insert_node: owner / claimed_by
    // -----------------------------------------------------------------

    /// An admin creates a node owned by somebody else and already
    /// leased — the two fields a faithful copy cannot do without.
    #[tokio::test]
    async fn an_admin_may_create_a_node_owned_by_another_identity() {
        let _guard = disk_guard();
        let (app, _db) = app();

        let response = app
            .clone()
            .oneshot(transaction(
                ADMIN_TOKEN,
                insert_op(
                    "migrate:owned-elsewhere",
                    serde_json::json!({
                        "owner": TENANT,
                        "claimed_by": "worker-7",
                    }),
                ),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);

        let node = json_body(
            app.oneshot(request("GET", ADMIN_TOKEN, "/node/migrate:owned-elsewhere"))
                .await
                .expect("router response"),
        )
        .await;

        assert_eq!(node["owner"], TENANT, "the named owner was not honoured");
        assert_eq!(node["claimed_by"], "worker-7", "the lease was dropped");
    }

    /// The gate. A non-admin naming an owner is refused — and refused
    /// rather than silently written under its own identity, which would
    /// look like a successful migration of somebody else's data.
    #[tokio::test]
    async fn a_non_admin_cannot_name_an_owner() {
        let _guard = disk_guard();
        let (app, _db) = app();

        let response = app
            .clone()
            .oneshot(transaction(
                USER_TOKEN,
                insert_op(
                    "migrate:stolen",
                    serde_json::json!({"owner": TENANT}),
                ),
            ))
            .await
            .expect("router response");

        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "a non-admin must not be able to name another owner"
        );

        // Nothing applied — not the requested node, and not a node
        // owned by the caller instead.
        let lookup = app
            .oneshot(request("GET", ADMIN_TOKEN, "/node/migrate:stolen"))
            .await
            .expect("router response");

        assert_eq!(
            lookup.status(),
            StatusCode::NOT_FOUND,
            "the refused insert landed anyway"
        );
    }

    /// `claimed_by` is gated by the same rule, and separately: a lease
    /// on somebody else's behalf is the same authority as an owner.
    #[tokio::test]
    async fn a_non_admin_cannot_name_a_claim() {
        let _guard = disk_guard();
        let (app, _db) = app();

        let response = app
            .oneshot(transaction(
                USER_TOKEN,
                insert_op(
                    "migrate:preclaimed",
                    serde_json::json!({"claimed_by": "worker-7"}),
                ),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// Omitting both fields is unchanged: the writer owns what it
    /// writes, and it is unclaimed.
    #[tokio::test]
    async fn omitting_the_fields_still_stamps_the_writer() {
        let _guard = disk_guard();
        let (app, _db) = app();

        let response = app
            .clone()
            .oneshot(transaction(
                USER_TOKEN,
                insert_op("migrate:ordinary", serde_json::json!({})),
            ))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);

        let node = json_body(
            app.oneshot(request("GET", USER_TOKEN, "/node/migrate:ordinary"))
                .await
                .expect("router response"),
        )
        .await;

        assert_eq!(node["owner"], USER_OWNER);
        assert!(node["claimed_by"].is_null());
    }

    // -----------------------------------------------------------------
    // event frames
    // -----------------------------------------------------------------

    /// Past the horizon, `GET /events?after=` refuses. This is the
    /// whole point of the resume: a position this server cannot supply
    /// must not be answered with a 200 and a live stream, because that
    /// is byte-for-byte what a complete resume looks like.
    #[tokio::test]
    async fn a_resume_this_server_cannot_honour_is_refused() {
        let _guard = disk_guard();
        let (app, _db) = app();

        // Unchanged for every existing subscriber: no `after`, no
        // resume, and the stream opens at the live edge as it always
        // has.
        let live = app
            .clone()
            .oneshot(request("GET", USER_TOKEN, "/events"))
            .await
            .expect("router response");

        assert_eq!(live.status(), StatusCode::OK);

        let response = app
            .oneshot(request("GET", USER_TOKEN, "/events?after=0"))
            .await
            .expect("router response");

        assert_eq!(
            response.status(),
            StatusCode::GONE,
            "a resume from before this feed existed must not silently \
             start at the live edge"
        );
    }

    /// An update and a delete both name the kind, and both carry a
    /// position. The delete is the one that could not be recovered any
    /// other way — the node is gone by the time the event arrives.
    #[tokio::test]
    async fn node_updated_and_node_deleted_name_the_kind() {
        let _guard = disk_guard();
        let (app, db) = app();

        let from = start(&db);

        app.clone()
            .oneshot(transaction(
                USER_TOKEN,
                insert_op("migrate:short-lived", serde_json::json!({})),
            ))
            .await
            .expect("router response");

        let updated = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/node/migrate:short-lived")
                    .header("x-api-key", USER_TOKEN)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"data":"{}"}"#))
                    .expect("build request"),
            )
            .await
            .expect("router response");

        assert_eq!(updated.status(), StatusCode::OK);

        let deleted = app
            .oneshot(request("DELETE", USER_TOKEN, "/node/migrate:short-lived"))
            .await
            .expect("router response");

        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        let events = events_since(&db, from);

        for name in ["node_updated", "node_deleted"] {
            let event = events
                .iter()
                .find(|e| e["event"] == name)
                .unwrap_or_else(|| panic!("no {name} event was published"));

            assert_eq!(event["kind"], KIND, "{name} did not name the kind");
            assert!(
                event["seq"].is_u64(),
                "{name} carried no position, so a gap is undetectable"
            );
        }
    }
}

#[cfg(test)]
mod change_scan_tests {
    //! `GET /changes`, driven through the real router.
    //!
    //! The property under test is the one whose failure is silent
    //! disclosure: a durable scan must not become the way to enumerate
    //! nodes a caller could never fetch. It is checked end to end rather
    //! than against `changes::scan` directly, because the leak that
    //! matters is the one a *request* can produce — the handler resolves
    //! the identity, and a filter applied anywhere else is a filter that
    //! can be skipped by a route.
    //!
    //! A delete gets its own case because it is the hard half: the WAL's
    //! delete record carries an address and nothing else, so the only
    //! thing that says whose node it was is the archive staged ahead of
    //! it. Get that wrong and every private deletion is announced to
    //! everybody.
    use super::*;
    use crate::core::user::{Role, UserRecord};
    use crate::database::Database;
    use crate::storage::engine::test_support::disk_guard;
    use crate::storage::engine::StorageEngine;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    const ALICE_TOKEN: &str = "scan-alice-token";
    const BOB_TOKEN: &str = "scan-bob-token";

    const PRIVATE: &str = "scan:private";
    const PUBLIC: &str = "scan:public";

    fn app() -> axum::Router {
        let engine = StorageEngine::open().expect("open storage engine");

        for (token, owner) in [(ALICE_TOKEN, "scan-alice"), (BOB_TOKEN, "scan-bob")] {
            engine.seed_user(UserRecord {
                token_hash: hash_token(token),
                owner: owner.to_string(),
                role: Role::User,
            });
        }

        create_router(Arc::new(Database::attach(engine)))
    }

    fn create(token: &str, address: &str, public: bool) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/node")
            .header("x-api-key", token)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "address": address,
                    "kind": "ScannedNode",
                    "x": 0, "y": 0, "z": 0, "q": 0,
                    "data": "{}",
                    "public": public,
                })
                .to_string(),
            ))
            .expect("build request")
    }

    fn plain(method: &str, token: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("x-api-key", token)
            .body(Body::empty())
            .expect("build request")
    }

    /// The scan, reduced to the changes about *this* test's addresses.
    ///
    /// The WAL is shared by every test in this binary, so an assertion
    /// on the whole page would be an assertion about the test binary's
    /// history. What each case actually claims is about two addresses.
    async fn changes_for(app: &axum::Router, token: &str) -> Vec<(String, String)> {
        let response = app
            .clone()
            .oneshot(plain("GET", token, "/changes?after=0&limit=2000"))
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");

        let page: serde_json::Value =
            serde_json::from_slice(&bytes).expect("valid JSON");

        page["changes"]
            .as_array()
            .expect("changes array")
            .iter()
            .filter_map(|change| {
                let address = change["address"].as_str()?.to_string();

                if address != PRIVATE && address != PUBLIC {
                    return None;
                }

                Some((address, change["change"].as_str()?.to_string()))
            })
            .collect()
    }

    #[tokio::test]
    async fn a_scan_shows_only_what_the_caller_could_have_read() {
        let _guard = disk_guard();

        let app = app();

        for request in [
            create(ALICE_TOKEN, PRIVATE, false),
            create(ALICE_TOKEN, PUBLIC, true),
        ] {
            let response =
                app.clone().oneshot(request).await.expect("router response");

            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let alice = changes_for(&app, ALICE_TOKEN).await;

        assert!(
            alice.contains(&(PRIVATE.to_string(), "created".to_string())),
            "the owner sees the creation of its own private node: {alice:?}"
        );

        assert!(
            alice.contains(&(PUBLIC.to_string(), "created".to_string())),
            "and of its public one: {alice:?}"
        );

        let bob = changes_for(&app, BOB_TOKEN).await;

        assert!(
            bob.contains(&(PUBLIC.to_string(), "created".to_string())),
            "a public node is announced to everyone, as it is on /events: {bob:?}"
        );

        assert!(
            !bob.iter().any(|(address, _)| address == PRIVATE),
            "a stranger learned that a private node exists: {bob:?}"
        );
    }

    /// A delete carries only an address. Its audience comes from the
    /// archive staged ahead of it, and if that attribution were lost the
    /// deletion of a private node would be visible to every token.
    #[tokio::test]
    async fn a_delete_is_withheld_from_a_caller_that_could_not_read_it() {
        let _guard = disk_guard();

        let app = app();

        let created = app
            .clone()
            .oneshot(create(ALICE_TOKEN, PRIVATE, false))
            .await
            .expect("router response");

        assert_eq!(created.status(), StatusCode::CREATED);

        let deleted = app
            .clone()
            .oneshot(plain("DELETE", ALICE_TOKEN, &format!("/node/{PRIVATE}")))
            .await
            .expect("router response");

        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        let alice = changes_for(&app, ALICE_TOKEN).await;

        assert!(
            alice.contains(&(PRIVATE.to_string(), "deleted".to_string())),
            "the owner must see its own deletion, or a mover copying \
             alice's cell would leave the row behind on the destination: \
             {alice:?}"
        );

        let bob = changes_for(&app, BOB_TOKEN).await;

        assert!(
            !bob.iter().any(|(address, _)| address == PRIVATE),
            "a stranger learned that a private node was deleted: {bob:?}"
        );
    }
}
