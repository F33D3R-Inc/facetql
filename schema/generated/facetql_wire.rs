// Code generated from an FCT wire schema. DO NOT EDIT.
// Source: SCHEMA_IDL_SCOPE.md Tier C (FCT-as-IDL).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    #[serde(rename = "Private")]
    Private,
    #[serde(rename = "Public")]
    Public,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    #[serde(rename = "User")]
    User,
    #[serde(rename = "Admin")]
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    #[serde(rename = "cascade")]
    Cascade,
    #[serde(rename = "restrict")]
    Restrict,
    #[serde(rename = "set_null")]
    SetNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Coordinate {
    pub x: i64,
    pub y: i64,
    pub z: i64,
    pub q: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub address: String,
    pub coordinate: Box<Coordinate>,
    pub value: i64,
    pub kind: String,
    pub data: String,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub claimed_by: Option<String>,
    pub visibility: Visibility,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub token_hash: String,
    pub owner: String,
    pub role: Role,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub address: String,
    pub archived_at_unix: i64,
    pub node: Box<Node>,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expr {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub val: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vtype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub obj: Option<Box<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<Box<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub op: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub args: Option<Vec<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub l: Option<Box<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub r: Option<Box<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub x: Option<Box<Expr>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub var: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub operations: Vec<TxOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeSpec {
    pub to: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNodeRequest {
    pub address: String,
    pub kind: String,
    pub x: i64,
    pub y: i64,
    pub z: i64,
    pub q: i64,
    pub data: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub public: Option<bool>,
    #[serde(default = "default_createnoderequest_edges")]
    pub edges: Vec<EdgeSpec>,
    #[serde(default = "default_createnoderequest_if_absent")]
    pub if_absent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateNodeRequest {
    pub data: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEdgeRequest {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteEdgeRequest {
    pub from: String,
    pub to: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNodeResponse {
    pub address: String,
    pub edges_created: Vec<Edge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNodeError {
    pub error: String,
    pub edges_created_before_failure: Vec<Edge>,
}

/// Bound from the URL query string, never a JSON body.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryParams {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryWhereRequest {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
    #[serde(default = "default_querywhererequest_item_var")]
    pub item_var: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub order: Option<String>,
    #[serde(default = "default_querywhererequest_desc")]
    pub desc: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPage {
    pub nodes: Vec<Node>,
    pub next: String,
    pub examined: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountRequest {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
    #[serde(default = "default_countrequest_item_var")]
    pub item_var: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountResponse {
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupCount {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub value: Option<serde_json::Value>,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountByRequest {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
    #[serde(default = "default_countbyrequest_item_var")]
    pub item_var: String,
    pub group_by: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub values: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountByResponse {
    pub counts: Vec<GroupCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateRequest {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
    #[serde(default = "default_aggregaterequest_item_var")]
    pub item_var: String,
    pub func: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateResponse {
    pub result: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAggregate {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub value: Option<serde_json::Value>,
    pub result: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateByRequest {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub r#where: Option<Box<Expr>>,
    #[serde(default = "default_aggregatebyrequest_item_var")]
    pub item_var: String,
    pub group_by: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub values: Option<Vec<serde_json::Value>>,
    pub func: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateByResponse {
    pub groups: Vec<GroupAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceRequest {
    #[serde(default = "default_sequencerequest_count")]
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceResponse {
    pub first: i64,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiGetRequest {
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishRequest {
    pub payload: String,
}

/// Bound from the URL query string, never a JSON body.
#[derive(Debug, Clone, Deserialize)]
pub struct EventsQuery {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub after: Option<i64>,
}

/// Bound from the URL query string, never a JSON body.
#[derive(Debug, Clone, Deserialize)]
pub struct ChangesQuery {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub after: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserRequest {
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserResponse {
    pub owner: String,
    pub role: Role,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSummary {
    pub owner: String,
    pub role: Role,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub kind: String,
    pub field: String,
    #[serde(default = "default_createindexrequest_unique")]
    pub unique: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReferenceRequest {
    pub name: String,
    pub kind: String,
    pub field: String,
    pub parent_kind: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent_field: Option<String>,
    pub on_delete: ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TxOp {
    InsertNode {
        address: String,
        kind: String,
        x: i64,
        y: i64,
        z: i64,
        q: i64,
        data: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        public: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        owner: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        claimed_by: Option<String>,
    },
    InsertEdge {
        from: String,
        to: String,
        kind: String,
    },
    DeleteEdge {
        from: String,
        to: String,
        kind: String,
    },
    DeleteNode {
        address: String,
    },
    ClearKind {
        kind: String,
    },
    DeleteWhere {
        kind: String,
        #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
        r#where: Option<Box<Expr>>,
    },
    SetIf {
        address: String,
        field: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        expect_le: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        expect_eq: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        expect_absent: Option<bool>,
        #[serde(default = "default_txop_set_if_set")]
        set: serde_json::Value,
    },
}

fn default_createnoderequest_edges() -> Vec<EdgeSpec> { Vec::new() }
fn default_createnoderequest_if_absent() -> bool { false }
fn default_querywhererequest_item_var() -> String { "item".to_string() }
fn default_querywhererequest_desc() -> bool { false }
fn default_countrequest_item_var() -> String { "item".to_string() }
fn default_countbyrequest_item_var() -> String { "item".to_string() }
fn default_aggregaterequest_item_var() -> String { "item".to_string() }
fn default_aggregatebyrequest_item_var() -> String { "item".to_string() }
fn default_sequencerequest_count() -> i64 { 1 }
fn default_createindexrequest_unique() -> bool { false }
fn default_txop_set_if_set() -> serde_json::Value { serde_json::json!({}) }
