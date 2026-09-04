//! Thin HTTP client over FacetQL's existing API.
//!
//! This is deliberately just another API client. A data directory is
//! owned by exactly one process — `storage::lock` takes an advisory lock
//! to enforce that, because the page allocator, the index metadata and
//! the WAL counters are all process-local state over shared files — so a
//! second tool that opened those files would corrupt them structurally
//! rather than fail. Every tool therefore talks to a *running* `facetql
//! start` over HTTP instead, and nothing here opens a data file.
//!
//! Requests are built by hand — `content-type` header plus a string
//! body, responses read via `.text()` and parsed with `serde_json` —
//! rather than with reqwest's `.json()` helper, because the project
//! depends on reqwest with `default-features = false` and cannot assume
//! the `json` feature is compiled in.

use serde_json::{json, Value};

use super::error::CliError;

/// An authenticated handle to one FacetQL server.
///
/// The bearer token is held here and sent as `x-api-key` on every
/// request, exactly like every other FacetQL client. It is never logged
/// and never included in an error — see the `CliError` variants, which
/// only ever carry server response bodies (which do not contain the
/// caller's token).
pub struct FacetClient {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl FacetClient {
    pub fn new(base: &str, token: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base.trim_end_matches('/').to_string(),
            token: token.to_string(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    // ── admin: user management ─────────────────────────────────────────

    /// `POST /admin/users` — create a persistent identity and return the
    /// server's response, which includes the one-time plaintext token.
    pub async fn create_user(&self, owner: &str, admin: bool) -> Result<Value, CliError> {
        let body = json!({
            "owner": owner,
            "role": if admin { "admin" } else { "user" },
        });
        let resp = self.post("/admin/users", &body).await?;
        read_json(resp).await
    }

    /// `GET /admin/users` — list persistent identities.
    pub async fn list_users(&self) -> Result<Vec<Value>, CliError> {
        let resp = self.get("/admin/users").await?;
        read_json_array(resp).await
    }

    /// `DELETE /admin/users/:owner` — revoke every persistent record for
    /// `owner`.
    pub async fn delete_user(&self, owner: &str) -> Result<(), CliError> {
        let resp = self
            .send(self.http.delete(self.url(&format!("/admin/users/{owner}"))))
            .await?;
        expect_success(resp).await
    }

    // ── data ───────────────────────────────────────────────────────────

    /// `GET /node/:address`.
    pub async fn get_node(&self, address: &str) -> Result<Value, CliError> {
        let resp = self.get(&format!("/node/{address}")).await?;
        read_json(resp).await
    }

    /// `POST /node` — client-supplied address, coordinate defaults to
    /// 0/0/0/0. `data` is stored as an opaque string by the server (same
    /// as every other write), so we send the caller's validated JSON as a
    /// string.
    pub async fn put_node(
        &self,
        address: &str,
        kind: &str,
        data: &str,
        public: bool,
    ) -> Result<Value, CliError> {
        let body = json!({
            "address": address,
            "kind": kind,
            "x": 0, "y": 0, "z": 0, "q": 0,
            "data": data,
            "public": public,
        });
        let resp = self.post("/node", &body).await?;
        read_json(resp).await
    }

    /// `DELETE /node/:address`.
    pub async fn delete_node(&self, address: &str) -> Result<(), CliError> {
        let resp = self
            .send(self.http.delete(self.url(&format!("/node/{address}"))))
            .await?;
        expect_success(resp).await
    }

    /// `POST /nodes/query` — native predicate query. This CLI drives the
    /// `kind`/`order`/`desc`/`limit` fields; it never sends a `where`
    /// predicate, so there is no SQL and no expression to translate — the
    /// server runs its own native filter.
    pub async fn query(
        &self,
        kind: &str,
        limit: Option<usize>,
        order: Option<&str>,
        desc: bool,
    ) -> Result<Value, CliError> {
        let mut body = serde_json::Map::new();
        body.insert("kind".into(), json!(kind));
        if let Some(limit) = limit {
            body.insert("limit".into(), json!(limit));
        }
        if let Some(order) = order {
            body.insert("order".into(), json!(order));
        }
        body.insert("desc".into(), json!(desc));
        let resp = self.post("/nodes/query", &Value::Object(body)).await?;
        read_json(resp).await
    }

    /// `GET /nodes?limit=&offset=` — one page of the plain listing. Used
    /// by `stats` to enumerate nodes and count kinds client-side.
    pub async fn list_nodes(&self, limit: usize, offset: usize) -> Result<Vec<Value>, CliError> {
        let resp = self
            .get(&format!("/nodes?limit={limit}&offset={offset}"))
            .await?;
        read_json_array(resp).await
    }

    // ── low-level helpers ──────────────────────────────────────────────

    async fn get(&self, path: &str) -> Result<reqwest::Response, CliError> {
        self.send(self.http.get(self.url(path))).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<reqwest::Response, CliError> {
        self.send(
            self.http
                .post(self.url(path))
                .header("content-type", "application/json")
                .body(body.to_string()),
        )
        .await
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response, CliError> {
        req.header("x-api-key", &self.token)
            .send()
            .await
            .map_err(|e| CliError::Request(transport_message(&e)))
    }
}

/// Build a transport-level error message that never leaks the URL's
/// credentials. reqwest's `Error` Display can include the request URL;
/// our URLs carry no secrets (the token travels in a header, not the
/// URL), but we keep the message to the underlying cause to be safe.
fn transport_message(e: &reqwest::Error) -> String {
    if e.is_connect() {
        "could not connect to the server — is `facetql start` running at the given --url?".to_string()
    } else if e.is_timeout() {
        "request timed out".to_string()
    } else {
        format!("request failed: {e}")
    }
}

async fn read_json(resp: reqwest::Response) -> Result<Value, CliError> {
    let status = resp.status().as_u16();
    let text = body_text(resp).await?;
    if (200..300).contains(&status) {
        serde_json::from_str(&text)
            .map_err(|e| CliError::Request(format!("invalid JSON from server: {e}")))
    } else {
        Err(CliError::api(status, text))
    }
}

async fn read_json_array(resp: reqwest::Response) -> Result<Vec<Value>, CliError> {
    match read_json(resp).await? {
        Value::Array(items) => Ok(items),
        other => Err(CliError::Request(format!(
            "expected a JSON array from server, got: {other}"
        ))),
    }
}

async fn expect_success(resp: reqwest::Response) -> Result<(), CliError> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        let text = body_text(resp).await?;
        Err(CliError::api(status, text))
    }
}

async fn body_text(resp: reqwest::Response) -> Result<String, CliError> {
    resp.text()
        .await
        .map_err(|e| CliError::Request(format!("could not read server response: {e}")))
}
