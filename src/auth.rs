use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::core::user::Role;
use crate::database::Database;

/// Authenticated identity for the current request, attached to request
/// extensions by `auth_middleware`. Carries a role now, not just an
/// owner — this is what lets a handler decide "does this identity get
/// to bypass ownership checks" the way a Postgres superuser bypasses
/// row-level security, or a MySQL root user bypasses GRANTs.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub owner: String,
    pub role: Role,
}

impl AuthIdentity {
    pub fn is_admin(&self) -> bool {
        self.role == Role::Admin
    }
}

const DEV_TOKEN: &str = "dev-local-key-change-me";
const DEV_OWNER: &str = "dev";

/// Hashes a token with SHA-256 for comparison against persistent user
/// records. Tokens are bearer credentials — treated the same way a
/// password would be, not stored or logged in plaintext anywhere past
/// the moment they're generated and returned to the caller once.
pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// The static, env-var bootstrap layer. Format: `token1:alice,token2:bob`
/// for a normal user, or `token3:root:admin` (a third colon-separated
/// field) to mark that identity as Admin. This exists to solve the
/// bootstrap problem every one of these systems has: something has to
/// be able to authenticate before there's a persistent user store to
/// authenticate against — exactly the role POSTGRES_USER/
/// POSTGRES_PASSWORD play for a fresh Postgres container, or root's
/// initial password in MySQL.
///
/// Everything created after bootstrap should go through
/// `POST /admin/users` (persistent, admin-manageable, hashed at rest)
/// instead of growing this env var forever.
fn static_token_map() -> &'static HashMap<String, (String, Role)> {
    static MAP: OnceLock<HashMap<String, (String, Role)>> = OnceLock::new();
    MAP.get_or_init(|| {
        match std::env::var("FACETQL_TOKENS") {
            Ok(raw) => raw
                .split(',')
                .filter_map(|entry| {
                    let mut parts = entry.splitn(3, ':');
                    let token = parts.next()?.trim().to_string();
                    let owner = parts.next()?.trim().to_string();
                    let role = match parts.next().map(str::trim) {
                        Some("admin") => Role::Admin,
                        _ => Role::User,
                    };
                    if token.is_empty() || owner.is_empty() {
                        None
                    } else {
                        Some((token, (owner, role)))
                    }
                })
                .collect(),
            Err(_) => {
                eprintln!(
                    "warning: FACETQL_TOKENS not set — using a single dev token \
                     ('{DEV_TOKEN}' -> owner '{DEV_OWNER}', role Admin) so there's a way \
                     to bootstrap the first real admin via POST /admin/users. \
                     Do not run production traffic against this."
                );
                HashMap::from([(DEV_TOKEN.to_string(), (DEV_OWNER.to_string(), Role::Admin))])
            }
        }
    })
}

/// Verifies the request's credential and, on success, attaches the
/// resolved `AuthIdentity` before passing it on.
///
/// Checks the `x-api-key` header first — that's the normal path for
/// every request type. Falls back to a `?key=` query parameter only if
/// the header is absent. That fallback exists for exactly one reason:
/// browser `EventSource` (what `GET /events` needs for live updates)
/// cannot set custom headers at all — it's a real, documented
/// limitation of the browser API, not an oversight here. The tradeoff,
/// stated plainly: a token in a URL can end up in server access logs,
/// browser history, or a reverse proxy's logs in a way a header
/// wouldn't. That's a real security downgrade versus header-only auth,
/// accepted here because there's no other way to authenticate a plain
/// browser EventSource without a separate short-lived-token exchange
/// flow (a real feature, not yet built). If `/events` starts carrying
/// sensitive data, build that exchange flow before relying on this.
/// Pulls a single value out of a raw query string (`req.uri().query()`),
/// e.g. `query_param(Some("key=abc&x=1"), "key")` -> `Some("abc")`.
/// Not URL-decoding beyond the basic case — tokens are opaque hex
/// strings in this codebase (see `generate_token` in api/routes.rs), so
/// there's nothing in a real token that would need percent-decoding.
fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

pub async fn auth_middleware(
    State(db): State<Arc<Database>>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> Response {
    let header_key = headers.get("x-api-key").and_then(|v| v.to_str().ok());
    let query_key = header_key.is_none().then(|| query_param(req.uri().query(), "key")).flatten();

    let provided = match header_key.or(query_key.as_deref()) {
        Some(v) => v,
        None => {
            return (StatusCode::UNAUTHORIZED, "missing x-api-key header (or ?key= for SSE)").into_response();
        }
    };

    let identity = if let Some((owner, role)) = static_token_map().get(provided) {
        Some(AuthIdentity { owner: owner.clone(), role: *role })
    } else {
        let hash = hash_token(provided);
        let engine = db.engine.read().expect("storage engine lock poisoned");
        engine
            .find_user_by_hash(&hash)
            .map(|record| AuthIdentity { owner: record.owner.clone(), role: record.role })
    };

    match identity {
        Some(identity) => {
            req.extensions_mut().insert(identity);
            next.run(req).await
        }
        None => (StatusCode::UNAUTHORIZED, "invalid x-api-key").into_response(),
    }
}
