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

pub const TOKENS_ENV: &str = "ENOCHIAN_TOKENS";

/// Why the bootstrap credential store must not be used as configured.
///
/// [`static_token_map`] has always had a fallback — an admin identity
/// under a token whose value is published in this file, in the README
/// and in every copy of this source — and it has always announced that
/// fallback with a `warning:` on stderr and served traffic anyway. That
/// is not a control. It is invisible to a supervisor that only captures
/// stdout, it scrolls past in the first seconds of a busy start, and no
/// code downstream behaves differently because of it. A server that
/// took the fallback is a server anybody on the network is an
/// administrator of.
///
/// This type is what lets that be a *refusal* instead. It reports the
/// finding; the deployment posture (`config::deployment`) decides
/// whether the finding is fatal, and `main` renders it. Splitting it
/// that way keeps the knowledge of "what counts as a development
/// credential" in the module that owns the credentials, rather than
/// spreading a string constant into a start-up check that would then
/// have to be kept in step with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialDefect {
    /// `ENOCHIAN_TOKENS` is unset, so the published dev admin token is
    /// the only credential this server accepts.
    NoTokensConfigured,

    /// `ENOCHIAN_TOKENS` is set but nothing in it parsed into a usable
    /// `token:owner[:role]` entry — a trailing comma, a missing colon, a
    /// value that survived a shell mangling. The map ends up empty,
    /// which is the same outcome as not setting it at all except that it
    /// *looks* configured.
    TokensConfiguredButEmpty,

    /// A configured entry hands out the published dev token. Setting
    /// `ENOCHIAN_TOKENS` does not help if the token inside it is the one
    /// printed in this file.
    DevTokenConfigured,
}

impl std::fmt::Display for CredentialDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialDefect::NoTokensConfigured => write!(
                f,
                "{TOKENS_ENV} is not set, so the only credential this server \
                 would accept is the built-in development admin token — a \
                 value published in this source file"
            ),

            CredentialDefect::TokensConfiguredButEmpty => write!(
                f,
                "{TOKENS_ENV} is set but no entry in it parsed as \
                 `token:owner[:admin]`, which leaves the credential store \
                 empty and falls back to the built-in development admin token"
            ),

            CredentialDefect::DevTokenConfigured => write!(
                f,
                "{TOKENS_ENV} hands out the built-in development admin \
                 token, whose value is published in this source file"
            ),
        }
    }
}

/// What is wrong with the bootstrap credentials, if anything.
///
/// Deliberately reads the environment directly rather than inspecting
/// [`static_token_map`]. The map is a `OnceLock` that prints its warning
/// on first use, and a pre-flight check must be able to run *before*
/// anything has authenticated without itself being the thing that
/// initialises — and silently blesses — the fallback.
pub fn credential_defect() -> Option<CredentialDefect> {
    credential_defect_in(std::env::var(TOKENS_ENV).ok().as_deref())
}

/// The judgement itself, separated from where the string came from.
///
/// An environment variable is process-wide state shared by every test in
/// the binary, so a test that set one would be testing the harness as
/// much as the rule. This split is the same one `limits::parse_rate_from`
/// makes, for the same reason.
fn credential_defect_in(raw: Option<&str>) -> Option<CredentialDefect> {
    let raw = match raw {
        Some(raw) => raw,
        None => return Some(CredentialDefect::NoTokensConfigured),
    };

    let entries = parse_token_entries(raw);

    if entries.is_empty() {
        return Some(CredentialDefect::TokensConfiguredButEmpty);
    }

    if entries.iter().any(|(token, ..)| token == DEV_TOKEN) {
        return Some(CredentialDefect::DevTokenConfigured);
    }

    None
}

/// One parse of `ENOCHIAN_TOKENS`, shared by the credential store and
/// the pre-flight check above.
///
/// It matters that there is exactly one. The check's whole claim is
/// "the store you are about to build is unsafe", and a second parser
/// that disagreed with the first — about a trailing comma, about
/// whitespace, about how many colons make a role — would make that
/// claim false in precisely the cases nobody tests.
///
/// Returns the plaintext token deliberately: the caller that builds the
/// store immediately hashes it and drops it, and the caller that checks
/// the configuration has to compare against a known value. Neither
/// retains it.
fn parse_token_entries(raw: &str) -> Vec<(String, String, Role)> {
    raw.split(',')
        .filter_map(|entry| {
            let mut parts = entry.splitn(3, ':');
            let token = parts.next()?.trim().to_string();
            let owner = parts.next()?.trim().to_string();
            let role = match parts.next().map(str::trim) {
                Some("admin") => Role::Admin,
                _ => Role::User,
            };

            (!token.is_empty() && !owner.is_empty()).then_some((token, owner, role))
        })
        .collect()
}

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
///
/// # Keyed by hash, not by the token
///
/// The map is keyed by `hash_token(token)` and looked up with the hash
/// of what the request presented, so the bootstrap path never compares a
/// caller-supplied string against a secret. That matters for two
/// reasons, and neither is theoretical enough to skip:
///
/// * **Comparison time.** `String` equality short-circuits at the first
///   differing byte, and a `HashMap` probe compares the key it lands on.
///   Comparing digests instead makes the work independent of how much of
///   the real token a guess got right — an attacker would have to invert
///   SHA-256 to steer it.
///
/// * **One shape for both credential stores.** The persistent user store
///   already holds hashes and is already looked up by hash. Keeping the
///   bootstrap map in plaintext meant two different comparisons on the
///   same code path, one of which would have had to be remembered
///   separately every time this file changed.
///
/// The plaintext token is still read from the environment — it has to
/// be, an operator types it — but it is hashed once at first use and the
/// plaintext is not retained past that.
fn static_token_map() -> &'static HashMap<String, (String, Role)> {
    static MAP: OnceLock<HashMap<String, (String, Role)>> = OnceLock::new();
    MAP.get_or_init(|| {
        match std::env::var(TOKENS_ENV) {
            Ok(raw) => parse_token_entries(&raw)
                .into_iter()
                .map(|(token, owner, role)| (hash_token(&token), (owner, role)))
                .collect(),
            Err(_) => {
                eprintln!(
                    "warning: ENOCHIAN_TOKENS not set — using a single dev token \
                     ('{DEV_TOKEN}' -> owner '{DEV_OWNER}', role Admin) so there's a way \
                     to bootstrap the first real admin via POST /admin/users. \
                     Do not run production traffic against this."
                );
                HashMap::from([(
                    hash_token(DEV_TOKEN),
                    (DEV_OWNER.to_string(), Role::Admin),
                )])
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

    // Hash once, then look the digest up in both credential stores. The
    // presented secret is never compared byte-for-byte against a stored
    // one on either path.
    let hash = hash_token(provided);

    let identity = if let Some((owner, role)) = static_token_map().get(&hash) {
        Some(AuthIdentity { owner: owner.clone(), role: *role })
    } else {
        let engine = db.engine();
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

#[cfg(test)]
mod credential_posture_tests {
    //! What counts as a development credential. Each case here is one
    //! the production pre-flight in `main` must refuse, and the last one
    //! is the case it must not.
    use super::*;

    #[test]
    fn an_unset_variable_is_a_defect() {
        assert_eq!(
            credential_defect_in(None),
            Some(CredentialDefect::NoTokensConfigured)
        );
    }

    /// The case that looks configured and is not: a value that parses to
    /// nothing leaves the store empty, which falls back to the dev token
    /// exactly as an unset variable does.
    #[test]
    fn a_variable_that_parses_to_nothing_is_a_defect() {
        for raw in ["", "   ", ",,,", "no-colon-here", ":owner", "token:"] {
            assert_eq!(
                credential_defect_in(Some(raw)),
                Some(CredentialDefect::TokensConfiguredButEmpty),
                "{raw:?} should have been recognised as an empty store"
            );
        }
    }

    /// Setting the variable does not help if the token inside it is the
    /// published one.
    #[test]
    fn configuring_the_dev_token_is_still_a_defect() {
        assert_eq!(
            credential_defect_in(Some(&format!("{DEV_TOKEN}:root:admin"))),
            Some(CredentialDefect::DevTokenConfigured)
        );

        // …including when it is hiding among real entries.
        assert_eq!(
            credential_defect_in(Some(&format!("realtoken:alice,{DEV_TOKEN}:root:admin"))),
            Some(CredentialDefect::DevTokenConfigured)
        );
    }

    #[test]
    fn a_real_credential_is_not_a_defect() {
        assert_eq!(
            credential_defect_in(Some("9f2c4a:root:admin,7b1e:alice")),
            None
        );
    }

    /// The store and the check must parse identically, or the check
    /// describes a store that was never built. One function, exercised
    /// from both directions.
    #[test]
    fn the_check_and_the_store_agree_on_what_parses() {
        let entries = parse_token_entries("a:alice,b:bob:admin, c : carol ,,d");

        assert_eq!(
            entries,
            vec![
                ("a".to_string(), "alice".to_string(), Role::User),
                ("b".to_string(), "bob".to_string(), Role::Admin),
                ("c".to_string(), "carol".to_string(), Role::User),
            ]
        );
    }
}
