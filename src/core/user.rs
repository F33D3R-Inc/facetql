use serde::{Deserialize, Serialize};

/// Two roles for v0.6. Not trying to match Postgres's full GRANT
/// system (per-table, per-column privileges) — that's a much bigger
/// feature. This is closer to "regular user vs. superuser": an Admin
/// bypasses ownership checks entirely (see Node::can_read/can_write
/// callers in api/routes.rs), same shape as a Postgres superuser
/// bypassing row-level security by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    User,
    Admin,
}

/// A persistent, admin-manageable identity — the durable counterpart
/// to the static FACETQL_TOKENS env var. The env var is still how you
/// get the *first* admin in (bootstrap problem: something has to exist
/// before there's an admin to create more users), exactly the role
/// POSTGRES_PASSWORD/POSTGRES_USER play for a fresh Postgres Docker
/// container. Every user created *after* that goes through
/// `POST /admin/users` and lives here instead.
///
/// The plaintext token is never stored — only its hash. It's shown to
/// the caller exactly once, at creation, in the API response — same
/// pattern as a GitHub personal access token or an AWS IAM access key.
/// If it's lost, the fix is revoke and create a new one, not "look it
/// up."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub token_hash: String,
    pub owner: String,
    pub role: Role,
}
