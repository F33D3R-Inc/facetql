//! The `facetql` operator CLI: an HTTP client over the server's existing
//! API that lets an admin manage identities and read/write/query data
//! from the terminal, so a downloaded FacetQL is fully operable without a
//! separate tool.
//!
//! Everything here is a client of a *running* `facetql start` — see
//! `client.rs` for why, and `storage::lock` for the single-writer rule it
//! follows from. No command in this module touches storage directly or
//! adds a server endpoint; each maps one-to-one onto a route that already
//! exists in `api/routes.rs`.

mod client;
mod error;
mod output;

use clap::{Args, Subcommand};
use std::io::{self, Write};

use crate::storage::index::MAX_INDEX_NAME_LEN;

pub use error::CliError;

use client::FacetClient;

/// Connection + output options shared by every client subcommand.
///
/// Flattened into each leaf command rather than made a single global on
/// the top-level parser: a global `--token` would collide with the
/// pre-existing `import` subcommand's own `--token`, and per-leaf flags
/// keep this module from having to reach into `main`'s arg tree.
#[derive(Args, Clone, Debug)]
pub struct ClientArgs {
    /// FacetQL server URL. Falls back to FACETQL_URL, then localhost.
    #[arg(long, env = "FACETQL_URL", default_value = "http://localhost:8080")]
    pub url: String,

    /// Admin API token (x-api-key). Falls back to FACETQL_TOKEN.
    /// Prefer the env var so the token never lands in shell history.
    #[arg(long, env = "FACETQL_TOKEN")]
    pub token: Option<String>,

    /// Emit raw JSON (as the server returned it) instead of the
    /// human-readable default. Applies to read commands.
    #[arg(long)]
    pub json: bool,
}

impl ClientArgs {
    fn require_token(&self) -> Result<&str, CliError> {
        match self.token.as_deref() {
            Some(t) if !t.is_empty() => Ok(t),
            _ => Err(CliError::MissingToken),
        }
    }

    fn client(&self) -> Result<FacetClient, CliError> {
        Ok(FacetClient::new(&self.url, self.require_token()?))
    }
}

/// `facetql user <action>` — identity administration (admin only).
#[derive(Subcommand, Debug)]
pub enum UserAction {
    /// Create an identity and print its generated token exactly once.
    Create {
        /// Owner name the new token authenticates as.
        owner: String,
        /// Grant the Admin role (bypasses ownership like a superuser).
        #[arg(long)]
        admin: bool,
        #[command(flatten)]
        common: ClientArgs,
    },
    /// List persistent identities.
    List {
        #[command(flatten)]
        common: ClientArgs,
    },
    /// Revoke every persistent record for an owner. Destructive.
    Delete {
        /// Owner whose token(s) to revoke.
        owner: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        common: ClientArgs,
    },
}

/// `facetql index <action>` — secondary-index administration (admin
/// only).
///
/// An index is an operator's declaration that one `data` field of one
/// `kind` is worth keeping in sorted order. It exists to change how
/// `POST /nodes/query` reads: with `order` on an indexed field the
/// server walks an already-ordered access path and stops at `limit`,
/// instead of materializing every matching row and sorting it — which is
/// bounded by FACETQL_MAX_SCAN_ROWS and fails outright past that bound.
/// Nothing about the query's *result* changes, only what it costs, which
/// is why declaring and dropping indexes is an operational act on the
/// control plane and not something an application asks for mid-request.
///
/// Modelled as a nested subcommand rather than three top-level verbs
/// (`index-create`, …) for the same reason `user` is: these three share
/// one noun and one admin-only route family, and grouping them keeps
/// `facetql --help` a list of things you can manage rather than a list of
/// every verb crossed with every noun.
#[derive(Subcommand, Debug)]
pub enum IndexAction {
    /// Declare an index over one `data` field of one kind.
    ///
    /// Backfilling reads every existing node of the kind once, so this
    /// costs the size of the kind — run it when that's affordable, not
    /// blindly in a hot path. Re-declaring the identical index is a
    /// successful no-op, so a setup script may run twice.
    Create {
        /// Index name. Letters, digits, '_' and '-' only (max 64 bytes)
        /// — it becomes a filename.
        name: String,
        /// Node kind the index covers. An index is per-kind because
        /// `data` has no schema across kinds.
        #[arg(long)]
        kind: String,
        /// Top-level `data` field to keep ordered — the same name you
        /// would pass to `query --order`.
        #[arg(long)]
        field: String,
        /// Refuse any write that would give two nodes of this kind the
        /// same value for this field.
        ///
        /// A constraint, not a hint: it is checked inside the writer
        /// lock ahead of the WAL, so two callers racing for one value
        /// cannot both win. Declaring it over data that already holds a
        /// duplicate is refused rather than silently created false —
        /// and it is also what a reference by `--parent-field` resolves
        /// through, since a value two nodes can hold names neither.
        #[arg(long)]
        unique: bool,
        /// Build an inverted index over the field's *text* instead of an
        /// ordered index over its value.
        ///
        /// This is the index a search box needs. Without it,
        /// `contains(field, q)` reads every node of the kind, decodes
        /// its JSON and tests it; with it, the server reads only the
        /// rows whose text holds every three-byte window of `q` and
        /// tests those. The answer is identical either way — the index
        /// narrows which rows are read, it never decides which ones
        /// match — so declaring or dropping it changes only the cost.
        ///
        /// It serves `contains`, `starts_with` and `ends_with`, since
        /// all three are substring tests. It cannot be `--unique`: it
        /// stores windows of a value, not the value.
        #[arg(long)]
        text: bool,
        #[command(flatten)]
        common: ClientArgs,
    },
    /// List every declared index.
    List {
        #[command(flatten)]
        common: ClientArgs,
    },
    /// Drop a declared index, of either kind.
    ///
    /// Never breaks a query: one that was being served by this index
    /// falls back to the scan it used before the index existed — the
    /// materialize-and-sort path for an ordered index, the row-by-row
    /// substring test for a text one. It is still gated behind a
    /// confirmation like the other removals, because rebuilding costs
    /// another full read of the kind.
    Drop {
        /// Name of the index to drop.
        name: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        common: ClientArgs,
    },
}

/// `facetql reference <action>` — referential integrity between kinds
/// (admin only).
///
/// A reference is the operator's declaration that one `data` field of
/// one kind points at another kind, and what deleting the referenced
/// node does to the nodes referencing it. Unlike an index it changes
/// what a write *means*, not what it costs: with a `cascade` declared,
/// deleting a post removes its comments in the same frame, which is the
/// one thing an application cannot do for itself without holding the
/// whole graph in memory and risking a crash between two transactions.
///
/// Admin-only for a reason the index endpoints do not have: a reference
/// decides what a delete does, so an application that could declare one
/// could arrange for another owner's rows to be removed.
#[derive(Subcommand, Debug)]
pub enum ReferenceAction {
    /// Declare a reference between two kinds.
    ///
    /// Refused unless the access paths that make it cheap already exist:
    /// an index over `<kind>.<field>`, and — when `--parent-field` is
    /// given — a *unique* index over `<parent-kind>.<parent-field>`.
    /// Also refused when the data already breaks the rule, because a
    /// constraint that is false the moment it is created is worse than
    /// no constraint.
    Create {
        /// Reference name. Letters, digits, '_' and '-' only (max 64
        /// bytes) — it becomes a URL path segment.
        name: String,
        /// The kind that holds the reference — the child.
        #[arg(long)]
        kind: String,
        /// The `data` field on that kind carrying the referenced node's
        /// key. Null or absent in a row means it references nothing,
        /// which is always allowed.
        #[arg(long)]
        field: String,
        /// The kind being referenced — the parent.
        #[arg(long)]
        parent_kind: String,
        /// The parent's `data` field the value matches. Omit — the
        /// usual case — to reference the parent's address.
        #[arg(long)]
        parent_field: Option<String>,
        /// What deleting the parent does: `cascade` removes the
        /// referencing nodes too, `restrict` refuses the delete while
        /// any remain, `set-null` clears the field and keeps them.
        #[arg(long, value_parser = ["cascade", "restrict", "set-null"])]
        on_delete: String,
        #[command(flatten)]
        common: ClientArgs,
    },
    /// List every declared reference.
    List {
        #[command(flatten)]
        common: ClientArgs,
    },
    /// Drop a declared reference.
    ///
    /// The rows it governed are untouched; what stops is the
    /// enforcement, so a later delete of a referenced node leaves
    /// whatever pointed at it behind. Confirmed like the other removals
    /// because that consequence is silent.
    Drop {
        /// Name of the reference to drop.
        name: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long)]
        yes: bool,
        #[command(flatten)]
        common: ClientArgs,
    },
}

pub async fn run_reference(action: ReferenceAction) -> Result<(), CliError> {
    match action {
        ReferenceAction::Create {
            name,
            kind,
            field,
            parent_kind,
            parent_field,
            on_delete,
            common,
        } => {
            validate_index_name(&name)?;
            validate_kind(&kind)?;
            validate_field(&field)?;
            validate_kind(&parent_kind)?;

            if let Some(parent_field) = &parent_field {
                validate_field(parent_field)?;
            }

            // The wire spells the action snake_case (it is a serde enum
            // on both sides); the flag spells it the way flags are
            // spelled. One translation, here, rather than a wire value
            // shaped by a CLI convention.
            let wire = on_delete.replace('-', "_");

            let def = common
                .client()?
                .create_reference(
                    &name,
                    &kind,
                    &field,
                    &parent_kind,
                    parent_field.as_deref(),
                    &wire,
                )
                .await?;

            if common.json {
                println!("{}", output::json_pretty(&def));
            } else {
                let parent = match &parent_field {
                    Some(f) => format!("{parent_kind}.{f}"),
                    None => parent_kind.clone(),
                };

                println!(
                    "Declared reference {name:?}: {kind}.{field} -> {parent} \
                     on delete {on_delete}."
                );
            }

            Ok(())
        }

        ReferenceAction::List { common } => {
            let references = common.client()?.list_references().await?;

            if common.json {
                println!(
                    "{}",
                    output::json_pretty(&serde_json::Value::Array(references))
                );
            } else {
                println!("{}", output::render_references(&references));
            }

            Ok(())
        }

        ReferenceAction::Drop { name, yes, common } => {
            validate_index_name(&name)?;

            confirm_or_abort(
                yes,
                &format!(
                    "Drop reference {name:?}? Deletes stop cascading \
                     immediately, so rows that referenced a deleted node are \
                     left behind with nothing pointing them out."
                ),
            )?;

            common.client()?.drop_reference(&name).await?;
            println!("Dropped reference {name:?}.");

            Ok(())
        }
    }
}

#[derive(Args, Debug)]
pub struct GetArgs {
    /// Node address to fetch.
    pub address: String,
    #[command(flatten)]
    pub common: ClientArgs,
}

#[derive(Args, Debug)]
pub struct PutArgs {
    /// Node address to write (client-supplied; coordinate defaults to 0).
    pub address: String,
    /// Entity kind, e.g. "Client", "Goal".
    #[arg(long)]
    pub kind: String,
    /// Node payload as a JSON document. Stored opaquely by the server.
    #[arg(long)]
    pub data: String,
    /// Make the node readable by any authenticated identity.
    #[arg(long)]
    pub public: bool,
    #[command(flatten)]
    pub common: ClientArgs,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// Node address to delete.
    pub address: String,
    /// Skip the interactive confirmation prompt.
    #[arg(long)]
    pub yes: bool,
    #[command(flatten)]
    pub common: ClientArgs,
}

#[derive(Args, Debug)]
pub struct QueryArgs {
    /// Entity kind to query (native predicate query; no SQL).
    #[arg(long)]
    pub kind: String,
    /// Maximum rows to return (server caps at 500).
    #[arg(long)]
    pub limit: Option<usize>,
    /// Field within each node's `data` to order by.
    #[arg(long)]
    pub order: Option<String>,
    /// Order descending instead of ascending.
    #[arg(long)]
    pub desc: bool,
    #[command(flatten)]
    pub common: ClientArgs,
}

#[derive(Args, Debug)]
pub struct StatsArgs {
    #[command(flatten)]
    pub common: ClientArgs,
}

// ── input validation ───────────────────────────────────────────────────

/// A node address / owner becomes a path segment (`/node/:address`,
/// `/admin/users/:owner`). Reject anything that would change the route or
/// is obviously not an identifier, before it ever hits the wire.
fn validate_segment(label: &str, value: &str) -> Result<(), CliError> {
    if value.is_empty() {
        return Err(CliError::InvalidInput(format!("{label} must not be empty")));
    }
    if value.chars().any(|c| c == '/' || c.is_whitespace() || c.is_control()) {
        return Err(CliError::InvalidInput(format!(
            "invalid {label} {value:?}: must not contain '/', whitespace, or control characters"
        )));
    }
    Ok(())
}

fn validate_kind(kind: &str) -> Result<(), CliError> {
    if kind.trim().is_empty() {
        return Err(CliError::InvalidInput("kind must not be empty".to_string()));
    }
    Ok(())
}

/// An index name is both a path segment (`/admin/indexes/:name`) and,
/// server-side, part of the index's filename. The engine enforces this
/// same alphabet and length — we check it here too so an obvious typo
/// costs a usage error (exit 2) instead of a round trip and a 400, and
/// so the failure reads as "your argument is wrong" rather than "the
/// server said no".
fn validate_index_name(name: &str) -> Result<(), CliError> {
    if name.is_empty() || name.len() > MAX_INDEX_NAME_LEN {
        return Err(CliError::InvalidInput(format!(
            "index name must be 1..={MAX_INDEX_NAME_LEN} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(CliError::InvalidInput(format!(
            "invalid index name {name:?}: may contain only letters, digits, '_' and '-'"
        )));
    }
    Ok(())
}

/// The indexed field names a top-level key inside each node's `data`.
/// Only emptiness is checkable here: any other JSON key is legal, and
/// whether the field actually exists on a given node is a property of
/// the data, not of the argument.
fn validate_field(field: &str) -> Result<(), CliError> {
    if field.trim().is_empty() {
        return Err(CliError::InvalidInput(
            "field must not be empty".to_string(),
        ));
    }
    Ok(())
}

/// Validate and normalize a `--data` argument: it must be a JSON
/// document. We re-serialize the parsed value so what we send is
/// canonical and we've proven it parses (a malformed blob is a client
/// mistake worth catching before the write).
fn validate_data(data: &str) -> Result<String, CliError> {
    let value: serde_json::Value = serde_json::from_str(data)
        .map_err(|e| CliError::InvalidInput(format!("--data is not valid JSON: {e}")))?;
    Ok(value.to_string())
}

// ── confirmation for destructive commands ──────────────────────────────

/// Pure predicate for an affirmative answer, so the prompt logic is
/// testable without a terminal.
fn is_affirmative(input: &str) -> bool {
    matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

/// Prompt on stderr (so `--json` stdout stays clean) and read one line
/// from stdin. Returns false on EOF/error — declining is the safe default
/// for a destructive action.
fn confirm(prompt: &str) -> bool {
    eprint!("{prompt} [y/N]: ");
    let _ = io::stderr().flush();
    let mut line = String::new();
    match io::stdin().read_line(&mut line) {
        Ok(0) => false,
        Ok(_) => is_affirmative(&line),
        Err(_) => false,
    }
}

/// Gate a destructive action: `--yes` bypasses, otherwise ask.
fn confirm_or_abort(skip: bool, prompt: &str) -> Result<(), CliError> {
    if skip || confirm(prompt) {
        Ok(())
    } else {
        Err(CliError::Aborted)
    }
}

// ── command runners ────────────────────────────────────────────────────

pub async fn run_user(action: UserAction) -> Result<(), CliError> {
    match action {
        UserAction::Create { owner, admin, common } => {
            validate_segment("owner", &owner)?;
            let resp = common.client()?.create_user(&owner, admin).await?;
            if common.json {
                // The one intended reveal: the response includes `token`.
                println!("{}", output::json_pretty(&resp));
            } else {
                let token = resp.get("token").and_then(|v| v.as_str()).unwrap_or("");
                let role = resp
                    .get("role")
                    .map(|r| r.to_string().trim_matches('"').to_string())
                    .unwrap_or_default();
                println!("Created identity {owner:?} (role {role}).");
                println!("token: {token}");
                eprintln!(
                    "This token is shown once and cannot be retrieved again. \
                     Store it now; if lost, revoke and create a new one."
                );
            }
            Ok(())
        }
        UserAction::List { common } => {
            let users = common.client()?.list_users().await?;
            if common.json {
                println!("{}", output::json_pretty(&serde_json::Value::Array(users)));
            } else {
                println!("{}", output::render_users(&users));
            }
            Ok(())
        }
        UserAction::Delete { owner, yes, common } => {
            validate_segment("owner", &owner)?;
            confirm_or_abort(
                yes,
                &format!("Revoke every persistent token for owner {owner:?}?"),
            )?;
            common.client()?.delete_user(&owner).await?;
            println!("Revoked persistent identity {owner:?}.");
            Ok(())
        }
    }
}

pub async fn run_index(action: IndexAction) -> Result<(), CliError> {
    match action {
        IndexAction::Create { name, kind, field, unique, text, common } => {
            validate_index_name(&name)?;
            validate_kind(&kind)?;
            validate_field(&field)?;

            // Refused here rather than round-tripped, for the reason
            // `validate_index_name` is: it is a contradiction the client
            // can see on its own. A text index has no whole value to
            // hold unique.
            if text && unique {
                return Err(CliError::InvalidInput(
                    "--unique cannot be combined with --text: a text index \
                     stores windows of a value, not the value. Declare an \
                     ordered unique index over the same field instead."
                        .to_string(),
                ));
            }

            let mode = if text { "text" } else { "ordered" };

            let def = common
                .client()?
                .create_index(&name, &kind, &field, unique, mode)
                .await?;
            if common.json {
                println!("{}", output::json_pretty(&def));
            } else if text {
                println!("Declared text index {name:?} on {kind}.{field}.");
                // Say the cost out loud: the command has already paid it
                // by the time this prints, and an operator who did not
                // expect a full read of the kind should learn that here
                // rather than from a latency graph.
                eprintln!(
                    "Backfilled from every existing {kind} node. Searching \
                     {field:?} with contains/starts_with/ends_with now reads \
                     the rows whose text holds the query's three-byte windows \
                     instead of every node of the kind."
                );
            } else {
                let rule = if unique { " (unique)" } else { "" };
                println!("Declared index {name:?} on {kind}.{field}{rule}.");
                eprintln!(
                    "Backfilled from every existing {kind} node. Queries on \
                     this kind ordering by {field:?} now read through the \
                     index instead of sorting a full scan."
                );
            }
            Ok(())
        }
        IndexAction::List { common } => {
            let indexes = common.client()?.list_indexes().await?;
            if common.json {
                println!("{}", output::json_pretty(&serde_json::Value::Array(indexes)));
            } else {
                println!("{}", output::render_indexes(&indexes));
            }
            Ok(())
        }
        IndexAction::Drop { name, yes, common } => {
            validate_index_name(&name)?;
            confirm_or_abort(
                yes,
                &format!(
                    "Drop index {name:?}? Queries it served fall back to \
                     sorting a full scan; rebuilding it re-reads the kind."
                ),
            )?;
            common.client()?.drop_index(&name).await?;
            println!("Dropped index {name:?}.");
            Ok(())
        }
    }
}

pub async fn run_get(args: GetArgs) -> Result<(), CliError> {
    validate_segment("address", &args.address)?;
    let node = args.common.client()?.get_node(&args.address).await?;
    if args.common.json {
        println!("{}", output::json_pretty(&node));
    } else {
        println!("{}", output::render_node(&node));
    }
    Ok(())
}

pub async fn run_put(args: PutArgs) -> Result<(), CliError> {
    validate_segment("address", &args.address)?;
    validate_kind(&args.kind)?;
    let data = validate_data(&args.data)?;
    let resp = args
        .common
        .client()?
        .put_node(&args.address, &args.kind, &data, args.public)
        .await?;
    if args.common.json {
        println!("{}", output::json_pretty(&resp));
    } else {
        let address = resp
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or(&args.address);
        println!("Wrote node {address:?} (kind {:?}).", args.kind);
    }
    Ok(())
}

pub async fn run_delete(args: DeleteArgs) -> Result<(), CliError> {
    validate_segment("address", &args.address)?;
    confirm_or_abort(args.yes, &format!("Delete node {:?}?", args.address))?;
    args.common.client()?.delete_node(&args.address).await?;
    println!("Deleted node {:?}.", args.address);
    Ok(())
}

pub async fn run_query(args: QueryArgs) -> Result<(), CliError> {
    validate_kind(&args.kind)?;
    let page = args
        .common
        .client()?
        .query(&args.kind, args.limit, args.order.as_deref(), args.desc)
        .await?;
    if args.common.json {
        println!("{}", output::json_pretty(&page));
    } else {
        let empty = Vec::new();
        let nodes = page.get("nodes").and_then(|v| v.as_array()).unwrap_or(&empty);
        println!("{}", output::render_nodes(nodes));
    }
    Ok(())
}

/// `stats` — count nodes per kind. There is no "enumerate kinds"
/// endpoint, so this drives the real capability that exists: it paginates
/// `GET /nodes` and tallies the `kind` field client-side. Counts reflect
/// what the supplied token can see (an admin token sees every node; a
/// user token sees only its own and public nodes).
pub async fn run_stats(args: StatsArgs) -> Result<(), CliError> {
    use std::collections::BTreeMap;

    let client = args.common.client()?;
    const PAGE: usize = 500; // server's max page size
    let mut offset = 0;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;

    loop {
        let nodes = client.list_nodes(PAGE, offset).await?;
        let batch = nodes.len();
        for node in &nodes {
            let kind = node
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("<unknown>")
                .to_string();
            *counts.entry(kind).or_insert(0) += 1;
            total += 1;
        }
        if batch < PAGE {
            break;
        }
        offset += PAGE;
    }

    if args.common.json {
        let obj: serde_json::Map<String, serde_json::Value> = counts
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect();
        let out = serde_json::json!({ "total": total, "by_kind": obj });
        println!("{}", output::json_pretty(&out));
    } else if counts.is_empty() {
        println!("(no nodes visible to this token)");
    } else {
        let width = counts.keys().map(String::len).max().unwrap_or(4).max(4);
        println!("{:<width$}  COUNT", "KIND", width = width);
        for (kind, count) in &counts {
            println!("{kind:<width$}  {count}");
        }
        println!("\n{total} node(s) across {} kind(s)", counts.len());
    }
    Ok(())
}

/// Render an error to stderr and exit with its policy code. Called by
/// `main` for any client command that returns `Err`.
pub fn report_error(err: CliError) -> ! {
    eprintln!("error: {err}");
    std::process::exit(err.exit_code());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_token_rejects_missing_and_empty() {
        let none = ClientArgs { url: "u".into(), token: None, json: false };
        assert!(matches!(none.require_token(), Err(CliError::MissingToken)));
        let empty = ClientArgs { url: "u".into(), token: Some(String::new()), json: false };
        assert!(matches!(empty.require_token(), Err(CliError::MissingToken)));
        let ok = ClientArgs { url: "u".into(), token: Some("abc".into()), json: false };
        assert_eq!(ok.require_token().unwrap(), "abc");
    }

    #[test]
    fn validate_segment_rules() {
        assert!(validate_segment("address", "pg_client_1").is_ok());
        assert!(validate_segment("address", "").is_err());
        assert!(validate_segment("address", "a/b").is_err());
        assert!(validate_segment("address", "a b").is_err());
        assert!(validate_segment("address", "a\tb").is_err());
    }

    #[test]
    fn validate_kind_rules() {
        assert!(validate_kind("Client").is_ok());
        assert!(validate_kind("").is_err());
        assert!(validate_kind("   ").is_err());
    }

    #[test]
    fn validate_index_name_alphabet_and_length() {
        assert!(validate_index_name("post_created_at").is_ok());
        assert!(validate_index_name("idx-1").is_ok());
        assert!(validate_index_name("").is_err());
        // The name becomes a filename and a path segment, so anything
        // that could be read as a path is rejected outright.
        assert!(validate_index_name("a/b").is_err());
        assert!(validate_index_name("a.b").is_err());
        assert!(validate_index_name("a b").is_err());
        assert!(validate_index_name(&"x".repeat(MAX_INDEX_NAME_LEN)).is_ok());
        assert!(validate_index_name(&"x".repeat(MAX_INDEX_NAME_LEN + 1)).is_err());
    }

    #[test]
    fn validate_index_name_failures_are_usage_errors() {
        // Exit code 2, not 1: a bad name is the operator's mistake and
        // never reaches the wire.
        let err = validate_index_name("bad/name").unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn validate_field_rules() {
        assert!(validate_field("created_at").is_ok());
        assert!(validate_field("").is_err());
        assert!(validate_field("   ").is_err());
    }

    #[test]
    fn validate_data_requires_json() {
        assert!(validate_data("{\"a\":1}").is_ok());
        assert!(validate_data("not json").is_err());
        // Normalizes: parsed then re-serialized.
        let normalized = validate_data("{ \"a\" : 1 }").unwrap();
        assert_eq!(normalized, "{\"a\":1}");
    }

    #[test]
    fn affirmative_answers() {
        for yes in ["y", "Y", "yes", "YES", " yes \n"] {
            assert!(is_affirmative(yes), "{yes:?} should be affirmative");
        }
        for no in ["", "n", "no", "nope", "\n", "sure"] {
            assert!(!is_affirmative(no), "{no:?} should not be affirmative");
        }
    }

    #[test]
    fn confirm_or_abort_yes_flag_bypasses_prompt() {
        // With skip=true, no stdin is read and it must succeed.
        assert!(confirm_or_abort(true, "delete?").is_ok());
    }
}

/// `facetql routes` — print the authorization matrix this build
/// enforces.
///
/// The one command in this module that is *not* a client of a running
/// server: it reports what the binary in your hand will do, which is the
/// question an operator has before they start it, not after. No token,
/// no URL, no network.
pub fn run_routes(json: bool) -> Result<(), CliError> {
    if json {
        let rows: Vec<serde_json::Value> = crate::api::routes::ROUTES
            .iter()
            .map(|spec| {
                serde_json::json!({
                    "method": spec.method,
                    "path": spec.path,
                    "caller": match spec.access {
                        crate::api::routes::Access::Anonymous => "anonymous",
                        crate::api::routes::Access::Authenticated => "authenticated",
                        crate::api::routes::Access::AdminOnly => "admin",
                    },
                    "cost_class": format!("{:?}", spec.class).to_lowercase(),
                    "rule": spec.objects.split_whitespace().collect::<Vec<_>>().join(" "),
                })
            })
            .collect();

        println!("{}", output::json_pretty(&serde_json::Value::Array(rows)));
    } else {
        println!("{}", output::render_routes(crate::api::routes::ROUTES));
    }

    Ok(())
}
