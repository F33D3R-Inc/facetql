//! Rendering helpers for read commands.
//!
//! Every read command supports two shapes: a human-readable default and,
//! under the global `--json` flag, the raw JSON exactly as the server
//! returned it (pretty-printed) so it can be piped into `jq` or another
//! program. Keeping the shaping here — pure `Value -> String` functions
//! with no I/O — is what lets the tests assert on output without a live
//! server.

use serde_json::Value;

/// Pretty-print any JSON value. Used by every `--json` path.
pub fn json_pretty(value: &Value) -> String {
    // Pretty rather than compact: a human ran a CLI command, and the
    // machine-readable consumers (jq et al.) parse either form fine.
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

/// Render a single node object (as returned by `GET /node/:address`) in
/// the human-readable default form.
pub fn render_node(node: &Value) -> String {
    let field = |k: &str| node.get(k).and_then(Value::as_str).unwrap_or("-");
    let mut out = String::new();
    out.push_str(&format!("address:    {}\n", field("address")));
    out.push_str(&format!("kind:       {}\n", field("kind")));
    out.push_str(&format!("owner:      {}\n", field("owner")));
    out.push_str(&format!("visibility: {}\n", visibility(node)));
    out.push_str(&format!("claimed_by: {}\n", field("claimed_by")));
    if let Some(coord) = node.get("coordinate") {
        out.push_str(&format!("coordinate: {}\n", coord));
    }
    out.push_str(&format!("data:       {}", field("data")));
    out
}

/// Render a list of nodes (as returned by `GET /nodes` or the `nodes`
/// array of `POST /nodes/query`) in the human-readable default form.
pub fn render_nodes(nodes: &[Value]) -> String {
    if nodes.is_empty() {
        return "(no rows)".to_string();
    }
    let mut out = String::new();
    for (i, node) in nodes.iter().enumerate() {
        if i > 0 {
            out.push_str("\n---\n");
        }
        out.push_str(&render_node(node));
    }
    out.push_str(&format!("\n\n{} row(s)", nodes.len()));
    out
}

/// Render the user list (`GET /admin/users`) as an aligned table.
pub fn render_users(users: &[Value]) -> String {
    if users.is_empty() {
        return "(no users)".to_string();
    }
    let owner_width = users
        .iter()
        .filter_map(|u| u.get("owner").and_then(Value::as_str))
        .map(str::len)
        .max()
        .unwrap_or(5)
        .max(5);

    let mut out = format!("{:<width$}  ROLE\n", "OWNER", width = owner_width);
    for user in users {
        let owner = user.get("owner").and_then(Value::as_str).unwrap_or("-");
        let role = role_str(user);
        out.push_str(&format!("{:<width$}  {}\n", owner, role, width = owner_width));
    }
    out.push_str(&format!("\n{} user(s)", users.len()));
    out
}

/// Render the index list (`GET /admin/indexes`) as an aligned table.
///
/// Four columns rather than the one-record-per-block form
/// [`render_node`] uses: an index definition is a few short scalars, and
/// the question an operator asks of this list — "is the field I'm
/// ordering by, or searching, covered?" — is answered by scanning a
/// column, which only works if the columns line up.
///
/// `MODE` says which question the index answers: `ordered` for the
/// B+tree over whole values, `text` for the inverted index over
/// substrings. A row from a server that predates the distinction has no
/// `mode` and is shown as `ordered`, which is what it is — the only kind
/// such a server has.
pub fn render_indexes(indexes: &[Value]) -> String {
    if indexes.is_empty() {
        return "(no indexes declared)".to_string();
    }
    let cell = |index: &Value, key: &str, fallback: &'static str| {
        index
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or(fallback)
            .to_string()
    };
    let width = |header: &str, key: &str, fallback: &'static str| {
        indexes
            .iter()
            .map(|i| cell(i, key, fallback).len())
            .max()
            .unwrap_or(0)
            .max(header.len())
    };
    let name_width = width("NAME", "name", "-");
    let kind_width = width("KIND", "kind", "-");
    let field_width = width("FIELD", "field", "-");

    let mut out = format!(
        "{:<name_width$}  {:<kind_width$}  {:<field_width$}  MODE\n",
        "NAME", "KIND", "FIELD"
    );
    for index in indexes {
        out.push_str(&format!(
            "{:<name_width$}  {:<kind_width$}  {:<field_width$}  {}\n",
            cell(index, "name", "-"),
            cell(index, "kind", "-"),
            cell(index, "field", "-"),
            cell(index, "mode", "ordered"),
        ));
    }
    out.push_str(&format!("\n{} index(es)", indexes.len()));
    out
}

/// The declared references, as a table.
///
/// Renders the rule as one arrow — `Comment.post → Post` — because that
/// is what an operator is checking: which way it points and what a
/// delete of the right-hand side does to the left. Splitting those
/// across four columns makes the reader reassemble the sentence.
pub fn render_references(references: &[Value]) -> String {
    if references.is_empty() {
        return "(no references declared)".to_string();
    }

    let field = |r: &Value, k: &str| {
        r.get(k).and_then(Value::as_str).unwrap_or("-").to_string()
    };

    let rule = |r: &Value| {
        let parent = match r.get("parent_field").and_then(Value::as_str) {
            Some(f) => format!("{}.{f}", field(r, "parent_kind")),
            None => field(r, "parent_kind"),
        };

        format!("{}.{} -> {parent}", field(r, "kind"), field(r, "field"))
    };

    let width = |header: &str, cell: &dyn Fn(&Value) -> String| {
        references
            .iter()
            .map(|r| cell(r).len())
            .max()
            .unwrap_or(0)
            .max(header.len())
    };

    let name_width = width("NAME", &|r: &Value| field(r, "name"));
    let rule_width = width("REFERENCE", &rule);

    let mut out = format!(
        "{:<name_width$}  {:<rule_width$}  ON DELETE\n",
        "NAME", "REFERENCE"
    );

    for reference in references {
        out.push_str(&format!(
            "{:<name_width$}  {:<rule_width$}  {}\n",
            field(reference, "name"),
            rule(reference),
            field(reference, "on_delete"),
        ));
    }

    out.push_str(&format!("\n{} reference(s)", references.len()));
    out
}

fn visibility(node: &Value) -> String {
    match node.get("visibility") {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "-".to_string(),
    }
}

fn role_str(user: &Value) -> String {
    match user.get("role") {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "-".to_string(),
    }
}

/// Render the authorization matrix (`api::routes::ROUTES`) as an aligned
/// table.
///
/// # Why the table is printed at all
///
/// It is the answer to "who can call what", and until now that answer
/// existed only as whatever each handler happened to do — which is how
/// two endpoints came to disagree with their own siblings about
/// ownership without anyone noticing. An operator reviewing a deployment
/// cannot read twenty-four handlers; they can read this.
///
/// It also gives the matrix a consumer in the shipped binary rather than
/// only in the test build. That matters more than it sounds: a table
/// that exists solely for tests is one refactor away from being deleted
/// as dead weight, and the guarantee would go with it.
///
/// Offline by construction — this prints what this build enforces, so it
/// needs no server, no token and no network.
pub fn render_routes(routes: &[crate::api::routes::RouteSpec]) -> String {
    use crate::api::routes::Access;

    let width = |f: fn(&crate::api::routes::RouteSpec) -> usize| {
        routes.iter().map(f).max().unwrap_or(0)
    };

    let method_width = width(|r| r.method.len()).max("METHOD".len());
    let path_width = width(|r| r.path.len()).max("PATH".len());
    let access_width = "CALLER".len().max("authenticated".len());

    let mut out = format!(
        "{:<method_width$}  {:<path_width$}  {:<access_width$}  {:<9}  RULE\n",
        "METHOD", "PATH", "CALLER", "COST"
    );

    for spec in routes {
        let access = match spec.access {
            Access::Anonymous => "anonymous",
            Access::Authenticated => "authenticated",
            Access::AdminOnly => "admin",
        };

        // The per-object rule is a sentence; collapse the source's line
        // wrapping so each route stays on one row.
        let rule = spec.objects.split_whitespace().collect::<Vec<_>>().join(" ");

        out.push_str(&format!(
            "{:<method_width$}  {:<path_width$}  {:<access_width$}  {:<9}  {rule}\n",
            spec.method,
            spec.path,
            access,
            format!("{:?}", spec.class).to_lowercase(),
        ));
    }

    out.push_str(&format!("\n{} route(s)", routes.len()));
    out

}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_pretty_is_valid_json() {
        let v = json!({"a": 1, "b": [true, null]});
        let s = json_pretty(&v);
        let round: Value = serde_json::from_str(&s).expect("valid json");
        assert_eq!(round, v);
    }

    #[test]
    fn render_node_includes_key_fields() {
        let node = json!({
            "address": "pg_client_1",
            "kind": "Client",
            "owner": "alice",
            "visibility": "Private",
            "claimed_by": null,
            "data": "{\"name\":\"Acme\"}"
        });
        let out = render_node(&node);
        assert!(out.contains("address:    pg_client_1"));
        assert!(out.contains("kind:       Client"));
        assert!(out.contains("owner:      alice"));
        assert!(out.contains("visibility: Private"));
        assert!(out.contains("data:       {\"name\":\"Acme\"}"));
    }

    #[test]
    fn render_nodes_empty_is_no_rows() {
        assert_eq!(render_nodes(&[]), "(no rows)");
    }

    #[test]
    fn render_nodes_counts_rows() {
        let nodes = vec![
            json!({"address": "a", "kind": "K", "owner": "o"}),
            json!({"address": "b", "kind": "K", "owner": "o"}),
        ];
        let out = render_nodes(&nodes);
        assert!(out.contains("2 row(s)"));
        assert!(out.contains("---"));
    }

    #[test]
    fn render_users_aligns_and_counts() {
        let users = vec![
            json!({"owner": "alice", "role": "User"}),
            json!({"owner": "root", "role": "Admin"}),
        ];
        let out = render_users(&users);
        assert!(out.contains("OWNER"));
        assert!(out.contains("alice"));
        assert!(out.contains("Admin"));
        assert!(out.contains("2 user(s)"));
    }

    #[test]
    fn render_users_empty() {
        assert_eq!(render_users(&[]), "(no users)");
    }

    #[test]
    fn render_indexes_aligns_and_counts() {
        let indexes = vec![
            json!({"name": "post_created", "kind": "Post", "field": "created_at",
                   "mode": "ordered"}),
            json!({"name": "s_exp", "kind": "__session", "field": "_expires_unix",
                   "mode": "ordered"}),
        ];
        let out = render_indexes(&indexes);
        assert!(out.contains("NAME"));
        assert!(out.contains("KIND"));
        assert!(out.contains("FIELD"));
        assert!(out.contains("post_created"));
        assert!(out.contains("_expires_unix"));
        assert!(out.contains("2 index(es)"));
        // Columns are padded to the widest cell, header included, so the
        // KIND column starts at the same offset on every line.
        let lines: Vec<&str> = out.lines().collect();
        let kind_col = lines[0].find("KIND").expect("header has a KIND column");
        assert!(lines[1][kind_col..].starts_with("Post"));
        assert!(lines[2][kind_col..].starts_with("__session"));
    }

    /// A row from a server that does not report a mode is an ordered
    /// index, because that is the only kind such a server has.
    #[test]
    fn render_indexes_defaults_a_missing_mode_to_ordered() {
        let indexes = vec![
            json!({"name": "post_created", "kind": "Post", "field": "created_at"}),
            json!({"name": "post_body", "kind": "Post", "field": "body",
                   "mode": "text"}),
        ];
        let out = render_indexes(&indexes);
        assert!(out.contains("MODE"));

        let lines: Vec<&str> = out.lines().collect();
        let mode_col = lines[0].find("MODE").expect("header has a MODE column");
        assert!(lines[1][mode_col..].starts_with("ordered"));
        assert!(lines[2][mode_col..].starts_with("text"));
    }

    #[test]
    fn render_indexes_empty() {
        assert_eq!(render_indexes(&[]), "(no indexes declared)");
    }

    #[test]
    fn render_references_shows_the_rule_as_one_arrow() {
        let references = vec![
            json!({"name": "post_comments", "kind": "Comment", "field": "post",
                   "parent_kind": "Post", "parent_field": null,
                   "on_delete": "cascade"}),
            json!({"name": "author", "kind": "Post", "field": "author_id",
                   "parent_kind": "User", "parent_field": "id",
                   "on_delete": "restrict"}),
        ];

        let out = render_references(&references);

        assert!(out.contains("Comment.post -> Post"));
        // A reference by data field says which field it resolves
        // through; one by address does not, because there is nothing to
        // name.
        assert!(out.contains("Post.author_id -> User.id"));
        assert!(out.contains("cascade"));
        assert!(out.contains("restrict"));
        assert!(out.contains("2 reference(s)"));

        let lines: Vec<&str> = out.lines().collect();
        let column = lines[0].find("ON DELETE").expect("header column");
        assert!(lines[1][column..].starts_with("cascade"));
    }

    #[test]
    fn render_references_empty() {
        assert_eq!(render_references(&[]), "(no references declared)");
    }

    /// The matrix renders every route, and renders each one on a single
    /// line — a rule that wraps in the source would otherwise smear one
    /// route across several rows and make the table unreadable exactly
    /// where the long rules are.
    #[test]
    fn routes_render_one_line_each() {
        let rendered = render_routes(crate::api::routes::ROUTES);
        let lines: Vec<&str> = rendered.lines().collect();

        // Header + one line per route + blank + count.
        assert_eq!(lines.len(), crate::api::routes::ROUTES.len() + 3);

        assert!(lines[0].starts_with("METHOD"));

        for spec in crate::api::routes::ROUTES {
            assert!(
                lines
                    .iter()
                    .any(|line| line.starts_with(spec.method)
                        && line.contains(spec.path)),
                "{} {} is missing from the rendered matrix",
                spec.method,
                spec.path
            );
        }

        assert!(rendered.ends_with(&format!(
            "{} route(s)",
            crate::api::routes::ROUTES.len()
        )));
    }
}
