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
}
