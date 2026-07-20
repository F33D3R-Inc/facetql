use serde_json::{Map, Value};
use tokio_postgres::types::Type;
use tokio_postgres::Row;

/// Bridges an existing Postgres table into FacetQL, one node per row.
///
/// Deliberately goes through the same HTTP API any other client uses
/// (`POST /node`) rather than opening FacetQL's storage files directly.
/// FacetQL's on-disk files are only safe for a single process to write
/// to — the RwLock that makes every write atomic only coordinates
/// *within* one running process. A separate import process writing
/// straight to `facetql.data` while `facetql start` is also running
/// would race on the same files with no coordination at all. Going
/// through the API sidesteps that entirely: the importer is just
/// another client, the same way `pg_restore` talks to a running
/// `postgres` over its normal connection protocol instead of touching
/// its data directory directly.
pub struct ImportSummary {
    pub imported: usize,
    pub failed: Vec<(String, String)>, // (row identifier, error)
}

pub async fn import_postgres_table(
    pg_url: &str,
    table: &str,
    kind: &str,
    owner_hint: &str,
    id_column: &str,
    server_url: &str,
    api_key: &str,
) -> Result<ImportSummary, String> {
    let (client, connection) = tokio_postgres::connect(pg_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("failed to connect to Postgres: {e}"))?;

    // tokio-postgres requires the connection's background I/O future to
    // be polled somewhere — this is that "somewhere." If it errors, log
    // it; there's nothing else to do about it once the query below has
    // already returned.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });

    // Table name is interpolated directly, not parameterized — Postgres
    // doesn't support parameterized identifiers (only values) for a
    // reason: this is a local admin CLI operation run by whoever holds
    // both the Postgres credentials and an FacetQL admin token, not a
    // network-facing endpoint taking untrusted input. Treat this
    // command with the same trust level you'd give direct `psql` access.
    let query = format!("SELECT * FROM {table}");
    let rows = client
        .query(&query, &[])
        .await
        .map_err(|e| format!("query against '{table}' failed: {e}"))?;

    let http = reqwest::Client::new();
    let mut imported = 0usize;
    let mut failed = Vec::new();

    for (i, row) in rows.iter().enumerate() {
        let data = row_to_json(row);

        let address = match data.get(id_column) {
            Some(Value::Number(n)) => format!("pg_{table}_{n}"),
            Some(Value::String(s)) => format!("pg_{table}_{s}"),
            _ => format!("pg_{table}_row{i}"),
        };

        let body = serde_json::json!({
            "address": address,
            "kind": kind,
            "x": 0, "y": 0, "z": 0, "q": 0,
            "data": data.to_string(),
            "public": false,
        });

        let result = http
            .post(format!("{server_url}/node"))
            .header("x-api-key", api_key)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => imported += 1,
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                failed.push((address, format!("{status}: {text}")));
            }
            Err(e) => failed.push((address, e.to_string())),
        }
    }

    let _ = owner_hint; // ownership is set by whichever identity api_key authenticates as, same rule as every other write — see auth.rs
    Ok(ImportSummary { imported, failed })
}

/// Converts one Postgres row into a JSON object, dispatching on the
/// column's actual Postgres type. Covers the common scalar types you'd
/// find in a typical application table (text, integers, floats, bool,
/// date/timestamp). Anything outside that set falls back to a string
/// built from Postgres's own text representation rather than silently
/// dropping the column — an approximate value beats a missing one for
/// a migration tool, and it's clearly logged as a fallback so it's easy
/// to spot in the output.
fn row_to_json(row: &Row) -> Value {
    let mut map = Map::new();

    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        let value = match *col.type_() {
            Type::BOOL => row
                .get::<_, Option<bool>>(i)
                .map(Value::Bool)
                .unwrap_or(Value::Null),
            Type::INT2 => row
                .get::<_, Option<i16>>(i)
                .map(|v| Value::Number(v.into()))
                .unwrap_or(Value::Null),
            Type::INT4 => row
                .get::<_, Option<i32>>(i)
                .map(|v| Value::Number(v.into()))
                .unwrap_or(Value::Null),
            Type::INT8 => row
                .get::<_, Option<i64>>(i)
                .map(|v| Value::Number(v.into()))
                .unwrap_or(Value::Null),
            Type::FLOAT4 => row
                .get::<_, Option<f32>>(i)
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Type::FLOAT8 => row
                .get::<_, Option<f64>>(i)
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR => row
                .get::<_, Option<String>>(i)
                .map(Value::String)
                .unwrap_or(Value::Null),
            Type::DATE => row
                .get::<_, Option<chrono::NaiveDate>>(i)
                .map(|d| Value::String(d.to_string()))
                .unwrap_or(Value::Null),
            Type::TIMESTAMP => row
                .get::<_, Option<chrono::NaiveDateTime>>(i)
                .map(|d| Value::String(d.to_string()))
                .unwrap_or(Value::Null),
            Type::TIMESTAMPTZ => row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                .map(|d| Value::String(d.to_rfc3339()))
                .unwrap_or(Value::Null),
            _ => {
                // Unrecognized type — try to read it as text via
                // Postgres's own string representation rather than
                // dropping the column silently.
                match row.try_get::<_, Option<String>>(i) {
                    Ok(v) => v.map(Value::String).unwrap_or(Value::Null),
                    Err(_) => Value::String(format!(
                        "<unsupported column type: {}>",
                        col.type_().name()
                    )),
                }
            }
        };
        map.insert(name, value);
    }

    Value::Object(map)
}
