use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Wire-compatible mirror of FCT's `ir.Expr` (internal/ir/ir.go).
///
/// This is deliberately the *whole* shape FCT can serialize, not just
/// the pushable subset — so a client can send exactly what it already
/// has in memory without picking fields apart first. `eval` rejects
/// anything outside the subset FCT's own SQL compiler pushes down
/// (`exprSQL` in runtime/sql.go), with the same rejection semantics:
/// a clear error naming what couldn't be evaluated, so the caller can
/// fall back to loading rows and filtering client-side rather than
/// getting a wrong answer.
///
/// Field names match `ir.Expr`'s JSON tags exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Expr {
    pub kind: String,

    #[serde(default)]
    pub val: Option<Value>,

    #[serde(default, rename = "vtype")]
    pub vtype: Option<String>,

    #[serde(default)]
    pub name: Option<String>,

    #[serde(default)]
    pub field: Option<String>,

    #[serde(default)]
    pub obj: Option<Box<Expr>>,

    #[serde(default)]
    pub key: Option<Box<Expr>>,

    #[serde(default)]
    pub op: Option<String>,

    #[serde(default)]
    pub args: Option<Vec<Expr>>,

    #[serde(default)]
    pub l: Option<Box<Expr>>,

    #[serde(default)]
    pub r: Option<Box<Expr>>,

    #[serde(default)]
    pub x: Option<Box<Expr>>,

    #[serde(default, rename = "var")]
    pub var: Option<String>,

    #[serde(default, rename = "where")]
    pub where_: Option<Box<Expr>>,
}

/// Evaluate a pushable predicate against one node's decoded `data`.
///
/// `item_var` is the loop variable name the predicate's `get` nodes are
/// expected to reference (mirrors `Query.ItemVar` / `exprSQL`'s
/// `itemVar` parameter) — a `get` whose object isn't `ref(item_var)` is
/// rejected, same as FCT's compiler does, because FacetQL has nothing
/// to push it down to either.
///
/// Returns a JSON `Value` so callers can chain comparisons the same way
/// SQL does (`(a + b) > c`); top-level callers should expect a `Bool`.
pub fn eval(expr: &Expr, item_var: &str, data: &Value) -> Result<Value, String> {
    eval_at(expr, item_var, data, 0)
}

/// Deepest a predicate may nest.
///
/// `Expr` is a tree that arrives from the wire, and `eval` walks it
/// recursively, so its depth is the recursion depth of this thread. A
/// stack overflow is not a caught error — it aborts the whole process,
/// taking every other in-flight request with it — which makes this the
/// one bound that must not be left to a dependency's default.
///
/// `serde_json` does happen to stop at its own nesting limit today, so
/// this is not a live hole. It is also not a guarantee anyone stated:
/// it is a constant inside another crate, it protects deserialization
/// rather than evaluation, and nothing would notice if a future version
/// raised it or if a predicate ever reached `eval` by another route
/// (a `delete_where` built in-process, say). Stating the bound here
/// makes it a property of this evaluator instead of an inherited
/// accident.
///
/// 64 is far past any predicate a compiler emits — FCT's own expressions
/// nest a handful of levels — and shallow enough to be safe on a small
/// async task stack.
const MAX_PREDICATE_DEPTH: usize = 64;

fn eval_at(
    expr: &Expr,
    item_var: &str,
    data: &Value,
    depth: usize,
) -> Result<Value, String> {
    if depth >= MAX_PREDICATE_DEPTH {
        return Err(format!(
            "predicate nests deeper than {MAX_PREDICATE_DEPTH} levels; \
             simplify it or split the query"
        ));
    }

    let eval = |e: &Expr| eval_at(e, item_var, data, depth + 1);

    match expr.kind.as_str() {
        "lit" => Ok(lit_value(expr)),

        "get" => {
            let obj = expr
                .obj
                .as_deref()
                .ok_or_else(|| "get expression missing obj".to_string())?;

            let is_item_ref = obj.kind == "ref"
                && obj.name.as_deref() == Some(item_var);

            if !is_item_ref {
                return Err(format!(
                    "cannot evaluate field access {:?}: not a reference to {item_var}",
                    expr.field
                ));
            }

            let field = expr
                .field
                .as_deref()
                .ok_or_else(|| "get expression missing field".to_string())?;

            Ok(data.get(field).cloned().unwrap_or(Value::Null))
        }

        "un" => {
            let x = expr
                .x
                .as_deref()
                .ok_or_else(|| "un expression missing x".to_string())?;

            let value = eval(x)?;

            match expr.op.as_deref() {
                Some("!") => Ok(Value::Bool(!truthy(&value))),
                Some("-") => {
                    let n = as_f64(&value)
                        .ok_or_else(|| "cannot negate a non-numeric value".to_string())?;
                    Ok(number_value(-n))
                }
                other => Err(format!(
                    "cannot evaluate unary operator {other:?}"
                )),
            }
        }

        "bin" => {
            let l = expr
                .l
                .as_deref()
                .ok_or_else(|| "bin expression missing l".to_string())?;
            let r = expr
                .r
                .as_deref()
                .ok_or_else(|| "bin expression missing r".to_string())?;

            // Short-circuit && / || the same way SQL AND/OR would in
            // practice, and so a right-hand side that only makes sense
            // when the left-hand side already ruled it out (e.g.
            // `x != null && x.field > 0`) never gets evaluated.
            match expr.op.as_deref() {
                Some("&&") => {
                    let lv = eval(l)?;
                    if !truthy(&lv) {
                        return Ok(Value::Bool(false));
                    }
                    let rv = eval(r)?;
                    Ok(Value::Bool(truthy(&rv)))
                }
                Some("||") => {
                    let lv = eval(l)?;
                    if truthy(&lv) {
                        return Ok(Value::Bool(true));
                    }
                    let rv = eval(r)?;
                    Ok(Value::Bool(truthy(&rv)))
                }
                Some(op) => {
                    let lv = eval(l)?;
                    let rv = eval(r)?;
                    eval_bin_op(op, &lv, &rv)
                }
                None => Err("bin expression missing op".to_string()),
            }
        }

        other => Err(format!("cannot evaluate {other:?} expression")),
    }
}

fn eval_bin_op(op: &str, l: &Value, r: &Value) -> Result<Value, String> {
    match op {
        "==" => Ok(Value::Bool(values_equal(l, r))),
        "!=" => Ok(Value::Bool(!values_equal(l, r))),

        "<" | "<=" | ">" | ">=" => {
            let (lf, rf) = numeric_pair(l, r, op)?;
            let result = match op {
                "<" => lf < rf,
                "<=" => lf <= rf,
                ">" => lf > rf,
                ">=" => lf >= rf,
                _ => unreachable!(),
            };
            Ok(Value::Bool(result))
        }

        "+" => {
            // Matches FCT's own model: `+` is numeric addition, except
            // when both sides are strings, where it's concatenation —
            // there's no separate string-concat operator in the IR.
            if let (Value::String(ls), Value::String(rs)) = (l, r) {
                return Ok(Value::String(format!("{ls}{rs}")));
            }
            let (lf, rf) = numeric_pair(l, r, op)?;
            Ok(number_value(lf + rf))
        }

        "-" | "*" | "/" | "%" => {
            let (lf, rf) = numeric_pair(l, r, op)?;
            let result = match op {
                "-" => lf - rf,
                "*" => lf * rf,
                "/" => {
                    if rf == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    lf / rf
                }
                "%" => {
                    if rf == 0.0 {
                        return Err("modulo by zero".to_string());
                    }
                    lf % rf
                }
                _ => unreachable!(),
            };
            Ok(number_value(result))
        }

        other => Err(format!("cannot evaluate operator {other:?}")),
    }
}

fn numeric_pair(l: &Value, r: &Value, op: &str) -> Result<(f64, f64), String> {
    match (as_f64(l), as_f64(r)) {
        (Some(lf), Some(rf)) => Ok((lf, rf)),
        _ => Err(format!(
            "operator {op:?} requires numeric operands, got {l:?} and {r:?}"
        )),
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

fn number_value(n: f64) -> Value {
    serde_json::Number::from_f64(n)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Null => false,
        _ => true,
    }
}

/// `==` / `!=` semantics: numbers compare by value regardless of
/// int/float representation, everything else compares structurally.
fn values_equal(l: &Value, r: &Value) -> bool {
    match (l, r) {
        (Value::Number(_), Value::Number(_)) => as_f64(l) == as_f64(r),
        _ => l == r,
    }
}

/// A literal's value, in the type its `vtype` declares.
///
/// FCT emits exactly three literal types — `int`, `text` and `bool`
/// (`lower` in `internal/ir/build.go`; `money` and `date` are field
/// types, never literal kinds) — and each is handled explicitly.
///
/// # Why the fallback is the value's own type
///
/// The previous fallback stringified anything that was not already a
/// string, which is right for `text` and wrong for everything else. It
/// never fired for FCT, which always sets a `vtype`, but a hand-written
/// request is a documented client of this endpoint (see
/// `QueryWhereRequest`), and `{"kind":"lit","val":5}` is the obvious way
/// to write one. Under the old rule that literal became `"5"`, and the
/// consequences were silent rather than loud:
///
/// ```text
///   item.score == 5    →  Number(3) vs String("5")  →  never matches
///   item.score != 5    →  ...                       →  matches every row
/// ```
///
/// An ordering comparison at least failed with "requires numeric
/// operands", but equality just answered the wrong question. A wrong
/// answer that looks like a valid empty page is the worst outcome
/// available to a query engine, so an absent or unrecognized `vtype`
/// now means "this literal is whatever JSON says it is" — which is both
/// the least surprising reading and a no-op for FCT.
fn lit_value(expr: &Expr) -> Value {
    let val = expr.val.clone().unwrap_or(Value::Null);

    match expr.vtype.as_deref() {
        // Already an exact integer: keep it, rather than round-tripping
        // it through `f64` and losing precision past 2^53.
        Some("int") if val.is_i64() || val.is_u64() => val,

        Some("int") => as_f64(&val)
            .map(|n| number_value(n.trunc()))
            .unwrap_or(Value::Null),

        Some("bool") => Value::Bool(val.as_bool().unwrap_or(false)),

        // The one type for which coercing a non-string is the intent.
        Some("text") => match val {
            Value::String(_) => val,
            other => Value::String(other.to_string()),
        },

        _ => val,
    }
}
