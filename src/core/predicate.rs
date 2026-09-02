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

            let value = eval(x, item_var, data)?;

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
                    let lv = eval(l, item_var, data)?;
                    if !truthy(&lv) {
                        return Ok(Value::Bool(false));
                    }
                    let rv = eval(r, item_var, data)?;
                    Ok(Value::Bool(truthy(&rv)))
                }
                Some("||") => {
                    let lv = eval(l, item_var, data)?;
                    if truthy(&lv) {
                        return Ok(Value::Bool(true));
                    }
                    let rv = eval(r, item_var, data)?;
                    Ok(Value::Bool(truthy(&rv)))
                }
                Some(op) => {
                    let lv = eval(l, item_var, data)?;
                    let rv = eval(r, item_var, data)?;
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

fn lit_value(expr: &Expr) -> Value {
    let val = expr.val.clone().unwrap_or(Value::Null);

    match expr.vtype.as_deref() {
        Some("int") => as_f64(&val)
            .map(|n| number_value(n.trunc()))
            .unwrap_or(Value::Null),
        Some("bool") => Value::Bool(val.as_bool().unwrap_or(false)),
        _ => match val {
            Value::String(_) => val,
            other => Value::String(other.to_string()),
        },
    }
}
