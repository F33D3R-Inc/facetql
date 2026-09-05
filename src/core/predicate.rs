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

/// Expression nodes one predicate may contain.
///
/// Depth was the bound that stopped the *stack* from overflowing.
/// This is the bound that stops the *clock*, and it is the one an
/// attacker actually reaches: `eval` runs the whole tree once per
/// candidate row, so the cost of a request is the product of two
/// numbers the caller supplies — how many nodes the predicate has, and
/// how many rows the scan visits.
///
/// Only one of those was bounded. `FACETQL_MAX_SCAN_ROWS` caps the rows
/// at 100 000; nothing capped the nodes, so the ceiling was whatever
/// fits in a request body. Four mebibytes of minimal `bin`/`get`/`lit`
/// JSON is on the order of fifty thousand nodes, and fifty thousand
/// nodes over a hundred thousand rows is billions of evaluations — from
/// one request, holding the engine's read lock and one runtime worker
/// for the duration, at a cost to the sender of a single upload. That
/// is not a slow query; it is an amplifier, and it is reachable by any
/// identity that may read anything at all.
///
/// A tree cannot be both wide and shallow here — [`MAX_PREDICATE_DEPTH`]
/// already bounds the height — so bounding the node count is what makes
/// the *product* bounded: 256 nodes × 100 000 rows is tens of
/// milliseconds, and the two caps together mean no predicate can ask for
/// more work than an operator has agreed to.
///
/// 256 is far past anything a compiler emits. FCT's own `exprSQL`
/// pushdown produces a handful of comparisons joined by `&&`; a
/// hand-written filter is smaller still. A predicate that genuinely
/// needs more than 256 nodes is a program, and this is not the place to
/// run one.
pub const MAX_PREDICATE_NODES: usize = 256;

/// Longest string an operator inside a predicate may produce.
///
/// `+` concatenates when both sides are strings, and the result feeds
/// the next `+` up the tree. Without a bound the intermediate value is
/// limited only by depth times the largest literal in the body: a
/// literal near the body limit, doubled through a few dozen levels, is
/// gigabytes — allocated and copied *per row*, so the memory is
/// transient and the copying is not.
///
/// Bounding the node count alone does not close this, because the
/// multiplier is the literal's size rather than the node count. What
/// closes it is the observation that a predicate's job is to answer a
/// yes-or-no question about one record: an intermediate longer than any
/// field it could be compared against is pure cost with no reachable
/// purpose. 64 KiB is generous for that and small enough that the whole
/// evaluation stays bounded.
pub const MAX_PREDICATE_STRING: usize = 64 * 1024;

/// Values one `in` set may hold.
///
/// `in` is the operator a personalised feed is written with — "posts by
/// anyone I follow" is a membership test against the follow set — so the
/// bound has to clear a real following list rather than a token one, and
/// 1 000 does. It is also the same ceiling `count_by` puts on pinned
/// group values, for the same reason: a set is compared against once per
/// candidate row, so its size multiplies the scan.
pub const MAX_IN_SET: usize = 1_000;

/// Check a predicate's shape once, before it is run against every row.
///
/// This is the bound that has to be applied at the boundary rather than
/// inside [`eval`]. `eval` is called once per candidate row, so checking
/// there would either re-walk the tree per row — paying the very cost it
/// is meant to prevent — or catch the oversize predicate on row one
/// after the scan had already been set up. Checking here means a
/// refusal costs one walk of the request body and nothing else.
///
/// Counts *every* node in the tree, including the fields `eval` does not
/// interpret (`args`, `key`, `where`). They are deserialized whatever
/// `eval` does with them, and a caller sending a megabyte of them has
/// still spent a megabyte of this server's memory; a bound that only
/// counted the evaluated subset would be a bound with a documented way
/// around it.
pub fn validate(expr: &Expr) -> Result<(), String> {
    let mut budget = MAX_PREDICATE_NODES;

    validate_at(expr, 0, &mut budget)
}

fn validate_at(expr: &Expr, depth: usize, budget: &mut usize) -> Result<(), String> {
    if depth >= MAX_PREDICATE_DEPTH {
        return Err(format!(
            "predicate nests deeper than {MAX_PREDICATE_DEPTH} levels; \
             simplify it or split the query"
        ));
    }

    if *budget == 0 {
        return Err(format!(
            "predicate has more than {MAX_PREDICATE_NODES} expression \
             nodes. Every node is evaluated once per candidate row, so the \
             work a single request can ask for is the node count times the \
             row count; simplify the filter or narrow the query."
        ));
    }

    *budget -= 1;

    for child in [&expr.obj, &expr.key, &expr.l, &expr.r, &expr.x, &expr.where_]
        .into_iter()
        .flatten()
    {
        validate_at(child, depth + 1, budget)?;
    }

    for child in expr.args.iter().flatten() {
        validate_at(child, depth + 1, budget)?;
    }

    Ok(())
}

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

        // String tests. Both take a string on each side and are false —
        // not an error — when either is absent, because a row without
        // the field simply does not match. That differs on purpose from
        // `<`/`>`, which error on a non-numeric operand: an ordering
        // comparison against a missing value has no defensible answer,
        // while "does this missing text start with 'a'" plainly does.
        "starts_with" | "ends_with" | "contains" => {
            let (Value::String(haystack), Value::String(needle)) = (l, r) else {
                return Ok(Value::Bool(false));
            };

            let found = match op {
                "starts_with" => haystack.starts_with(needle.as_str()),
                "ends_with" => haystack.ends_with(needle.as_str()),
                _ => haystack.contains(needle.as_str()),
            };

            Ok(Value::Bool(found))
        }

        // Set membership. The right side must be an array literal; a
        // scalar is a mistake worth naming rather than silently reading
        // as a one-element set, because `x in "abc"` in most languages
        // means substring and here it would not.
        "in" | "not in" => {
            let Value::Array(set) = r else {
                return Err(format!(
                    "operator {op:?} needs an array on the right, got {r:?}"
                ));
            };

            if set.len() > MAX_IN_SET {
                return Err(format!(
                    "`{op}` set holds {} values; the maximum is {MAX_IN_SET}.                      A set is compared against once per candidate row, so its                      size multiplies the scan.",
                    set.len(),
                ));
            }

            let found = set.iter().any(|candidate| values_equal(l, candidate));

            Ok(Value::Bool(if op == "in" { found } else { !found }))
        }

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
                // See MAX_PREDICATE_STRING: the result of one `+` is an
                // operand of the next, so an unbounded concatenation
                // compounds up the tree and is rebuilt for every row.
                if ls.len() + rs.len() > MAX_PREDICATE_STRING {
                    return Err(format!(
                        "string concatenation inside a predicate would \
                         produce {} bytes; the maximum is \
                         {MAX_PREDICATE_STRING}. A predicate decides whether \
                         one record matches — it is not a place to build a \
                         value.",
                        ls.len() + rs.len()
                    ));
                }

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

// ---------------------------------------------------------------------
// Sargability: which part of a predicate an index can answer
// ---------------------------------------------------------------------

/// The literal a predicate requires one field to equal, if it requires
/// one unconditionally.
///
/// This is the analysis that lets a declared index *filter* rather than
/// only order: `item.status == "open"` over an index on `status` becomes
/// a scan of just the entries holding `"open"`, instead of a walk of
/// every node of the kind with a JSON decode and an evaluation per row.
///
/// # Why only equality
///
/// Because equality is the only comparison here that cannot fail.
/// [`eval_bin_op`]'s ordering operators demand numeric operands and
/// return `Err` otherwise — and a `get` of an absent field yields
/// `Null` — so `item.score > 5` does not skip a row whose `score` is a
/// string or missing, it *fails the whole query*. Narrowing such a scan
/// to a numeric range would quietly turn that error into a page of
/// results: the indexed and unindexed paths would then disagree about
/// what the request even means, which is precisely the divergence an
/// index must never introduce. `==` goes through `values_equal`, which
/// answers false rather than failing, so restricting the scan to the
/// matching entries removes only rows that were going to evaluate false
/// anyway.
///
/// A `null` literal is deliberately not sargable either: `get` returns
/// `Null` for a field that is absent as well as one that is explicitly
/// null, so `item.x == null` matches both — while the index keys them
/// apart (an absent value sorts after every present one). One prefix
/// cannot cover both, so this stays on the scan path.
///
/// # Why only top-level `&&`
///
/// A conjunct is a requirement: every row in the result satisfies it, so
/// narrowing to it removes nothing. Under `||` that is false — a row can
/// satisfy the other branch — and under `!` it is inverted. So the walk
/// descends through `&&` and stops at anything else. The result is
/// conservative by construction: it may fail to find a bound that
/// exists, which costs a scan, and can never invent one that does not,
/// which would cost correctness.
/// The literal a `starts_with` on `item.<field>` pins, if the predicate
/// pins one.
///
/// The sibling of [`equality_literal`], and deliberately just as narrow:
/// it descends through `&&` only, because a `starts_with` under `||` does
/// not narrow the candidate set — the other branch may admit rows the
/// prefix excludes. `ends_with` and `contains` are absent *here* for the
/// reason that matters: neither is a prefix of the ordered encoding, so
/// no range of a B+tree over whole values corresponds to them.
///
/// They are not unservable, though — they are servable by a different
/// structure, which is what [`substring_literals`] and
/// [`crate::storage::text`] are. An ordered index answers questions
/// about a value's *order*; an inverted index answers questions about
/// its *contents*, and `contains` is the second kind of question.
pub fn prefix_literal(expr: &Expr, item_var: &str, field: &str) -> Option<String> {
    prefix_literal_at(expr, item_var, field, 0)
}

fn prefix_literal_at(
    expr: &Expr,
    item_var: &str,
    field: &str,
    depth: usize,
) -> Option<String> {
    if depth >= MAX_PREDICATE_DEPTH || expr.kind != "bin" {
        return None;
    }

    let l = expr.l.as_deref()?;
    let r = expr.r.as_deref()?;

    match expr.op.as_deref() {
        Some("&&") => prefix_literal_at(l, item_var, field, depth + 1)
            .or_else(|| prefix_literal_at(r, item_var, field, depth + 1)),

        Some("starts_with") => {
            if r.kind != "lit" || !is_field_access(l, item_var, field) {
                return None;
            }

            match lit_value(r) {
                Value::String(s) if !s.is_empty() => Some(s),
                _ => None,
            }
        }

        _ => None,
    }
}

/// Every substring test a predicate *requires* of `item.<field>`.
///
/// The third member of the sargability family, beside
/// [`equality_literal`] and [`prefix_literal`], and the one that lets an
/// inverted index be planned. All three of `contains`, `starts_with` and
/// `ends_with` are collected, because all three are substring tests: a
/// prefix and a suffix are substrings, and an index over substrings
/// serves them all with the same postings.
///
/// # Why a list rather than one literal
///
/// Each returned literal is a *requirement*: every row in the result
/// satisfies all of them, so the trigrams of all of them must appear in
/// the row's text. Collecting them all makes
/// `contains(body,"rust") && contains(body,"async")` narrow through both
/// words instead of one — which is the shape a search box with two terms
/// actually produces.
///
/// # Why only top-level `&&`
///
/// Same reason as its siblings, and it matters more here because the
/// consumer intersects: a conjunct is a requirement, so narrowing to it
/// removes nothing. Under `||` a row can satisfy the other branch, and
/// under `!` the requirement is inverted. So the walk descends through
/// `&&` and stops at anything else, and is conservative by construction:
/// it may miss a literal that exists, which costs a scan, and can never
/// invent one that does not, which would cost correctness.
///
/// # Why the literal is returned raw
///
/// Folding, tokenizing and deciding whether the literal is long enough
/// to be indexable all belong to the index, not to the predicate: this
/// function's job is to say what the predicate *requires*, and the
/// storage layer's is to say what it can serve.
pub fn substring_literals(expr: &Expr, item_var: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    substring_literals_at(expr, item_var, field, 0, &mut out);
    out
}

fn substring_literals_at(
    expr: &Expr,
    item_var: &str,
    field: &str,
    depth: usize,
    out: &mut Vec<String>,
) {
    if depth >= MAX_PREDICATE_DEPTH || expr.kind != "bin" {
        return;
    }

    let (Some(l), Some(r)) = (expr.l.as_deref(), expr.r.as_deref()) else {
        return;
    };

    match expr.op.as_deref() {
        Some("&&") => {
            substring_literals_at(l, item_var, field, depth + 1, out);
            substring_literals_at(r, item_var, field, depth + 1, out);
        }

        Some("contains" | "starts_with" | "ends_with") => {
            // The right side must be a literal, not merely something
            // `lit_value` can read a `val` off. Probing an index with a
            // needle the evaluator would not have used is the one way a
            // candidate set can come out a *subset* of the answer rather
            // than a superset — see `match_equality`, which checks the
            // same thing for the same reason.
            if r.kind != "lit" || !is_field_access(l, item_var, field) {
                return;
            }

            // An empty literal is satisfied by every string, so it
            // requires nothing and narrows nothing.
            if let Value::String(s) = lit_value(r)
                && !s.is_empty()
            {
                out.push(s);
            }
        }

        _ => {}
    }
}

fn is_field_access(expr: &Expr, item_var: &str, field: &str) -> bool {
    expr.kind == "get"
        && expr.field.as_deref() == Some(field)
        && expr
            .obj
            .as_deref()
            .is_some_and(|obj| obj.kind == "ref" && obj.name.as_deref() == Some(item_var))
}

pub fn equality_literal(expr: &Expr, item_var: &str, field: &str) -> Option<Value> {
    equality_literal_at(expr, item_var, field, 0)
}

fn equality_literal_at(
    expr: &Expr,
    item_var: &str,
    field: &str,
    depth: usize,
) -> Option<Value> {
    if depth >= MAX_PREDICATE_DEPTH {
        return None;
    }

    if expr.kind != "bin" {
        return None;
    }

    let l = expr.l.as_deref()?;
    let r = expr.r.as_deref()?;

    match expr.op.as_deref() {
        Some("&&") => equality_literal_at(l, item_var, field, depth + 1)
            .or_else(|| equality_literal_at(r, item_var, field, depth + 1)),

        Some("==") => match_equality(l, r, item_var, field)
            .or_else(|| match_equality(r, l, item_var, field)),

        _ => None,
    }
}

/// `<field access> == <literal>`, in that order, for this exact field.
fn match_equality(
    access: &Expr,
    literal: &Expr,
    item_var: &str,
    field: &str,
) -> Option<Value> {
    if access.kind != "get" || literal.kind != "lit" {
        return None;
    }

    if access.field.as_deref() != Some(field) {
        return None;
    }

    let obj = access.obj.as_deref()?;

    if obj.kind != "ref" || obj.name.as_deref() != Some(item_var) {
        return None;
    }

    let value = lit_value(literal);

    // See the doc comment: `null` cannot be served by one prefix.
    if value.is_null() {
        return None;
    }

    Some(value)
}

#[cfg(test)]
mod bound_tests {
    //! The bounds that decide how much work one wire-supplied predicate
    //! may ask for. Each test here fails without the corresponding check
    //! in [`validate`] or [`eval_bin_op`].
    use super::*;

    fn node(kind: &str) -> Expr {
        Expr {
            kind: kind.to_string(),
            val: None,
            vtype: None,
            name: None,
            field: None,
            obj: None,
            key: None,
            op: None,
            args: None,
            l: None,
            r: None,
            x: None,
            var: None,
            where_: None,
        }
    }

    fn lit(value: serde_json::Value) -> Expr {
        let mut e = node("lit");
        e.val = Some(value);
        e
    }

    fn get(field: &str) -> Expr {
        let mut obj = node("ref");
        obj.name = Some("item".to_string());

        let mut e = node("get");
        e.field = Some(field.to_string());
        e.obj = Some(Box::new(obj));
        e
    }

    fn bin(op: &str, l: Expr, r: Expr) -> Expr {
        let mut e = node("bin");
        e.op = Some(op.to_string());
        e.l = Some(Box::new(l));
        e.r = Some(Box::new(r));
        e
    }

    /// A *balanced* conjunction of the given height — the shape that
    /// makes the node-count bound the binding one rather than the depth
    /// bound. Height h holds `2^(h+1) - 1` nodes while nesting only h
    /// levels, which is exactly how a caller buys a lot of per-row work
    /// out of a shallow tree.
    fn balanced_conjunction(height: u32) -> Expr {
        if height == 0 {
            return lit(serde_json::json!(true));
        }

        bin(
            "&&",
            balanced_conjunction(height - 1),
            balanced_conjunction(height - 1),
        )
    }

    /// What a real pushed-down filter looks like. It must keep passing —
    /// a bound that refuses ordinary predicates is not a bound, it is an
    /// outage.
    #[test]
    fn an_ordinary_predicate_passes() {
        let expr = bin(
            "&&",
            bin("==", get("status"), lit(serde_json::json!("open"))),
            bin(">=", get("score"), lit(serde_json::json!(5))),
        );

        assert!(validate(&expr).is_ok());
    }

    /// The node-count bound is the one that stops the amplifier: cost is
    /// nodes × rows, and only rows were capped before.
    #[test]
    fn a_predicate_with_too_many_nodes_is_refused() {
        // 2^10 - 1 = 1023 nodes at a depth of 9 — four times the node
        // bound, and a seventh of the depth bound, so the refusal can
        // only be the one under test.
        let expr = balanced_conjunction(9);

        let error = validate(&expr).expect_err("should refuse");

        assert!(
            error.contains("expression nodes"),
            "refused for the wrong reason: {error}"
        );
    }

    /// Depth is still checked here, not only inside `eval` — a tree can
    /// be refused before a scan is set up rather than on its first row.
    #[test]
    fn a_predicate_deeper_than_the_depth_bound_is_refused() {
        // A chain of unary `!`, which adds one node per level, so depth
        // is reached before the node budget is spent.
        let mut expr = lit(serde_json::json!(true));

        for _ in 0..MAX_PREDICATE_DEPTH + 2 {
            let mut outer = node("un");
            outer.op = Some("!".to_string());
            outer.x = Some(Box::new(expr));
            expr = outer;
        }

        let error = validate(&expr).expect_err("should refuse");

        assert!(
            error.contains("nests deeper"),
            "refused for the wrong reason: {error}"
        );
    }

    /// `args` is deserialized but never evaluated, which is exactly why
    /// it has to be counted: a bound that only saw the evaluated subset
    /// would ship with a documented way around it.
    #[test]
    fn nodes_eval_never_visits_are_still_counted() {
        let mut expr = node("call");

        expr.args = Some(
            (0..MAX_PREDICATE_NODES + 1)
                .map(|_| lit(serde_json::json!(1)))
                .collect(),
        );

        let error = validate(&expr).expect_err("should refuse");

        assert!(
            error.contains("expression nodes"),
            "refused for the wrong reason: {error}"
        );
    }

    /// String `+` compounds up the tree and is rebuilt per row, so the
    /// intermediate value needs its own bound — the node count does not
    /// imply one, because the multiplier is the literal's size.
    #[test]
    fn concatenation_beyond_the_string_bound_is_refused() {
        let half = "a".repeat(MAX_PREDICATE_STRING / 2 + 1);

        let expr = bin(
            "+",
            lit(serde_json::json!(half)),
            lit(serde_json::json!(half)),
        );

        let error = eval(&expr, "item", &serde_json::json!({}))
            .expect_err("should refuse");

        assert!(
            error.contains("concatenation"),
            "refused for the wrong reason: {error}"
        );
    }

    /// And an ordinary concatenation still works.
    #[test]
    fn a_short_concatenation_still_evaluates() {
        let expr = bin(
            "+",
            lit(serde_json::json!("a")),
            lit(serde_json::json!("b")),
        );

        assert_eq!(
            eval(&expr, "item", &serde_json::json!({})).expect("evaluates"),
            serde_json::json!("ab")
        );
    }
}
