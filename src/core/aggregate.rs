//! Folding many rows into one value.
//!
//! [`count_where`](crate::storage::engine::StorageEngine::count_where)
//! answers the only aggregate this engine had: how many rows match. Every
//! other one a rendered page asks for — the total of an order's line
//! items, the highest score, the average rating — was left to the caller,
//! which means shipping the rows across the wire and adding them up in
//! the client. That is the same N+1 `/nodes/count` exists to close, one
//! level up: the reply is a table when the answer is a number.
//!
//! This module is the fold, and nothing else. It does not know how a row
//! is found — the access paths in
//! [`StorageEngine`](crate::storage::engine::StorageEngine) own that, and
//! own it once, so a `sum` and the `count` beside it can never disagree
//! about which rows exist. What lives here is the part that differs
//! between aggregates: what each row contributes, and what the answer is
//! when no row contributed anything.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::storage::index as keys;

/// Which aggregate to compute.
///
/// `Count` is in the same enum as the rest rather than staying a separate
/// call, because it is the same traversal with a different fold —
/// splitting them is what let `count_by`'s two access paths drift apart
/// once already.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    /// Parse the wire spelling, refusing anything else by name.
    ///
    /// The error lists the whole set: a caller that sent `total` or
    /// `mean` has a typo, and a message that names the alternatives is
    /// the difference between one round trip and reading the source.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "count" => Ok(AggFunc::Count),
            "sum" => Ok(AggFunc::Sum),
            "avg" => Ok(AggFunc::Avg),
            "min" => Ok(AggFunc::Min),
            "max" => Ok(AggFunc::Max),
            other => Err(format!(
                "unknown aggregate {other:?}; expected one of count, sum, \
                 avg, min, max"
            )),
        }
    }

    /// The wire spelling, for an error message that quotes the request
    /// back.
    pub fn name(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }

    /// Does this aggregate have to read a field out of every row?
    ///
    /// This is the question the access paths ask, and the reason it is a
    /// method rather than a `match` at each call site: the index-only
    /// paths — counting a prefix range without decoding a single record —
    /// are correct for exactly the aggregates that answer `false` here.
    /// One `count` walks index keys; a `sum` over the same rows cannot,
    /// because the number it needs is in the record.
    pub fn needs_field(self) -> bool {
        !matches!(self, AggFunc::Count)
    }
}

/// What one aggregate is computing: the function, and the `data` field it
/// reads (absent only for `count`, which reads none).
#[derive(Debug, Clone)]
pub struct AggSpec {
    pub func: AggFunc,
    pub field: Option<String>,
}

impl AggSpec {
    /// The plain row count — the spec every `count_where` call folds
    /// through.
    pub fn count() -> Self {
        AggSpec { func: AggFunc::Count, field: None }
    }

    /// Check a request's function/field pair before any row is read.
    ///
    /// A `sum` with no field cannot be answered and a `count` with one
    /// would silently ignore it; both are the caller's mistake, and both
    /// are cheaper to refuse here than to discover in the reply. This is
    /// the same reason [`CountRequest`](crate::api::routes::CountRequest)
    /// has no `order`: a request whose shape is not answerable should not
    /// be representable as a successful call.
    pub fn new(func: AggFunc, field: Option<String>) -> Result<Self, String> {
        match (func.needs_field(), field.as_deref()) {
            (true, None) | (true, Some("")) => Err(format!(
                "{} needs a field to aggregate; pass `field`",
                func.name()
            )),
            (false, Some(f)) if !f.is_empty() => Err(format!(
                "count aggregates rows, not the field {f:?}; omit `field`, \
                 or use a predicate to select the rows to count"
            )),
            _ => Ok(AggSpec {
                func,
                field: field.filter(|f| !f.is_empty()),
            }),
        }
    }

    /// Does a row have to be decoded for this aggregate?
    pub fn needs_field(&self) -> bool {
        self.func.needs_field()
    }
}

/// The running total for one group (or for the whole result, which is one
/// group whose key is nothing).
///
/// # What a missing value contributes
///
/// **Nothing, and that is not the same as zero.** A row whose field is
/// absent or `null` is skipped by `sum`, `avg`, `min` and `max` alike, so
/// `avg` divides by the rows that had a value rather than by the rows
/// that matched — averaging over rows with no rating would report a
/// number nobody entered. `count` is the exception: it counts *rows*, so
/// it is unaffected by what any field holds.
///
/// # What a wrongly-typed value contributes
///
/// **An error, not a skipped row.** `sum` over a field holding `"12"`
/// could plausibly skip it, coerce it, or refuse; the first two answer a
/// question the caller did not ask and answer it silently, which is the
/// failure this codebase keeps finding. `min`/`max` do not have the
/// problem — they order values rather than adding them, and
/// [`keys::compare_order_values`] is a total order across JSON types, the
/// same one the indexes and `order` already sort by.
#[derive(Debug)]
pub struct Accumulator {
    func: AggFunc,
    /// Rows that matched the filter, whatever their field held. This is
    /// `count`'s answer.
    rows: u64,
    /// Rows that contributed a value — `avg`'s divisor.
    contributed: u64,
    /// The running sum kept in two forms. Integers accumulate in `i128`
    /// so a column of `i64`s cannot overflow into a wrong total midway,
    /// and `float_sum` takes over the moment a non-integer arrives.
    int_sum: i128,
    float_sum: f64,
    all_int: bool,
    /// The smallest (or largest) value seen, in its own JSON type.
    extreme: Option<Value>,
}

impl Accumulator {
    pub fn new(func: AggFunc) -> Self {
        Accumulator {
            func,
            rows: 0,
            contributed: 0,
            int_sum: 0,
            float_sum: 0.0,
            all_int: true,
            extreme: None,
        }
    }

    /// How many rows have been folded in. `count`'s answer, read directly
    /// rather than through [`Self::finish`], so
    /// [`count_where`](crate::storage::engine::StorageEngine::count_where)
    /// can keep returning a `u64` instead of unwrapping a JSON number
    /// back out of the general reply.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Fold one matching row in. `value` is what the row holds at the
    /// aggregated field — `None` when the field is absent, which
    /// [`Value::Null`] is treated as too.
    ///
    /// `field` is passed only to name the column in an error; it costs
    /// nothing on the path that does not fail.
    pub fn fold(&mut self, field: &str, value: Option<&Value>) -> Result<(), String> {
        self.rows += 1;

        if self.func == AggFunc::Count {
            return Ok(());
        }

        let value = match value {
            None | Some(Value::Null) => return Ok(()),
            Some(v) => v,
        };

        match self.func {
            AggFunc::Count => unreachable!("returned above"),

            AggFunc::Sum | AggFunc::Avg => {
                let Some(n) = value.as_f64() else {
                    return Err(format!(
                        "{} over {field:?} found {} in a row that matched; \
                         an aggregate over a field that is not always a \
                         number has no answer, so this is refused rather \
                         than reported as a total over the rows that \
                         happened to be numeric",
                        self.func.name(),
                        describe(value),
                    ));
                };

                if self.all_int
                    && let Some(i) = value.as_i64()
                {
                    self.int_sum += i128::from(i);
                } else {
                    if self.all_int {
                        // First non-integer: carry the exact integer
                        // total over once, then stay in floating point.
                        self.float_sum = self.int_sum as f64;
                        self.all_int = false;
                    }

                    self.float_sum += n;
                }
            }

            AggFunc::Min | AggFunc::Max => {
                let better = match &self.extreme {
                    None => true,
                    Some(current) => {
                        let ord =
                            keys::compare_order_values(Some(value), Some(current));

                        match self.func {
                            AggFunc::Min => ord == Ordering::Less,
                            _ => ord == Ordering::Greater,
                        }
                    }
                };

                if better {
                    self.extreme = Some(value.clone());
                }
            }
        }

        self.contributed += 1;

        Ok(())
    }

    /// Fold in `by` rows at once, for the access paths that count a range
    /// of index keys without reading the records behind them.
    ///
    /// Only legal for `count`: every other aggregate needs each row's
    /// value, and there is no value to supply for a row that was never
    /// read. The callers are gated on [`AggFunc::needs_field`], and this
    /// asserts the same rule rather than trusting them, because the
    /// failure it would otherwise produce is a plausible-looking number.
    pub fn fold_rows(&mut self, by: u64) {
        debug_assert!(
            !self.func.needs_field(),
            "fold_rows on {}, which needs each row's value",
            self.func.name()
        );

        self.rows += by;
    }

    /// The aggregate's value, as JSON.
    ///
    /// # The empty case, which differs by function
    ///
    /// `count` is `0` and `sum` is `0`: both have an identity, and a
    /// typed caller that renders `sum(...)` into an integer column wants
    /// the identity rather than a hole to special-case. `avg`, `min` and
    /// `max` are `null`: there is no average of no rows, and inventing
    /// one would be a number nobody measured.
    pub fn finish(self) -> Value {
        match self.func {
            AggFunc::Count => Value::from(self.rows),

            AggFunc::Sum => {
                if self.all_int {
                    number_from_i128(self.int_sum)
                } else {
                    number_from_f64(self.float_sum)
                }
            }

            AggFunc::Avg => {
                if self.contributed == 0 {
                    return Value::Null;
                }

                let total = if self.all_int {
                    self.int_sum as f64
                } else {
                    self.float_sum
                };

                number_from_f64(total / self.contributed as f64)
            }

            AggFunc::Min | AggFunc::Max => {
                self.extreme.unwrap_or(Value::Null)
            }
        }
    }
}

/// A JSON value's type, for an error that has to say what it found
/// without quoting a whole record into the message.
fn describe(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "a boolean".to_string(),
        Value::Number(_) => "a number".to_string(),
        Value::String(s) if s.len() <= 32 => format!("the text {s:?}"),
        Value::String(_) => "text".to_string(),
        Value::Array(_) => "a list".to_string(),
        Value::Object(_) => "an object".to_string(),
    }
}

/// An integer total as JSON, staying an integer while it fits.
///
/// A sum of `i64` columns can exceed `i64`; it is accumulated in `i128`
/// precisely so the total is right when it does. JSON has no `i128`, so
/// beyond that range the reply becomes a float — lossy in the last digits
/// and honest about it, which beats wrapping to a negative number.
fn number_from_i128(total: i128) -> Value {
    if let Ok(i) = i64::try_from(total) {
        return Value::from(i);
    }

    number_from_f64(total as f64)
}

/// A floating total as JSON. `serde_json` has no representation for NaN
/// or an infinity, and would serialize `null` for one; saying so is
/// better than a hole the caller reads as "no rows".
fn number_from_f64(total: f64) -> Value {
    serde_json::Number::from_f64(total)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fold_all(func: AggFunc, values: &[Value]) -> Result<Value, String> {
        let mut acc = Accumulator::new(func);

        for v in values {
            acc.fold("f", Some(v))?;
        }

        Ok(acc.finish())
    }

    #[test]
    fn a_sum_of_integers_stays_an_integer() {
        let out = fold_all(AggFunc::Sum, &[json!(1), json!(2), json!(39)]).unwrap();

        assert_eq!(out, json!(42));
        assert!(out.is_i64(), "an int column must not render as 42.0");
    }

    #[test]
    fn one_float_makes_the_whole_sum_a_float() {
        let out = fold_all(AggFunc::Sum, &[json!(1), json!(0.5)]).unwrap();

        assert_eq!(out.as_f64().unwrap(), 1.5);
    }

    #[test]
    fn an_integer_sum_wider_than_i64_is_still_right() {
        // Two i64 maxima: the answer only exists because the running
        // total is i128. In i64 this wraps to -2.
        let big = json!(i64::MAX);
        let out = fold_all(AggFunc::Sum, &[big.clone(), big]).unwrap();

        assert_eq!(out.as_f64().unwrap(), (i64::MAX as f64) * 2.0);
    }

    #[test]
    fn the_empty_sum_is_zero_and_the_empty_average_is_null() {
        assert_eq!(fold_all(AggFunc::Sum, &[]).unwrap(), json!(0));
        assert_eq!(fold_all(AggFunc::Avg, &[]).unwrap(), Value::Null);
        assert_eq!(fold_all(AggFunc::Min, &[]).unwrap(), Value::Null);
        assert_eq!(fold_all(AggFunc::Max, &[]).unwrap(), Value::Null);
        assert_eq!(fold_all(AggFunc::Count, &[]).unwrap(), json!(0));
    }

    #[test]
    fn an_average_divides_by_the_rows_that_had_a_value() {
        let mut acc = Accumulator::new(AggFunc::Avg);

        acc.fold("f", Some(&json!(2))).unwrap();
        acc.fold("f", Some(&json!(4))).unwrap();
        // Two rows matched but carry nothing at `f`.
        acc.fold("f", None).unwrap();
        acc.fold("f", Some(&Value::Null)).unwrap();

        // 3, not 1.5: the rows with no rating are not zeroes.
        assert_eq!(acc.finish().as_f64().unwrap(), 3.0);
    }

    #[test]
    fn count_counts_rows_whatever_the_field_holds() {
        let mut acc = Accumulator::new(AggFunc::Count);

        acc.fold("f", None).unwrap();
        acc.fold("f", Some(&Value::Null)).unwrap();
        acc.fold("f", Some(&json!("text"))).unwrap();

        assert_eq!(acc.rows(), 3);
        assert_eq!(acc.finish(), json!(3));
    }

    #[test]
    fn a_sum_over_text_is_refused_rather_than_skipped() {
        let err = fold_all(AggFunc::Sum, &[json!(1), json!("12")]).unwrap_err();

        assert!(err.contains("sum"), "{err}");
        assert!(err.contains("\"12\""), "{err}");
    }

    #[test]
    fn min_and_max_order_text_as_the_indexes_do() {
        let words = [json!("pear"), json!("apple"), json!("quince")];

        assert_eq!(fold_all(AggFunc::Min, &words).unwrap(), json!("apple"));
        assert_eq!(fold_all(AggFunc::Max, &words).unwrap(), json!("quince"));
    }

    #[test]
    fn a_spec_refuses_a_function_field_pair_it_cannot_answer() {
        assert!(AggSpec::new(AggFunc::Sum, None).is_err());
        assert!(AggSpec::new(AggFunc::Sum, Some(String::new())).is_err());
        assert!(AggSpec::new(AggFunc::Count, Some("price".into())).is_err());

        assert!(AggSpec::new(AggFunc::Count, None).is_ok());
        assert!(AggSpec::new(AggFunc::Avg, Some("price".into())).is_ok());
    }

    #[test]
    fn only_count_may_skip_reading_the_rows() {
        assert!(!AggFunc::Count.needs_field());

        for f in [AggFunc::Sum, AggFunc::Avg, AggFunc::Min, AggFunc::Max] {
            assert!(f.needs_field(), "{} must read each row", f.name());
        }
    }

    #[test]
    fn an_unknown_function_names_the_ones_that_exist() {
        let err = AggFunc::parse("mean").unwrap_err();

        assert!(err.contains("avg"), "{err}");
    }
}
