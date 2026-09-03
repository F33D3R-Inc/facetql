//! The Lo Shu / magic-square invariant layer from the design brief.
//!
//! Not yet wired into insert/update — no code path currently defines what
//! "target" should be for a given structure, so there is nothing to call
//! these with. That decision is needed before this is plugged into
//! `StorageEngine::insert()`. The math itself is kept correct and
//! overflow-safe so that when it *is* wired in, it can't panic on hostile
//! input.

/// True iff `values` sums to exactly `target`.
///
/// Overflow-safe: the previous implementation used `iter().sum::<u64>()`,
/// which panics on overflow in debug builds. This folds with `checked_add`
/// so an overflowing set of values simply can't equal any representable
/// `target` and returns `false` instead of panicking.
#[allow(dead_code)]
pub fn validate_sum(values: &[u64], target: u64) -> bool {
    let total = values
        .iter()
        .try_fold(0u64, |acc, &value| acc.checked_add(value));
    total == Some(target)
}

/// The magic constant (row/column/diagonal sum) of a normal magic square
/// of the given `order` — the square filled with `1..=order^2`. For a
/// normal square it is `order * (order^2 + 1) / 2`; order 3 (Lo Shu) → 15.
///
/// Returns `None` if the computation would overflow `u64`, so it is total
/// and never panics. `order == 0` yields `Some(0)`.
#[allow(dead_code)]
pub fn magic_constant(order: u64) -> Option<u64> {
    // order * (order^2 + 1) / 2, every step checked.
    let order_sq = order.checked_mul(order)?;
    let inner = order_sq.checked_add(1)?;
    let numerator = order.checked_mul(inner)?;
    Some(numerator / 2)
}

/// True iff `square` (a flat, row-major `order`×`order` grid) is a magic
/// square: every row, every column, and both main diagonals share one
/// common sum.
///
/// Robust to malformed input — returns `false` (never panics) when
/// `square.len() != order * order`, and is overflow-safe throughout.
/// `order == 0` is treated as not a magic square.
#[allow(dead_code)]
pub fn is_magic_square(square: &[u64], order: usize) -> bool {
    if order == 0 {
        return false;
    }
    let expected_len = match order.checked_mul(order) {
        Some(len) => len,
        None => return false,
    };
    if square.len() != expected_len {
        return false;
    }

    fn checked_sum(mut cells: impl Iterator<Item = u64>) -> Option<u64> {
        cells.try_fold(0u64, |acc, value| acc.checked_add(value))
    }

    // The target every line must match: the first row's sum.
    let target = match checked_sum((0..order).map(|c| square[c])) {
        Some(sum) => sum,
        None => return false,
    };

    // Rows.
    for r in 0..order {
        let base = r * order;
        match checked_sum((0..order).map(|c| square[base + c])) {
            Some(sum) if sum == target => {}
            _ => return false,
        }
    }

    // Columns.
    for c in 0..order {
        match checked_sum((0..order).map(|r| square[r * order + c])) {
            Some(sum) if sum == target => {}
            _ => return false,
        }
    }

    // Main diagonal (top-left → bottom-right).
    match checked_sum((0..order).map(|i| square[i * order + i])) {
        Some(sum) if sum == target => {}
        _ => return false,
    }

    // Anti-diagonal (top-right → bottom-left).
    match checked_sum((0..order).map(|i| square[i * order + (order - 1 - i)])) {
        Some(sum) if sum == target => {}
        _ => return false,
    }

    true
}
