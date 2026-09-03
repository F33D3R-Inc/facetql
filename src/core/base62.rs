//! Base62 encoding used for FacetQL addresses.
//!
//! Two layers live here:
//!
//! * A *single-symbol* layer (`encode` / `encode_digit` / `decode_digit`)
//!   that maps one base62 digit (`0..62`) to/from one character. This is
//!   what the 4-symbol coordinate address (e.g. `"A9k2"`) is built from —
//!   one symbol per axis.
//! * A *multi-symbol* layer (`encode_u64` / `decode_u64`) that maps an
//!   arbitrary `u64` to/from a base62 string and is exact round-trip for
//!   every valid input.
//!
//! Nothing here panics: out-of-range input is either saturated
//! deliberately (documented, on the infallible `encode`) or reported as a
//! `Base62Error` (on the checked entry points).

use std::fmt;

/// The 62 base62 symbols, in value order: `0-9`, then `A-Z`, then `a-z`.
/// Index into this slice IS the digit value, so it doubles as the
/// encode table; `decode_digit` walks it for the inverse.
const SYMBOLS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Base of the numbering system. Kept in sync with `SYMBOLS.len()`.
pub const RADIX: u32 = 62;

/// Errors from the checked base62 entry points. Malformed input never
/// panics — it is reported here instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base62Error {
    /// `decode_u64` was given an empty string; there is no digit to read.
    EmptyInput,
    /// A character that is not one of the 62 base62 symbols was found.
    InvalidSymbol(char),
    /// Decoding a string whose value does not fit in the target integer.
    Overflow,
    /// A digit value `>= 62` was handed to a single-symbol encoder.
    DigitOutOfRange(u8),
}

impl fmt::Display for Base62Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Base62Error::EmptyInput => write!(f, "base62: empty input"),
            Base62Error::InvalidSymbol(c) => write!(f, "base62: invalid symbol '{c}'"),
            Base62Error::Overflow => write!(f, "base62: value does not fit in target integer"),
            Base62Error::DigitOutOfRange(v) => {
                write!(f, "base62: digit {v} out of range 0..{RADIX}")
            }
        }
    }
}

impl std::error::Error for Base62Error {}

/// Encode a single base62 digit to its symbol.
///
/// Total and panic-free for every `u8`. For `value < 62` the result is the
/// exact base62 symbol. Values `>= 62` cannot be represented by a single
/// symbol; rather than panic (the previous behavior — a direct
/// `SYMBOLS[value]` index that was out of bounds for `value >= 62`) this
/// **deliberately saturates** to the highest symbol (`'z'`). Callers that
/// must distinguish out-of-range input should use [`encode_digit`], which
/// reports it instead of saturating.
pub fn encode(value: u8) -> char {
    let idx = (value as usize).min(SYMBOLS.len() - 1);
    SYMBOLS[idx] as char
}

/// Checked single-symbol encode: `Some(symbol)` for `value < 62`, `None`
/// otherwise. No saturation, no panic.
pub fn encode_digit(value: u8) -> Option<char> {
    if (value as u32) < RADIX {
        Some(SYMBOLS[value as usize] as char)
    } else {
        None
    }
}

/// Decode a single base62 symbol back to its digit value (`0..62`).
/// Returns `None` for any character that is not a base62 symbol.
pub fn decode_digit(symbol: char) -> Option<u8> {
    // `char` may be multi-byte; base62 symbols are all ASCII, so anything
    // that isn't a single ASCII byte cannot be a symbol.
    let byte = u8::try_from(symbol as u32).ok()?;
    SYMBOLS.iter().position(|&s| s == byte).map(|p| p as u8)
}

/// True iff `symbol` is one of the 62 base62 symbols.
pub fn is_valid_symbol(symbol: char) -> bool {
    decode_digit(symbol).is_some()
}

/// Encode an arbitrary `u64` as a base62 string. `0` encodes to `"0"`.
/// Exact round-trip with [`decode_u64`] for every `u64`.
pub fn encode_u64(mut value: u64) -> String {
    if value == 0 {
        return "0".to_string();
    }
    let radix = RADIX as u64;
    // Most significant digit is produced last, so build reversed then flip.
    let mut buf = Vec::new();
    while value > 0 {
        let digit = (value % radix) as usize;
        buf.push(SYMBOLS[digit]);
        value /= radix;
    }
    buf.reverse();
    // All bytes came from `SYMBOLS`, which is valid ASCII/UTF-8.
    String::from_utf8(buf).expect("base62 symbols are valid UTF-8")
}

/// Decode a base62 string to a `u64`.
///
/// * Empty input → `Base62Error::EmptyInput`.
/// * Any non-symbol character → `Base62Error::InvalidSymbol`.
/// * A value that exceeds `u64::MAX` → `Base62Error::Overflow` (checked, so
///   it never wraps silently or panics).
pub fn decode_u64(text: &str) -> Result<u64, Base62Error> {
    if text.is_empty() {
        return Err(Base62Error::EmptyInput);
    }
    let radix = RADIX as u64;
    let mut acc: u64 = 0;
    for c in text.chars() {
        let digit = decode_digit(c).ok_or(Base62Error::InvalidSymbol(c))? as u64;
        acc = acc
            .checked_mul(radix)
            .and_then(|shifted| shifted.checked_add(digit))
            .ok_or(Base62Error::Overflow)?;
    }
    Ok(acc)
}
