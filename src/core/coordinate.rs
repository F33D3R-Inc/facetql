use std::fmt;

use super::base62;

/// A point in FacetQL's 4-axis address space.
///
/// The logical planar grid is 12 (`x`) × 13 (`y`) = 156 cells; `z` and `q`
/// extend the space along two further axes. Every axis is a single base62
/// digit in an address, so the *addressable* range of each axis is
/// `0..62` — see [`Coordinate::AXIS_MAX`]. Values outside that range still
/// construct via [`Coordinate::new`] (the historical, lenient constructor)
/// but cannot be encoded to a 4-symbol address; use [`Coordinate::try_new`]
/// when you need that guaranteed up front.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
}

/// Errors from coordinate validation and address parsing. Nothing here
/// panics — every boundary case is reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinateError {
    /// An axis value is `>= 62` and therefore not encodable as one base62
    /// symbol. `axis` is `"x"`/`"y"`/`"z"`/`"q"`.
    AxisNotAddressable { axis: &'static str, value: u8 },
    /// An address did not contain exactly 4 base62 symbols.
    AddressLength(usize),
    /// A character in an address was not a base62 symbol.
    InvalidSymbol(char),
}

impl fmt::Display for CoordinateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoordinateError::AxisNotAddressable { axis, value } => write!(
                f,
                "coordinate: axis {axis} value {value} is not addressable (must be < {})",
                Coordinate::AXIS_MAX as u16 + 1
            ),
            CoordinateError::AddressLength(len) => {
                write!(f, "coordinate: address must be exactly 4 symbols, got {len}")
            }
            CoordinateError::InvalidSymbol(c) => {
                write!(f, "coordinate: '{c}' is not a base62 symbol")
            }
        }
    }
}

impl std::error::Error for CoordinateError {}

impl Coordinate {
    /// Width of the planar grid along `x`.
    pub const X_LEN: u8 = 12;
    /// Height of the planar grid along `y`.
    pub const Y_LEN: u8 = 13;
    /// Total planar cells: `X_LEN * Y_LEN` = 156.
    pub const CELL_COUNT: u16 = Self::X_LEN as u16 * Self::Y_LEN as u16;
    /// Largest value any axis may hold and still round-trip through a
    /// single base62 address symbol (`61`, since symbols cover `0..62`).
    pub const AXIS_MAX: u8 = (base62::RADIX - 1) as u8;

    /// Lenient constructor kept for backward compatibility: accepts any
    /// `u8` per axis without range checking. Prefer [`Coordinate::try_new`]
    /// when the coordinate must be addressable.
    pub fn new(x: u8, y: u8, z: u8, q: u8) -> Self {
        Self { x, y, z, q }
    }

    /// Validated constructor: every axis must be `<= AXIS_MAX` so the
    /// coordinate is guaranteed to encode to (and decode back from) a
    /// 4-symbol address without loss.
    pub fn try_new(x: u8, y: u8, z: u8, q: u8) -> Result<Self, CoordinateError> {
        for (axis, value) in [("x", x), ("y", y), ("z", z), ("q", q)] {
            if value > Self::AXIS_MAX {
                return Err(CoordinateError::AxisNotAddressable { axis, value });
            }
        }
        Ok(Self { x, y, z, q })
    }

    /// True iff every axis is addressable (`<= AXIS_MAX`), i.e. the
    /// coordinate can be encoded to a 4-symbol address.
    pub fn is_addressable(&self) -> bool {
        self.x <= Self::AXIS_MAX
            && self.y <= Self::AXIS_MAX
            && self.z <= Self::AXIS_MAX
            && self.q <= Self::AXIS_MAX
    }

    /// True iff the coordinate falls inside the 12×13 planar grid on the
    /// `z = 0`, `q = 0` face.
    pub fn in_grid(&self) -> bool {
        self.x < Self::X_LEN && self.y < Self::Y_LEN && self.z == 0 && self.q == 0
    }

    /// Deterministic, collision-free linear index of this cell within the
    /// 156-cell planar grid, row-major (`y * X_LEN + x`). `None` if the
    /// coordinate is not on the planar grid. Inverse of
    /// [`Coordinate::from_grid_index`].
    pub fn grid_index(&self) -> Option<u16> {
        if self.in_grid() {
            Some(self.y as u16 * Self::X_LEN as u16 + self.x as u16)
        } else {
            None
        }
    }

    /// Reconstruct a planar-grid coordinate from its linear index.
    /// `None` for any index `>= CELL_COUNT`. Inverse of
    /// [`Coordinate::grid_index`].
    pub fn from_grid_index(index: u16) -> Option<Self> {
        if index >= Self::CELL_COUNT {
            return None;
        }
        let x = (index % Self::X_LEN as u16) as u8;
        let y = (index / Self::X_LEN as u16) as u8;
        Some(Self { x, y, z: 0, q: 0 })
    }

    /// Encode this coordinate to its canonical 4-symbol base62 address,
    /// one symbol per axis in `x, y, z, q` order (e.g. `"A9k2"`). Fails
    /// with `AxisNotAddressable` if any axis is `> AXIS_MAX`. This is a
    /// bijection between addressable coordinates and 4-symbol addresses,
    /// so it is collision-free by construction.
    pub fn to_address(&self) -> Result<String, CoordinateError> {
        let mut address = String::with_capacity(4);
        for (axis, value) in [
            ("x", self.x),
            ("y", self.y),
            ("z", self.z),
            ("q", self.q),
        ] {
            match base62::encode_digit(value) {
                Some(symbol) => address.push(symbol),
                None => return Err(CoordinateError::AxisNotAddressable { axis, value }),
            }
        }
        Ok(address)
    }

    /// Parse a canonical 4-symbol base62 address back into a coordinate.
    /// Exact inverse of [`Coordinate::to_address`]. Rejects any address
    /// whose length is not exactly 4 symbols or that contains a non-base62
    /// character — never panics.
    pub fn from_address(address: &str) -> Result<Self, CoordinateError> {
        let symbols: Vec<char> = address.chars().collect();
        if symbols.len() != 4 {
            return Err(CoordinateError::AddressLength(symbols.len()));
        }
        let mut digits = [0u8; 4];
        for (slot, &symbol) in digits.iter_mut().zip(symbols.iter()) {
            *slot = base62::decode_digit(symbol)
                .ok_or(CoordinateError::InvalidSymbol(symbol))?;
        }
        Ok(Self {
            x: digits[0],
            y: digits[1],
            z: digits[2],
            q: digits[3],
        })
    }
}
