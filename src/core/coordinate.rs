#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
}

impl Coordinate {
    pub fn new(x: u8, y: u8, z: u8, q: u8) -> Self {
        Self { x, y, z, q }
    }
}
