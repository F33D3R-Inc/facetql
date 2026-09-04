/// A point in FacetQL's 4-axis address space.
///
/// This is a **wire-contract type** (AGENT_LOG §4b): `x`/`y`/`z`/`q` are
/// serialized on every `insert_node` transaction op and on every `Node`
/// returned by `GET /nodes` and `POST /nodes/query`, and they are part of
/// the on-disk record encoding in `storage::binary`. The four fields, the
/// derives (`Serialize`/`Deserialize` above all) and the lenient
/// [`Coordinate::new`] constructor are therefore load-bearing and must not
/// be narrowed or renamed without a contract change on both the Rust and
/// the Go (`fct`) side.
///
/// Each axis is a plain `u8` with no range restriction. An earlier design
/// also carried a 12×13 planar "genesis grid" and a canonical 4-symbol
/// base62 address encoding (`try_new`/`to_address`/`from_address`/
/// `grid_index`, backed by `core::base62`). Nothing in the engine, the API
/// or the CLI ever reached that layer — addresses on the wire are
/// caller-supplied strings of the form `"<entity>:<id>"`, not encoded
/// coordinates — so it has been removed rather than left as unreachable
/// scaffolding. If a coordinate-derived address scheme is ever wanted, it
/// belongs next to the address parsing the API actually uses, not here.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Coordinate {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
}

impl Coordinate {
    /// Construct a coordinate from its four axes. Accepts any `u8` per
    /// axis: the wire contract puts no bound on the axes, so neither does
    /// this.
    pub fn new(x: u8, y: u8, z: u8, q: u8) -> Self {
        Self { x, y, z, q }
    }
}
