use super::{coordinate::Coordinate, node::Node};

/// Builds the 12×13 genesis coordinate grid: one `GenesisCell` per planar
/// cell, in deterministic row-major order.
///
/// Each address is the coordinate's canonical 4-symbol Base62 encoding
/// (`Coordinate::to_address`) — a bijection between addressable
/// coordinates and addresses, so every genesis cell gets a distinct
/// address by construction (no collisions, no duplicates). The previous
/// version hand-rolled the encoding and called a `base62::encode` that
/// panicked for any axis value `>= 62`; routing through `to_address` keeps
/// the encoding in one place and makes out-of-range axes an error rather
/// than a panic.
pub fn generate_genesis() -> Vec<Node> {
    let mut nodes = Vec::with_capacity(Coordinate::CELL_COUNT as usize);
    for y in 0..Coordinate::Y_LEN {
        for x in 0..Coordinate::X_LEN {
            let coordinate = Coordinate::new(x, y, 0, 0);
            // Genesis coordinates are always inside 0..12 / 0..13, well
            // within the addressable range, so this never errors; handle
            // it as a skip rather than an unwrap so the generator can
            // never panic even if the grid bounds are ever widened.
            if let Ok(address) = coordinate.to_address() {
                nodes.push(Node::new(
                    coordinate,
                    address,
                    "GenesisCell".to_string(),
                    "system".to_string(),
                ));
            }
        }
    }
    nodes
}
