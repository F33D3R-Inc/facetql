use super::{coordinate::Coordinate, node::Node, base62};

/// Builds the 12x13 genesis coordinate grid. Addresses are real Base62
/// encodings of (x, y, z, q) — the previous version used a decimal
/// string like "0100", which wasn't Base62 at all and didn't match the
/// spec's address format (e.g. "A9k2").
pub fn generate_genesis() -> Vec<Node> {
    let mut nodes = Vec::new();
    for y in 0..13u8 {
        for x in 0..12u8 {
            let coordinate = Coordinate::new(x, y, 0, 0);
            let address: String = [x, y, 0, 0]
                .iter()
                .map(|&component| base62::encode(component))
                .collect();
            nodes.push(Node::new(coordinate, address, "GenesisCell".to_string(), "system".to_string()));
        }
    }
    nodes
}
