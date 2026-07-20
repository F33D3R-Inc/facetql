use std::collections::HashMap;

pub struct Index {
    pub addresses: HashMap<String, u64>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            addresses: HashMap::new(),
        }
    }

    pub fn insert(&mut self, address: String, position: u64) {
        self.addresses.insert(address, position);
    }

    /// Not yet used — reads go through StorageEngine's in-memory
    /// HashMap, which load() rebuilds from disk at boot. This becomes
    /// useful once the dataset is too large to keep fully in memory
    /// and reads need to go straight to disk by offset.
    #[allow(dead_code)]
    pub fn get(&self, address: &str) -> Option<u64> {
        self.addresses.get(address).copied()
    }
}
