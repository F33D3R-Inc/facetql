use super::transaction::*;

/// Placeholder for real multi-op transaction commit — see
/// transaction.rs. Not called yet because nothing constructs a
/// Transaction yet.
#[allow(dead_code)]
pub fn commit(transaction: Transaction) {
    println!("Commit {}", transaction.id);
}
