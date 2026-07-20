/// The Lo Shu invariant-checking layer from the design brief. Not
/// wired into insert/update yet — no code path currently defines what
/// "target" should be for a given structure, so there's nothing to
/// call this with. Needs that decided before it's plugged into
/// StorageEngine::insert().
#[allow(dead_code)]
pub fn validate_sum(values: &[u64], target: u64) -> bool {
    values.iter().sum::<u64>() == target
}
