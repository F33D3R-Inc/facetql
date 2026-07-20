use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Where facetql.data / facetql.wal / facetql.edges /
/// facetql.tombstones actually live.
///
/// Every prior checkpoint wrote these to the current working directory,
/// which is fine for `cargo run` from inside the repo and not fine for
/// something you `brew install` or drop a binary for — a real install
/// shouldn't scatter data files wherever the user happened to be
/// standing when they typed `facetql`, the way Postgres uses
/// `/var/lib/postgresql` (or `initdb`'s target dir) rather than the
/// shell's CWD.
static DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Must be called at most once, before anything touches storage (i.e.
/// before `Database::new()`). `main.rs` calls this right after parsing
/// CLI args. If nothing calls it, `data_dir()` falls back to the
/// default below the first time it's read.
pub fn set_data_dir(path: PathBuf) {
    // OnceLock::set silently no-ops on a second call rather than
    // panicking — fine here since main.rs is the only caller and only
    // calls it once, but flagged so a future second call site doesn't
    // get silently ignored without anyone noticing.
    let _ = DATA_DIR.set(path);
}

pub fn data_dir() -> &'static Path {
    DATA_DIR.get_or_init(default_data_dir).as_path()
}

pub fn data_file(name: &str) -> PathBuf {
    data_dir().join(name)
}

/// Creates the data directory if it doesn't exist yet. Called from
/// both `facetql init` and `facetql start` — `start` calls it too so
/// running the server directly (without a separate `init` step first)
/// still works, matching how the old single-binary-no-subcommands
/// version behaved.
pub fn ensure_data_dir() -> std::io::Result<()> {
    std::fs::create_dir_all(data_dir())
}

/// `~/.facetql` on macOS/Linux (`HOME`) and Windows (`USERPROFILE`).
/// Falls back to the current directory only if neither env var is set,
/// which should be rare on a real install.
fn default_data_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(|home| PathBuf::from(home).join(".facetql"))
        .unwrap_or_else(|| PathBuf::from("."))
}
