use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Where the database actually lives: the catalog, the heap segments,
/// the index files, the WAL, the checkpoint and the user log.
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

// ---------------------------------------------------------------------
// Deployment posture
// ---------------------------------------------------------------------

/// Which kind of deployment this process is, as declared by the
/// operator.
///
/// This exists so the credential defaults in `auth.rs` and `crypto.rs`
/// can be *refused* rather than merely warned about. Both of those
/// modules ship a well-known development fallback — a published admin
/// token, and an all-zero AES key — and both previously announced
/// themselves on stderr and carried on. A warning is not a control: it
/// is invisible under a supervisor that captures stdout only, it scrolls
/// away in the first minute of traffic, and nothing downstream of it
/// behaves any differently. A server that boots with those two defaults
/// is compromised at the door and at rest simultaneously, and it looks
/// exactly like a healthy one.
///
/// # Why the default is [`Deployment::Production`]
///
/// The obvious alternative — "assume development unless
/// `FACETQL_ENV=production` is set" — puts the safe behaviour behind
/// something an operator has to *remember*. Forgetting it then produces
/// a running, serving, fully-compromised database, and the failure is
/// silent. Defaulting the other way inverts that: forgetting produces a
/// refusal to start, with a message naming the exact variable to set.
/// The cost of the safe default is a developer typing one environment
/// variable once; the cost of the unsafe one is the whole database.
///
/// Two other signals were considered and rejected:
///
/// * **"TLS is configured" ⇒ production.** Wrong on its own terms. This
///   binary's own start-up text tells operators that terminating TLS at
///   a reverse proxy in front of it is a supported deployment, so the
///   most standard production shape of all would be classified as
///   development.
/// * **A `--dev` command-line flag.** Equivalent in effect, but a flag
///   lives in a supervisor unit file while the credentials it governs
///   live in the environment. Keeping the posture in the same place as
///   the secrets it gates means one thing to review, not two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deployment {
    /// Development. The published dev token and the all-zero master key
    /// are permitted, and plaintext HTTP is permitted, because nothing
    /// here is real.
    Development,

    /// Anything else. Dev credentials and unacknowledged plaintext are
    /// start-up refusals.
    Production,
}

pub const ENV_VAR: &str = "FACETQL_ENV";

impl Deployment {
    pub fn is_production(self) -> bool {
        self == Deployment::Production
    }
}

/// The declared posture, resolved once.
///
/// Only the exact string `development` (or its `dev` abbreviation, and
/// `test`, which is what a CI harness naturally sets) opts into the
/// development posture. Anything else — including a typo, an empty
/// value, and the variable being absent — resolves to
/// [`Deployment::Production`], because a posture nobody successfully
/// declared is not one this process may assume.
pub fn deployment() -> Deployment {
    static DEPLOYMENT: OnceLock<Deployment> = OnceLock::new();

    *DEPLOYMENT.get_or_init(|| {
        deployment_from(std::env::var(ENV_VAR).ok().as_deref())
    })
}

/// The classification itself, separated from where the string came from
/// so the fail-closed property can be tested exhaustively without
/// mutating process-wide state.
fn deployment_from(raw: Option<&str>) -> Deployment {
    match raw.unwrap_or_default().trim().to_ascii_lowercase().as_str() {
        "development" | "dev" | "test" => Deployment::Development,
        _ => Deployment::Production,
    }
}

#[cfg(test)]
mod deployment_tests {
    use super::*;

    /// The whole point of the default: everything that is not a
    /// successful declaration of "development" is production, so a typo
    /// produces a refusal to start rather than a silently compromised
    /// server.
    #[test]
    fn anything_but_an_explicit_development_declaration_is_production() {
        for raw in [
            None,
            Some(""),
            Some("   "),
            Some("developement"),
            Some("prod"),
            Some("production"),
            Some("staging"),
            Some("1"),
        ] {
            assert_eq!(
                deployment_from(raw),
                Deployment::Production,
                "{raw:?} must not be read as a development declaration"
            );
        }
    }

    #[test]
    fn development_is_declared_explicitly() {
        for raw in ["development", "dev", "test", "  Development  ", "DEV"] {
            assert_eq!(
                deployment_from(Some(raw)),
                Deployment::Development,
                "{raw:?} should declare development"
            );
        }
    }
}

/// Exit code for "the configuration is not one this server will serve
/// with".
///
/// Continues the single numbering the whole binary shares, rather than
/// starting a second one: `cli::error` owns 2 (usage) and 3 (operator
/// declined), `DatabaseError::exit_code` owns 1 (storage), 4 (integrity)
/// and 5 (WAL), and `database::EXIT_ENGINE_POISONED` owns 6. This is 7.
///
/// It is worth its own code because a supervisor must not treat it the
/// way it treats 1. A storage failure is worth a restart — the mount may
/// come back. This one is not: the process will refuse identically every
/// time until a human changes the environment, and a restart loop around
/// it only hides the message that says which variable to change.
pub const EXIT_INSECURE_CONFIGURATION: i32 = 7;

/// Explicit acknowledgement that this production deployment terminates
/// TLS somewhere else.
///
/// Serving production traffic in the clear sends every `x-api-key` — a
/// long-lived bearer credential — across the network in plaintext, so it
/// cannot be the silent default. But it also cannot be an outright ban:
/// this binary's own start-up text offers "terminate TLS at a reverse
/// proxy in front of this" as a supported deployment, and it is the more
/// common one. A proxy in front is not something this process can
/// detect, and guessing would either forbid a correct deployment or bless
/// an incorrect one.
///
/// So the operator states it. The value of the variable is irrelevant —
/// setting it at all is the acknowledgement — which keeps it a decision
/// somebody made rather than a default somebody inherited.
pub const ALLOW_PLAINTEXT_ENV: &str = "FACETQL_ALLOW_PLAINTEXT";

pub fn plaintext_acknowledged() -> bool {
    std::env::var_os(ALLOW_PLAINTEXT_ENV).is_some()
}
