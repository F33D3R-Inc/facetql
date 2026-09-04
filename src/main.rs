mod api;
mod core;
mod storage;
mod rules;
mod database;
mod auth;
mod config;
mod importer;
mod crypto;
mod tls_server;
mod facet;
mod cli;

use api::routes::create_router;
use database::{Database, DatabaseError};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// FacetQL — a standalone, coordinate-native database server.
#[derive(Parser, Debug)]
#[command(name = "facetql", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Where data files live. Defaults to ~/.facetql if unset.
    /// Also settable via ENOCHIAN_DATA_DIR.
    #[arg(long, global = true, env = "ENOCHIAN_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create the data directory and exit.
    Init,

    /// Start the server. Also what running `facetql` with no
    /// subcommand does, for backward compatibility.
    Start {
        #[arg(long, env = "ENOCHIAN_PORT", default_value_t = 8080)]
        port: u16,

        /// Path to a PKCS#12 (.p12/.pfx) file containing both the TLS
        /// certificate and private key. If set (along with --tls-cert-password),
        /// the server speaks HTTPS instead of HTTP. Generate a dev one with:
        /// `openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"`
        /// `openssl pkcs12 -export -out identity.p12 -inkey key.pem -in cert.pem -passout pass:<password>`
        #[arg(long, env = "ENOCHIAN_TLS_IDENTITY")]
        tls_identity: Option<PathBuf>,

        /// Password for the PKCS#12 file above.
        #[arg(long, env = "ENOCHIAN_TLS_IDENTITY_PASSWORD")]
        tls_identity_password: Option<String>,
    },

    /// Copy every data file to `output_dir`, for offline backup — the
    /// same underlying idea as `pg_basebackup`: copy the files, don't
    /// touch a running server's writes. Run this against a data
    /// directory that ISN'T currently being written to by a live
    /// `facetql start` — this does a plain file copy, not a
    /// consistent-snapshot-of-a-live-server backup (that's a real,
    /// harder feature — see SECURITY_NOTES.md).
    Backup {
        output_dir: PathBuf,
    },

    /// Restore data files from a directory created by `backup` into
    /// the configured data directory. Refuses to run if the target
    /// data directory already has data in it, to avoid silently
    /// clobbering something — move/rename the existing directory
    /// first if that's really what you want.
    Restore {
        input_dir: PathBuf,
    },

    /// Bring rows from an existing database into FacetQL. Currently
    /// supports Postgres. Talks to a *running* `facetql start` over
    /// its normal API — start the server first.
    Import {
        #[command(subcommand)]
        source: ImportSource,
    },

    /// Manage identities (admin only). Talks to a *running* server over
    /// its API — start the server first.
    User {
        #[command(subcommand)]
        action: cli::UserAction,
    },

    /// Fetch a single node by address.
    Get(cli::GetArgs),

    /// Write a node at a client-supplied address.
    Put(cli::PutArgs),

    /// Delete a node by address. Destructive — asks to confirm.
    Delete(cli::DeleteArgs),

    /// Query nodes of a kind (native predicate query — no SQL).
    Query(cli::QueryArgs),

    /// Show node counts per kind, for the supplied token's view.
    Stats(cli::StatsArgs),
}

#[derive(Subcommand, Debug)]
enum ImportSource {
    /// `facetql import postgres --pg-url postgres://... --table clients --kind Client --token <admin-or-user-token>`
    Postgres {
        /// Postgres connection string, e.g. postgres://user:pass@host/db
        #[arg(long)]
        pg_url: String,

        /// Table to import, one FacetQL node per row.
        #[arg(long)]
        table: String,

        /// Node `kind` to assign every imported row.
        #[arg(long)]
        kind: String,

        /// Column used to build a stable node address (pg_<table>_<value>).
        /// Falls back to the row's position in the result set if this
        /// column isn't present.
        #[arg(long, default_value = "id")]
        id_column: String,

        /// FacetQL server to import into.
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,

        /// FacetQL x-api-key to authenticate the import as. Imported
        /// rows are owned by whichever identity this token maps to —
        /// same rule as every other write.
        #[arg(long, env = "ENOCHIAN_TOKEN")]
        token: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Some(dir) = cli.data_dir {
        config::set_data_dir(dir);
    }

    match cli.command {
        Some(Command::Init) => {
            config::ensure_data_dir()
                .expect("failed to create data directory");

            println!(
                "Initialized FacetQL data directory at {}",
                config::data_dir().display()
            );
        }

        Some(Command::Start {
                 port,
                 tls_identity,
                 tls_identity_password,
             }) => {
            run_server(
                port,
                tls_identity,
                tls_identity_password,
            )
                .await
        }

        None => run_server(8080, None, None).await,

        Some(Command::Backup { output_dir }) => {
            run_backup(output_dir)
        }

        Some(Command::Restore { input_dir }) => {
            run_restore(input_dir)
        }

        Some(Command::Import { source }) => {
            run_import(source).await
        }

        // Client commands: each maps onto an existing API route and, on
        // error, prints a structured message and exits non-zero via
        // `cli::report_error` — see src/cli/mod.rs.
        Some(Command::User { action }) => {
            if let Err(e) = cli::run_user(action).await {
                cli::report_error(e);
            }
        }

        Some(Command::Get(args)) => {
            if let Err(e) = cli::run_get(args).await {
                cli::report_error(e);
            }
        }

        Some(Command::Put(args)) => {
            if let Err(e) = cli::run_put(args).await {
                cli::report_error(e);
            }
        }

        Some(Command::Delete(args)) => {
            if let Err(e) = cli::run_delete(args).await {
                cli::report_error(e);
            }
        }

        Some(Command::Query(args)) => {
            if let Err(e) = cli::run_query(args).await {
                cli::report_error(e);
            }
        }

        Some(Command::Stats(args)) => {
            if let Err(e) = cli::run_stats(args).await {
                cli::report_error(e);
            }
        }
    }
}

/// Every durable file the storage layer writes, discovered rather than
/// listed.
///
/// It used to be a fixed list of four names, which stopped working the
/// moment the heap became a set of segments: `facetql.heap.000000.seg`,
/// `facetql.heap.000001.seg` and so on are created and retired as the
/// database grows and compacts, so no constant can name them. Backing up
/// what the directory actually holds is also the safer failure mode — a
/// future file that nobody remembered to add to a list would silently
/// not be backed up, and its absence would only be discovered during a
/// restore.
///
/// In-progress temp files (`*.tmp`) are skipped: they are the
/// write-then-rename halves of a catalog or checkpoint update, and a
/// copy of one is either redundant or garbage.
fn data_files() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(config::data_dir())
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.path().is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.ends_with(".tmp"))
        .collect();

    // Sorted so a backup lists its files in a stable order run to run.
    names.sort();

    names
}

fn run_backup(output_dir: PathBuf) {
    std::fs::create_dir_all(&output_dir)
        .expect("failed to create backup output directory");

    let mut copied = 0;

    for filename in data_files() {
        let src = config::data_file(&filename);

        if src.exists() {
            let dst = output_dir.join(filename);

            std::fs::copy(&src, &dst)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to copy {}: {e}",
                        src.display()
                    )
                });

            copied += 1;
        }
    }

    println!(
        "Backed up {copied} data file(s) from {} to {}",
        config::data_dir().display(),
        output_dir.display()
    );
}

fn run_restore(input_dir: PathBuf) {
    let existing: Vec<String> = data_files();

    if !existing.is_empty() {
        eprintln!(
            "refusing to restore: the target data directory ({}) \
             already has data ({}). Move it aside first if you really \
             want to overwrite it.",
            config::data_dir().display(),
            existing.join(", ")
        );

        std::process::exit(1);
    }

    config::ensure_data_dir()
        .expect("failed to create data directory");

    let mut restored = 0;

    // Restore reads the *backup's* directory, for the same reason backup
    // reads the live one: the file set is not knowable in advance.
    let backed_up: Vec<String> = std::fs::read_dir(&input_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.path().is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();

    for filename in backed_up {
        let src = input_dir.join(&filename);

        if src.exists() {
            let dst = config::data_file(&filename);

            std::fs::copy(&src, &dst)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to copy {}: {e}",
                        src.display()
                    )
                });

            restored += 1;
        }
    }

    println!(
        "Restored {restored} data file(s) from {} into {}",
        input_dir.display(),
        config::data_dir().display()
    );
}

async fn run_import(source: ImportSource) {
    match source {
        ImportSource::Postgres {
            pg_url,
            table,
            kind,
            id_column,
            server,
            token,
        } => {
            println!(
                "Importing table '{table}' from Postgres into \
                 FacetQL ({server})..."
            );

            match importer::import_postgres_table(
                &pg_url,
                &table,
                &kind,
                "",
                &id_column,
                &server,
                &token,
            )
                .await
            {
                Ok(summary) => {
                    println!(
                        "Imported {} row(s).",
                        summary.imported
                    );

                    if !summary.failed.is_empty() {
                        println!(
                            "{} row(s) failed:",
                            summary.failed.len()
                        );

                        for (address, err) in &summary.failed {
                            println!("  {address}: {err}");
                        }
                    }
                }

                Err(e) => {
                    eprintln!("import failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

/// Render a startup failure for whoever is reading the terminal or the
/// journal, then exit non-zero.
///
/// The single line this replaces was true but unusable: it said the same
/// thing for a directory that does not exist, a master key that does not
/// match, and a file that has rotted — when the operator's response to
/// those three is entirely different. What matters most here is that the
/// storage layer's own message is reproduced verbatim on the `Detail`
/// line: it names the file and the byte offset, and no summary of ours
/// can improve on it.
///
/// This function only ever exits. There is deliberately no flag, env var
/// or branch that starts the server anyway — serving from state that is
/// not known to be durable and valid is worse than being down.
fn report_startup_failure(error: DatabaseError) -> ! {
    let data_dir = config::data_dir().display().to_string();

    eprintln!();
    eprintln!("FacetQL failed to start — the database was not opened.");
    eprintln!();
    eprintln!("  Data directory: {data_dir}");
    eprintln!("  Detail:         {}", error.detail());
    eprintln!();

    /*
     * One eprintln! per rendered line, wrapped by hand. The alternative
     * — long literals split with line continuations — reads fine in the
     * editor and silently misaligns in the terminal, which is exactly
     * the failure this whole block exists to avoid.
     */
    match &error {
        DatabaseError::Storage { phase, .. } => {
            eprintln!("  What failed:    reaching the data files, while {phase}.");
            eprintln!("  Likely cause:   the data directory is missing, is owned by another");
            eprintln!("                  user, or its filesystem is full, read-only, or not");
            eprintln!("                  mounted. Nothing has been read yet, so nothing is");
            eprintln!("                  yet known to be wrong with the data itself.");
            eprintln!();
            eprintln!("  Next steps:");
            eprintln!("    1. Check the directory and its permissions:");
            eprintln!("       ls -ld {data_dir}");
            eprintln!("    2. Create it if it is genuinely absent:");
            eprintln!("       facetql init --data-dir {data_dir}");
            eprintln!("    3. If you meant a different directory, pass --data-dir or set");
            eprintln!("       ENOCHIAN_DATA_DIR, then start again.");
        }

        /*
         * The wrong-key explanation comes first on purpose. The two
         * causes are indistinguishable to the code, but not in the
         * field: a mismatched key is far more common than bit-rot, and
         * it is the one an operator can rule out in seconds without
         * touching a single byte of data.
         */
        DatabaseError::Integrity { authentication: true, .. } => {
            eprintln!("  What failed:    a stored record did not authenticate.");
            eprintln!("  Likely cause:   in order of likelihood —");
            eprintln!("                  1. the server was started with the wrong");
            eprintln!("                     ENOCHIAN_MASTER_KEY. Leaving it unset silently");
            eprintln!("                     uses the all-zero dev key, so an unset key looks");
            eprintln!("                     exactly like a wrong one;");
            eprintln!("                  2. failing that, the file named above is corrupt.");
            eprintln!();
            eprintln!("  Next steps:");
            eprintln!("    1. Check ENOCHIAN_MASTER_KEY is the same 64-hex-character key");
            eprintln!("       this data was written with, then start again. A wrong key is");
            eprintln!("       not destructive — nothing has been modified.");
            eprintln!("    2. If the key is right, copy the files aside before anything");
            eprintln!("       else: facetql backup <dir>");
            eprintln!("    3. Then restore a known-good copy into an empty data directory:");
            eprintln!("       facetql restore <dir>");
        }

        DatabaseError::Integrity { authentication: false, .. } => {
            eprintln!("  What failed:    a stored record failed verification — framing,");
            eprintln!("                  checksum, or deserialization.");
            eprintln!("  Likely cause:   the file named above is damaged: bit-rot, an");
            eprintln!("                  interrupted write, or a partial copy. A wrong");
            eprintln!("                  ENOCHIAN_MASTER_KEY can also present this way, when");
            eprintln!("                  a record decodes but then does not parse.");
            eprintln!();
            eprintln!("  Next steps:");
            eprintln!("    1. Copy the current files aside before touching anything:");
            eprintln!("       facetql backup <dir>");
            eprintln!("    2. Confirm ENOCHIAN_MASTER_KEY matches the key this data was");
            eprintln!("       written with.");
            eprintln!("    3. Restore a known-good copy into an empty data directory:");
            eprintln!("       facetql restore <dir>");
        }

        DatabaseError::WalRecovery { .. } => {
            eprintln!("  What failed:    replaying the write-ahead log. The frames verified,");
            eprintln!("                  but the history they describe is not a legal one.");
            eprintln!("  Likely cause:   facetql.wal was written by a different build or");
            eprintln!("                  format, or was copied in from another data");
            eprintln!("                  directory, or a write was interrupted in a way that");
            eprintln!("                  left the log inconsistent. Replaying it would invent");
            eprintln!("                  a state that never existed, so recovery stops.");
            eprintln!();
            eprintln!("  Next steps:");
            eprintln!("    1. Back up the whole directory first: facetql backup <dir>. The");
            eprintln!("       WAL is the only record of writes not yet folded into");
            eprintln!("       the heap and the indexes.");
            eprintln!("    2. Confirm facetql.wal belongs to this data directory and to this");
            eprintln!("       build of FacetQL.");
            eprintln!("    3. Restore a known-good copy into an empty data directory:");
            eprintln!("       facetql restore <dir>");
        }
    }

    eprintln!();
    eprintln!("  FacetQL will not start until this is resolved, and there is no flag");
    eprintln!("  to make it. Serving state that is not known to be durable and valid");
    eprintln!("  would be worse than being down.");
    eprintln!();

    /*
     * Exit codes, continuing the policy in src/cli/error.rs
     * (2 = usage error, 1 = runtime failure, 3 = operator declined)
     * rather than competing with it:
     *
     *   1  storage/IO   — the files could not be reached at all
     *   4  integrity    — bytes were read and failed to verify
     *                     (wrong master key, or corruption)
     *   5  WAL recovery — bytes verified, the logged history is illegal
     *
     * 2 and 3 keep their CLI meanings so one code means one thing across
     * the whole binary. Split this way because a supervisor can act on
     * the difference: 1 is worth retrying once a mount comes back, while
     * 4 and 5 never resolve on their own and need an operator.
     */
    std::process::exit(error.exit_code())
}

async fn run_server(
    port: u16,
    tls_identity: Option<PathBuf>,
    tls_identity_password: Option<String>,
) {
    /*
     * Database initialization includes:
     *
     *   1. Creating the data directory if it does not exist.
     *   2. Loading persistent storage files.
     *   3. Recovering the authenticated WAL.
     *   4. Rejecting corrupted or invalid recovery state.
     *
     * A database that cannot recover safely must never start serving
     * requests.
     *
     * Step 1 used to be a separate `ensure_data_dir().expect(...)` here.
     * It was removed rather than kept: Database::new() does it anyway,
     * and doing it first meant the one failure most likely to be a plain
     * permissions problem was the one failure that came out as a panic
     * instead of a diagnosis.
     */
    let db = match Database::new() {
        Ok(db) => std::sync::Arc::new(db),

        Err(error) => report_startup_failure(error),
    };

    let app = create_router(db);

    println!(
        "Data directory: {}",
        config::data_dir().display()
    );

    println!(
        "Auth: requests to /node* and /edge* require header \
         'x-api-key', mapped to an owner identity via ENOCHIAN_TOKENS \
         (format: token1:alice,token2:bob) plus any users created via \
         POST /admin/users. A single admin dev token is used if \
         ENOCHIAN_TOKENS is unset — do not run production traffic \
         against that."
    );

    let listener =
        tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "failed to bind FacetQL server to port {port}: {error}"
                )
            });

    match tls_identity {
        Some(identity_path) => {
            let password =
                tls_identity_password.unwrap_or_else(|| {
                    eprintln!(
                        "warning: --tls-identity was set without \
                         --tls-identity-password — trying an empty \
                         password. This will fail for any real \
                         PKCS#12 file."
                    );

                    String::new()
                });

            let identity_bytes =
                std::fs::read(&identity_path)
                    .unwrap_or_else(|error| {
                        panic!(
                            "failed to read TLS identity file {}: {error}",
                            identity_path.display()
                        )
                    });

            let identity =
                native_tls::Identity::from_pkcs12(
                    &identity_bytes,
                    &password,
                )
                    .unwrap_or_else(|error| {
                        panic!(
                            "failed to load TLS identity: {error}"
                        )
                    });

            let acceptor: tokio_native_tls::TlsAcceptor =
                native_tls::TlsAcceptor::new(identity)
                    .unwrap_or_else(|error| {
                        panic!(
                            "failed to create TLS acceptor: {error}"
                        )
                    })
                    .into();

            println!(
                "FacetQL Server Running on port {port} (HTTPS)"
            );

            tls_server::serve_tls(
                listener,
                app,
                acceptor,
            )
                .await;
        }

        None => {
            println!(
                "FacetQL Server Running on port {port} \
                 (HTTP — no TLS; either pass \
                 --tls-identity/--tls-identity-password for \
                 native HTTPS, or terminate TLS at a reverse \
                 proxy in front of this, before running with \
                 real traffic)"
            );

            axum::serve(listener, app)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "FacetQL HTTP server failed: {error}"
                    )
                });
        }
    }
}

#[cfg(test)]
mod cli_tests {
    //! Deterministic CLI tests: argument parsing, subcommand dispatch,
    //! flag defaults, and clap's built-in `--help`/`--version` handling.
    //! None of these require a running server — they only exercise the
    //! parse tree and pure helpers.
    use super::*;
    use clap::error::ErrorKind;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("should parse")
    }

    #[test]
    fn get_parses_address_and_token() {
        let cli = parse(&["facetql", "get", "addr1", "--token", "T"]);
        match cli.command {
            Some(Command::Get(a)) => {
                assert_eq!(a.address, "addr1");
                assert_eq!(a.common.token.as_deref(), Some("T"));
                // URL default applies when unset.
                assert_eq!(a.common.url, "http://localhost:8080");
                assert!(!a.common.json);
            }
            other => panic!("expected Get, got {other:?}"),
        }
    }

    #[test]
    fn json_flag_sets_output_mode() {
        let cli = parse(&["facetql", "get", "addr1", "--json"]);
        match cli.command {
            Some(Command::Get(a)) => assert!(a.common.json),
            other => panic!("expected Get, got {other:?}"),
        }
    }

    #[test]
    fn user_create_admin_flag() {
        let cli = parse(&["facetql", "user", "create", "alice", "--admin", "--token", "T"]);
        match cli.command {
            Some(Command::User { action: cli::UserAction::Create { owner, admin, .. } }) => {
                assert_eq!(owner, "alice");
                assert!(admin);
            }
            other => panic!("expected user create, got {other:?}"),
        }
    }

    #[test]
    fn user_create_defaults_to_non_admin() {
        let cli = parse(&["facetql", "user", "create", "bob", "--token", "T"]);
        match cli.command {
            Some(Command::User { action: cli::UserAction::Create { admin, .. } }) => {
                assert!(!admin);
            }
            other => panic!("expected user create, got {other:?}"),
        }
    }

    #[test]
    fn user_delete_requires_yes_flag_to_be_explicit() {
        let cli = parse(&["facetql", "user", "delete", "bob"]);
        match cli.command {
            Some(Command::User { action: cli::UserAction::Delete { owner, yes, .. } }) => {
                assert_eq!(owner, "bob");
                assert!(!yes, "yes must default false so deletes prompt");
            }
            other => panic!("expected user delete, got {other:?}"),
        }
        let cli = parse(&["facetql", "user", "delete", "bob", "--yes"]);
        match cli.command {
            Some(Command::User { action: cli::UserAction::Delete { yes, .. } }) => assert!(yes),
            other => panic!("expected user delete, got {other:?}"),
        }
    }

    #[test]
    fn put_parses_kind_data_public() {
        let cli = parse(&[
            "facetql", "put", "n1", "--kind", "Client", "--data", "{\"a\":1}", "--public",
            "--token", "T",
        ]);
        match cli.command {
            Some(Command::Put(a)) => {
                assert_eq!(a.address, "n1");
                assert_eq!(a.kind, "Client");
                assert_eq!(a.data, "{\"a\":1}");
                assert!(a.public);
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn delete_defaults_to_prompting() {
        let cli = parse(&["facetql", "delete", "n1"]);
        match cli.command {
            Some(Command::Delete(a)) => assert!(!a.yes),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn query_parses_kind_limit_order_desc() {
        let cli = parse(&[
            "facetql", "query", "--kind", "Client", "--limit", "5", "--order", "score", "--desc",
        ]);
        match cli.command {
            Some(Command::Query(a)) => {
                assert_eq!(a.kind, "Client");
                assert_eq!(a.limit, Some(5));
                assert_eq!(a.order.as_deref(), Some("score"));
                assert!(a.desc);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn query_requires_kind() {
        // --kind is mandatory; omitting it is a parse (usage) error.
        let err = Cli::try_parse_from(["facetql", "query"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn version_and_help_are_handled_by_clap() {
        let v = Cli::try_parse_from(["facetql", "--version"]).unwrap_err();
        assert_eq!(v.kind(), ErrorKind::DisplayVersion);
        let h = Cli::try_parse_from(["facetql", "--help"]).unwrap_err();
        assert_eq!(h.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn no_subcommand_still_parses_for_server_start() {
        let cli = parse(&["facetql"]);
        assert!(cli.command.is_none());
    }
}
