mod cli;

use facetql::{api, auth, config, crypto, database, storage, tls_server};

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

    /// Manage identities (admin only). Talks to a *running* server over
    /// its API — start the server first.
    User {
        #[command(subcommand)]
        action: cli::UserAction,
    },

    /// Manage secondary indexes over node `data` fields (admin only).
    /// Talks to a *running* server over its API — start the server
    /// first.
    Index {
        #[command(subcommand)]
        action: cli::IndexAction,
    },

    /// Manage references between kinds — cascade, restrict and
    /// set-null on delete (admin only). Talks to a *running* server
    /// over its API — start the server first.
    Reference {
        #[command(subcommand)]
        action: cli::ReferenceAction,
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

    /// Print the per-endpoint authorization matrix this build enforces:
    /// every route, who may call it, what it costs, and the per-object
    /// rule its handler applies. Offline — no server, no token.
    Routes {
        /// Emit JSON instead of the aligned table.
        #[arg(long)]
        json: bool,
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

        // Client commands: each maps onto an existing API route and, on
        // error, prints a structured message and exits non-zero via
        // `cli::report_error` — see src/cli/mod.rs.
        Some(Command::User { action }) => {
            if let Err(e) = cli::run_user(action).await {
                cli::report_error(e);
            }
        }

        Some(Command::Index { action }) => {
            if let Err(e) = cli::run_index(action).await {
                cli::report_error(e);
            }
        }

        Some(Command::Reference { action }) => {
            if let Err(e) = cli::run_reference(action).await {
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

        Some(Command::Routes { json }) => {
            if let Err(e) = cli::run_routes(json) {
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
/// Two kinds of file are skipped, for different reasons:
///
/// * In-progress temp files (`*.tmp`) — the write-then-rename halves of
///   a catalog, checkpoint or user-log update. A copy of one is either
///   redundant or garbage.
///
/// * The lock file (`facetql.lock`) — it holds no data. What it carries
///   is a kernel-held lock on an open descriptor, which is precisely the
///   thing a copy cannot reproduce, so backing it up would ship a file
///   whose only meaning is "some other process is running", into a
///   directory where that is false.
fn data_files() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(config::data_dir())
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.path().is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.ends_with(".tmp"))
        .filter(|name| name != storage::lock::LOCK_FILE)
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
    /*
     * Before anything is opened, read or bound: is this a configuration
     * we are willing to serve with at all? A database that starts under
     * the development credentials has already lost — the door and the
     * disk are both open — and it starts looking exactly like a healthy
     * one, so the check has to happen here rather than being noticed
     * later.
     */
    enforce_deployment_posture(tls_identity.is_some());

    let db = match Database::new() {
        Ok(db) => std::sync::Arc::new(db),

        Err(error) => report_startup_failure(error),
    };

    let app = create_router(std::sync::Arc::clone(&db));

    println!(
        "Data directory: {}",
        config::data_dir().display()
    );

    /*
     * The banner used to end with "a single admin dev token is used if
     * ENOCHIAN_TOKENS is unset — do not run production traffic against
     * that", which is no longer true and has not been the useful thing
     * to say since `enforce_deployment_posture` made it impossible: in
     * production that configuration does not start, and in development
     * the posture block above has already listed it as a finding. So the
     * banner states what this process will actually enforce, and points
     * at the command that prints the rest.
     */
    println!(
        "Auth: every route except GET / requires header 'x-api-key', \
         resolved to an owner identity via ENOCHIAN_TOKENS \
         (token:owner[:admin], comma-separated) or the persistent users \
         created through POST /admin/users. Run `facetql routes` for the \
         per-endpoint authorization matrix this build enforces."
    );

    println!(
        "Guards: body <= {} bytes, {} s per request, <= {} in-flight \
         requests, per-identity rate limits by endpoint class \
         (FACETQL_RATE_READ/WRITE/BULK/ADMIN/SUBSCRIBE).",
        api::limits::max_body_bytes(),
        api::limits::request_timeout().as_secs(),
        api::limits::max_concurrent_requests(),
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
                shutdown_signal(),
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

            /*
             * Graceful, but on a clock.
             *
             * `with_graceful_shutdown` waits for every open connection
             * to close, and `GET /events` is an SSE stream that stays
             * open for as long as the subscriber wants it. Waiting
             * unconditionally would mean a single live subscriber turns
             * every SIGTERM into a hang until the supervisor's SIGKILL
             * — strictly worse than not draining at all, because the
             * final checkpoint below would never run either.
             *
             * So the drain gets a deadline: in-flight requests finish
             * normally, and if something is still holding a connection
             * when it expires, the server stops waiting and moves on to
             * settle and exit. The oneshot is what starts the clock —
             * the deadline must run from the moment the signal arrives,
             * not from startup.
             */
            let (signalled, awaiting_signal) =
                tokio::sync::oneshot::channel::<()>();

            let graceful = async move {
                shutdown_signal().await;
                let _ = signalled.send(());
            };

            let drain_deadline = async move {
                if awaiting_signal.await.is_ok() {
                    tokio::time::sleep(SHUTDOWN_GRACE).await;
                } else {
                    // The server stopped for some other reason and the
                    // sender was dropped; never fire.
                    std::future::pending::<()>().await;
                }
            };

            tokio::select! {
                result = axum::serve(listener, app)
                    .with_graceful_shutdown(graceful) =>
                {
                    result.unwrap_or_else(|error| {
                        panic!(
                            "FacetQL HTTP server failed: {error}"
                        )
                    });
                }

                _ = drain_deadline => {
                    eprintln!(
                        "FacetQL: connections still open after \
                         {}s; closing them and stopping",
                        SHUTDOWN_GRACE.as_secs()
                    );
                }
            }
        }
    }

    settle(&db);
}

/// Refuse to serve with development credentials, or in the clear,
/// unless this is declared to be a development deployment.
///
/// # The problem this replaces
///
/// Both credential defaults announced themselves and carried on:
/// `auth.rs` printed `warning: ENOCHIAN_TOKENS not set` and admitted a
/// published admin token, `crypto.rs` printed `warning:
/// ENOCHIAN_MASTER_KEY not set` and encrypted the whole database under
/// thirty-two zero bytes. Together they are total compromise —
/// administrator at the door, plaintext at rest — reachable by doing
/// nothing at all, and the only signal was two lines on stderr that a
/// supervisor capturing stdout never sees and that scroll away in the
/// first seconds of traffic. A warning changes no behaviour; that is
/// what makes it the wrong instrument for a condition that must not be
/// served through.
///
/// # Why the default posture is production
///
/// See [`config::Deployment`]. The short version: the alternative puts
/// the safe behaviour behind something an operator has to remember, and
/// forgetting then produces a running, serving, fully-compromised
/// database. Here, forgetting produces this refusal, which names the
/// variable to set. One of those failure modes is recoverable in
/// seconds and the other is a breach.
///
/// # What is checked
///
/// Three things, and all findings are reported together rather than one
/// per restart — an operator fixing a production launch should learn
/// everything that is wrong in one pass:
///
///  1. the bootstrap credentials (`auth::credential_defect`),
///  2. the at-rest key (`crypto::key_defect`),
///  3. plaintext HTTP without the explicit acknowledgement that TLS is
///     terminated in front of this process
///     (`config::plaintext_acknowledged`).
///
/// In a development deployment the same findings are printed as
/// warnings and the server starts, which is the behaviour that existed
/// before — kept, because a developer running `cargo run` with no
/// environment at all is the case the defaults were written for.
fn enforce_deployment_posture(tls_configured: bool) {
    let mut findings: Vec<String> = Vec::new();

    if let Some(defect) = auth::credential_defect() {
        findings.push(defect.to_string());
    }

    if let Some(defect) = crypto::key_defect() {
        findings.push(defect.to_string());
    }

    if !tls_configured && !config::plaintext_acknowledged() {
        findings.push(
            "no TLS identity is configured, so every request — including \
             the x-api-key bearer token in its header — would cross the \
             network in the clear"
                .to_string(),
        );
    }

    if findings.is_empty() {
        return;
    }

    if !config::deployment().is_production() {
        eprintln!();
        eprintln!(
            "FacetQL is running in DEVELOPMENT posture ({}=development). \
             Not safe for real data:",
            config::ENV_VAR
        );

        for finding in &findings {
            eprintln!("  - {finding}");
        }

        eprintln!();

        return;
    }

    eprintln!();
    eprintln!("FacetQL refused to start — the configuration is not one it will serve with.");
    eprintln!();
    eprintln!("  Posture: production (the default; {} is not set to `development`)", config::ENV_VAR);
    eprintln!();
    eprintln!("  What is wrong:");

    for finding in &findings {
        eprintln!("    - {finding}");
    }

    eprintln!();
    eprintln!("  Next steps — set what applies, then start again:");
    eprintln!("    1. Real bootstrap credentials, at least one of them an admin:");
    eprintln!("         export {}=\"$(openssl rand -hex 32):root:admin\"", auth::TOKENS_ENV);
    eprintln!("       Then create every further identity through POST /admin/users.");
    eprintln!("    2. A real at-rest key — 64 hex characters, kept with the data it");
    eprintln!("       encrypts, because losing it loses the database:");
    eprintln!("         export {}=\"$(openssl rand -hex 32)\"", crypto::MASTER_KEY_ENV);
    eprintln!("    3. TLS. Either serve it here:");
    eprintln!("         --tls-identity <file.p12> --tls-identity-password <password>");
    eprintln!("       or, if TLS is terminated by a proxy in front of this process,");
    eprintln!("       say so explicitly:");
    eprintln!("         export {}=1", config::ALLOW_PLAINTEXT_ENV);
    eprintln!();
    eprintln!("  If this really is a development machine and none of the above is");
    eprintln!("  meant to be real:");
    eprintln!("         export {}=development", config::ENV_VAR);
    eprintln!();

    std::process::exit(config::EXIT_INSECURE_CONFIGURATION);
}

/// How long a shutdown waits for open connections before stopping
/// anyway.
///
/// Chosen to sit under the two defaults that would otherwise kill the
/// process mid-drain — Kubernetes' 30s `terminationGracePeriodSeconds`
/// and systemd's 90s `TimeoutStopSec` — with room left for the final
/// checkpoint afterwards. A request that has not finished in fifteen
/// seconds is not going to be helped by thirty.
const SHUTDOWN_GRACE: std::time::Duration =
    std::time::Duration::from_secs(15);

/// Resolves when the process is asked to stop.
///
/// Both signals a supervisor actually sends are handled. SIGTERM is what
/// systemd, Docker and Kubernetes send first, and answering only SIGINT
/// would mean every orchestrated shutdown reached the SIGKILL timeout
/// instead — the case where a clean stop matters most is exactly the one
/// that would not get one.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,

            // Without a SIGTERM handler the default disposition applies
            // and the process dies immediately on one. That is the old
            // behaviour, so fall back to it rather than refusing to
            // serve.
            Err(error) => {
                eprintln!(
                    "warning: could not install a SIGTERM handler \
                     ({error}); shutdown on SIGTERM will not be graceful"
                );

                std::future::pending::<()>().await;
                return;
            }
        };

        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    println!("FacetQL: shutdown signal received; finishing in-flight requests");
}

/// Take one last checkpoint on the way out.
///
/// Skipping this is *safe* — every acknowledged mutation is already
/// fsync'd to the WAL, so nothing is lost either way — but it is not
/// free. Whatever the last checkpoint did not cover is replayed at the
/// next start, and replay re-appends every record it redoes, so a
/// process stopped mid-interval starts slower and leaves a superseded
/// copy of each replayed record for compaction to clean up. Checkpointing
/// here is what makes a planned restart cost nothing: the heap, the
/// catalog and the indexes are on disk, the durability boundary is past
/// them, and the WAL rotates behind it.
///
/// A failure is reported and not fatal. The process is already stopping,
/// the data is already durable, and the only consequence is the replay
/// this was trying to avoid.
fn settle(db: &std::sync::Arc<Database>) {
    match db.engine_mut().checkpoint() {
        Ok(()) => println!("FacetQL: storage checkpointed; stopped cleanly"),

        Err(error) => eprintln!(
            "warning: the final checkpoint failed: {error}. Nothing is \
             lost — every acknowledged write is durable in the \
             write-ahead log — but the next start will replay more of it."
        ),
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

    /// `facetql routes` takes no connection options at all — it reports
    /// what this binary enforces, not what a server says.
    #[test]
    fn routes_parses_without_a_token_or_url() {
        match parse(&["facetql", "routes"]).command {
            Some(Command::Routes { json }) => assert!(!json),
            other => panic!("expected Routes, got {other:?}"),
        }

        match parse(&["facetql", "routes", "--json"]).command {
            Some(Command::Routes { json }) => assert!(json),
            other => panic!("expected Routes, got {other:?}"),
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
    fn index_create_parses_name_kind_field() {
        let cli = parse(&[
            "facetql", "index", "create", "post_created", "--kind", "Post", "--field",
            "created_at", "--token", "T",
        ]);
        match cli.command {
            Some(Command::Index {
                action:
                    cli::IndexAction::Create {
                        name, kind, field, unique, text, common,
                    },
            }) => {
                assert_eq!(name, "post_created");
                assert_eq!(kind, "Post");
                assert_eq!(field, "created_at");
                assert_eq!(common.token.as_deref(), Some("T"));

                // An index is not a constraint unless it was asked to
                // be: the flag's absence has to mean an ordinary index,
                // or every existing declaration would start refusing
                // duplicate values.
                assert!(!unique, "unique is opt-in");

                // And it is an ordered index unless it was asked to be
                // an inverted one, for the same reason: the flag's
                // absence has to keep meaning what it always meant.
                assert!(!text, "text is opt-in");
            }
            other => panic!("expected index create, got {other:?}"),
        }
    }

    #[test]
    fn index_create_takes_the_text_flag() {
        let cli = parse(&[
            "facetql", "index", "create", "post_body", "--kind", "Post", "--field",
            "body", "--text",
        ]);

        match cli.command {
            Some(Command::Index {
                action: cli::IndexAction::Create { text, unique, .. },
            }) => {
                assert!(text);
                assert!(!unique, "the two flags are independent");
            }
            other => panic!("expected index create, got {other:?}"),
        }
    }

    #[test]
    fn index_create_takes_the_unique_flag() {
        let cli = parse(&[
            "facetql", "index", "create", "handle", "--kind", "User", "--field",
            "handle", "--unique",
        ]);

        match cli.command {
            Some(Command::Index {
                action: cli::IndexAction::Create { unique, .. },
            }) => assert!(unique),
            other => panic!("expected index create, got {other:?}"),
        }
    }

    #[test]
    fn reference_create_parses_both_shapes() {
        // By address: no --parent-field, which is the common case and
        // the server's own default.
        let cli = parse(&[
            "facetql", "reference", "create", "post_comments", "--kind", "Comment",
            "--field", "post", "--parent-kind", "Post", "--on-delete", "cascade",
        ]);

        match cli.command {
            Some(Command::Reference {
                action:
                    cli::ReferenceAction::Create {
                        name,
                        kind,
                        field,
                        parent_kind,
                        parent_field,
                        on_delete,
                        ..
                    },
            }) => {
                assert_eq!(name, "post_comments");
                assert_eq!(kind, "Comment");
                assert_eq!(field, "post");
                assert_eq!(parent_kind, "Post");
                assert_eq!(parent_field, None);
                assert_eq!(on_delete, "cascade");
            }
            other => panic!("expected reference create, got {other:?}"),
        }

        // By a data field of the parent.
        let cli = parse(&[
            "facetql", "reference", "create", "author", "--kind", "Post", "--field",
            "author_id", "--parent-kind", "User", "--parent-field", "id",
            "--on-delete", "restrict",
        ]);

        match cli.command {
            Some(Command::Reference {
                action: cli::ReferenceAction::Create { parent_field, on_delete, .. },
            }) => {
                assert_eq!(parent_field.as_deref(), Some("id"));
                assert_eq!(on_delete, "restrict");
            }
            other => panic!("expected reference create, got {other:?}"),
        }
    }

    #[test]
    fn reference_create_rejects_an_action_that_is_not_one() {
        // The three actions are the whole vocabulary. A typo has to be
        // a usage error here rather than a 400 from the server after a
        // round trip — and rather than a rule that silently does
        // nothing.
        let err = Cli::try_parse_from([
            "facetql", "reference", "create", "r", "--kind", "Comment", "--field",
            "post", "--parent-kind", "Post", "--on-delete", "delete",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn reference_drop_requires_confirmation_or_yes() {
        let cli = parse(&["facetql", "reference", "drop", "post_comments", "--yes"]);

        match cli.command {
            Some(Command::Reference {
                action: cli::ReferenceAction::Drop { name, yes, .. },
            }) => {
                assert_eq!(name, "post_comments");
                assert!(yes);
            }
            other => panic!("expected reference drop, got {other:?}"),
        }
    }

    #[test]
    fn index_create_requires_kind_and_field() {
        // Both are mandatory: an index is defined by exactly one kind
        // and one field, so a partial declaration is a usage error, not
        // a request the server should have to reject.
        let err = Cli::try_parse_from(["facetql", "index", "create", "n", "--kind", "Post"])
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        let err = Cli::try_parse_from(["facetql", "index", "create", "n", "--field", "created_at"])
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn index_list_takes_json_flag() {
        let cli = parse(&["facetql", "index", "list", "--json"]);
        match cli.command {
            Some(Command::Index { action: cli::IndexAction::List { common } }) => {
                assert!(common.json);
            }
            other => panic!("expected index list, got {other:?}"),
        }
    }

    #[test]
    fn index_drop_defaults_to_prompting() {
        let cli = parse(&["facetql", "index", "drop", "post_created"]);
        match cli.command {
            Some(Command::Index { action: cli::IndexAction::Drop { name, yes, .. } }) => {
                assert_eq!(name, "post_created");
                assert!(!yes, "yes must default false so drops prompt");
            }
            other => panic!("expected index drop, got {other:?}"),
        }
        let cli = parse(&["facetql", "index", "drop", "post_created", "--yes"]);
        match cli.command {
            Some(Command::Index { action: cli::IndexAction::Drop { yes, .. } }) => assert!(yes),
            other => panic!("expected index drop, got {other:?}"),
        }
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
