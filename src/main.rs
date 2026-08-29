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

use api::routes::create_router;
use database::Database;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// FacetQL — a standalone, coordinate-native database server.
#[derive(Parser)]
#[command(name = "facetql", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Where data files live. Defaults to ~/.facetql if unset.
    /// Also settable via ENOCHIAN_DATA_DIR.
    #[arg(long, global = true, env = "ENOCHIAN_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
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
}

#[derive(Subcommand)]
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
    }
}

/// Every file this checkpoint's storage layer writes. Kept as one list
/// so backup/restore stay in sync automatically if a future checkpoint
/// adds another data file — update this list, both commands pick it up.
const DATA_FILES: &[&str] = &[
    "facetql.data",
    "facetql.wal",
    "facetql.edges",
    "facetql.tombstones",
    "facetql.users",
];

fn run_backup(output_dir: PathBuf) {
    std::fs::create_dir_all(&output_dir)
        .expect("failed to create backup output directory");

    let mut copied = 0;

    for filename in DATA_FILES {
        let src = config::data_file(filename);

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
    let existing: Vec<&str> = DATA_FILES
        .iter()
        .filter(|f| config::data_file(f).exists())
        .copied()
        .collect();

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

    for filename in DATA_FILES {
        let src = input_dir.join(filename);

        if src.exists() {
            let dst = config::data_file(filename);

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

async fn run_server(
    port: u16,
    tls_identity: Option<PathBuf>,
    tls_identity_password: Option<String>,
) {
    config::ensure_data_dir()
        .expect("failed to create data directory");

    /*
     * Database initialization includes:
     *
     *   1. Loading persistent storage files.
     *   2. Recovering the authenticated WAL.
     *   3. Rejecting corrupted or invalid recovery state.
     *
     * A database that cannot recover safely must never start serving
     * requests.
     */
    let db = match Database::new() {
        Ok(db) => std::sync::Arc::new(db),

        Err(error) => {
            eprintln!(
                "FacetQL database initialization failed: {error}"
            );

            std::process::exit(1);
        }
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
