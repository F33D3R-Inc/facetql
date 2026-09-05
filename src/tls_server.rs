use axum::body::Body;
use axum::Router;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio_native_tls::TlsAcceptor;
use tower::util::ServiceExt;

/// Serves `app` over TLS on `listener`.
///
/// `axum::serve` in this axum version (0.7) only accepts a plain
/// `tokio::net::TcpListener` — there's no generic `Listener` trait to
/// plug a TLS stream into (that arrived later, in axum 0.8). The
/// natural fix, `axum-server`, turned out to have a real trait-bound
/// incompatibility with this project's exact resolved dependency
/// versions that I could not resolve through version pinning.
///
/// So this replicates axum::serve's own internal accept loop directly
/// — the `Builder::new(TokioExecutor::new()).serve_connection_with_upgrades(...)`
/// call below is copied from axum's own `src/serve.rs`, not invented —
/// with one difference: each accepted TCP connection is TLS-handshaked
/// before being handed to hyper, instead of served as plaintext. This
/// reuses the exact hyper/hyper-util machinery axum already depends on
/// and that's already proven working (every plaintext request this
/// whole project has served went through the equivalent call), rather
/// than pulling in a wrapper crate with its own separate version
/// constraints.
///
/// `shutdown` is awaited alongside the accept loop. `axum::serve` grew a
/// `with_graceful_shutdown` for the plaintext path; this loop is our own,
/// so it has to select on the signal itself — otherwise the TLS
/// deployment would be the one that never stops cleanly, and TLS is the
/// deployment that is actually in production.
///
/// Shutdown here means "stop accepting". Connections already handed to
/// hyper are spawned tasks and finish on their own; the process then
/// takes its final checkpoint (`settle` in `main`) before exiting.
pub async fn serve_tls(
    listener: TcpListener,
    app: Router,
    acceptor: TlsAcceptor,
    shutdown: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(shutdown);

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = &mut shutdown => return,
        };

        let (tcp_stream, remote_addr) = match accepted {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("warning: failed to accept a connection: {e}");
                continue;
            }
        };

        /*
         * The connection cap, and the only place either serving path
         * can apply one.
         *
         * A TLS connection costs a file descriptor, a task and a full
         * handshake — asymmetric public-key work this process performs
         * before one byte of HTTP has been parsed, and therefore before
         * any credential has been presented. It is the one cost a
         * completely unauthenticated client can impose, so it cannot be
         * governed by any of the per-identity bounds, and it is not
         * reachable from a tower layer either: a layer runs per
         * *request*, which is already past the handshake.
         *
         * Dropping the stream is the whole refusal. There is nothing to
         * answer with — no session has been negotiated — and answering
         * would mean doing the work being refused.
         */
        let permit = match crate::api::limits::connection_permit() {
            Some(permit) => permit,

            None => {
                eprintln!(
                    "warning: refusing a connection from {remote_addr}: at \
                     the concurrent-connection limit"
                );

                drop(tcp_stream);
                continue;
            }
        };

        let acceptor = acceptor.clone();
        let app = app.clone();

        tokio::spawn(async move {
            // Released when this task ends, i.e. when the connection
            // closes — not when its first request finishes.
            let _connection_slot = permit;

            let tls_stream = match acceptor.accept(tcp_stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    // Routine — happens for any plain-HTTP request sent to
                    // this port, or a client that just probes and
                    // disconnects. Not worth more than a debug-level note.
                    eprintln!("TLS handshake failed for {remote_addr}: {e}");
                    return;
                }
            };

            let io = TokioIo::new(tls_stream);

            // Bridge hyper's raw Request<Incoming> to axum's
            // Request<axum::body::Body> — the same adapter axum::serve
            // applies internally before handing a connection to the
            // router.
            let tower_service = app.map_request(|req: hyper::Request<Incoming>| req.map(Body::new));
            let hyper_service = TowerToHyperService::new(tower_service);

            if let Err(e) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, hyper_service)
                .await
            {
                // Matches axum::serve's own handling: this fires mainly
                // when a client disconnects without completing a
                // request, which is normal traffic noise, not a bug.
                let _ = e;
            }
        });
    }
}
