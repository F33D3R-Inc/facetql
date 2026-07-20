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
pub async fn serve_tls(listener: TcpListener, app: Router, acceptor: TlsAcceptor) {
    loop {
        let (tcp_stream, remote_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("warning: failed to accept a connection: {e}");
                continue;
            }
        };

        let acceptor = acceptor.clone();
        let app = app.clone();

        tokio::spawn(async move {
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
