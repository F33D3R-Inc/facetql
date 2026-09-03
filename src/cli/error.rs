//! CLI error type and process exit-code policy.
//!
//! Every fallible CLI path returns `Result<_, CliError>`. `main` renders
//! the error to stderr and exits with the code from [`CliError::exit_code`]
//! so scripts can branch on failure. Nothing here ever carries the
//! caller's token — the only externally-sourced string a variant holds is
//! a server response body, which does not include the request's
//! `x-api-key`.

use std::fmt;

#[derive(Debug)]
pub enum CliError {
    /// No token was supplied via `--token` or `FACETQL_TOKEN`. Every
    /// endpoint this CLI uses is authenticated, so this is a usage error.
    MissingToken,
    /// The caller passed a malformed argument (empty/kind, bad address,
    /// non-JSON `--data`). Usage error.
    InvalidInput(String),
    /// A transport-level failure — could not reach the server, timeout,
    /// unreadable response.
    Request(String),
    /// The server answered with a non-2xx status. `message` is the
    /// server's own response body.
    Api { status: u16, message: String },
    /// A destructive command was not confirmed by the operator.
    Aborted,
}

impl CliError {
    pub fn api(status: u16, message: String) -> Self {
        CliError::Api {
            status,
            message: message.trim().to_string(),
        }
    }

    /// Non-zero exit codes, split so scripts can distinguish a usage
    /// mistake (2, matching clap's own parse-error code) from a runtime
    /// failure (1) and an operator-declined destructive action (3).
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::MissingToken | CliError::InvalidInput(_) => 2,
            CliError::Request(_) | CliError::Api { .. } => 1,
            CliError::Aborted => 3,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::MissingToken => write!(
                f,
                "no admin token provided — pass --token or set FACETQL_TOKEN"
            ),
            CliError::InvalidInput(msg) => write!(f, "{msg}"),
            CliError::Request(msg) => write!(f, "{msg}"),
            CliError::Api { status, message } => {
                if message.is_empty() {
                    write!(f, "server returned HTTP {status}")
                } else {
                    write!(f, "server returned HTTP {status}: {message}")
                }
            }
            CliError::Aborted => write!(f, "aborted"),
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_distinct_and_nonzero() {
        assert_eq!(CliError::MissingToken.exit_code(), 2);
        assert_eq!(CliError::InvalidInput("x".into()).exit_code(), 2);
        assert_eq!(CliError::Request("x".into()).exit_code(), 1);
        assert_eq!(
            CliError::Api { status: 500, message: "x".into() }.exit_code(),
            1
        );
        assert_eq!(CliError::Aborted.exit_code(), 3);
    }

    #[test]
    fn api_error_trims_and_formats() {
        let e = CliError::api(404, "  node not found\n".into());
        assert_eq!(e.to_string(), "server returned HTTP 404: node not found");
    }

    #[test]
    fn api_error_empty_body() {
        let e = CliError::api(403, "".into());
        assert_eq!(e.to_string(), "server returned HTTP 403");
    }
}
