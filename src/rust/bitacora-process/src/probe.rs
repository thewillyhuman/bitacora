//! A minimal HTTP client for a daemon's endpoint.
//!
//! # Why this file exists
//!
//! The tests of this crate, the test that runs a cluster of real processes, and operator tooling all
//! need to ask a daemon's endpoint one question: what does `/readyz` or `/metrics` answer right now?
//! A full HTTP client is a large dependency for one `GET`. This file does exactly that `GET` over a
//! plain TCP connection.
//!
//! # What it contains
//!
//! - [`http_get`]: one `GET` request, bounded by a timeout.
//! - [`HttpAnswer`]: the status and the body that came back.
//! - [`ProbeError`]: why no answer came back.
//!
//! # How it works
//!
//! The request asks the server to close the connection after its answer (`Connection: close`), so the
//! answer is everything the server sends until it closes. The status is taken from the first line and
//! the body is whatever follows the blank line after the headers.
//!
//! This is enough for the endpoint of this crate, which always answers with a body of known length. It
//! is not a general HTTP client: it does not follow redirects, decode chunked bodies or speak TLS.
//!
//! # Invariants
//!
//! - The whole exchange, connecting included, takes no longer than the timeout.
//!
//! # See also
//!
//! - `http_endpoint.rs`: the endpoint this client is for.

use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// What a daemon's endpoint answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpAnswer {
    /// The HTTP status code, for example 200 or 503.
    pub status: u16,
    /// The body of the answer, as text.
    pub body: String,
}

/// Why a probe got no answer.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ProbeError {
    /// The endpoint could not be reached, or the connection broke.
    #[error("cannot reach {address}: {reason}")]
    Unreachable {
        /// The address that was probed.
        address: String,
        /// What the operating system reported.
        reason: String,
    },
    /// The endpoint did not answer in time.
    #[error("{address} did not answer within {waited:?}")]
    TimedOut {
        /// The address that was probed.
        address: String,
        /// How long the probe waited.
        waited: Duration,
    },
    /// Something answered, but not with HTTP.
    #[error("{address} did not answer with HTTP: {reason}")]
    NotHttp {
        /// The address that was probed.
        address: String,
        /// What is wrong with the answer.
        reason: String,
    },
}

/// Sends one `GET` request to a daemon's endpoint.
///
/// # Arguments
///
/// * `address` - Where the endpoint listens, as `host:port`.
/// * `path` - The path to ask for, for example `/readyz`.
/// * `request_timeout` - How long connecting, sending and receiving may take together.
///
/// # Returns
///
/// The status and the body of the answer, whatever the status is: a 503 is an answer, not an error.
///
/// # Errors
///
/// * [`ProbeError::Unreachable`] if no connection could be made or it broke.
/// * [`ProbeError::TimedOut`] if the exchange took longer than `request_timeout`.
/// * [`ProbeError::NotHttp`] if what came back is not an HTTP answer.
///
/// # Examples
///
/// See [`crate::serve_observability`].
pub async fn http_get(address: &str, path: &str, request_timeout: Duration) -> Result<HttpAnswer, ProbeError> {
    let exchanged = timeout(request_timeout, exchange(address, path)).await;
    let raw_answer = match exchanged {
        Ok(Ok(raw_answer)) => raw_answer,
        Ok(Err(io_error)) => {
            return Err(ProbeError::Unreachable {
                address: address.to_owned(),
                reason: io_error.to_string(),
            });
        }
        Err(_elapsed) => {
            return Err(ProbeError::TimedOut {
                address: address.to_owned(),
                waited: request_timeout,
            });
        }
    };
    parse_answer(address, &raw_answer)
}

/// Connects, sends the request and reads until the server closes the connection.
async fn exchange(address: &str, path: &str) -> std::io::Result<Vec<u8>> {
    let mut connection = TcpStream::connect(address).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    connection.write_all(request.as_bytes()).await?;
    let mut raw_answer = Vec::new();
    connection.read_to_end(&mut raw_answer).await?;
    Ok(raw_answer)
}

/// Takes the status and the body out of a raw HTTP answer.
fn parse_answer(address: &str, raw_answer: &[u8]) -> Result<HttpAnswer, ProbeError> {
    let not_http = |reason: &str| ProbeError::NotHttp {
        address: address.to_owned(),
        reason: reason.to_owned(),
    };
    let answer_text = String::from_utf8_lossy(raw_answer);
    let Some((head, body)) = answer_text.split_once("\r\n\r\n") else {
        return Err(not_http("the answer has no end of headers"));
    };
    // The status line is `HTTP/1.1 200 OK`: the code is its second word.
    let mut status_line_words = head.split_whitespace();
    let Some(protocol) = status_line_words.next() else {
        return Err(not_http("the answer is empty"));
    };
    if !protocol.starts_with("HTTP/") {
        return Err(not_http("the answer does not start with an HTTP status line"));
    }
    let Some(status_text) = status_line_words.next() else {
        return Err(not_http("the status line has no status code"));
    };
    let Ok(status) = status_text.parse::<u16>() else {
        return Err(not_http("the status code is not a number"));
    };
    Ok(HttpAnswer {
        status,
        body: body.to_owned(),
    })
}

#[cfg(test)]
#[path = "probe_test.rs"]
mod tests;
