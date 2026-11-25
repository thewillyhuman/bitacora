//! Tests of the minimal HTTP client.
//!
//! # What is covered
//!
//! - How a raw answer is taken apart: status, body, and the ways it can fail to be HTTP.
//! - What a caller sees when nothing listens, when the server stays silent, and when it does not speak
//!   HTTP.
//!
//! A successful request against the real endpoint is covered in `http_endpoint_test.rs`.
//!
//! # How the fixtures work
//!
//! The servers here are bare sockets on a loopback port chosen by the operating system: one that is
//! closed again (nothing listens), one that accepts and never answers, and one that answers with
//! something that is not HTTP.

use tokio::net::TcpListener;

use super::*;

/// The address parse errors are reported for in these tests.
const ADDRESS: &str = "127.0.0.1:9100";

/// Binds a loopback port chosen by the operating system.
///
/// # Returns
///
/// The listener and its address as `host:port`.
async fn loopback_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port can be bound");
    let address = listener
        .local_addr()
        .expect("a bound listener has an address")
        .to_string();
    (listener, address)
}

#[test]
fn status_is_taken_from_the_status_line() {
    let answer = parse_answer(
        ADDRESS,
        b"HTTP/1.1 503 Service Unavailable\r\n\r\nnot ready\n",
    );

    assert_eq!(answer.map(|answer| answer.status), Ok(503));
}

#[test]
fn body_is_what_follows_the_headers() {
    let answer = parse_answer(
        ADDRESS,
        b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\nready\n",
    );

    assert_eq!(answer.map(|answer| answer.body), Ok("ready\n".to_owned()));
}

#[test]
fn answer_without_a_body_has_an_empty_body() {
    let answer = parse_answer(ADDRESS, b"HTTP/1.1 204 No Content\r\n\r\n");

    assert_eq!(answer.map(|answer| answer.body), Ok(String::new()));
}

#[test]
fn answer_without_an_end_of_headers_is_not_http() {
    let answer = parse_answer(ADDRESS, b"HTTP/1.1 200 OK\r\n");

    assert!(
        matches!(answer, Err(ProbeError::NotHttp { .. })),
        "{answer:?}"
    );
}

#[test]
fn answer_that_starts_with_something_else_is_not_http() {
    let answer = parse_answer(ADDRESS, b"SSH-2.0-OpenSSH\r\n\r\n");

    assert!(
        matches!(answer, Err(ProbeError::NotHttp { .. })),
        "{answer:?}"
    );
}

#[test]
fn status_that_is_not_a_number_is_not_http() {
    let answer = parse_answer(ADDRESS, b"HTTP/1.1 fine OK\r\n\r\n");

    assert!(
        matches!(answer, Err(ProbeError::NotHttp { .. })),
        "{answer:?}"
    );
}

#[test]
fn empty_answer_is_not_http() {
    let answer = parse_answer(ADDRESS, b"");

    assert!(
        matches!(answer, Err(ProbeError::NotHttp { .. })),
        "{answer:?}"
    );
}

#[tokio::test]
async fn port_nobody_listens_on_is_unreachable() {
    let (listener, address) = loopback_listener().await;
    drop(listener);

    let answer = http_get(&address, "/readyz", Duration::from_secs(5)).await;

    assert!(
        matches!(answer, Err(ProbeError::Unreachable { .. })),
        "{answer:?}"
    );
}

#[tokio::test]
async fn server_that_never_answers_times_out() {
    // The listener is kept open and never accepts: the connection is made, and nothing comes back.
    let (_listener, address) = loopback_listener().await;
    let short_timeout = Duration::from_millis(200);

    let answer = http_get(&address, "/readyz", short_timeout).await;

    assert_eq!(
        answer,
        Err(ProbeError::TimedOut {
            address,
            waited: short_timeout
        })
    );
}

#[tokio::test]
async fn server_that_does_not_speak_http_is_reported_as_such() {
    let (listener, address) = loopback_listener().await;
    let not_an_http_server = tokio::spawn(async move {
        let (mut connection, _peer) = listener.accept().await.expect("a client connects");
        // Take the request off the socket first: closing a connection with unread data resets it, and
        // the client would see a broken connection instead of the greeting.
        let mut request = [0_u8; 1024];
        let _request_bytes = connection
            .read(&mut request)
            .await
            .expect("the request arrives");
        connection
            .write_all(b"hello, I am not a web server\n")
            .await
            .expect("the greeting is sent");
    });

    let answer = http_get(&address, "/readyz", Duration::from_secs(5)).await;

    not_an_http_server.await.expect("the server task finishes");
    assert!(
        matches!(answer, Err(ProbeError::NotHttp { .. })),
        "{answer:?}"
    );
}
