//! A stand-in for the Kubernetes API server, for the tests.
//!
//! # Why this file exists
//!
//! The calls of `kubernetes_api.rs` are HTTP requests, and what matters about them is what goes over the wire and
//! what the tool makes of each kind of answer: a refusal, a missing object, a server that is slow or gone. A real
//! API server cannot be asked to misbehave on demand, and the tests must not need a cluster. This file is a small
//! HTTP server on a loopback port that remembers every request and answers as the test tells it to.
//!
//! # What it contains
//!
//! - [`StandIn`]: the server, its script of answers, and what it has seen.
//! - [`Seen`]: one request as the server received it.
//! - [`TEST_AUTHORITY_PEM`]: a certificate for the settings of a client. It signs nothing.
//!
//! # How it works
//!
//! Every request, whatever its path, is recorded and then answered with the next answer of the script. When the
//! script is empty the answer is what a healthy API server would say to the method: an empty list to a GET,
//! "created" to a POST, "ok" to a DELETE. An answer of the script can be delayed, which is how a test makes an
//! attempt run into its time limit.
//!
//! The server speaks plain HTTP: the tests are about requests and answers. That the tool checks the API server's
//! certificate against the cluster's authority is not covered here.
//!
//! # Invariants
//!
//! - Requests are recorded in the order they arrive, before they are answered.
//! - The server ends when the stand-in is dropped.
//!
//! # See also
//!
//! - `kubernetes_api_test.rs`, `kubernetes_faults_test.rs`: the tests that use it.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::kubernetes_api::ApiSettings;

/// A certificate authority for the settings of a client that talks to the stand-in. Nothing is signed by it and
/// its key was thrown away: the stand-in speaks plain HTTP.
pub(crate) const TEST_AUTHORITY_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBfDCCASKgAwIBAgIUPM+/Wfb7mZBmZ45VI0pRDj0it1gwCgYIKoZIzj0EAwIw
IzEhMB8GA1UEAwwYYml0YS1zb2FrIHRlc3QgYXV0aG9yaXR5MCAXDTI2MTEyNjEx
NDQyMloYDzIxMjYxMTAyMTE0NDIyWjAjMSEwHwYDVQQDDBhiaXRhLXNvYWsgdGVz
dCBhdXRob3JpdHkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAS48WEtW0Xfvl44
Y/EibBJJeAD888sjndPy9eUGxYYfhr2IPv5dzrs56t/BATBT4wFwqNsZl63BHWi3
d5H8a99tozIwMDAdBgNVHQ4EFgQU3gvQ80WQNXCcXJMvu6v79EJel64wDwYDVR0T
AQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiAEYs3w9Iw5op59sgvfNNOa1xHD
ZiWEFcjnM5t6bdMZQgIhAOlT61JqJ7fid9XE0LZBdSL6CyCx53hKhpDFT0+wN6yN
-----END CERTIFICATE-----
";

/// One request as the stand-in received it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    /// The HTTP method.
    pub(crate) method: String,
    /// The path.
    pub(crate) path: String,
    /// The query, as it was encoded on the wire. Empty if there was none.
    pub(crate) query: String,
    /// The body. Empty if there was none.
    pub(crate) body: String,
    /// The value of the `Authorization` header. Empty if there was none.
    pub(crate) authorization: String,
    /// The value of the `Content-Type` header. Empty if there was none.
    pub(crate) content_type: String,
}

impl Seen {
    /// The method and the path, which is how most tests name a request.
    pub(crate) fn line(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

/// One answer of the script.
struct Scripted {
    /// The HTTP status.
    status: u16,
    /// The body.
    body: String,
    /// How long the server waits before it answers.
    delay: Duration,
}

/// What the server and the test share.
#[derive(Default)]
struct Shared {
    /// Every request so far, in order.
    seen: Mutex<Vec<Seen>>,
    /// The answers still to give, in order.
    script: Mutex<VecDeque<Scripted>>,
}

/// A stand-in for the Kubernetes API server on a loopback port.
pub(crate) struct StandIn {
    /// Where the server listens.
    address: SocketAddr,
    /// What the server and the test share.
    shared: Arc<Shared>,
    /// The server's task.
    server: JoinHandle<()>,
}

impl StandIn {
    /// Starts the server on a port the operating system picks.
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback port");
        let address = listener.local_addr().expect("the port that was picked");
        let shared = Arc::new(Shared::default());
        let router = Router::new().fallback(answer).with_state(Arc::clone(&shared));
        let server = tokio::spawn(async move {
            // The server runs until its task is ended.
            drop(axum::serve(listener, router).await);
        });
        Self {
            address,
            shared,
            server,
        }
    }

    /// The settings of a client for this server: three attempts of half a second each, ten milliseconds apart, with
    /// the token of `token_file`.
    pub(crate) fn settings(&self, token_file: &Path) -> ApiSettings {
        ApiSettings {
            base_url: format!("http://{}", self.address),
            token_file: token_file.to_path_buf(),
            certificate_authority_pem: TEST_AUTHORITY_PEM.to_owned(),
            attempt_time_limit: Duration::from_millis(500),
            attempts: 3,
            retry_pause: Duration::from_millis(10),
        }
    }

    /// Makes the server answer the next request that has no answer yet with `status` and `body`.
    pub(crate) fn answer_next_with(&self, status: u16, body: &str) {
        self.script_answer(status, body, Duration::ZERO);
    }

    /// Makes the server answer the next request that has no answer yet only after `delay`.
    pub(crate) fn answer_next_after(&self, delay: Duration) {
        self.script_answer(200, "{}", delay);
    }

    /// Every request the server has received so far, in order.
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.shared.seen.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The method and the path of every request so far, in order.
    pub(crate) fn seen_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for request in self.seen() {
            lines.push(request.line());
        }
        lines
    }

    /// Adds an answer to the end of the script.
    fn script_answer(&self, status: u16, body: &str, delay: Duration) {
        let scripted = Scripted {
            status,
            body: body.to_owned(),
            delay,
        };
        self.shared
            .script
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(scripted);
    }
}

impl Drop for StandIn {
    /// Ends the server with the test that started it.
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Records `request` and answers it with the next answer of the script, or as a healthy API server would.
async fn answer(State(shared): State<Arc<Shared>>, request: Request) -> Response {
    let method = request.method().to_string();
    let seen = Seen {
        method: method.clone(),
        path: request.uri().path().to_owned(),
        query: request.uri().query().unwrap_or_default().to_owned(),
        authorization: header_of(&request, "authorization"),
        content_type: header_of(&request, "content-type"),
        body: body_of(request).await,
    };
    shared.seen.lock().unwrap_or_else(PoisonError::into_inner).push(seen);
    let next = shared.script.lock().unwrap_or_else(PoisonError::into_inner).pop_front();
    let scripted = next.unwrap_or_else(|| healthy_answer_to(&method));
    tokio::time::sleep(scripted.delay).await;
    let status = StatusCode::from_u16(scripted.status).expect("a status the test wrote");
    (status, scripted.body).into_response()
}

/// The value of the header `name` of `request`, or nothing.
fn header_of(request: &Request, name: &str) -> String {
    match request.headers().get(name) {
        Some(value) => value.to_str().unwrap_or_default().to_owned(),
        None => String::new(),
    }
}

/// The body of `request`, as text.
async fn body_of(request: Request) -> String {
    let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("the body of a request from this process");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// What a healthy API server says to a request of `method` that needs nothing special: an empty list to a GET,
/// "created" to a POST, "ok" to everything else.
fn healthy_answer_to(method: &str) -> Scripted {
    let (status, body) = match method {
        "GET" => (200, "{\"items\":[]}"),
        "POST" => (201, "{}"),
        _other_method => (200, "{}"),
    };
    Scripted {
        status,
        body: body.to_owned(),
        delay: Duration::ZERO,
    }
}
