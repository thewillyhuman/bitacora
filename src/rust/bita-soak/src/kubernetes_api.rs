//! The four calls `bita-soak` makes to the Kubernetes API.
//!
//! # Why this file exists
//!
//! On Kubernetes a fault is a call to the API server: delete a pod, create a network policy. The tool runs in a
//! pod and has the pod's service account, so it needs no more than an HTTPS request with the account's token. A
//! general Kubernetes client would bring a few hundred crates for four requests. This file is those four
//! requests, over the HTTP client the build already has, and nothing else of Kubernetes.
//!
//! # What it contains
//!
//! - [`ApiSettings`]: where the API server is, how it is trusted, and how patient a call is.
//! - [`ApiClient`]: the calls: [`ApiClient::list_pods`], [`ApiClient::delete_pod`],
//!   [`ApiClient::create_network_policy`], [`ApiClient::delete_network_policy`].
//! - [`Pod`]: a pod as far as the tool needs to know it: its name and its labels.
//! - [`Removal`]: whether a delete removed something or found nothing.
//! - [`ApiError`]: why a call failed.
//!
//! # How it works
//!
//! Every call builds one request and sends it up to a fixed number of times. An attempt that got no answer, or an
//! answer that says "try again" (a status of 500 or above, or 429), is repeated after a pause; any other answer
//! is final. Each attempt has a time limit, and each attempt reads the service account's token again from its
//! file: Kubernetes replaces that token every hour, and a tool that runs for a week would otherwise start to be
//! refused on its first day.
//!
//! Two answers that look like failures are not: a delete that finds nothing (404) says so in its result, and a
//! create that finds the object already there (409) succeeds. Both happen when an attempt is repeated after its
//! answer was lost.
//!
//! The server is trusted through the certificate authority the settings carry, and through nothing else: no
//! system roots, no proxy from the environment.
//!
//! # Invariants
//!
//! - No call takes longer than the number of attempts times the time limit, plus the pauses between them.
//! - A name is put into a path only if it is a valid Kubernetes object name, so that no name can change which
//!   object a request addresses.
//! - The token is never written anywhere: not into an error, not into a log.
//! - This file reads one file per attempt with a blocking call. The file is a few hundred bytes on a memory
//!   file system.
//!
//! # See also
//!
//! - `kubernetes_faults.rs`: turns the faults of a soak into these calls.
//! - `docs/soak.md`: the permissions the service account needs.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::{Certificate, Client, Method, Url};
use serde::Deserialize;
use thiserror::Error;
use tokio::time::sleep;

/// The longest name Kubernetes accepts for an object.
const LONGEST_OBJECT_NAME: usize = 253;

/// The most bytes of an answer's body that are kept for an error message.
const MOST_BYTES_OF_AN_ANSWER_IN_AN_ERROR: usize = 300;

/// Where the API server is, how it is trusted, and how patient a call is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApiSettings {
    /// The address of the API server, such as `https://10.96.0.1:443`, without a path.
    pub(crate) base_url: String,
    /// The file that holds the service account's token. It is read again for every attempt.
    pub(crate) token_file: PathBuf,
    /// The certificate authority that signed the API server's certificate, in PEM.
    pub(crate) certificate_authority_pem: String,
    /// How long one attempt may take, from connecting to the last byte of the answer.
    pub(crate) attempt_time_limit: Duration,
    /// How many times a call is sent before it fails. At least one.
    pub(crate) attempts: u32,
    /// How long a call waits between two attempts.
    pub(crate) retry_pause: Duration,
}

/// A pod, as far as the tool needs to know it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pod {
    /// The name of the pod.
    pub(crate) name: String,
    /// The labels of the pod.
    pub(crate) labels: BTreeMap<String, String>,
}

/// What a delete did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Removal {
    /// The object was there and the API server accepted its deletion.
    Removed,
    /// There was no such object.
    NothingThere,
}

/// Why a call to the Kubernetes API failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum ApiError {
    /// The settings cannot make a client.
    #[error("the Kubernetes API cannot be used: {reason}")]
    Unusable {
        /// What is wrong with the settings.
        reason: String,
    },
    /// A name that would have gone into a path is not a valid object name.
    #[error("{name:?} is not a name Kubernetes accepts")]
    InvalidName {
        /// The name.
        name: String,
    },
    /// The service account's token could not be read.
    #[error("the service account token cannot be read from {path}: {reason}")]
    NoToken {
        /// The file the token was expected in.
        path: String,
        /// What the operating system said.
        reason: String,
    },
    /// No attempt got an answer.
    #[error("the Kubernetes API did not answer {request}: {reason}")]
    NoAnswer {
        /// The method and the path of the request.
        request: String,
        /// Why the last attempt got none.
        reason: String,
    },
    /// The API server refused the request.
    #[error("the Kubernetes API answered {status} to {request}: {answer}")]
    Refused {
        /// The method and the path of the request.
        request: String,
        /// The HTTP status of the answer.
        status: u16,
        /// The beginning of the answer's body.
        answer: String,
    },
    /// The API server answered with something the tool cannot read.
    #[error("the Kubernetes API answered {request} with something unreadable: {reason}")]
    Unreadable {
        /// The method and the path of the request.
        request: String,
        /// What is wrong with the answer.
        reason: String,
    },
}

/// One request, before it is sent.
struct Call {
    /// The HTTP method.
    method: Method,
    /// The path, from the root of the API server.
    path: String,
    /// The query parameters, not yet encoded.
    query: Vec<(&'static str, String)>,
    /// The body, as JSON, if the request has one.
    body: Option<String>,
}

impl Call {
    /// The method and the path, for an error message.
    fn description(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

/// What the API server answered.
struct Answer {
    /// The HTTP status.
    status: u16,
    /// The body.
    body: String,
}

/// The list of pods the API server answers with, as far as it is read.
#[derive(Deserialize)]
struct PodList {
    /// The pods.
    items: Vec<PodItem>,
}

/// One pod of a [`PodList`].
#[derive(Deserialize)]
struct PodItem {
    /// The pod's metadata.
    metadata: PodMetadata,
}

/// The metadata of a [`PodItem`], as far as it is read.
#[derive(Deserialize)]
struct PodMetadata {
    /// The name of the pod.
    name: String,
    /// The labels of the pod. A pod without labels has no such field.
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

/// A client for the four calls.
#[derive(Clone, Debug)]
pub(crate) struct ApiClient {
    /// The HTTP client, which trusts the cluster's certificate authority only.
    http: Client,
    /// Where the API server is and how patient a call is.
    settings: ApiSettings,
}

impl ApiClient {
    /// Makes a client from `settings`.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::Unusable`] if the certificate authority is not PEM or holds no certificate, if the
    /// settings allow no attempt, or if the HTTP client cannot be built.
    pub(crate) fn new(settings: ApiSettings) -> Result<Self, ApiError> {
        if settings.attempts == 0 {
            return Err(unusable("a call needs at least one attempt"));
        }
        // The HTTP client is built without a TLS provider of its own; a second installation is refused and harmless.
        let _already_installed = rustls::crypto::ring::default_provider().install_default();
        let authorities = match Certificate::from_pem_bundle(settings.certificate_authority_pem.as_bytes()) {
            Ok(authorities) => authorities,
            Err(pem_error) => return Err(unusable(&format!("the certificate authority is not PEM: {pem_error}"))),
        };
        if authorities.is_empty() {
            return Err(unusable("the certificate authority holds no certificate"));
        }
        let built = Client::builder()
            .tls_certs_only(authorities)
            .no_proxy()
            .timeout(settings.attempt_time_limit)
            .connect_timeout(settings.attempt_time_limit)
            .build();
        match built {
            Ok(http) => Ok(Self { http, settings }),
            Err(build_error) => Err(unusable(&format!("no HTTP client: {build_error}"))),
        }
    }

    /// Lists the pods of `namespace` that carry every label of `labels`.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to list.
    /// * `labels` - The labels a pod must carry, with their values.
    ///
    /// # Errors
    ///
    /// Fails if the API server refused, did not answer, or answered with something that is not a list of pods.
    pub(crate) async fn list_pods(
        &self,
        namespace: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<Pod>, ApiError> {
        let call = Call {
            method: Method::GET,
            path: format!("/api/v1/namespaces/{}/pods", object_name(namespace)?),
            query: vec![("labelSelector", label_selector(labels))],
            body: None,
        };
        let answer = self.send(&call).await?;
        if answer.status != 200 {
            return Err(refused(&call, &answer));
        }
        let list: PodList = match serde_json::from_str(&answer.body) {
            Ok(list) => list,
            Err(json_error) => {
                return Err(ApiError::Unreadable {
                    request: call.description(),
                    reason: json_error.to_string(),
                });
            }
        };
        let mut pods = Vec::with_capacity(list.items.len());
        for item in list.items {
            pods.push(Pod {
                name: item.metadata.name,
                labels: item.metadata.labels,
            });
        }
        Ok(pods)
    }

    /// Deletes the pod `name` of `namespace` without a grace period: its containers are killed, not asked to stop.
    ///
    /// # Returns
    ///
    /// Whether there was such a pod.
    ///
    /// # Errors
    ///
    /// Fails if the API server refused or did not answer.
    pub(crate) async fn delete_pod(&self, namespace: &str, name: &str) -> Result<Removal, ApiError> {
        let call = Call {
            method: Method::DELETE,
            path: format!(
                "/api/v1/namespaces/{}/pods/{}",
                object_name(namespace)?,
                object_name(name)?
            ),
            query: vec![("gracePeriodSeconds", "0".to_owned())],
            body: None,
        };
        self.send_removal(&call).await
    }

    /// Creates the network policy `policy` in `namespace`. A policy of that name that is already there is left as
    /// it is and counts as created.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace of the policy.
    /// * `policy` - The whole object, as the API takes it.
    ///
    /// # Errors
    ///
    /// Fails if the API server refused or did not answer.
    pub(crate) async fn create_network_policy(
        &self,
        namespace: &str,
        policy: &serde_json::Value,
    ) -> Result<(), ApiError> {
        let call = Call {
            method: Method::POST,
            path: format!(
                "/apis/networking.k8s.io/v1/namespaces/{}/networkpolicies",
                object_name(namespace)?
            ),
            query: Vec::new(),
            body: Some(policy.to_string()),
        };
        let answer = self.send(&call).await?;
        match answer.status {
            200..=299 | 409 => Ok(()),
            _refused => Err(refused(&call, &answer)),
        }
    }

    /// Deletes the network policy `name` of `namespace`.
    ///
    /// # Returns
    ///
    /// Whether there was such a policy.
    ///
    /// # Errors
    ///
    /// Fails if the API server refused or did not answer.
    pub(crate) async fn delete_network_policy(&self, namespace: &str, name: &str) -> Result<Removal, ApiError> {
        let call = Call {
            method: Method::DELETE,
            path: format!(
                "/apis/networking.k8s.io/v1/namespaces/{}/networkpolicies/{}",
                object_name(namespace)?,
                object_name(name)?
            ),
            query: Vec::new(),
            body: None,
        };
        self.send_removal(&call).await
    }

    /// Sends a delete and reads its answer: success removed something, 404 found nothing.
    async fn send_removal(&self, call: &Call) -> Result<Removal, ApiError> {
        let answer = self.send(call).await?;
        match answer.status {
            200..=299 => Ok(Removal::Removed),
            404 => Ok(Removal::NothingThere),
            _refused => Err(refused(call, &answer)),
        }
    }

    /// Sends `call`, again after a pause while the answer is missing or says to try again, up to the number of
    /// attempts of the settings.
    ///
    /// # Errors
    ///
    /// Returns why the last attempt failed. An answer that is final, whatever its status, is not an error here.
    async fn send(&self, call: &Call) -> Result<Answer, ApiError> {
        let mut attempt = 1;
        loop {
            let failure = match self.send_once(call).await {
                Ok(answer) if !is_worth_another_attempt(answer.status) => return Ok(answer),
                Ok(answer) => refused(call, &answer),
                Err(api_error) => api_error,
            };
            let may_try_again = matches!(failure, ApiError::NoAnswer { .. } | ApiError::Refused { .. });
            if attempt >= self.settings.attempts || !may_try_again {
                return Err(failure);
            }
            attempt += 1;
            sleep(self.settings.retry_pause).await;
        }
    }

    /// Sends `call` once, with the token as it is in its file right now.
    async fn send_once(&self, call: &Call) -> Result<Answer, ApiError> {
        let token = self.read_token()?;
        let url = self.url_of(call)?;
        let mut request = self.http.request(call.method.clone(), url).bearer_auth(token);
        if let Some(body) = &call.body {
            request = request.header("content-type", "application/json").body(body.clone());
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(send_error) => return Err(no_answer(call, &send_error)),
        };
        let status = response.status().as_u16();
        match response.text().await {
            Ok(body) => Ok(Answer { status, body }),
            Err(read_error) => Err(no_answer(call, &read_error)),
        }
    }

    /// Reads the service account's token from its file.
    fn read_token(&self) -> Result<String, ApiError> {
        match std::fs::read_to_string(&self.settings.token_file) {
            Ok(token) => Ok(token.trim().to_owned()),
            Err(io_error) => Err(ApiError::NoToken {
                path: self.settings.token_file.display().to_string(),
                reason: io_error.to_string(),
            }),
        }
    }

    /// The address `call` is sent to, with its query encoded.
    fn url_of(&self, call: &Call) -> Result<Url, ApiError> {
        let address = format!("{}{}", self.settings.base_url, call.path);
        let mut url = match Url::parse(&address) {
            Ok(url) => url,
            Err(parse_error) => return Err(unusable(&format!("{address} is not an address: {parse_error}"))),
        };
        if !call.query.is_empty() {
            let mut query = url.query_pairs_mut();
            for (name, value) in &call.query {
                query.append_pair(name, value);
            }
        }
        Ok(url)
    }
}

/// Whether an answer with `status` says that the same request may work a moment later.
const fn is_worth_another_attempt(status: u16) -> bool {
    status >= 500 || status == 429
}

/// Checks that `name` may be put into a path: a Kubernetes object name is lower-case letters, digits, hyphens and
/// dots, and so cannot name another path.
fn object_name(name: &str) -> Result<&str, ApiError> {
    let mut is_valid = !name.is_empty() && name.len() <= LONGEST_OBJECT_NAME;
    for character in name.chars() {
        let is_allowed =
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-' || character == '.';
        is_valid = is_valid && is_allowed;
    }
    if is_valid {
        Ok(name)
    } else {
        Err(ApiError::InvalidName { name: name.to_owned() })
    }
}

/// Writes `labels` as a label selector: `name=value` pairs separated by commas.
fn label_selector(labels: &BTreeMap<String, String>) -> String {
    let mut pairs = Vec::with_capacity(labels.len());
    for (name, value) in labels {
        pairs.push(format!("{name}={value}"));
    }
    pairs.join(",")
}

/// The error for settings that cannot work, for `reason`.
fn unusable(reason: &str) -> ApiError {
    ApiError::Unusable {
        reason: reason.to_owned(),
    }
}

/// The error for an attempt of `call` that got no answer because of `send_error`.
fn no_answer(call: &Call, send_error: &reqwest::Error) -> ApiError {
    let reason = if send_error.is_timeout() {
        "the time limit of the attempt passed".to_owned()
    } else if send_error.is_connect() {
        "no connection could be made".to_owned()
    } else {
        send_error.to_string()
    };
    ApiError::NoAnswer {
        request: call.description(),
        reason,
    }
}

/// The error for `answer`, which refused `call`.
fn refused(call: &Call, answer: &Answer) -> ApiError {
    let mut beginning = String::new();
    for character in answer.body.chars() {
        if beginning.len() >= MOST_BYTES_OF_AN_ANSWER_IN_AN_ERROR {
            break;
        }
        beginning.push(character);
    }
    ApiError::Refused {
        request: call.description(),
        status: answer.status,
        answer: beginning,
    }
}

#[cfg(test)]
#[path = "kubernetes_api_test.rs"]
mod tests;
