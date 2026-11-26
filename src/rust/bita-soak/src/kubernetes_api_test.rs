//! Tests of the four calls to the Kubernetes API.
//!
//! # What is covered
//!
//! - What each call sends: its method, its path, its query, its body, and the token.
//! - What each call makes of the answers that are not failures: a delete that finds nothing, a create that finds
//!   the object already there, a list of pods with and without labels.
//! - What fails without another attempt: a refusal (403), an answer that is not a list of pods, a name that is
//!   not an object name, a token that cannot be read.
//! - What is tried again, and how often: a status of 500 or 429, an attempt that runs into its time limit, a
//!   server that is not there.
//! - The token is read again for every call.
//! - Settings that cannot work are refused when the client is made.
//!
//! # How the fixtures work
//!
//! `Api` is the stand-in server of `kubernetes_stand_in.rs`, a token file in a temporary directory, and a client
//! for both: three attempts of half a second each. The tests run in real time, because the requests are real.

// The tests write the token file a pod would find mounted.
#![allow(clippy::disallowed_methods)]

use tempfile::TempDir;

use super::*;
use crate::kubernetes_stand_in::{StandIn, TEST_AUTHORITY_PEM};

/// The namespace of every call in these tests.
const NAMESPACE: &str = "storage";

/// A stand-in server, a token file, and a client for both.
struct Api {
    /// The server.
    stand_in: StandIn,
    /// The directory of the token file.
    directory: TempDir,
    /// The client under test.
    client: ApiClient,
}

impl Api {
    /// Starts the server and makes a client whose token is `token-1`.
    async fn start() -> Self {
        let stand_in = StandIn::start().await;
        let directory = tempfile::tempdir().expect("a temporary directory");
        let token_file = directory.path().join("token");
        std::fs::write(&token_file, "token-1\n").expect("the token is written");
        let client = ApiClient::new(stand_in.settings(&token_file)).expect("settings that work");
        Self {
            stand_in,
            directory,
            client,
        }
    }

    /// Replaces the token, as Kubernetes does every hour.
    fn rotate_token(&self, token: &str) {
        std::fs::write(self.directory.path().join("token"), token).expect("the token is written");
    }
}

/// The labels `app.kubernetes.io/name=etcd`.
fn etcd_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_owned(), "etcd".to_owned())])
}

/// A network policy, as far as these tests care: an object with a name.
fn some_policy() -> serde_json::Value {
    serde_json::json!({"kind": "NetworkPolicy", "metadata": {"name": "bita-soak-cut-replica"}})
}

/// Settings that reach no server: a loopback port that was free a moment ago and that nothing listens on.
async fn settings_without_a_server(directory: &TempDir) -> ApiSettings {
    let token_file = directory.path().join("token");
    std::fs::write(&token_file, "token-1").expect("the token is written");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let free_address = listener.local_addr().expect("the port that was picked");
    drop(listener);
    ApiSettings {
        base_url: format!("http://{free_address}"),
        token_file,
        certificate_authority_pem: TEST_AUTHORITY_PEM.to_owned(),
        attempt_time_limit: Duration::from_millis(500),
        attempts: 2,
        retry_pause: Duration::from_millis(10),
    }
}

#[tokio::test]
async fn listing_pods_asks_for_the_pods_of_the_namespace_with_the_labels() {
    let api = Api::start().await;

    api.client.list_pods(NAMESPACE, &etcd_labels()).await.expect("the list");

    let seen = api.stand_in.seen();
    assert_eq!(seen[0].line(), "GET /api/v1/namespaces/storage/pods");
    assert_eq!(seen[0].query, "labelSelector=app.kubernetes.io%2Fname%3Detcd");
}

#[tokio::test]
async fn every_call_carries_the_token_of_the_service_account() {
    let api = Api::start().await;

    api.client.list_pods(NAMESPACE, &etcd_labels()).await.expect("the list");

    assert_eq!(api.stand_in.seen()[0].authorization, "Bearer token-1");
}

#[tokio::test]
async fn listed_pods_come_with_their_names_and_labels() {
    let api = Api::start().await;
    api.stand_in.answer_next_with(
        200,
        r#"{"kind":"PodList","items":[
            {"metadata":{"name":"etcd-0","labels":{"topology.kubernetes.io/zone":"zone-a"}},"spec":{}},
            {"metadata":{"name":"etcd-1"}}]}"#,
    );

    let pods = api.client.list_pods(NAMESPACE, &etcd_labels()).await.expect("the list");

    assert_eq!(
        pods,
        [
            Pod {
                name: "etcd-0".to_owned(),
                labels: BTreeMap::from([("topology.kubernetes.io/zone".to_owned(), "zone-a".to_owned())]),
            },
            Pod {
                name: "etcd-1".to_owned(),
                labels: BTreeMap::new(),
            },
        ]
    );
}

#[tokio::test]
async fn answer_that_is_not_a_list_of_pods_is_unreadable() {
    let api = Api::start().await;
    api.stand_in.answer_next_with(200, "<html>a proxy's login page</html>");

    let listed = api.client.list_pods(NAMESPACE, &etcd_labels()).await;

    assert!(matches!(listed, Err(ApiError::Unreadable { .. })), "{listed:?}");
}

#[tokio::test]
async fn deleting_a_pod_asks_for_no_grace_period() {
    let api = Api::start().await;

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(removal, Ok(Removal::Removed));
    let seen = api.stand_in.seen();
    assert_eq!(
        seen[0].line(),
        "DELETE /api/v1/namespaces/storage/pods/bitacora-replica-1"
    );
    assert_eq!(seen[0].query, "gracePeriodSeconds=0");
}

#[tokio::test]
async fn deleting_a_pod_that_is_not_there_says_so() {
    let api = Api::start().await;
    api.stand_in
        .answer_next_with(404, r#"{"kind":"Status","reason":"NotFound"}"#);

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(removal, Ok(Removal::NothingThere));
}

#[tokio::test]
async fn creating_a_policy_posts_the_object_as_json() {
    let api = Api::start().await;

    let created = api.client.create_network_policy(NAMESPACE, &some_policy()).await;

    assert_eq!(created, Ok(()));
    let seen = api.stand_in.seen();
    assert_eq!(
        seen[0].line(),
        "POST /apis/networking.k8s.io/v1/namespaces/storage/networkpolicies"
    );
    assert_eq!(seen[0].content_type, "application/json");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&seen[0].body).ok(),
        Some(some_policy())
    );
}

/// An attempt whose answer was lost is sent again, and then finds what the first attempt made.
#[tokio::test]
async fn creating_a_policy_that_is_already_there_succeeds() {
    let api = Api::start().await;
    api.stand_in
        .answer_next_with(409, r#"{"kind":"Status","reason":"AlreadyExists"}"#);

    let created = api.client.create_network_policy(NAMESPACE, &some_policy()).await;

    assert_eq!(created, Ok(()));
}

#[tokio::test]
async fn deleting_a_policy_names_it_in_the_path() {
    let api = Api::start().await;

    let removal = api.client.delete_network_policy(NAMESPACE, "bita-soak-cut-etcd").await;

    assert_eq!(removal, Ok(Removal::Removed));
    assert_eq!(
        api.stand_in.seen_lines(),
        ["DELETE /apis/networking.k8s.io/v1/namespaces/storage/networkpolicies/bita-soak-cut-etcd"]
    );
}

#[tokio::test]
async fn deleting_a_policy_that_is_not_there_says_so() {
    let api = Api::start().await;
    api.stand_in
        .answer_next_with(404, r#"{"kind":"Status","reason":"NotFound"}"#);

    let removal = api.client.delete_network_policy(NAMESPACE, "bita-soak-cut-etcd").await;

    assert_eq!(removal, Ok(Removal::NothingThere));
}

/// The service account lacks the permission. Asking again would change nothing.
#[tokio::test]
async fn refusal_fails_at_once_with_the_status_and_the_answer() {
    let api = Api::start().await;
    api.stand_in.answer_next_with(403, r#"{"message":"pods is forbidden"}"#);

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(
        removal,
        Err(ApiError::Refused {
            request: "DELETE /api/v1/namespaces/storage/pods/bitacora-replica-1".to_owned(),
            status: 403,
            answer: r#"{"message":"pods is forbidden"}"#.to_owned(),
        })
    );
    assert_eq!(api.stand_in.seen().len(), 1);
}

#[tokio::test]
async fn refusal_of_a_create_fails_too() {
    let api = Api::start().await;
    api.stand_in
        .answer_next_with(403, r#"{"message":"networkpolicies is forbidden"}"#);

    let created = api.client.create_network_policy(NAMESPACE, &some_policy()).await;

    assert!(
        matches!(created, Err(ApiError::Refused { status: 403, .. })),
        "{created:?}"
    );
}

#[tokio::test]
async fn server_error_is_tried_again_and_the_next_answer_counts() {
    let api = Api::start().await;
    api.stand_in.answer_next_with(503, "the API server is starting");

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(removal, Ok(Removal::Removed));
    assert_eq!(api.stand_in.seen().len(), 2);
}

#[tokio::test]
async fn too_many_requests_is_tried_again() {
    let api = Api::start().await;
    api.stand_in.answer_next_with(429, "slow down");

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(removal, Ok(Removal::Removed));
}

#[tokio::test]
async fn server_error_on_every_attempt_fails_after_the_last_one() {
    let api = Api::start().await;
    for _attempt in 0..3 {
        api.stand_in.answer_next_with(500, "etcd is unavailable");
    }

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert!(
        matches!(removal, Err(ApiError::Refused { status: 500, .. })),
        "{removal:?}"
    );
    assert_eq!(api.stand_in.seen().len(), 3);
}

#[tokio::test]
async fn attempt_that_runs_into_its_time_limit_is_tried_again() {
    let api = Api::start().await;
    api.stand_in.answer_next_after(Duration::from_secs(5));

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(removal, Ok(Removal::Removed));
    assert_eq!(api.stand_in.seen().len(), 2);
}

#[tokio::test]
async fn server_that_never_answers_in_time_fails_after_the_last_attempt() {
    let api = Api::start().await;
    for _attempt in 0..3 {
        api.stand_in.answer_next_after(Duration::from_secs(5));
    }

    let removal = api.client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(
        removal,
        Err(ApiError::NoAnswer {
            request: "DELETE /api/v1/namespaces/storage/pods/bitacora-replica-1".to_owned(),
            reason: "the time limit of the attempt passed".to_owned(),
        })
    );
    assert_eq!(api.stand_in.seen().len(), 3);
}

#[tokio::test]
async fn server_that_is_not_there_fails_as_no_answer() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let client = ApiClient::new(settings_without_a_server(&directory).await).expect("settings that work");

    let removal = client.delete_pod(NAMESPACE, "bitacora-replica-1").await;

    assert_eq!(
        removal,
        Err(ApiError::NoAnswer {
            request: "DELETE /api/v1/namespaces/storage/pods/bitacora-replica-1".to_owned(),
            reason: "no connection could be made".to_owned(),
        })
    );
}

/// Kubernetes replaces the token every hour; a soak runs for a week.
#[tokio::test]
async fn token_is_read_again_for_every_call() {
    let api = Api::start().await;
    api.client.list_pods(NAMESPACE, &etcd_labels()).await.expect("the list");
    api.rotate_token("token-2");

    api.client.list_pods(NAMESPACE, &etcd_labels()).await.expect("the list");

    assert_eq!(api.stand_in.seen()[1].authorization, "Bearer token-2");
}

#[tokio::test]
async fn call_without_a_token_fails_and_sends_nothing() {
    let api = Api::start().await;
    std::fs::remove_file(api.directory.path().join("token")).expect("the token is removed");

    let listed = api.client.list_pods(NAMESPACE, &etcd_labels()).await;

    assert!(matches!(listed, Err(ApiError::NoToken { .. })), "{listed:?}");
    assert_eq!(api.stand_in.seen(), []);
}

/// A name with a slash in it would address another object than the one the caller meant.
#[tokio::test]
async fn name_that_is_not_an_object_name_is_refused_and_nothing_is_sent() {
    let api = Api::start().await;

    let removal = api.client.delete_pod(NAMESPACE, "../../secrets/admin").await;

    assert_eq!(
        removal,
        Err(ApiError::InvalidName {
            name: "../../secrets/admin".to_owned(),
        })
    );
    assert_eq!(api.stand_in.seen(), []);
}

#[test]
fn object_names_are_lower_case_letters_digits_hyphens_and_dots() {
    assert_eq!(
        object_name("bitacora-replica-0.storage"),
        Ok("bitacora-replica-0.storage")
    );
    assert!(object_name("").is_err());
    assert!(object_name("Replica").is_err());
    assert!(object_name("replica?force=true").is_err());
    assert!(object_name(&"a".repeat(254)).is_err());
}

#[test]
fn labels_are_written_as_a_selector() {
    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "bitacora".to_owned()),
        ("app.kubernetes.io/component".to_owned(), "replica".to_owned()),
    ]);

    let selector = label_selector(&labels);

    assert_eq!(
        selector,
        "app.kubernetes.io/component=replica,app.kubernetes.io/name=bitacora"
    );
}

#[tokio::test]
async fn certificate_authority_that_is_not_pem_is_refused() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut settings = settings_without_a_server(&directory).await;
    settings.certificate_authority_pem = "not a certificate".to_owned();

    let client = ApiClient::new(settings);

    assert!(matches!(client, Err(ApiError::Unusable { .. })), "{client:?}");
}

#[tokio::test]
async fn settings_that_allow_no_attempt_are_refused() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut settings = settings_without_a_server(&directory).await;
    settings.attempts = 0;

    let client = ApiClient::new(settings);

    assert_eq!(
        client.err(),
        Some(ApiError::Unusable {
            reason: "a call needs at least one attempt".to_owned(),
        })
    );
}
