//! Tests of the translation between the artifact store interface and `object_store`, without a network.
//!
//! # What is covered
//!
//! - The contract every artifact store must honour, with and without a key prefix.
//! - Key prefixes: two stores with different prefixes on one service do not see each other, objects land
//!   under the prefix, and listings come back without it.
//! - Ranges: every way a range can lie outside an object reports the object's real size.
//! - Listings by characters on top of a client that lists by path components.
//! - A service that fails or never answers is reported as unavailable, within the request timeout.
//!
//! # How the fixtures work
//!
//! The store under test runs over `object_store`'s in-memory store, which has the semantics of S3 for
//! everything this file translates. [`BrokenService`] is a client whose every request fails in one named
//! way. The tests against a real S3 server are in `tests/s3/`.

use std::fmt;

use bitacora_artifact::contract;
use futures_util::stream::{self, BoxStream};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutResult,
};

use super::*;

/// Longer than any in-memory request takes, so that it only matters in the tests about timeouts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A store without a key prefix over a new, empty in-memory service.
fn store() -> S3ArtifactStore {
    store_in(&Arc::new(InMemory::new()), "")
}

/// A store over `service` that keeps its objects under `key_prefix`.
fn store_in(service: &Arc<InMemory>, key_prefix: &str) -> S3ArtifactStore {
    let object_store: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(service);
    S3ArtifactStore::over(object_store, key_prefix, REQUEST_TIMEOUT)
}

/// Stores `contents` under `object_key`.
async fn put(store: &S3ArtifactStore, object_key: &str, contents: &'static [u8]) {
    store
        .put_object(object_key, Bytes::from_static(contents))
        .await
        .expect("the object is stored");
}

/// The keys a listing of `key_prefix` returns.
async fn listed_keys(store: &S3ArtifactStore, key_prefix: &str) -> Vec<String> {
    let objects = store.list_prefix(key_prefix).await.expect("the listing succeeds");
    let mut object_keys = Vec::new();
    for object in objects {
        object_keys.push(object.object_key);
    }
    object_keys
}

/// How a [`BrokenService`] treats every request.
#[derive(Clone, Copy, Debug)]
enum ServiceFault {
    /// Every request fails, as when the service answers with a server error.
    FailsEveryRequest,
    /// No request is ever answered.
    NeverAnswers,
}

/// A client of a service that is broken in one way.
#[derive(Debug)]
struct BrokenService {
    /// How the service is broken.
    fault: ServiceFault,
}

impl BrokenService {
    /// Answers one request the way the fault says: with an error, or never.
    async fn answer<Response>(&self) -> object_store::Result<Response> {
        match self.fault {
            ServiceFault::FailsEveryRequest => Err(server_error()),
            ServiceFault::NeverAnswers => std::future::pending().await,
        }
    }

    /// Answers one streaming request the way the fault says.
    fn answer_stream<Item: Send + 'static>(&self) -> BoxStream<'static, object_store::Result<Item>> {
        match self.fault {
            ServiceFault::FailsEveryRequest => stream::iter([Err(server_error())]).boxed(),
            ServiceFault::NeverAnswers => stream::pending().boxed(),
        }
    }
}

/// The error a client reports for a server error.
fn server_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "BrokenService",
        source: "the service answered 503".into(),
    }
}

impl fmt::Display for BrokenService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BrokenService({:?})", self.fault)
    }
}

#[async_trait]
impl ObjectStore for BrokenService {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.answer().await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.answer().await
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> object_store::Result<GetResult> {
        self.answer().await
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.answer_stream()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.answer_stream()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.answer().await
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> object_store::Result<()> {
        self.answer().await
    }
}

/// A store over a service with the given fault.
fn store_over_broken_service(fault: ServiceFault) -> S3ArtifactStore {
    S3ArtifactStore::over(Arc::new(BrokenService { fault }), "", REQUEST_TIMEOUT)
}

/// Whether `outcome` is an `Unavailable` error.
fn is_unavailable<Response>(outcome: &Result<Response, ArtifactError>) -> bool {
    matches!(outcome, Err(ArtifactError::Unavailable { .. }))
}

#[tokio::test]
async fn store_honours_the_artifact_store_contract() {
    contract::check(&store()).await;
}

#[tokio::test]
async fn store_with_a_key_prefix_honours_the_artifact_store_contract() {
    let service = Arc::new(InMemory::new());

    contract::check(&store_in(&service, "clusters/production")).await;
}

#[tokio::test]
async fn object_lands_under_the_key_prefix_in_the_service() {
    let service = Arc::new(InMemory::new());
    let store = store_in(&service, "clusters/production");

    put(&store, "streams/7/segments/3", b"a segment").await;

    let location = Path::from("clusters/production/streams/7/segments/3");
    let stored = service.get(&location).await.expect("the object is there");
    let contents = stored.bytes().await.expect("the object is read");
    assert_eq!(contents.as_ref(), b"a segment");
}

#[tokio::test]
async fn object_is_stored_under_exactly_its_key_without_a_prefix() {
    let service = Arc::new(InMemory::new());
    let store = store_in(&service, "");

    put(&store, "streams/7/segments/3", b"a segment").await;

    let location = Path::from("streams/7/segments/3");
    assert!(service.head(&location).await.is_ok());
}

#[tokio::test]
async fn stores_with_different_key_prefixes_do_not_read_each_other() {
    let service = Arc::new(InMemory::new());
    put(&store_in(&service, "cluster-a"), "shared-name", b"from a").await;

    let read_by_the_other = store_in(&service, "cluster-b").get_object("shared-name").await;

    assert_eq!(
        read_by_the_other,
        Err(ArtifactError::NotFound {
            object_key: "shared-name".to_owned()
        })
    );
}

#[tokio::test]
async fn stores_with_different_key_prefixes_do_not_list_each_other() {
    let service = Arc::new(InMemory::new());
    put(&store_in(&service, "cluster-a"), "shared-name", b"from a").await;

    let listed_by_the_other = listed_keys(&store_in(&service, "cluster-b"), "").await;

    assert_eq!(listed_by_the_other, Vec::<String>::new());
}

/// `cluster` is the beginning of `cluster-b` as text, but not as a path.
#[tokio::test]
async fn key_prefix_that_begins_another_one_does_not_see_its_objects() {
    let service = Arc::new(InMemory::new());
    put(&store_in(&service, "cluster-b"), "shared-name", b"from b").await;

    let listed_by_the_shorter = listed_keys(&store_in(&service, "cluster"), "").await;

    assert_eq!(listed_by_the_shorter, Vec::<String>::new());
}

#[tokio::test]
async fn stores_with_the_same_key_prefix_share_their_objects() {
    let service = Arc::new(InMemory::new());
    put(&store_in(&service, "cluster-a"), "shared-name", b"from a").await;

    let read_by_the_other = store_in(&service, "cluster-a").get_object("shared-name").await;

    assert_eq!(read_by_the_other, Ok(Bytes::from_static(b"from a")));
}

#[tokio::test]
async fn listing_returns_keys_without_the_key_prefix() {
    let service = Arc::new(InMemory::new());
    let store = store_in(&service, "clusters/production");
    put(&store, "streams/7/segments/3", b"a segment").await;

    let object_keys = listed_keys(&store, "streams/").await;

    assert_eq!(object_keys, ["streams/7/segments/3"]);
}

#[tokio::test]
async fn listed_key_can_be_read_back() {
    let service = Arc::new(InMemory::new());
    let store = store_in(&service, "clusters/production");
    put(&store, "streams/7/segments/3", b"a segment").await;
    let object_keys = listed_keys(&store, "").await;

    let contents = store.get_object(&object_keys[0]).await;

    assert_eq!(contents, Ok(Bytes::from_static(b"a segment")));
}

#[tokio::test]
async fn prefix_that_ends_inside_a_name_matches_by_characters() {
    let store = store();
    for object_key in ["logs/segment-1", "logs/segment-20", "logs/other"] {
        put(&store, object_key, b"x").await;
    }

    let object_keys = listed_keys(&store, "logs/segment-").await;

    assert_eq!(object_keys, ["logs/segment-1", "logs/segment-20"]);
}

#[tokio::test]
async fn prefix_that_is_a_whole_key_lists_that_object() {
    let store = store();
    for object_key in ["segments/0003", "segments/0004"] {
        put(&store, object_key, b"x").await;
    }

    let object_keys = listed_keys(&store, "segments/0003").await;

    assert_eq!(object_keys, ["segments/0003"]);
}

#[tokio::test]
async fn prefix_without_a_directory_matches_top_level_names_by_characters() {
    let store = store();
    for object_key in ["alpha/1", "alphabet/2", "beta/3"] {
        put(&store, object_key, b"x").await;
    }

    let object_keys = listed_keys(&store, "alpha").await;

    assert_eq!(object_keys, ["alpha/1", "alphabet/2"]);
}

#[tokio::test]
async fn empty_prefix_lists_everything_sorted() {
    let store = store();
    for object_key in ["b", "a/2", "a/1"] {
        put(&store, object_key, b"x").await;
    }

    let object_keys = listed_keys(&store, "").await;

    assert_eq!(object_keys, ["a/1", "a/2", "b"]);
}

#[tokio::test]
async fn prefix_that_no_key_can_have_lists_nothing() {
    let store = store();
    put(&store, "a", b"x").await;

    let object_keys = listed_keys(&store, "../").await;

    assert_eq!(object_keys, Vec::<String>::new());
}

#[tokio::test]
async fn absolute_prefix_lists_nothing() {
    let store = store();
    put(&store, "a/b", b"x").await;

    let object_keys = listed_keys(&store, "/a/").await;

    assert_eq!(object_keys, Vec::<String>::new());
}

#[tokio::test]
async fn listing_reports_the_size_of_each_object() {
    let store = store();
    put(&store, "a", b"12345").await;

    let objects = store.list_prefix("").await.expect("the listing succeeds");

    assert_eq!(objects[0].size_bytes, 5);
}

#[tokio::test]
async fn range_that_ends_past_the_object_reports_its_size() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 5..11).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "ranged".to_owned(),
            start: 5,
            end: 11,
            size_bytes: 10,
        })
    );
}

#[tokio::test]
async fn range_that_starts_at_the_end_of_the_object_reports_its_size() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 10..12).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "ranged".to_owned(),
            start: 10,
            end: 12,
            size_bytes: 10,
        })
    );
}

#[tokio::test]
async fn range_that_starts_past_the_object_reports_its_size() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 50..60).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "ranged".to_owned(),
            start: 50,
            end: 60,
            size_bytes: 10,
        })
    );
}

#[tokio::test]
async fn empty_range_past_the_object_reports_its_size() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 11..11).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "ranged".to_owned(),
            start: 11,
            end: 11,
            size_bytes: 10,
        })
    );
}

#[tokio::test]
async fn inverted_range_reports_the_size_of_the_object() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;
    let inverted = Range { start: 7, end: 3 };

    let outcome = store.get_range("ranged", inverted).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "ranged".to_owned(),
            start: 7,
            end: 3,
            size_bytes: 10,
        })
    );
}

#[tokio::test]
async fn empty_range_inside_the_object_is_empty() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 4..4).await;

    assert_eq!(outcome, Ok(Bytes::new()));
}

#[tokio::test]
async fn empty_range_of_an_empty_object_is_empty() {
    let store = store();
    put(&store, "empty", b"").await;

    let outcome = store.get_range("empty", 0..0).await;

    assert_eq!(outcome, Ok(Bytes::new()));
}

#[tokio::test]
async fn any_other_range_of_an_empty_object_reports_size_zero() {
    let store = store();
    put(&store, "empty", b"").await;

    let outcome = store.get_range("empty", 0..1).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::RangeOutOfBounds {
            object_key: "empty".to_owned(),
            start: 0,
            end: 1,
            size_bytes: 0,
        })
    );
}

#[tokio::test]
async fn range_of_the_whole_object_returns_it() {
    let store = store();
    put(&store, "ranged", b"0123456789").await;

    let outcome = store.get_range("ranged", 0..10).await;

    assert_eq!(outcome, Ok(Bytes::from_static(b"0123456789")));
}

#[tokio::test]
async fn empty_range_of_a_missing_object_is_not_found() {
    let outcome = store().get_range("never-written", 0..0).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::NotFound {
            object_key: "never-written".to_owned()
        })
    );
}

#[tokio::test]
async fn key_with_a_control_character_is_refused() {
    let outcome = store().put_object("line\nbreak", Bytes::from_static(b"x")).await;

    assert_eq!(
        outcome,
        Err(ArtifactError::InvalidKey {
            object_key: "line\nbreak".to_owned()
        })
    );
}

/// Characters that some clients encode behind the caller's back: the object must keep the name it was
/// given.
#[tokio::test]
async fn key_with_unusual_characters_comes_back_unchanged() {
    let store = store();
    let unusual_key = "reports/100% done #1 [final]?.txt";
    put(&store, unusual_key, b"x").await;

    let object_keys = listed_keys(&store, "reports/").await;

    assert_eq!(object_keys, [unusual_key]);
}

#[tokio::test]
async fn failing_service_makes_a_put_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.put_object("a", Bytes::from_static(b"x")).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_a_get_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.get_object("a").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_a_range_read_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.get_range("a", 0..4).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_an_empty_range_read_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.get_range("a", 4..4).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_a_delete_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.delete_object("a").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_a_listing_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.list_prefix("").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn unavailable_error_says_what_was_attempted_and_why() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);

    let outcome = store.get_object("streams/7/segments/3").await;

    let Err(ArtifactError::Unavailable { reason }) = outcome else {
        panic!("expected an unavailable error, got {outcome:?}");
    };
    assert!(reason.contains("cannot read \"streams/7/segments/3\""), "{reason}");
    assert!(reason.contains("the service answered 503"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn put_to_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let asked_at = tokio::time::Instant::now();

    let outcome = store.put_object("a", Bytes::from_static(b"x")).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn get_from_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let asked_at = tokio::time::Instant::now();

    let outcome = store.get_object("a").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn range_read_from_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let asked_at = tokio::time::Instant::now();

    let outcome = store.get_range("a", 0..4).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn delete_on_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let asked_at = tokio::time::Instant::now();

    let outcome = store.delete_object("a").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn listing_on_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let asked_at = tokio::time::Instant::now();

    let outcome = store.list_prefix("").await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn timeout_error_names_the_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);

    let outcome = store.get_object("a").await;

    let Err(ArtifactError::Unavailable { reason }) = outcome else {
        panic!("expected an unavailable error, got {outcome:?}");
    };
    assert!(reason.contains("did not answer within 30s"), "{reason}");
}

/// The answer an S3 service gives for a request to a bucket that does not exist, as the client reports it.
fn missing_bucket_error() -> object_store::Error {
    object_store::Error::NotFound {
        path: "a".to_owned(),
        source: "Server returned non-2xx status code: 404 Not Found: <Error><Code>NoSuchBucket</Code>\
                 <Message>The specified bucket does not exist</Message></Error>"
            .into(),
    }
}

/// The answer an S3 service gives for a missing object, as the client reports it.
fn missing_key_error() -> object_store::Error {
    object_store::Error::NotFound {
        path: "a".to_owned(),
        source: "Server returned non-2xx status code: 404 Not Found: <Error><Code>NoSuchKey</Code>\
                 <Key>NoSuchBucket</Key></Error>"
            .into(),
    }
}

#[test]
fn missing_key_is_a_missing_object() {
    assert!(is_missing_object(&missing_key_error()));
}

#[test]
fn missing_bucket_is_not_a_missing_object() {
    assert!(!is_missing_object(&missing_bucket_error()));
}

#[test]
fn server_error_is_not_a_missing_object() {
    assert!(!is_missing_object(&server_error()));
}

#[test]
fn read_in_a_missing_bucket_is_unavailable_and_says_so() {
    let error = not_found_or_unavailable("read", "a", &missing_bucket_error());

    let ArtifactError::Unavailable { reason } = error else {
        panic!("expected an unavailable error, got {error:?}");
    };
    assert!(reason.contains("The specified bucket does not exist"), "{reason}");
}
