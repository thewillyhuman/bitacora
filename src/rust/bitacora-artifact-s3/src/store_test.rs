//! Tests of the translation between the artifact store interface and `object_store`, without a network.
//!
//! # What is covered
//!
//! - The contract every artifact store must honour, with and without a key prefix.
//! - Key prefixes: two stores with different prefixes on one service do not see each other, objects land
//!   under the prefix, and listings come back without it.
//! - Ranges: every way a range can lie outside an object reports the object's real size.
//! - Listings by characters on top of a client that lists by path components, and the rule that picks the
//!   directory to ask the client for.
//! - A service that fails or never answers is reported as unavailable, within the request timeout.
//! - Uploads: which requests an object costs (one `PUT` below a part; otherwise parts of one size and a
//!   shorter last one), that less than a part is held back between pieces, what an abort asks of the
//!   service, and that an upload which lost a part is never completed.
//!
//! # How the fixtures work
//!
//! The store under test runs over `object_store`'s in-memory store, which has the semantics of S3 for
//! everything this file translates. [`BrokenService`] is a client whose every request fails in one named
//! way. [`RecordingService`] is the in-memory store with a list of what was asked of it to store an
//! object, and can lose one part; the stores over it send parts of [`SMALL_PART_BYTES`], so that a line of
//! text is several parts. The tests against a real S3 server are in `tests/s3/`.

use std::fmt;
use std::sync::Mutex;

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
    S3ArtifactStore::over(object_store, key_prefix, REQUEST_TIMEOUT, UPLOAD_PART_BYTES)
}

/// The part size of the stores in the tests of uploads: small, so that a few bytes make several parts.
const SMALL_PART_BYTES: usize = 10;

/// One thing a store asked of a [`RecordingService`] while it stored an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreRequest {
    /// A whole object in one request.
    Put {
        /// The size of the object.
        object_bytes: usize,
    },
    /// The start of a multipart upload.
    OpenMultipart,
    /// One part of a multipart upload.
    Part {
        /// The size of the part.
        part_bytes: usize,
    },
    /// The request that makes the parts of a multipart upload the object.
    CompleteMultipart,
    /// The request that makes the service drop the parts of a multipart upload.
    AbortMultipart,
}

/// What a [`RecordingService`] and its multipart uploads share.
#[derive(Debug, Default)]
struct Recording {
    /// What was asked so far, in order.
    requests: Vec<StoreRequest>,
    /// How many parts were sent so far, over all uploads.
    parts_sent: usize,
}

/// An in-memory service that writes down how objects are stored in it, and can lose one part.
#[derive(Debug)]
struct RecordingService {
    /// The service that does the work.
    service: InMemory,
    /// What was asked of the service.
    recording: Arc<Mutex<Recording>>,
    /// Which part fails to arrive, counted from zero over all uploads; `None` for a service that takes
    /// every part.
    lost_part_number: Option<usize>,
}

impl RecordingService {
    /// A service that takes everything, or loses the part with the given number.
    fn losing(lost_part_number: Option<usize>) -> Self {
        Self {
            service: InMemory::new(),
            recording: Arc::new(Mutex::new(Recording::default())),
            lost_part_number,
        }
    }
}

impl fmt::Display for RecordingService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RecordingService")
    }
}

#[async_trait]
impl ObjectStore for RecordingService {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        record(
            &self.recording,
            StoreRequest::Put {
                object_bytes: payload.content_length(),
            },
        );
        self.service.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        record(&self.recording, StoreRequest::OpenMultipart);
        let multipart = self.service.put_multipart_opts(location, options).await?;
        Ok(Box::new(RecordingMultipart {
            multipart,
            recording: Arc::clone(&self.recording),
            lost_part_number: self.lost_part_number,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.service.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.service.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.service.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.service.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.service.copy_opts(from, to, options).await
    }
}

/// A multipart upload of a [`RecordingService`].
#[derive(Debug)]
struct RecordingMultipart {
    /// The multipart upload of the service that does the work.
    multipart: Box<dyn MultipartUpload>,
    /// What was asked of the service.
    recording: Arc<Mutex<Recording>>,
    /// Which part fails to arrive, as in the service.
    lost_part_number: Option<usize>,
}

#[async_trait]
impl MultipartUpload for RecordingMultipart {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let part_number = {
            let mut recording = self.recording.lock().expect("no test panics while it records");
            recording.requests.push(StoreRequest::Part {
                part_bytes: payload.content_length(),
            });
            recording.parts_sent += 1;
            recording.parts_sent - 1
        };
        if self.lost_part_number == Some(part_number) {
            return Box::pin(std::future::ready(Err(server_error())));
        }
        self.multipart.put_part(payload)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        record(&self.recording, StoreRequest::CompleteMultipart);
        self.multipart.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        record(&self.recording, StoreRequest::AbortMultipart);
        self.multipart.abort().await
    }
}

/// Writes down one request.
fn record(recording: &Mutex<Recording>, request: StoreRequest) {
    recording
        .lock()
        .expect("no test panics while it records")
        .requests
        .push(request);
}

/// A store that sends parts of [`SMALL_PART_BYTES`], and the record of what it asked of its service.
struct RecordedStore {
    /// The store under test.
    store: S3ArtifactStore,
    /// What the store asked of the service.
    recording: Arc<Mutex<Recording>>,
}

impl RecordedStore {
    /// What the store asked of the service so far, in order.
    fn requests(&self) -> Vec<StoreRequest> {
        self.recording
            .lock()
            .expect("no test panics while it records")
            .requests
            .clone()
    }
}

/// A store with small parts over a recording service that loses the part with the given number, if any.
fn recorded_store_losing(lost_part_number: Option<usize>) -> RecordedStore {
    let service = RecordingService::losing(lost_part_number);
    let recording = Arc::clone(&service.recording);
    RecordedStore {
        store: S3ArtifactStore::over(Arc::new(service), "", REQUEST_TIMEOUT, SMALL_PART_BYTES),
        recording,
    }
}

/// A store with small parts over a recording service that takes every part.
fn recorded_store() -> RecordedStore {
    recorded_store_losing(None)
}

/// Starts an upload to `streams/7/segments/3` and hands it `pieces` in order.
async fn upload_of(store: &S3ArtifactStore, pieces: &[&'static [u8]]) -> Box<dyn ObjectUpload> {
    let mut upload = store
        .start_upload("streams/7/segments/3")
        .await
        .expect("an upload to a valid key starts");
    for piece in pieces {
        upload
            .write(Bytes::from_static(piece))
            .await
            .expect("the service takes the piece");
    }
    upload
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

/// A store over a service with the given fault. Its parts are small, so that an upload reaches the
/// service with its first few bytes.
fn store_over_broken_service(fault: ServiceFault) -> S3ArtifactStore {
    S3ArtifactStore::over(Arc::new(BrokenService { fault }), "", REQUEST_TIMEOUT, SMALL_PART_BYTES)
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

#[test]
fn keys_under_a_prefix_without_a_separator_lie_at_the_root_of_the_bucket() {
    let directory = directory_holding("streams");

    assert_eq!(directory.ok(), Some(None));
}

#[test]
fn keys_under_a_prefix_lie_in_the_directory_before_its_last_separator() {
    let directory = directory_holding("streams/7/segments/0000");

    assert_eq!(directory.ok(), Some(Some(Path::from("streams/7/segments"))));
}

#[test]
fn prefix_that_ends_in_a_separator_names_its_own_directory() {
    let directory = directory_holding("streams/7/");

    assert_eq!(directory.ok(), Some(Some(Path::from("streams/7"))));
}

#[test]
fn directory_with_an_empty_component_cannot_be_addressed() {
    let directory = directory_holding("streams//segments/0000");

    assert!(directory.is_err(), "{directory:?}");
}

// Uploads.

/// The contract's large object is 20 MiB, which is three parts for a store as it is opened. Here it is
/// several hundred, so the gathering of pieces into parts is held to the contract as well.
#[tokio::test]
async fn store_with_small_parts_honours_the_artifact_store_contract() {
    let parts_of_64_kibibytes = 64 * 1024;
    let store = S3ArtifactStore::over(Arc::new(InMemory::new()), "", REQUEST_TIMEOUT, parts_of_64_kibibytes);

    contract::check(&store).await;
}

#[tokio::test]
async fn object_smaller_than_a_part_is_stored_with_one_request() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"12345", b"6789"]).await;

    upload.complete().await.expect("the service stores the object");

    assert_eq!(recorded.requests(), [StoreRequest::Put { object_bytes: 9 }]);
}

/// Twenty-five bytes in pieces that do not fit the parts: two full parts go out while the pieces come in,
/// and the rest goes out as the last part when the upload is completed.
#[tokio::test]
async fn upload_is_sent_in_parts_of_one_size_and_a_shorter_last_one() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567", b"890abcdefghi", b"jklmno"]).await;

    upload.complete().await.expect("the service stores the object");

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 5 },
            StoreRequest::CompleteMultipart,
        ]
    );
}

/// The bound on what an upload keeps in memory: once a piece has been taken, every full part of what was
/// handed over is with the service, so less than a part is held back.
#[tokio::test]
async fn upload_holds_back_less_than_a_part_between_pieces() {
    let recorded = recorded_store();

    let _upload = upload_of(&recorded.store, &[b"1234567", b"890abcdefghi", b"jklmno"]).await;

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 10 },
        ]
    );
}

/// Pieces of every kind: shorter than what is left of a part, exactly filling it, several parts long.
const PIECES_OF_MANY_SIZES: [&[u8]; 6] = [
    b"123",
    b"4567890",
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHI",
    b"J",
    b"KLMNOPQRSTUVWXYZ0123",
    b"45678",
];

/// What a buffer of parts of [`SMALL_PART_BYTES`] did with [`PIECES_OF_MANY_SIZES`].
struct FilledParts {
    /// The parts the buffer gave out, in order.
    parts: Vec<Bytes>,
    /// What the buffer held at the end.
    rest: Bytes,
    /// The largest capacity the buffer had after any step.
    largest_buffer_bytes: usize,
}

/// Hands [`PIECES_OF_MANY_SIZES`] to a buffer of parts of [`SMALL_PART_BYTES`], the way an upload does.
///
/// # Arguments
///
/// * `parts_are_sent_at_once` - Whether each part is dropped before the buffer is filled again, as when
///   the service has taken it. Otherwise every part is still alive while the next ones are filled.
fn fill_parts_with_pieces_of_many_sizes(parts_are_sent_at_once: bool) -> FilledParts {
    let mut part_buffer = PartBuffer::of_parts_of(SMALL_PART_BYTES);
    let mut parts = Vec::new();
    let mut parts_still_alive = Vec::new();
    let mut largest_buffer_bytes = 0;
    for piece in PIECES_OF_MANY_SIZES {
        let mut rest_of_the_piece = Bytes::from_static(piece);
        while !rest_of_the_piece.is_empty() {
            let full_part = part_buffer.fill_from(&mut rest_of_the_piece);
            largest_buffer_bytes = largest_buffer_bytes.max(part_buffer.unsent.capacity());
            if let Some(part) = full_part {
                parts.push(Bytes::copy_from_slice(&part));
                if !parts_are_sent_at_once {
                    parts_still_alive.push(part);
                }
            }
        }
    }
    FilledParts {
        parts,
        rest: part_buffer.take_rest(),
        largest_buffer_bytes,
    }
}

#[test]
fn part_buffer_cuts_the_pieces_into_parts_of_one_size() {
    let filled = fill_parts_with_pieces_of_many_sizes(true);

    assert_eq!(
        filled.parts,
        [
            Bytes::from_static(b"1234567890"),
            Bytes::from_static(b"abcdefghij"),
            Bytes::from_static(b"klmnopqrst"),
            Bytes::from_static(b"uvwxyzABCD"),
            Bytes::from_static(b"EFGHIJKLMN"),
            Bytes::from_static(b"OPQRSTUVWX"),
            Bytes::from_static(b"YZ01234567"),
        ]
    );
}

#[test]
fn part_buffer_keeps_what_does_not_fill_a_part() {
    let filled = fill_parts_with_pieces_of_many_sizes(true);

    assert_eq!(filled.rest, Bytes::from_static(b"8"));
}

/// The bound in bytes: the memory of an upload is one buffer of one part, which is filled, sent and filled
/// again. A buffer that took a whole piece before it cut parts from it would pass every test of the
/// requests, and hold twice as much or more.
#[test]
fn part_buffer_is_never_larger_than_one_part() {
    let filled = fill_parts_with_pieces_of_many_sizes(true);

    assert_eq!(filled.largest_buffer_bytes, SMALL_PART_BYTES);
}

/// A service may still hold a part when the next one is filled; the in-memory service of these tests
/// holds every part until the upload is completed. The buffer must then start a new part, not grow.
#[test]
fn part_buffer_is_never_larger_than_one_part_while_earlier_parts_are_still_alive() {
    let filled = fill_parts_with_pieces_of_many_sizes(false);

    assert_eq!(filled.largest_buffer_bytes, SMALL_PART_BYTES);
}

#[test]
fn part_buffer_that_was_handed_nothing_holds_nothing() {
    let mut part_buffer = PartBuffer::of_parts_of(SMALL_PART_BYTES);

    assert_eq!(part_buffer.unsent.capacity(), 0);
    assert_eq!(part_buffer.take_rest(), Bytes::new());
}

#[test]
fn part_buffer_is_empty_after_its_rest_was_taken() {
    let mut part_buffer = PartBuffer::of_parts_of(SMALL_PART_BYTES);
    let _no_part = part_buffer.fill_from(&mut Bytes::from_static(b"123"));
    let _rest = part_buffer.take_rest();

    assert_eq!(part_buffer.take_rest(), Bytes::new());
}

/// A part of no bytes would never be full of anything else, and an upload would never end.
#[test]
fn part_buffer_has_parts_of_at_least_one_byte() {
    let mut part_buffer = PartBuffer::of_parts_of(0);
    let mut piece = Bytes::from_static(b"12");

    let first_part = part_buffer.fill_from(&mut piece);

    assert_eq!(first_part, Some(Bytes::from_static(b"1")));
}

#[tokio::test]
async fn piece_of_several_parts_is_sent_as_whole_parts_at_once() {
    let recorded = recorded_store();

    let _upload = upload_of(&recorded.store, &[b"1234567890abcdefghijklmnopqrstuvwxy"]).await;

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 10 },
        ]
    );
}

/// An object that ends exactly at a part has no shorter last part: an empty one would be refused by a
/// real service.
#[tokio::test]
async fn object_of_whole_parts_is_completed_without_an_empty_last_part() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567890", b"abcdefghij"]).await;

    upload.complete().await.expect("the service stores the object");

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::CompleteMultipart,
        ]
    );
}

#[tokio::test]
async fn object_sent_in_parts_is_read_back_whole() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567", b"890abcdefghi", b"jklmno"]).await;

    upload.complete().await.expect("the service stores the object");

    assert_eq!(
        recorded.store.get_object("streams/7/segments/3").await,
        Ok(Bytes::from_static(b"1234567890abcdefghijklmno"))
    );
}

#[tokio::test]
async fn aborted_upload_tells_the_service_to_drop_its_parts() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    upload.abort().await.expect("the service drops the parts");

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::AbortMultipart,
        ]
    );
}

#[tokio::test]
async fn aborted_upload_that_sent_no_part_asks_nothing_of_the_service() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"12345"]).await;

    upload.abort().await.expect("there is nothing to drop");

    assert_eq!(recorded.requests(), []);
}

#[tokio::test]
async fn piece_that_fills_a_part_the_service_loses_is_refused() {
    let recorded = recorded_store_losing(Some(0));
    let mut upload = upload_of(&recorded.store, &[b"12345"]).await;

    let written = upload.write(Bytes::from_static(b"67890")).await;

    assert!(is_unavailable(&written), "{written:?}");
}

#[tokio::test]
async fn upload_that_lost_a_part_takes_no_more_pieces() {
    let recorded = recorded_store_losing(Some(0));
    let mut upload = upload_of(&recorded.store, &[b"12345"]).await;
    let _lost_part = upload.write(Bytes::from_static(b"67890")).await;

    let written = upload.write(Bytes::from_static(b"abcde")).await;

    assert!(is_unavailable(&written), "{written:?}");
}

/// The service holds a part less than the upload was handed. Completing would make an object with a hole
/// in it, under a key that a reader trusts.
#[tokio::test]
async fn upload_that_lost_a_part_is_never_completed() {
    let recorded = recorded_store_losing(Some(1));
    let mut upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;
    let _lost_part = upload.write(Bytes::from_static(b"defghijklm")).await;

    let completed = upload.complete().await;

    assert!(is_unavailable(&completed), "{completed:?}");
    assert!(!recorded.requests().contains(&StoreRequest::CompleteMultipart));
}

#[tokio::test]
async fn upload_that_lost_its_last_part_is_not_completed() {
    let recorded = recorded_store_losing(Some(1));
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    let completed = upload.complete().await;

    assert!(is_unavailable(&completed), "{completed:?}");
    assert!(!recorded.requests().contains(&StoreRequest::CompleteMultipart));
}

/// A completion takes the upload with it, so nobody could abort it afterwards: the completion that fails
/// does it itself.
#[tokio::test]
async fn completion_that_fails_tells_the_service_to_drop_the_parts() {
    let recorded = recorded_store_losing(Some(1));
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    let _failed = upload.complete().await;

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::Part { part_bytes: 3 },
            StoreRequest::AbortMultipart,
        ]
    );
}

/// What happens to an upload whose caller was itself given up on, for example a request that was
/// cancelled. A drop cannot wait, so the abort runs in a task of its own; the test lets it run.
#[tokio::test]
async fn upload_dropped_after_it_sent_a_part_tells_the_service_to_drop_its_parts() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    drop(upload);
    tokio::task::yield_now().await;

    assert_eq!(
        recorded.requests(),
        [
            StoreRequest::OpenMultipart,
            StoreRequest::Part { part_bytes: 10 },
            StoreRequest::AbortMultipart,
        ]
    );
}

#[tokio::test]
async fn upload_dropped_before_it_sent_a_part_asks_nothing_of_the_service() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"12345"]).await;

    drop(upload);
    tokio::task::yield_now().await;

    assert_eq!(recorded.requests(), []);
}

#[tokio::test]
async fn completed_upload_is_not_aborted_when_it_is_dropped() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    upload.complete().await.expect("the service stores the object");
    tokio::task::yield_now().await;

    assert!(!recorded.requests().contains(&StoreRequest::AbortMultipart));
}

#[tokio::test]
async fn aborted_upload_is_aborted_once() {
    let recorded = recorded_store();
    let upload = upload_of(&recorded.store, &[b"1234567890abc"]).await;

    upload.abort().await.expect("the service drops the parts");
    tokio::task::yield_now().await;

    let aborts = recorded
        .requests()
        .into_iter()
        .filter(|request| *request == StoreRequest::AbortMultipart)
        .count();
    assert_eq!(aborts, 1);
}

/// An upload can be dropped where no runtime is left to run the abort, for example while a process shuts
/// down. Then the parts stay for the bucket's lifecycle rule, and the drop must still be quiet.
#[test]
fn upload_dropped_outside_a_runtime_is_dropped_quietly() {
    let recorded = recorded_store();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a runtime with a clock can be built");
    let upload = runtime.block_on(upload_of(&recorded.store, &[b"1234567890abc"]));
    drop(runtime);

    drop(upload);

    assert!(!recorded.requests().contains(&StoreRequest::AbortMultipart));
}

#[tokio::test]
async fn failing_service_makes_the_first_part_of_an_upload_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);
    let mut upload = store.start_upload("a").await.expect("starting an upload sends nothing");

    let outcome = upload.write(Bytes::from_static(b"1234567890")).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test]
async fn failing_service_makes_the_completion_of_a_small_upload_unavailable() {
    let store = store_over_broken_service(ServiceFault::FailsEveryRequest);
    let upload = upload_of(&store, &[b"12345"]).await;

    let outcome = upload.complete().await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
}

#[tokio::test(start_paused = true)]
async fn part_sent_to_a_service_that_never_answers_fails_at_the_request_timeout() {
    let store = store_over_broken_service(ServiceFault::NeverAnswers);
    let mut upload = store.start_upload("a").await.expect("starting an upload sends nothing");
    let asked_at = tokio::time::Instant::now();

    let outcome = upload.write(Bytes::from_static(b"1234567890")).await;

    assert!(is_unavailable(&outcome), "{outcome:?}");
    assert_eq!(asked_at.elapsed(), REQUEST_TIMEOUT);
}

#[tokio::test]
async fn upload_to_a_key_with_a_control_character_is_refused() {
    let outcome = store().start_upload("reports/a\u{7}b").await;

    assert!(matches!(outcome, Err(ArtifactError::InvalidKey { .. })));
}
