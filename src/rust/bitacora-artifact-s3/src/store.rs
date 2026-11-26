//! The artifact store on an object store service.
//!
//! # Why this file exists
//!
//! Tiered log segments and everything a materializer writes are read and written through the small
//! `bitacora_artifact::ArtifactStore` interface. A real cluster keeps those objects in an S3-compatible
//! service, and the `object_store` crate is the client for it. The two do not agree on details that matter
//! to callers: what a range outside an object means, what a listing prefix is, whether deleting a missing
//! object is an error. This file translates, so that the code above behaves on S3 exactly as it does on the
//! in-memory store that the simulator uses.
//!
//! # What it contains
//!
//! - [`S3ArtifactStore`]: the store. It implements `ArtifactStore` over any
//!   `object_store::ObjectStore`; `s3.rs` builds it for an S3 service.
//! - `S3Upload`: an object on its way in piece by piece, as the parts of a multipart upload.
//! - `PartBuffer`: the one buffer of an upload, which turns pieces of any size into parts of one size.
//!
//! # How it works
//!
//! Every key gets the store's key prefix on the way in and loses it on the way out. A key is used as it
//! is, without any encoding, so the object in the bucket has exactly the name the caller chose.
//!
//! Each method is one operation with one deadline, the request timeout of the settings. What needs
//! explaining is where the two interfaces differ:
//!
//! - **Ranges.** S3 answers a range that ends past the object with the bytes there are, and refuses one
//!   that starts past it without saying how large the object is. The interface wants
//!   `RangeOutOfBounds` with the real size in both cases. A range read is therefore one ranged `GET`: its
//!   answer carries the size of the whole object, so a short answer is recognised and reported at no extra
//!   cost. Only when the `GET` fails is the size asked for with a `HEAD`, to tell a range that starts past
//!   the end from an outage. An empty range cannot be asked for over HTTP at all; it is answered from a
//!   `HEAD` alone.
//! - **Listings.** `object_store` lists whole path components: `logs/a` matches `logs/a/x` and not
//!   `logs/ab`. The interface lists by characters. The store lists the directory that the prefix names
//!   (`logs` for `logs/a`) and keeps the keys that start with the prefix. A prefix that ends at a `/` is
//!   therefore exact, and one that ends inside a name costs a listing of that name's directory.
//! - **Uploads.** The interface takes a large object in pieces of any size and makes it appear at the end.
//!   The service has multipart uploads for that, with rules of its own: every part but the last must be
//!   at least 5 MiB, and the stricter services want them all of one size. The store copies the pieces
//!   into a buffer of one part of 8 MiB, sends the part each time the buffer is full, one part after the
//!   other, and fills the same buffer again. That buffer is all an upload holds, whatever the size of
//!   the object and of the pieces. An object that never fills a part is stored with one plain `PUT`. The
//!   service keeps the parts of a multipart upload until it is completed or aborted, so
//!   the store aborts every upload that ends any other way than by a completion: one that is aborted,
//!   one whose completion fails, and one that is dropped, whose abort runs in a task of its own because
//!   a drop cannot wait. The parts of an upload whose process died, or whose abort did not reach the
//!   service, stay until a lifecycle rule of the bucket removes them.
//! - **Deleting.** Deleting an object that is not there succeeds, whatever the service says.
//! - **A missing bucket** answers `404` like a missing object does. The store looks at the error code in
//!   the answer and reports a missing bucket as an outage, with the reason, and not as a missing object.
//!
//! Everything else that goes wrong, a timeout included, is `Unavailable` with the reason.
//!
//! # Invariants
//!
//! - No operation outlives the request timeout. For an upload, each part, and its completion, is one
//!   operation.
//! - An upload that lost a part is never completed: the object would lack what the part held.
//! - An upload that sent a part and does not end in a completion asks the service to drop its parts, once.
//! - A key that comes back from a listing is a key that can be passed to every other method.
//! - The store reports `NotFound` only for an object that the service says is absent from a bucket that
//!   exists. The one exception is an empty or inverted range: it is answered from a `HEAD`, whose answer
//!   has no body to tell a missing bucket from a missing object.
//!
//! # See also
//!
//! - `s3.rs`: how the store is built for an S3 service, and what is retried.
//! - `settings.rs`: the settings.
//! - `bitacora-artifact/src/contract.rs`: the behaviour every artifact store must have.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitacora_artifact::{ArtifactError, ArtifactStore, ObjectInfo, ObjectUpload, validate_key};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, MultipartUpload, ObjectStore, ObjectStoreExt as _, PutPayload, UploadPart};
use tokio::runtime::Handle;
use tokio::time::timeout;

/// The error code an S3 service puts in its answer when the bucket of a request does not exist. The `404`
/// status alone does not tell a missing bucket from a missing object.
const MISSING_BUCKET_CODE: &str = "<Code>NoSuchBucket</Code>";

/// How many bytes of an upload go to the service as one part: 8 MiB.
///
/// S3 refuses a part smaller than 5 MiB unless it is the last one, and takes at most 10,000 parts, so this
/// size stores objects of up to 78 GiB; a segment is 1 GiB. It is also what an upload holds in memory: one
/// buffer of this size. Every part but the last has exactly this size, which the stricter S3-compatible
/// services insist on.
pub(crate) const UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;

/// An artifact store that keeps its objects in an object store service.
///
/// Build one with [`S3ArtifactStore::open`]. Clones are cheap and share the same connections, so one store
/// is opened at start and handed to everything that needs it.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use bitacora_artifact::ArtifactStore;
/// use bitacora_artifact_s3::{S3ArtifactStore, S3Settings};
/// use bytes::Bytes;
///
/// let store = S3ArtifactStore::open(&S3Settings::new("bitacora-artifacts", "eu-west-1"))?;
/// store
///     .put_object("streams/7/segments/3", Bytes::from_static(b"a sealed segment"))
///     .await?;
/// let footer = store.get_range("streams/7/segments/3", 9..16).await?;
/// assert_eq!(footer.as_ref(), b"segment");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct S3ArtifactStore {
    /// The client of the service. Behind an `Arc` of the interface, and not the S3 client itself, so that
    /// the translation in this file can be tested against `object_store`'s in-memory store.
    object_store: Arc<dyn ObjectStore>,

    /// The text put in front of every key: empty, or the configured key prefix followed by a `/`.
    key_namespace: String,

    /// The longest one operation may take, the retries the client makes included.
    request_timeout: Duration,

    /// How many bytes of an upload are sent to the service as one part. Always [`UPLOAD_PART_BYTES`]
    /// outside the tests of this file, which use small parts to send many of them.
    upload_part_bytes: usize,
}

impl S3ArtifactStore {
    /// Wraps a client of an object store service.
    ///
    /// Sends nothing. The caller is responsible for the values: `s3.rs` validates the settings they come
    /// from.
    ///
    /// # Arguments
    ///
    /// * `object_store` - The client every operation goes through.
    /// * `key_prefix` - The path inside the service that every key is put under: empty, or a plain
    ///   relative path without a leading or trailing `/`.
    /// * `request_timeout` - The longest one operation may take.
    /// * `upload_part_bytes` - The size of the parts an upload is sent in. An S3 service wants at least
    ///   5 MiB.
    pub(crate) fn over(
        object_store: Arc<dyn ObjectStore>,
        key_prefix: &str,
        request_timeout: Duration,
        upload_part_bytes: usize,
    ) -> Self {
        let key_namespace = if key_prefix.is_empty() {
            String::new()
        } else {
            format!("{key_prefix}/")
        };
        Self {
            object_store,
            key_namespace,
            request_timeout,
            upload_part_bytes,
        }
    }

    /// Returns the longest one operation of this store may take before it fails as unavailable.
    #[cfg(test)]
    pub(crate) const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Maps a key of the interface to where the object lives in the service.
    ///
    /// # Errors
    ///
    /// Fails with `InvalidKey` when the key is not a plain relative path, or contains an ASCII control
    /// character, which the service cannot address.
    fn location_of(&self, object_key: &str) -> Result<Path, ArtifactError> {
        validate_key(object_key)?;
        let full_key = format!("{}{object_key}", self.key_namespace);
        match Path::parse(&full_key) {
            Ok(location) => Ok(location),
            Err(_unaddressable) => Err(ArtifactError::InvalidKey {
                object_key: object_key.to_owned(),
            }),
        }
    }

    /// Runs one operation under the store's deadline.
    ///
    /// # Arguments
    ///
    /// * `operation` - What is being done, as a verb, for the error message.
    /// * `subject` - The key or prefix it is done to, for the error message.
    /// * `request` - The operation itself.
    ///
    /// # Errors
    ///
    /// Returns what `request` returns, or `Unavailable` when it has not finished within the request
    /// timeout. The request is dropped then; whether it took effect is not known.
    async fn within_deadline<Response>(
        &self,
        operation: &str,
        subject: &str,
        request: impl Future<Output = Result<Response, ArtifactError>> + Send,
    ) -> Result<Response, ArtifactError> {
        match timeout(self.request_timeout, request).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(ArtifactError::Unavailable {
                reason: format!(
                    "cannot {operation} {subject:?}: the object store did not answer within {:?}",
                    self.request_timeout
                ),
            }),
        }
    }

    /// Stores an object. Any failure is an outage: a `PUT` has no other way to fail.
    async fn store_at(&self, location: &Path, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        let stored = self.object_store.put(location, PutPayload::from(contents)).await;
        match stored {
            Ok(_version) => Ok(()),
            Err(error) => Err(unavailable("store", object_key, &error)),
        }
    }

    /// Reads a whole object.
    async fn read_at(&self, location: &Path, object_key: &str) -> Result<Bytes, ArtifactError> {
        let answer = match self.object_store.get(location).await {
            Ok(answer) => answer,
            Err(error) => return Err(not_found_or_unavailable("read", object_key, &error)),
        };
        match answer.bytes().await {
            Ok(contents) => Ok(contents),
            Err(error) => Err(unavailable("read", object_key, &error)),
        }
    }

    /// Reads a range of an object. See the file header for why it is done this way.
    async fn read_range_at(
        &self,
        location: &Path,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError> {
        if byte_range.start >= byte_range.end {
            // Empty or inverted: there is no such request in HTTP, and only the object's size decides
            // whether the range lies inside it.
            let size_bytes = self.size_at(location, object_key).await?;
            if byte_range.start == byte_range.end && byte_range.end <= size_bytes {
                return Ok(Bytes::new());
            }
            return Err(out_of_bounds(object_key, &byte_range, size_bytes));
        }

        let ranged = GetOptions::new().with_range(Some(byte_range.clone()));
        let answer = match self.object_store.get_opts(location, ranged).await {
            Ok(answer) => answer,
            Err(error) if is_missing_object(&error) => {
                return Err(not_found(object_key));
            }
            Err(error) => {
                // The service refuses a range that starts at or past the end of the object, and its
                // refusal does not say how large the object is. Ask, to tell that case from an outage.
                return match self.object_store.head(location).await {
                    Ok(object) if byte_range.start >= object.size => {
                        Err(out_of_bounds(object_key, &byte_range, object.size))
                    }
                    _in_bounds_or_unknown => Err(unavailable("read", object_key, &error)),
                };
            }
        };
        if answer.range != byte_range {
            // The range ends past the object: the service sent what there is. The answer carries the size
            // of the whole object, so no second request is needed.
            return Err(out_of_bounds(object_key, &byte_range, answer.meta.size));
        }
        match answer.bytes().await {
            Ok(contents) => Ok(contents),
            Err(error) => Err(unavailable("read", object_key, &error)),
        }
    }

    /// Asks for the size of an object.
    async fn size_at(&self, location: &Path, object_key: &str) -> Result<u64, ArtifactError> {
        match self.object_store.head(location).await {
            Ok(object) => Ok(object.size),
            Err(error) => Err(not_found_or_unavailable("measure", object_key, &error)),
        }
    }

    /// Deletes an object. An object that is already gone is what the caller wanted.
    async fn delete_at(&self, location: &Path, object_key: &str) -> Result<(), ArtifactError> {
        match self.object_store.delete(location).await {
            Ok(()) => Ok(()),
            Err(error) if is_missing_object(&error) => Ok(()),
            Err(error) => Err(unavailable("delete", object_key, &error)),
        }
    }

    /// Lists the objects whose key starts with `key_prefix`, sorted by key.
    async fn list_under(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let full_prefix = format!("{}{key_prefix}", self.key_namespace);
        // The client lists whole path components, so ask for the directory the prefix names and pick the
        // keys that start with the prefix out of it.
        let Ok(directory_location) = directory_holding(&full_prefix) else {
            // No valid key lies in a directory that is not a valid path.
            return Ok(Vec::new());
        };

        let mut listed = Vec::new();
        let mut entries = self.object_store.list(directory_location.as_ref());
        while let Some(entry) = entries.next().await {
            let object = match entry {
                Ok(object) => object,
                Err(error) => return Err(unavailable("list", key_prefix, &error)),
            };
            let full_key: &str = object.location.as_ref();
            if !full_key.starts_with(&full_prefix) {
                continue;
            }
            listed.push(ObjectInfo {
                object_key: full_key[self.key_namespace.len()..].to_owned(),
                size_bytes: object.size,
            });
        }
        listed.sort_by(|left, right| left.object_key.cmp(&right.object_key));
        Ok(listed)
    }
}

/// The directory that every key starting with `full_prefix` lies in: what comes before the prefix's
/// last separator.
///
/// # Returns
///
/// `None` for a prefix without a separator, whose keys lie at the root of the bucket.
///
/// # Errors
///
/// Returns the client's refusal if the directory is not a path the client can address.
fn directory_holding(full_prefix: &str) -> Result<Option<Path>, object_store::path::Error> {
    let directory = match full_prefix.rfind('/') {
        Some(last_separator) => &full_prefix[..last_separator],
        None => "",
    };
    if directory.is_empty() {
        return Ok(None);
    }
    Ok(Some(Path::parse(directory)?))
}

/// Whether `error` says that the object is absent from a bucket that exists.
fn is_missing_object(error: &object_store::Error) -> bool {
    match error {
        object_store::Error::NotFound { source, .. } => !source.to_string().contains(MISSING_BUCKET_CODE),
        _other => false,
    }
}

/// The error for an object that is not there.
fn not_found(object_key: &str) -> ArtifactError {
    ArtifactError::NotFound {
        object_key: object_key.to_owned(),
    }
}

/// The error for a failed read of an object: `NotFound` if the service says the object is absent,
/// `Unavailable` for everything else.
fn not_found_or_unavailable(operation: &str, object_key: &str, error: &object_store::Error) -> ArtifactError {
    if is_missing_object(error) {
        not_found(object_key)
    } else {
        unavailable(operation, object_key, error)
    }
}

/// The error for a range that does not lie inside its object.
fn out_of_bounds(object_key: &str, byte_range: &Range<u64>, size_bytes: u64) -> ArtifactError {
    ArtifactError::RangeOutOfBounds {
        object_key: object_key.to_owned(),
        start: byte_range.start,
        end: byte_range.end,
        size_bytes,
    }
}

/// The error for an operation that the service did not carry out. The reason says what was attempted, on
/// what, and what the client reported, so that an operator can tell a wrong bucket from wrong credentials
/// from a network problem.
fn unavailable(operation: &str, subject: &str, error: &object_store::Error) -> ArtifactError {
    ArtifactError::Unavailable {
        reason: format!("cannot {operation} {subject:?}: {error}"),
    }
}

/// The one buffer of an upload: pieces of any size go in, parts of one size come out.
///
/// The buffer is as large as one part and no larger. A piece is copied into it only as far as the part
/// that is being filled has room, and the part is given out, to be sent, before the rest of the piece is
/// looked at. Appending the whole piece first and cutting parts from it afterwards would be less code,
/// and would let the buffer grow to a part and a piece, doubled by how a buffer grows. This way the
/// memory of an upload is a number that can be written down: one part.
///
/// Once a part has been sent and dropped, its bytes are the buffer of the next part: nothing is
/// allocated after the first part of an upload.
#[derive(Debug)]
struct PartBuffer {
    /// How many bytes a part holds: at least one.
    part_bytes: usize,
    /// The part that is being filled. Never longer than a part, and never with room for more than one.
    unsent: BytesMut,
}

impl PartBuffer {
    /// An empty buffer that gives out parts of `part_bytes`. It holds no memory until it is handed
    /// something.
    ///
    /// # Arguments
    ///
    /// * `part_bytes` - The size of a part. Zero is taken as one: a part of no bytes would never be full
    ///   of anything else, and an upload would never end.
    fn of_parts_of(part_bytes: usize) -> Self {
        Self {
            part_bytes: part_bytes.max(1),
            unsent: BytesMut::new(),
        }
    }

    /// Moves bytes from the front of `piece` into the part that is being filled, as many as fit.
    ///
    /// # Arguments
    ///
    /// * `piece` - The bytes that follow those already taken. What was taken is removed from its front;
    ///   what did not fit stays, for the next call.
    ///
    /// # Returns
    ///
    /// The part, if it is full now; the buffer then starts the next one. `None` if the piece ended first.
    fn fill_from(&mut self, piece: &mut Bytes) -> Option<Bytes> {
        let room_bytes = self.part_bytes - self.unsent.len();
        // Room for the rest of this part and never more. Where the part before was sent and dropped,
        // this takes its bytes back; where it is still alive, this starts a buffer of one part.
        self.unsent.reserve(room_bytes);
        let fitting = piece.split_to(room_bytes.min(piece.len()));
        self.unsent.extend_from_slice(&fitting);
        if self.unsent.len() < self.part_bytes {
            return None;
        }
        Some(self.unsent.split().freeze())
    }

    /// Takes what the buffer holds: the last part of an object, shorter than the others, or empty.
    fn take_rest(&mut self) -> Bytes {
        self.unsent.split().freeze()
    }
}

/// An object on its way into the service piece by piece.
///
/// The pieces are gathered into parts of the store's part size, and each part is sent as soon as it is
/// full, as one part of a multipart upload of the service. What the upload keeps is the part that is being
/// filled, in a buffer of one part that it uses again and again, whatever the size of the object and of
/// the pieces. The multipart upload is opened when the first part is full, so an object smaller than a
/// part costs one `PUT` and leaves nothing to clean up.
///
/// A multipart upload exists at the service until it is completed or aborted, and its parts are billed
/// meanwhile although no listing shows them. The upload aborts it when it is told to, when its completion
/// fails, and when it is dropped. What it cannot cover is a process that dies and a service that cannot be
/// reached for the abort: those parts stay until a lifecycle rule of the bucket removes them.
struct S3Upload {
    /// The store the object goes into.
    store: S3ArtifactStore,
    /// Where the object will live in the service.
    location: Path,
    /// The key the object will be stored under, for error messages.
    object_key: String,
    /// What was handed over and not sent yet: the part that is being filled.
    unsent: PartBuffer,
    /// The multipart upload of the service, from the moment the first part is sent.
    multipart: Option<Box<dyn MultipartUpload>>,
    /// Whether a part could not be sent. The service then holds a part less than it was handed, so the
    /// upload must not go on, and above all must not be completed.
    has_lost_a_part: bool,
}

impl S3Upload {
    /// Sends one part to the service, opening the multipart upload first if this is the first part.
    ///
    /// # Errors
    ///
    /// Fails with `Unavailable` if the service fails or does not answer in time. The part is lost then,
    /// and the upload remembers it.
    async fn send_part(&mut self, part: Bytes) -> Result<(), ArtifactError> {
        let sent = self.try_to_send_part(part).await;
        if sent.is_err() {
            self.has_lost_a_part = true;
        }
        sent
    }

    /// Opens the multipart upload if there is none yet, and sends `part` as its next part.
    async fn try_to_send_part(&mut self, part: Bytes) -> Result<(), ArtifactError> {
        if self.multipart.is_none() {
            let opening = open_multipart_at(&self.store.object_store, &self.location, &self.object_key);
            let opened = self.store.within_deadline("store", &self.object_key, opening).await?;
            self.multipart = Some(opened);
        }
        let multipart = self
            .multipart
            .as_mut()
            .expect("the multipart upload was opened above if there was none");
        let sending = send_part_of(multipart.put_part(PutPayload::from(part)), &self.object_key);
        self.store.within_deadline("store", &self.object_key, sending).await
    }

    /// Sends what is left as the last part and completes the multipart upload; or, if no part was ever
    /// full, stores the whole object with one `PUT`.
    ///
    /// # Errors
    ///
    /// Fails with `Unavailable` if a part was lost earlier, or if the service fails or does not answer in
    /// time. The multipart upload, if there is one, is still open at the service then.
    async fn try_to_complete(&mut self) -> Result<(), ArtifactError> {
        if self.has_lost_a_part {
            return Err(self.lost_a_part());
        }
        let rest = self.unsent.take_rest();
        if self.multipart.is_none() {
            let request = self.store.store_at(&self.location, &self.object_key, rest);
            return self.store.within_deadline("store", &self.object_key, request).await;
        }
        if !rest.is_empty() {
            self.send_part(rest).await?;
        }
        let multipart = self
            .multipart
            .as_mut()
            .expect("the case of no multipart upload returned above");
        let completing = complete_multipart(multipart.as_mut(), &self.object_key);
        self.store.within_deadline("store", &self.object_key, completing).await
    }

    /// Tells the service to drop the parts of the multipart upload, if one was opened.
    ///
    /// The service is asked once. Whatever it answers, the upload forgets the multipart upload, so that
    /// its drop does not ask again.
    ///
    /// # Errors
    ///
    /// Fails with `Unavailable` if the service fails or does not answer in time. The parts may still be
    /// there then.
    async fn drop_the_parts(&mut self) -> Result<(), ArtifactError> {
        let Some(multipart) = self.multipart.as_mut() else {
            return Ok(());
        };
        let aborting = abort_multipart(multipart.as_mut(), &self.object_key);
        let aborted = self
            .store
            .within_deadline("abort the upload of", &self.object_key, aborting)
            .await;
        self.multipart = None;
        aborted
    }

    /// The error of a step that comes after a part was lost.
    fn lost_a_part(&self) -> ArtifactError {
        ArtifactError::Unavailable {
            reason: format!(
                "cannot store {:?}: an earlier part of the upload could not be sent",
                self.object_key
            ),
        }
    }
}

/// Opens a multipart upload at the service. Any failure is an outage.
async fn open_multipart_at(
    object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    object_key: &str,
) -> Result<Box<dyn MultipartUpload>, ArtifactError> {
    match object_store.put_multipart(location).await {
        Ok(multipart) => Ok(multipart),
        Err(error) => Err(unavailable("store", object_key, &error)),
    }
}

/// Waits for one part to reach the service. Any failure is an outage.
async fn send_part_of(part_being_sent: UploadPart, object_key: &str) -> Result<(), ArtifactError> {
    match part_being_sent.await {
        Ok(()) => Ok(()),
        Err(error) => Err(unavailable("store", object_key, &error)),
    }
}

/// Makes the parts of a multipart upload the object. Any failure is an outage.
async fn complete_multipart(multipart: &mut dyn MultipartUpload, object_key: &str) -> Result<(), ArtifactError> {
    match multipart.complete().await {
        Ok(_version) => Ok(()),
        Err(error) => Err(unavailable("store", object_key, &error)),
    }
}

/// Tells the service to drop the parts of a multipart upload. Any failure is an outage.
async fn abort_multipart(multipart: &mut dyn MultipartUpload, object_key: &str) -> Result<(), ArtifactError> {
    match multipart.abort().await {
        Ok(()) => Ok(()),
        Err(error) => Err(unavailable("abort the upload of", object_key, &error)),
    }
}

#[async_trait]
impl ObjectUpload for S3Upload {
    /// Moves the piece into the part that is being filled, and sends the part each time it is full.
    async fn write(&mut self, piece: Bytes) -> Result<(), ArtifactError> {
        if self.has_lost_a_part {
            return Err(self.lost_a_part());
        }
        let mut rest_of_the_piece = piece;
        while !rest_of_the_piece.is_empty() {
            if let Some(full_part) = self.unsent.fill_from(&mut rest_of_the_piece) {
                self.send_part(full_part).await?;
            }
        }
        Ok(())
    }

    /// Sends what is left as the last part and completes the multipart upload; or, if no part was ever
    /// full, stores the whole object with one `PUT`.
    ///
    /// A completion that fails tells the service to drop the parts before it returns: the upload ends
    /// here either way, and nobody could abort it afterwards.
    async fn complete(mut self: Box<Self>) -> Result<(), ArtifactError> {
        let completed = self.try_to_complete().await;
        if completed.is_ok() {
            // The parts are the object now. Nothing is left that a drop would have to abort.
            self.multipart = None;
            return Ok(());
        }
        // The failure of the completion is what the caller must hear, whatever the abort says.
        let _aborted = self.drop_the_parts().await;
        completed
    }

    /// Tells the service to drop the parts it was sent. An upload that never sent a part has nothing
    /// there.
    async fn abort(mut self: Box<Self>) -> Result<(), ArtifactError> {
        self.drop_the_parts().await
    }
}

impl Drop for S3Upload {
    /// Aborts an upload that was dropped while the service holds parts of it: one whose caller failed
    /// and did not abort it, or was itself given up on half way, as a request that is cancelled.
    ///
    /// A drop cannot wait for the service, so the abort runs in a task of its own and nobody hears how
    /// it went. Without a runtime to run it, for example while the process shuts down, the parts stay
    /// for the lifecycle rule of the bucket, as they do when the process dies.
    fn drop(&mut self) {
        let Some(mut multipart) = self.multipart.take() else {
            return;
        };
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let request_timeout = self.store.request_timeout;
        runtime.spawn(async move {
            let _aborted = timeout(request_timeout, multipart.abort()).await;
        });
    }
}

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        let location = self.location_of(object_key)?;
        let request = self.store_at(&location, object_key, contents);
        self.within_deadline("store", object_key, request).await
    }

    /// Sends nothing yet: the service hears of the upload when its first part is full, or when it is
    /// completed.
    async fn start_upload(&self, object_key: &str) -> Result<Box<dyn ObjectUpload>, ArtifactError> {
        let location = self.location_of(object_key)?;
        Ok(Box::new(S3Upload {
            store: self.clone(),
            location,
            object_key: object_key.to_owned(),
            unsent: PartBuffer::of_parts_of(self.upload_part_bytes),
            multipart: None,
            has_lost_a_part: false,
        }))
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        let location = self.location_of(object_key)?;
        let request = self.read_at(&location, object_key);
        self.within_deadline("read", object_key, request).await
    }

    async fn get_range(&self, object_key: &str, byte_range: Range<u64>) -> Result<Bytes, ArtifactError> {
        let location = self.location_of(object_key)?;
        let request = self.read_range_at(&location, object_key, byte_range);
        self.within_deadline("read", object_key, request).await
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        let location = self.location_of(object_key)?;
        let request = self.delete_at(&location, object_key);
        self.within_deadline("delete", object_key, request).await
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let request = self.list_under(key_prefix);
        self.within_deadline("list", key_prefix, request).await
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
