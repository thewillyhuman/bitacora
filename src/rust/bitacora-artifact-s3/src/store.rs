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
//! - **Deleting.** Deleting an object that is not there succeeds, whatever the service says.
//! - **A missing bucket** answers `404` like a missing object does. The store looks at the error code in
//!   the answer and reports a missing bucket as an outage, with the reason, and not as a missing object.
//!
//! Everything else that goes wrong, a timeout included, is `Unavailable` with the reason.
//!
//! # Invariants
//!
//! - No operation outlives the request timeout.
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
use bitacora_artifact::{ArtifactError, ArtifactStore, ObjectInfo, validate_key};
use bytes::Bytes;
use futures_util::StreamExt as _;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _, PutPayload};
use tokio::time::timeout;

/// The error code an S3 service puts in its answer when the bucket of a request does not exist. The `404`
/// status alone does not tell a missing bucket from a missing object.
const MISSING_BUCKET_CODE: &str = "<Code>NoSuchBucket</Code>";

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
    pub(crate) fn over(object_store: Arc<dyn ObjectStore>, key_prefix: &str, request_timeout: Duration) -> Self {
        let key_namespace = if key_prefix.is_empty() {
            String::new()
        } else {
            format!("{key_prefix}/")
        };
        Self {
            object_store,
            key_namespace,
            request_timeout,
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
        let directory = match full_prefix.rfind('/') {
            Some(last_separator) => &full_prefix[..last_separator],
            None => "",
        };
        let directory_location = if directory.is_empty() {
            None
        } else {
            match Path::parse(directory) {
                Ok(location) => Some(location),
                // No valid key lies in a directory that is not a valid path.
                Err(_unaddressable) => return Ok(Vec::new()),
            }
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

#[async_trait]
impl ArtifactStore for S3ArtifactStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        let location = self.location_of(object_key)?;
        let request = self.store_at(&location, object_key, contents);
        self.within_deadline("store", object_key, request).await
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
