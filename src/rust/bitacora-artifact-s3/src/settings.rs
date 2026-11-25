//! What a store needs to know to reach its bucket.
//!
//! # Why this file exists
//!
//! Every process that reads or writes artifacts (replica, controller, client library, materializer runner)
//! has to say which bucket, where the service is and how long it is willing to wait. Collecting those values
//! in one plain type keeps them out of function signatures and gives each one a single documented meaning
//! and default. Credentials are kept out of that type on purpose, so that printing or logging a
//! configuration can never print a secret.
//!
//! # What it contains
//!
//! - [`S3Settings`]: bucket, region, endpoint, key prefix, addressing style and the two timeouts.
//! - [`S3Credentials`]: an access key pair for the rare program that holds one itself.
//! - [`InvalidS3Settings`]: why a set of values was refused.
//!
//! # How it works
//!
//! The settings are plain data with public fields. [`S3Settings::new`] fills in the defaults, and a caller
//! overrides the fields it cares about. Nothing is checked until the settings are handed to
//! [`crate::S3ArtifactStore::open`]; that is the one place where a wrong value is reported, so a caller
//! cannot forget to validate.
//!
//! # Invariants
//!
//! - [`S3Settings`] holds no secret, so it is safe to print.
//! - [`S3Credentials`] never prints its secret: its `Debug` output replaces it.
//!
//! # See also
//!
//! - `s3.rs`: builds the store from these settings.
//! - `store.rs`: the store itself.

use std::fmt;
use std::time::Duration;

use bitacora_artifact::validate_key;
use thiserror::Error;

/// How long one attempt to open a connection may take by default.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one operation may take by default, retries included. Generous, because one operation can be the
/// upload of a whole log segment of a gigabyte; callers that expect a quick answer, such as a reader
/// fetching one block, put their own shorter deadline around the call.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

/// Which bucket to use, where the service is and how long to wait for it.
///
/// Build one with [`S3Settings::new`] and change the public fields that should differ from the defaults.
/// The values are checked when they are handed to [`crate::S3ArtifactStore::open`].
///
/// There are no credential fields. Credentials come from the environment of the process, see
/// [`crate::S3ArtifactStore::open`].
///
/// # Examples
///
/// A bucket on an S3-compatible service inside a data centre, a Ceph cluster for example:
///
/// ```
/// use bitacora_artifact_s3::S3Settings;
///
/// let mut settings = S3Settings::new("bitacora-artifacts", "us-east-1");
/// settings.endpoint = Some("https://s3.storage.example.org".to_owned());
/// settings.key_prefix = "clusters/production".to_owned();
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Settings {
    /// The bucket that holds the objects. It must exist: the store never creates one.
    pub bucket: String,

    /// The region requests are signed for, for example `eu-west-1`. S3-compatible services that have no
    /// regions accept any name; `us-east-1` is the customary one.
    pub region: String,

    /// The URL of the service, for example `https://s3.storage.example.org`, for anything that is not
    /// Amazon S3 itself: a Ceph cluster, a gateway. `None` means Amazon S3 in [`S3Settings::region`], unless
    /// the environment names an endpoint in `AWS_ENDPOINT_URL` or `AWS_ENDPOINT`.
    pub endpoint: Option<String>,

    /// A path inside the bucket that every object key is put under, and that is removed again from every
    /// key that comes back, for example `clusters/production`. Two stores with different prefixes share
    /// one bucket without seeing each other. It is a plain relative path without a leading or trailing
    /// `/`. Empty means the store owns the whole bucket.
    pub key_prefix: String,

    /// Whether requests name the bucket in the path (`https://service/bucket/key`) and not in the host
    /// name (`https://bucket.service/key`). Path style works with every S3-compatible service and needs
    /// no DNS entry per bucket, so it is the default. Set it to `false` for Amazon S3 buckets that only
    /// answer to their host name.
    pub force_path_style: bool,

    /// Whether an `http://` endpoint is acceptable. Off by default, because credentials and data then
    /// travel in the clear; turn it on only for a service on a trusted network or on the same machine.
    pub allow_http: bool,

    /// The longest one attempt to open a connection to the service may take. Must be longer than zero.
    pub connect_timeout: Duration,

    /// The longest one operation of the store may take, the retries of failed requests included, before
    /// it fails as unavailable. Must be longer than zero.
    pub request_timeout: Duration,
}

impl S3Settings {
    /// Returns settings for a bucket with every other value at its default: Amazon S3 itself, no key
    /// prefix, path-style addressing, HTTPS only, five seconds to connect and five minutes per operation.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket. It is stored as given; an unusable one is refused later, by
    ///   [`crate::S3ArtifactStore::open`].
    /// * `region` - The region requests are signed for.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_artifact_s3::S3Settings;
    ///
    /// let settings = S3Settings::new("bitacora-artifacts", "eu-west-1");
    ///
    /// assert_eq!(settings.endpoint, None);
    /// assert!(!settings.allow_http);
    /// ```
    #[must_use]
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            key_prefix: String::new(),
            force_path_style: true,
            allow_http: false,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Checks the values that this crate can judge without calling the service.
    ///
    /// Whether the bucket exists and the credentials are accepted only shows on the first request.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidS3Settings`] when the bucket or the region is missing, the bucket name contains
    /// a `/` or white space, the endpoint is not an `http://` or `https://` URL, the endpoint is `http://`
    /// and [`S3Settings::allow_http`] is off, the key prefix is not a plain relative path, or one of the
    /// timeouts is zero.
    pub(crate) fn validate(&self) -> Result<(), InvalidS3Settings> {
        if self.bucket.is_empty() {
            return Err(InvalidS3Settings::because("there is no bucket"));
        }
        if self.bucket.contains('/') || self.bucket.contains(char::is_whitespace) {
            return Err(InvalidS3Settings::because(format!(
                "bucket {:?} is not a bucket name: it contains a `/` or white space",
                self.bucket
            )));
        }
        if self.region.is_empty() {
            return Err(InvalidS3Settings::because("there is no region"));
        }
        if let Some(endpoint) = &self.endpoint {
            self.validate_endpoint(endpoint)?;
        }
        if !self.key_prefix.is_empty() && validate_key(&self.key_prefix).is_err() {
            return Err(InvalidS3Settings::because(format!(
                "key prefix {:?} is not a plain relative path without a leading or trailing `/`",
                self.key_prefix
            )));
        }
        if self.connect_timeout.is_zero() {
            return Err(InvalidS3Settings::because("the connect timeout is zero"));
        }
        if self.request_timeout.is_zero() {
            return Err(InvalidS3Settings::because("the request timeout is zero"));
        }
        Ok(())
    }

    /// Checks that `endpoint` is a URL this store may talk to.
    fn validate_endpoint(&self, endpoint: &str) -> Result<(), InvalidS3Settings> {
        let is_https = endpoint.starts_with("https://");
        let is_http = endpoint.starts_with("http://");
        if !is_https && !is_http {
            return Err(InvalidS3Settings::because(format!(
                "endpoint {endpoint:?} is not an http:// or https:// URL"
            )));
        }
        if is_http && !self.allow_http {
            return Err(InvalidS3Settings::because(format!(
                "endpoint {endpoint:?} is not encrypted; use https:// or allow http explicitly"
            )));
        }
        Ok(())
    }
}

/// An access key pair, for a program that holds its credentials itself.
///
/// Most programs never build one: [`crate::S3ArtifactStore::open`] takes the credentials from the
/// environment. This type is for tests and for a program that fetches its keys from a secret manager and
/// hands them to [`crate::S3ArtifactStore::open_with_credentials`].
///
/// # Examples
///
/// ```
/// use bitacora_artifact_s3::S3Credentials;
///
/// let credentials = S3Credentials::new("AKIAEXAMPLE", "a-secret");
///
/// assert!(!format!("{credentials:?}").contains("a-secret"));
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct S3Credentials {
    /// The public half of the key pair, which names the account.
    pub(crate) access_key_id: String,

    /// The secret half of the key pair, which signs requests. Never printed.
    pub(crate) secret_access_key: String,
}

impl S3Credentials {
    /// Builds a key pair.
    ///
    /// # Arguments
    ///
    /// * `access_key_id` - The public half of the key pair.
    /// * `secret_access_key` - The secret half of the key pair.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }
}

impl fmt::Debug for S3Credentials {
    /// Prints the access key identifier and hides the secret, so that a key pair that ends up in a log or
    /// a panic message gives nothing away.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<hidden>")
            .finish()
    }
}

/// The settings handed to [`crate::S3ArtifactStore::open`] cannot work.
///
/// This is a configuration mistake, not an outage: retrying with the same settings fails the same way, so a
/// process should report it and stop.
///
/// # Examples
///
/// ```
/// use bitacora_artifact_s3::{S3ArtifactStore, S3Settings};
///
/// let settings_without_a_bucket = S3Settings::new("", "us-east-1");
///
/// let refusal = S3ArtifactStore::open(&settings_without_a_bucket).expect_err("there is no bucket");
///
/// assert_eq!(refusal.to_string(), "s3 settings are invalid: there is no bucket");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("s3 settings are invalid: {reason}")]
pub struct InvalidS3Settings {
    /// Which value is wrong and why, for the operator who has to fix it.
    pub reason: String,
}

impl InvalidS3Settings {
    /// Builds the error from the reason the settings were refused.
    pub(crate) fn because(reason: impl Into<String>) -> Self {
        Self { reason: reason.into() }
    }
}

#[cfg(test)]
#[path = "settings_test.rs"]
mod tests;
