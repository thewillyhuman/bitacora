//! Opening the artifact store on an S3 service.
//!
//! # Why this file exists
//!
//! `store.rs` translates between two interfaces and does not care which service is behind the client. This
//! file is where the service is chosen: it turns [`S3Settings`] into a configured S3 client, with the
//! timeouts, the retries and the source of credentials that bitacora wants, and wraps it in the store.
//!
//! # What it contains
//!
//! - [`S3ArtifactStore::open`]: opens the store with credentials from the environment.
//! - [`S3ArtifactStore::open_with_credentials`]: opens it with a key pair the caller holds.
//!
//! # How it works
//!
//! Both functions validate the settings and build the client without sending anything. A wrong bucket,
//! wrong credentials or an unreachable service therefore show on the first request, as `Unavailable` with
//! the reason; a process that wants to fail at start makes one request, for example a listing.
//!
//! **Credentials** are never part of the settings. [`S3ArtifactStore::open`] takes them from the
//! environment the way every AWS tool does: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (with
//! `AWS_SESSION_TOKEN` if there is one), or a web identity token (`AWS_WEB_IDENTITY_TOKEN_FILE` and
//! `AWS_ROLE_ARN`, which is what a Kubernetes service account provides), or the container and instance
//! metadata services. Everything the settings name (bucket, region, endpoint, addressing style, whether
//! `http://` is allowed) overrides what the environment says about it.
//!
//! **Timeouts.** Each HTTP request may take the request timeout, and opening a connection the connect
//! timeout. On top of that the store bounds every operation as a whole with the request timeout.
//!
//! **Retries.** A request that fails with a server error or a broken connection is tried again at most
//! [`MAX_RETRIES`] times, with a short growing pause, and never after the request timeout has passed since
//! the first attempt. That absorbs the hiccups every object store has without hiding an outage: the
//! callers above (the controller's passes, a reader's retry loop) decide what to do about a real one.
//!
//! **Deleting** uses one plain `DELETE` request per object and not S3's bulk deletion, which some
//! S3-compatible services lack. The store only ever deletes one object at a time.
//!
//! **TLS** is rustls with the trusted roots of the platform. Its cryptography, and the signing of
//! requests, come from ring. rustls wants the provider installed once per process, so opening a store
//! installs ring unless the program has installed a provider already.
//!
//! # Invariants
//!
//! - Opening a store sends nothing.
//! - A setting always wins over an environment variable about the same thing.
//!
//! # See also
//!
//! - `settings.rs`: the settings and what each one means.
//! - `store.rs`: what happens on each operation.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientOptions, RetryConfig};

use crate::settings::{InvalidS3Settings, S3Credentials, S3Settings};
use crate::store::S3ArtifactStore;

/// How many times a failed request is tried again before the failure is reported. Two, so that a single
/// dropped connection or a `503` during a deployment of the service is not an incident.
const MAX_RETRIES: usize = 2;

impl S3ArtifactStore {
    /// Opens the store on the bucket the settings name, with credentials from the environment.
    ///
    /// Validates the settings and builds the client. Sends nothing: see the file header for what that
    /// means and for where the credentials come from.
    ///
    /// # Arguments
    ///
    /// * `settings` - Which bucket, where the service is and how long to wait for it.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidS3Settings`] when a value cannot work (see the fields of [`S3Settings`]) or
    /// the client refuses the combination. This is a configuration mistake: report it and stop.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use bitacora_artifact::ArtifactStore;
    /// use bitacora_artifact_s3::{S3ArtifactStore, S3Settings};
    ///
    /// let mut settings = S3Settings::new("bitacora-artifacts", "us-east-1");
    /// settings.endpoint = Some("https://s3.storage.example.org".to_owned());
    /// let store = S3ArtifactStore::open(&settings)?;
    ///
    /// // Fail at start, not on the first upload, if the bucket or the credentials are wrong.
    /// store.list_prefix("").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(settings: &S3Settings) -> Result<Self, InvalidS3Settings> {
        open_with_client_builder(settings, AmazonS3Builder::from_env())
    }

    /// Opens the store on the bucket the settings name, with a key pair the caller holds.
    ///
    /// Like [`S3ArtifactStore::open`], except that the environment is not consulted at all. For tests,
    /// and for a program that fetches its keys from a secret manager.
    ///
    /// # Arguments
    ///
    /// * `settings` - Which bucket, where the service is and how long to wait for it.
    /// * `credentials` - The key pair every request is signed with.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidS3Settings`] like [`S3ArtifactStore::open`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_artifact_s3::{S3ArtifactStore, S3Credentials, S3Settings};
    ///
    /// let mut settings = S3Settings::new("bitacora-artifacts", "us-east-1");
    /// settings.endpoint = Some("http://127.0.0.1:9000".to_owned());
    /// settings.allow_http = true;
    /// let credentials = S3Credentials::new("local-access-key", "local-secret-key");
    ///
    /// let store = S3ArtifactStore::open_with_credentials(&settings, &credentials);
    ///
    /// assert!(store.is_ok());
    /// ```
    pub fn open_with_credentials(
        settings: &S3Settings,
        credentials: &S3Credentials,
    ) -> Result<Self, InvalidS3Settings> {
        let client_builder = AmazonS3Builder::new()
            .with_access_key_id(&credentials.access_key_id)
            .with_secret_access_key(&credentials.secret_access_key);
        open_with_client_builder(settings, client_builder)
    }
}

/// Finishes a client that already knows where its credentials come from, and wraps it in the store.
///
/// Every value the settings name is set last, so that it wins over whatever `client_builder` picked up
/// from the environment.
fn open_with_client_builder(
    settings: &S3Settings,
    client_builder: AmazonS3Builder,
) -> Result<S3ArtifactStore, InvalidS3Settings> {
    settings.validate()?;
    install_tls_provider();

    let http_client = ClientOptions::new()
        .with_allow_http(settings.allow_http)
        .with_connect_timeout(settings.connect_timeout)
        .with_timeout(settings.request_timeout);
    let retries = RetryConfig {
        max_retries: MAX_RETRIES,
        retry_timeout: settings.request_timeout,
        ..RetryConfig::default()
    };
    let mut client_builder = client_builder
        .with_bucket_name(&settings.bucket)
        .with_region(&settings.region)
        .with_allow_http(settings.allow_http)
        .with_virtual_hosted_style_request(!settings.force_path_style)
        .with_disable_bulk_delete(true)
        .with_client_options(http_client)
        .with_retry(retries);
    if let Some(endpoint) = &settings.endpoint {
        // The environment can name an endpoint in two ways, and the S3-specific one wins inside the
        // client. Set both, so that the setting wins over either.
        client_builder = client_builder
            .with_endpoint(endpoint)
            .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
    }

    match client_builder.build() {
        Ok(client) => Ok(S3ArtifactStore::over(
            Arc::new(client),
            &settings.key_prefix,
            settings.request_timeout,
        )),
        Err(refusal) => Err(InvalidS3Settings::because(refusal.to_string())),
    }
}

/// Makes ring the provider of cryptography for TLS in this process, unless one is installed already.
///
/// The HTTP client is built without a provider of its own (see the workspace's `Cargo.toml` for why) and
/// refuses to work until the process has one.
fn install_tls_provider() {
    // The only failure is that a provider is installed already: by an earlier call, or by the program
    // this crate is linked into. Either way there is one, which is all that is needed.
    let _already_installed = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
#[path = "s3_test.rs"]
mod tests;
