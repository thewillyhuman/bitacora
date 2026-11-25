//! Tests of opening the store on an S3 service, without a network.
//!
//! # What is covered
//!
//! - A store opens for every kind of endpoint the settings allow, HTTPS included, which needs the TLS
//!   provider to be in place.
//! - Settings that cannot work are refused before anything is built.
//! - The opened store carries the request timeout of its settings.
//!
//! # How the fixtures work
//!
//! Opening a store sends nothing, so these tests need no service. `settings` returns valid settings for an
//! HTTPS endpoint that does not exist. What a store does once it talks to a real server is tested in
//! `tests/s3/`.

use std::time::Duration;

use super::*;

/// Valid settings for a service that nothing is listening at.
fn settings() -> S3Settings {
    let mut settings = S3Settings::new("bitacora-artifacts", "us-east-1");
    settings.endpoint = Some("https://s3.storage.example.org".to_owned());
    settings
}

/// A key pair that no service knows.
fn credentials() -> S3Credentials {
    S3Credentials::new("an-access-key", "a-secret-key")
}

#[test]
fn store_opens_for_an_encrypted_endpoint() {
    let opened = S3ArtifactStore::open_with_credentials(&settings(), &credentials());

    assert!(opened.is_ok(), "{opened:?}");
}

#[test]
fn store_opens_for_amazon_s3_itself() {
    let amazon = S3Settings::new("bitacora-artifacts", "eu-west-1");

    let opened = S3ArtifactStore::open_with_credentials(&amazon, &credentials());

    assert!(opened.is_ok(), "{opened:?}");
}

#[test]
fn store_opens_with_the_bucket_in_the_host_name() {
    let mut virtual_hosted = S3Settings::new("bitacora-artifacts", "eu-west-1");
    virtual_hosted.force_path_style = false;

    let opened = S3ArtifactStore::open_with_credentials(&virtual_hosted, &credentials());

    assert!(opened.is_ok(), "{opened:?}");
}

#[test]
fn store_opens_for_an_unencrypted_endpoint_when_allowed() {
    let mut local = settings();
    local.endpoint = Some("http://127.0.0.1:9000".to_owned());
    local.allow_http = true;

    let opened = S3ArtifactStore::open_with_credentials(&local, &credentials());

    assert!(opened.is_ok(), "{opened:?}");
}

#[test]
fn store_opens_with_credentials_from_the_environment() {
    let opened = S3ArtifactStore::open(&settings());

    assert!(opened.is_ok(), "{opened:?}");
}

#[test]
fn opened_store_carries_the_request_timeout_of_its_settings() {
    let mut patient = settings();
    patient.request_timeout = Duration::from_secs(1234);

    let store = S3ArtifactStore::open_with_credentials(&patient, &credentials()).expect("the settings are valid");

    assert_eq!(store.request_timeout(), Duration::from_secs(1234));
}

#[test]
fn invalid_settings_are_refused() {
    let mut unencrypted = settings();
    unencrypted.endpoint = Some("http://127.0.0.1:9000".to_owned());

    let refused = S3ArtifactStore::open_with_credentials(&unencrypted, &credentials());

    assert!(refused.is_err(), "{refused:?}");
}

#[test]
fn invalid_settings_are_refused_with_credentials_from_the_environment_too() {
    let mut without_bucket = settings();
    without_bucket.bucket = String::new();

    let refused = S3ArtifactStore::open(&without_bucket);

    assert_eq!(refused.err(), Some(InvalidS3Settings::because("there is no bucket")));
}

#[test]
fn opening_twice_keeps_the_tls_provider_of_the_first_time() {
    let first = S3ArtifactStore::open_with_credentials(&settings(), &credentials());

    let second = S3ArtifactStore::open_with_credentials(&settings(), &credentials());

    assert!(first.is_ok(), "{first:?}");
    assert!(second.is_ok(), "{second:?}");
}
