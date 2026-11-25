//! Tests of the settings of the S3 store.
//!
//! # What is covered
//!
//! - The defaults are valid and are the safe ones: HTTPS only, path-style addressing.
//! - Each value that cannot work is refused with a reason that names it.
//! - Credentials never print their secret.
//!
//! # How the fixtures work
//!
//! `settings` returns valid settings; each test breaks them in one place and checks the verdict.

use super::*;

/// Returns settings that are valid, for a test to break in one place.
fn settings() -> S3Settings {
    S3Settings::new("bitacora-artifacts", "us-east-1")
}

/// The reason `settings` are refused for, or `None` if they are valid.
fn refusal_of(settings: &S3Settings) -> Option<String> {
    match settings.validate() {
        Ok(()) => None,
        Err(invalid) => Some(invalid.reason),
    }
}

#[test]
fn default_settings_are_valid() {
    assert_eq!(settings().validate(), Ok(()));
}

#[test]
fn default_settings_refuse_unencrypted_endpoints() {
    assert!(!settings().allow_http);
}

#[test]
fn default_settings_address_the_bucket_in_the_path() {
    assert!(settings().force_path_style);
}

#[test]
fn settings_without_a_bucket_are_refused() {
    let settings = S3Settings::new("", "us-east-1");

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidS3Settings::because("there is no bucket"))
    );
}

#[test]
fn bucket_name_with_a_path_separator_is_refused() {
    let settings = S3Settings::new("bucket/with-a-path", "us-east-1");

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(refusal.contains("bucket/with-a-path"), "{refusal}");
}

#[test]
fn bucket_name_with_white_space_is_refused() {
    let settings = S3Settings::new("my bucket", "us-east-1");

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(refusal.contains("my bucket"), "{refusal}");
}

#[test]
fn settings_without_a_region_are_refused() {
    let settings = S3Settings::new("bitacora-artifacts", "");

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidS3Settings::because("there is no region"))
    );
}

#[test]
fn https_endpoint_is_accepted() {
    let mut settings = settings();
    settings.endpoint = Some("https://s3.storage.example.org".to_owned());

    assert_eq!(settings.validate(), Ok(()));
}

#[test]
fn endpoint_that_is_not_a_url_is_refused() {
    let mut settings = settings();
    settings.endpoint = Some("s3.storage.example.org".to_owned());

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(
        refusal.contains("is not an http:// or https:// URL"),
        "{refusal}"
    );
}

#[test]
fn unencrypted_endpoint_is_refused_unless_allowed() {
    let mut settings = settings();
    settings.endpoint = Some("http://127.0.0.1:9000".to_owned());

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(refusal.contains("is not encrypted"), "{refusal}");
}

#[test]
fn unencrypted_endpoint_is_accepted_when_allowed() {
    let mut settings = settings();
    settings.endpoint = Some("http://127.0.0.1:9000".to_owned());
    settings.allow_http = true;

    assert_eq!(settings.validate(), Ok(()));
}

#[test]
fn key_prefix_of_several_path_components_is_accepted() {
    let mut settings = settings();
    settings.key_prefix = "clusters/production".to_owned();

    assert_eq!(settings.validate(), Ok(()));
}

#[test]
fn key_prefix_with_a_trailing_separator_is_refused() {
    let mut settings = settings();
    settings.key_prefix = "clusters/production/".to_owned();

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(refusal.contains("clusters/production/"), "{refusal}");
}

#[test]
fn key_prefix_that_leaves_the_bucket_is_refused() {
    let mut settings = settings();
    settings.key_prefix = "../elsewhere".to_owned();

    let refusal = refusal_of(&settings).expect("the settings are refused");

    assert!(refusal.contains("../elsewhere"), "{refusal}");
}

#[test]
fn zero_connect_timeout_is_refused() {
    let mut settings = settings();
    settings.connect_timeout = Duration::ZERO;

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidS3Settings::because("the connect timeout is zero"))
    );
}

#[test]
fn zero_request_timeout_is_refused() {
    let mut settings = settings();
    settings.request_timeout = Duration::ZERO;

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidS3Settings::because("the request timeout is zero"))
    );
}

#[test]
fn credentials_print_their_access_key_identifier() {
    let credentials = S3Credentials::new("AKIAEXAMPLE", "a-secret");

    let printed = format!("{credentials:?}");

    assert!(printed.contains("AKIAEXAMPLE"), "{printed}");
}

#[test]
fn credentials_never_print_their_secret() {
    let credentials = S3Credentials::new("AKIAEXAMPLE", "a-secret");

    let printed = format!("{credentials:?}");

    assert!(!printed.contains("a-secret"), "{printed}");
}
