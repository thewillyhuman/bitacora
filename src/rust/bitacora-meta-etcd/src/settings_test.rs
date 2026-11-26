//! Tests of the etcd settings and their validation.
//!
//! # What is covered
//!
//! - The defaults [`EtcdSettings::new`] fills in.
//! - Validation: settings with the defaults pass, and each value that cannot work is refused with a reason that names
//!   it.
//! - The message an operator reads when settings are refused.
//!
//! # How the fixtures work
//!
//! `valid_settings` returns settings that pass validation, for a test to break in one place.

use super::*;

/// Returns settings that are valid, for a test to break in one place.
fn valid_settings() -> EtcdSettings {
    EtcdSettings::new(vec!["http://127.0.0.1:2379".to_owned()])
}

#[test]
fn new_settings_keep_the_endpoints_they_were_given() {
    let endpoints = vec!["http://10.0.0.1:2379".to_owned(), "http://10.0.0.2:2379".to_owned()];

    let settings = EtcdSettings::new(endpoints.clone());

    assert_eq!(settings.endpoints, endpoints);
}

#[test]
fn new_settings_use_the_default_key_prefix() {
    let settings = valid_settings();

    assert_eq!(settings.key_prefix, "/bitacora/");
}

#[test]
fn new_settings_use_the_default_timeouts() {
    let settings = valid_settings();

    assert_eq!(settings.connect_timeout, Duration::from_secs(2));
    assert_eq!(settings.request_timeout, Duration::from_secs(5));
}

#[test]
fn default_settings_are_valid() {
    let settings = valid_settings();

    let verdict = settings.validate();

    assert_eq!(verdict, Ok(()));
}

#[test]
fn settings_without_endpoints_are_refused() {
    let settings = EtcdSettings::new(Vec::new());

    let verdict = settings.validate();

    assert_eq!(verdict, Err(InvalidEtcdSettings::because("there is no endpoint")));
}

#[test]
fn zero_connect_timeout_is_refused() {
    let mut settings = valid_settings();
    settings.connect_timeout = Duration::ZERO;

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidEtcdSettings::because("the connect timeout is zero"))
    );
}

#[test]
fn zero_request_timeout_is_refused() {
    let mut settings = valid_settings();
    settings.request_timeout = Duration::ZERO;

    let verdict = settings.validate();

    assert_eq!(
        verdict,
        Err(InvalidEtcdSettings::because("the request timeout is zero"))
    );
}

/// An empty prefix is a choice, not a mistake: the store then owns the whole key space of its etcd.
#[test]
fn empty_key_prefix_is_valid() {
    let mut settings = valid_settings();
    settings.key_prefix = String::new();

    let verdict = settings.validate();

    assert_eq!(verdict, Ok(()));
}

#[test]
fn refusal_says_which_setting_is_wrong() {
    let refusal = InvalidEtcdSettings::because("there is no endpoint");

    let message = refusal.to_string();

    assert_eq!(message, "etcd settings are invalid: there is no endpoint");
}
