//! Tests of how the daemon reads its configuration at start and for `--check-config`.
//!
//! # What is covered
//!
//! - The example configuration of the repository passes, which is what `--check-config` reports for it.
//! - A file that is missing, that is not a configuration, or that names a setting that cannot work is
//!   refused with a message that names the file and what is wrong.
//!
//! # How the fixtures work
//!
//! `load_settings` is what both a start and a check call first. The tests give it the repository's
//! `example.toml` and small files written into a temporary directory.

// The tests read the example configuration from the repository and write files of their own.
#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;

use super::*;

/// Where the example configuration lies, seen from where Bazel and Cargo run the tests.
const EXAMPLE_LOCATIONS: [&str; 2] = ["src/rust/bita-replica/example.toml", "example.toml"];

/// The path of `example.toml`.
fn example_path() -> PathBuf {
    for location in EXAMPLE_LOCATIONS {
        let candidate = PathBuf::from(location);
        if candidate.exists() {
            return candidate;
        }
    }
    panic!("the example configuration is at none of {EXAMPLE_LOCATIONS:?}");
}

/// Writes `file_text` as a configuration file in a new temporary directory.
///
/// Returns the directory too: the file is gone once it is dropped.
fn config_file_with(file_text: &str) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().expect("a temporary directory can be created");
    let config_path = directory.path().join("replica.toml");
    std::fs::write(&config_path, file_text).expect("the configuration file is written");
    (directory, config_path)
}

#[test]
fn example_configuration_is_accepted() {
    let loaded = load_settings(&example_path());

    assert!(loaded.is_ok(), "{:?}", loaded.err());
}

#[test]
fn missing_file_is_refused_by_its_path() {
    let missing_path = PathBuf::from("/nonexistent/replica.toml");

    let problem = load_settings(&missing_path).expect_err("there is no such file");

    assert!(problem.contains("/nonexistent/replica.toml"), "{problem}");
}

#[test]
fn file_that_is_not_a_configuration_is_refused_by_its_path() {
    let (_directory, config_path) = config_file_with("this is not toml = = =");

    let problem = load_settings(&config_path).expect_err("the file is not TOML");

    assert!(problem.contains(&config_path.display().to_string()), "{problem}");
}

#[test]
fn misspelled_setting_is_refused_by_its_name() {
    let example = std::fs::read_to_string(example_path()).expect("the example is readable");
    let (_directory, config_path) = config_file_with(&format!("no_such_setting = 1\n{example}"));

    let problem = load_settings(&config_path).expect_err("the setting is unknown");

    assert!(problem.contains("no_such_setting"), "{problem}");
}

#[test]
fn setting_that_cannot_work_is_refused_by_its_name() {
    let example = std::fs::read_to_string(example_path()).expect("the example is readable");
    let broken = example.replace(
        "metrics_listen_address = \"0.0.0.0:9100\"",
        "metrics_listen_address = \"nowhere\"",
    );
    let (_directory, config_path) = config_file_with(&broken);

    let problem = load_settings(&config_path).expect_err("the address has no port");

    assert!(problem.contains("metrics_listen_address"), "{problem}");
}

#[test]
fn problem_with_a_setting_names_the_file_too() {
    let example = std::fs::read_to_string(example_path()).expect("the example is readable");
    let broken = example.replace(
        "metrics_listen_address = \"0.0.0.0:9100\"",
        "metrics_listen_address = \"nowhere\"",
    );
    let (_directory, config_path) = config_file_with(&broken);

    let problem = load_settings(&config_path).expect_err("the address has no port");

    assert!(problem.contains(&config_path.display().to_string()), "{problem}");
}
