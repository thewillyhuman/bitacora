//! Tests of reading a configuration file.
//!
//! # What is covered
//!
//! - A valid file is loaded, with defaults for what it leaves out.
//! - A missing file, a file that is not TOML, an unknown field, a missing field and a value of the wrong
//!   type are each refused with an error that names the file and what is wrong.
//! - A setting can take its value from the environment, and a variable that is not set is refused with
//!   the file, the line and the variable.
//!
//! # How the fixtures work
//!
//! `TestConfig` stands for a daemon's configuration: one required field, one optional field, one nested
//! table, and `deny_unknown_fields` everywhere, as the real ones have. `write_config` puts a file with
//! the given text in a temporary directory.

// The tests write the files they then load.
#![allow(clippy::disallowed_methods)]

use serde::Deserialize;
use tempfile::TempDir;

use super::*;

/// A configuration with the shapes the real ones have.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    /// A field every file must set.
    listen_address: String,
    /// A field with a default.
    #[serde(default = "default_scrub_interval_seconds")]
    scrub_interval_seconds: u64,
    /// A nested table with a default.
    #[serde(default)]
    log: TestLogConfig,
}

/// A nested table of [`TestConfig`].
#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestLogConfig {
    /// A field of the nested table.
    #[serde(default)]
    level: String,
}

/// The default of [`TestConfig::scrub_interval_seconds`].
fn default_scrub_interval_seconds() -> u64 {
    3_600
}

/// Writes `file_text` to a file named `daemon.toml` in a new temporary directory.
///
/// # Returns
///
/// The directory, which must be kept alive while the file is used, and the path of the file.
fn write_config(file_text: &str) -> (TempDir, PathBuf) {
    let directory = TempDir::new().expect("a temporary directory can be created");
    let path = directory.path().join("daemon.toml");
    std::fs::write(&path, file_text).expect("the file can be written");
    (directory, path)
}

/// Loads `file_text` as a [`TestConfig`] and returns the message of the error it must cause.
fn error_message_for(file_text: &str) -> String {
    let (_directory, path) = write_config(file_text);
    let loaded: Result<TestConfig, ConfigFileError> = load_config_file(&path);
    match loaded {
        Ok(config) => panic!("the file was accepted as {config:?}"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn valid_file_is_loaded() {
    let (_directory, path) =
        write_config("listen_address = \"0.0.0.0:7000\"\nscrub_interval_seconds = 60\n\n[log]\nlevel = \"debug\"\n");

    let loaded: Result<TestConfig, ConfigFileError> = load_config_file(&path);

    assert_eq!(
        loaded,
        Ok(TestConfig {
            listen_address: "0.0.0.0:7000".to_owned(),
            scrub_interval_seconds: 60,
            log: TestLogConfig {
                level: "debug".to_owned()
            },
        })
    );
}

#[test]
fn field_left_out_of_the_file_gets_its_default() {
    let (_directory, path) = write_config("listen_address = \"0.0.0.0:7000\"\n");

    let loaded: TestConfig = load_config_file(&path).expect("the file is valid");

    assert_eq!(loaded.scrub_interval_seconds, 3_600);
}

#[test]
fn missing_file_is_reported_as_unreadable() {
    let directory = TempDir::new().expect("a temporary directory can be created");
    let missing_path = directory.path().join("no-such-file.toml");

    let loaded: Result<TestConfig, ConfigFileError> = load_config_file(&missing_path);

    assert!(
        matches!(&loaded, Err(ConfigFileError::Unreadable { path, .. }) if path == &missing_path),
        "{loaded:?}"
    );
}

#[test]
fn error_for_a_missing_file_names_the_file() {
    let directory = TempDir::new().expect("a temporary directory can be created");
    let missing_path = directory.path().join("no-such-file.toml");

    let loaded: Result<TestConfig, ConfigFileError> = load_config_file(&missing_path);

    let message = loaded.expect_err("the file does not exist").to_string();
    assert!(message.contains("no-such-file.toml"), "{message}");
}

#[test]
fn file_that_is_not_toml_is_reported_as_invalid() {
    let (_directory, path) = write_config("listen_address = = \"0.0.0.0:7000\"\n");

    let loaded: Result<TestConfig, ConfigFileError> = load_config_file(&path);

    assert!(matches!(loaded, Err(ConfigFileError::Invalid { .. })), "{loaded:?}");
}

#[test]
fn error_for_an_invalid_file_names_the_file() {
    let message = error_message_for("listen_address = = \"0.0.0.0:7000\"\n");

    assert!(message.contains("daemon.toml"), "{message}");
}

#[test]
fn unknown_field_is_refused_by_name() {
    let message = error_message_for("listen_address = \"0.0.0.0:7000\"\nlisten_adress = \"x\"\n");

    assert!(message.contains("unknown field `listen_adress`"), "{message}");
}

#[test]
fn unknown_field_in_a_nested_table_is_refused_by_name() {
    let message = error_message_for("listen_address = \"0.0.0.0:7000\"\n\n[log]\nlevl = \"debug\"\n");

    assert!(message.contains("unknown field `levl`"), "{message}");
}

#[test]
fn missing_required_field_is_named() {
    let message = error_message_for("scrub_interval_seconds = 60\n");

    assert!(message.contains("missing field `listen_address`"), "{message}");
}

#[test]
fn value_of_the_wrong_type_is_reported_with_its_field() {
    let message = error_message_for("listen_address = \"0.0.0.0:7000\"\nscrub_interval_seconds = \"often\"\n");

    assert!(message.contains("scrub_interval_seconds"), "{message}");
}

#[test]
fn value_of_the_wrong_type_is_reported_with_what_was_expected() {
    let message = error_message_for("listen_address = \"0.0.0.0:7000\"\nscrub_interval_seconds = \"often\"\n");

    assert!(message.contains("invalid type: string"), "{message}");
}

/// The environment the tests of references load with: `POD_NAME` is `replica-0`, nothing else is set.
fn environment_with_a_pod_name() -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();
    variables.insert("POD_NAME".to_owned(), "replica-0".to_owned());
    variables
}

#[test]
fn setting_takes_its_value_from_the_environment() {
    let (_directory, path) = write_config("listen_address = \"${POD_NAME}.replicas:7000\"\n");

    let config: TestConfig = load_config_file_with(&path, &environment_with_a_pod_name()).expect("valid file");

    assert_eq!(config.listen_address, "replica-0.replicas:7000");
}

#[test]
fn reference_to_an_unset_variable_is_refused_by_name() {
    let (_directory, path) = write_config("listen_address = \"${NODE_NAME}:7000\"\n");

    let refused: Result<TestConfig, ConfigFileError> = load_config_file_with(&path, &environment_with_a_pod_name());

    let message = refused.expect_err("the variable is not set").to_string();
    assert!(message.contains("`NODE_NAME` is not set"), "{message}");
}

#[test]
fn reference_to_an_unset_variable_is_refused_with_the_file() {
    let (_directory, path) = write_config("listen_address = \"${NODE_NAME}:7000\"\n");

    let refused: Result<TestConfig, ConfigFileError> = load_config_file_with(&path, &environment_with_a_pod_name());

    let message = refused.expect_err("the variable is not set").to_string();
    assert!(message.contains(&path.display().to_string()), "{message}");
}

/// `PATH` is set wherever tests run, under Bazel and under Cargo.
#[test]
fn references_are_resolved_from_the_environment_of_the_process() {
    let (_directory, path) = write_config("listen_address = \"${PATH}\"\n");

    let config: TestConfig = load_config_file(&path).expect("PATH is set");

    assert_eq!(Ok(config.listen_address), std::env::var("PATH"));
}
