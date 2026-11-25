//! Tests of the controller daemon's configuration.
//!
//! # What is covered
//!
//! - The example configuration of the repository is valid, and spells out the defaults.
//! - A minimal file gets the controller's own defaults.
//! - Unknown settings are refused when the file is read.
//! - Every check of `validate` refuses what it should, naming the setting.
//!
//! # How the fixtures work
//!
//! `MINIMAL_CONFIG` is the least an operator has to write. Each validation test changes one setting of
//! it through `minimal_config` and expects that setting to be named in the error.

// The test reads the example configuration from the repository.
#![allow(clippy::disallowed_methods)]

use super::*;

/// The least an operator has to write.
const MINIMAL_CONFIG: &str = r#"
identity = "controller-a"
metrics_listen_address = "0.0.0.0:9101"

[cluster]
etcd_endpoints = ["http://127.0.0.1:2379"]
"#;

/// Where the example configuration lies, seen from where Bazel and Cargo run the tests.
const EXAMPLE_LOCATIONS: [&str; 2] = ["src/rust/bita-controller/example.toml", "example.toml"];

/// Reads `example.toml`.
fn read_example() -> String {
    for location in EXAMPLE_LOCATIONS {
        if let Ok(example) = std::fs::read_to_string(location) {
            return example;
        }
    }
    panic!("the example configuration is at none of {EXAMPLE_LOCATIONS:?}");
}

/// Parses a configuration file from its text.
fn parse(file_text: &str) -> Result<ControllerConfigFile, toml::de::Error> {
    toml::from_str(file_text)
}

/// The minimal configuration, parsed, for a test to change one setting of.
fn minimal_config() -> ControllerConfigFile {
    parse(MINIMAL_CONFIG).expect("the minimal configuration is well-formed")
}

/// The setting that `config` is refused for.
fn refused_setting(config: ControllerConfigFile) -> Option<&'static str> {
    match config.validate() {
        Ok(_settings) => None,
        Err(invalid) => Some(invalid.setting),
    }
}

/// The controller's default settings under the identity of the minimal configuration.
fn defaults_of_controller_a() -> ControllerSettings {
    ControllerSettings {
        identity: "controller-a".to_owned(),
        ..ControllerSettings::default()
    }
}

#[test]
fn example_configuration_is_valid() {
    let example = parse(&read_example()).expect("the example is well-formed");

    let validated = example.validate();

    assert!(validated.is_ok(), "{validated:?}");
}

#[test]
fn example_configuration_spells_out_the_controllers_defaults() {
    let example = parse(&read_example()).expect("the example is well-formed");

    let settings = example.validate().expect("valid");

    assert_eq!(settings.controller, defaults_of_controller_a());
}

#[test]
fn minimal_configuration_gets_the_controllers_defaults() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.controller, defaults_of_controller_a());
}

#[test]
fn identity_of_the_file_becomes_the_identity_of_the_controller() {
    let mut config = minimal_config();
    config.identity = "controller-b".to_owned();

    let settings = config.validate().expect("valid");

    assert_eq!(settings.controller.identity, "controller-b");
}

#[test]
fn pass_interval_is_read_in_milliseconds() {
    let mut config = minimal_config();
    config.pass_interval_milliseconds = 250;

    let settings = config.validate().expect("valid");

    assert_eq!(
        settings.controller.pass_interval,
        Duration::from_millis(250)
    );
}

#[test]
fn tiering_settings_are_read_in_seconds() {
    let mut config = minimal_config();
    config.tiering_delay_seconds = 60;
    config.hot_read_window_seconds = 120;

    let settings = config.validate().expect("valid");

    assert_eq!(settings.controller.tiering_delay, Duration::from_mins(1));
    assert_eq!(settings.controller.hot_read_window, Duration::from_mins(2));
}

#[test]
fn placement_table_becomes_the_placement_policy() {
    let file_text = format!(
        "{MINIMAL_CONFIG}\n[placement]\nliveness_window_seconds = 30\nrefuse_at_used_percent = 70\n"
    );

    let settings = parse(&file_text)
        .expect("well-formed")
        .validate()
        .expect("valid");

    assert_eq!(
        settings.controller.placement,
        PlacementPolicy {
            liveness_window: Duration::from_secs(30),
            refuse_at_used_percent: 70,
        }
    );
}

#[test]
fn tiering_delay_of_zero_is_accepted() {
    let mut config = minimal_config();
    config.tiering_delay_seconds = 0;

    assert_eq!(refused_setting(config), None);
}

#[test]
fn unknown_setting_is_refused_when_the_file_is_read() {
    let file_text = format!("pass_interval = 5\n{MINIMAL_CONFIG}");

    let parsed = parse(&file_text);

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn unknown_setting_of_the_placement_table_is_refused_when_the_file_is_read() {
    let file_text = format!("{MINIMAL_CONFIG}\n[placement]\nliveness_window = 30\n");

    let parsed = parse(&file_text);

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn file_without_an_identity_is_refused_when_it_is_read() {
    let parsed = parse(
        "metrics_listen_address = \"0.0.0.0:9101\"\n\n[cluster]\netcd_endpoints = [\"http://127.0.0.1:2379\"]\n",
    );

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn empty_identity_is_refused() {
    let mut config = minimal_config();
    config.identity = String::new();

    assert_eq!(refused_setting(config), Some("identity"));
}

#[test]
fn identity_with_whitespace_is_refused() {
    let mut config = minimal_config();
    config.identity = "controller a".to_owned();

    assert_eq!(refused_setting(config), Some("identity"));
}

#[test]
fn metrics_address_with_a_host_name_is_refused() {
    let mut config = minimal_config();
    config.metrics_listen_address = "localhost:9101".to_owned();

    assert_eq!(refused_setting(config), Some("metrics_listen_address"));
}

#[test]
fn pass_interval_of_zero_is_refused() {
    let mut config = minimal_config();
    config.pass_interval_milliseconds = 0;

    assert_eq!(refused_setting(config), Some("pass_interval_milliseconds"));
}

#[test]
fn leadership_shorter_than_two_pass_intervals_is_refused() {
    let mut config = minimal_config();
    config.pass_interval_milliseconds = 6_000;
    config.leadership_duration_seconds = 10;

    assert_eq!(refused_setting(config), Some("leadership_duration_seconds"));
}

#[test]
fn leadership_of_exactly_two_pass_intervals_is_accepted() {
    let mut config = minimal_config();
    config.pass_interval_milliseconds = 5_000;
    config.leadership_duration_seconds = 10;

    assert_eq!(refused_setting(config), None);
}

#[test]
fn liveness_window_of_zero_is_refused() {
    let mut config = minimal_config();
    config.placement.liveness_window_seconds = 0;

    assert_eq!(
        refused_setting(config),
        Some("placement.liveness_window_seconds")
    );
}

#[test]
fn placement_threshold_above_the_whole_disk_is_refused() {
    let mut config = minimal_config();
    config.placement.refuse_at_used_percent = 101;

    assert_eq!(
        refused_setting(config),
        Some("placement.refuse_at_used_percent")
    );
}

#[test]
fn placement_threshold_of_zero_is_refused() {
    let mut config = minimal_config();
    config.placement.refuse_at_used_percent = 0;

    assert_eq!(
        refused_setting(config),
        Some("placement.refuse_at_used_percent")
    );
}

/// A replica that is given up as soon as it counts as dead would be re-replicated on every restart.
#[test]
fn replica_given_up_no_later_than_it_counts_as_dead_is_refused() {
    let mut config = minimal_config();
    config.placement.liveness_window_seconds = 15;
    config.replica_lost_after_seconds = 15;

    assert_eq!(refused_setting(config), Some("replica_lost_after_seconds"));
}

#[test]
fn error_names_the_setting_and_the_reason() {
    let mut config = minimal_config();
    config.pass_interval_milliseconds = 0;

    let message = config
        .validate()
        .expect_err("the interval is zero")
        .to_string();

    assert_eq!(
        message,
        "setting `pass_interval_milliseconds` is invalid: it is 0; a controller that never rests \
         would flood the metadata store"
    );
}
