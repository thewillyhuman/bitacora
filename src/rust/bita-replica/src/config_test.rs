//! Tests of the replica daemon's configuration.
//!
//! # What is covered
//!
//! - The example configuration of the repository is valid, and says what it seems to say.
//! - A minimal file gets the documented defaults.
//! - Unknown settings are refused when the file is read.
//! - Every check of `validate` refuses what it should, naming the setting.
//!
//! # How the fixtures work
//!
//! `MINIMAL_CONFIG` is the least an operator has to write. Each validation test changes one setting of
//! it through `minimal_config` and expects that setting to be named in the error.

// The test reads the example configuration from the repository.
#![allow(clippy::disallowed_methods)]

use bitacora::ObjectStoreConfig;
use bitacora_process::{LogFormat, LogLevel};

use super::*;

/// The least an operator has to write.
const MINIMAL_CONFIG: &str = r#"
replica_id = "replica-a"
data_directory = "/var/lib/bitacora/replica"
listen_address = "0.0.0.0:7000"
advertised_address = "replica-a.example.org:7000"
metrics_listen_address = "0.0.0.0:9100"

[cluster]
etcd_endpoints = ["http://127.0.0.1:2379"]
"#;

/// Where the example configuration lies, seen from where Bazel and Cargo run the tests.
const EXAMPLE_LOCATIONS: [&str; 2] = ["src/rust/bita-replica/example.toml", "example.toml"];

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
fn parse(file_text: &str) -> Result<ReplicaConfigFile, toml::de::Error> {
    toml::from_str(file_text)
}

/// The minimal configuration, parsed, for a test to change one setting of.
fn minimal_config() -> ReplicaConfigFile {
    parse(MINIMAL_CONFIG).expect("the minimal configuration is well-formed")
}

/// The setting that `config` is refused for.
fn refused_setting(config: ReplicaConfigFile) -> Option<&'static str> {
    match config.validate() {
        Ok(_settings) => None,
        Err(invalid) => Some(invalid.setting),
    }
}

#[test]
fn example_configuration_is_valid() {
    let example = parse(&read_example()).expect("the example is well-formed");

    let validated = example.validate();

    assert!(validated.is_ok(), "{validated:?}");
}

#[test]
fn example_configuration_names_three_failure_domains() {
    let example = parse(&read_example()).expect("the example is well-formed");

    assert_eq!(example.failure_domains, ["zone-a", "rack-12", "replica-a.example.org"]);
}

#[test]
fn example_configuration_tiers_to_a_local_directory() {
    let example = parse(&read_example()).expect("the example is well-formed");

    assert_eq!(
        example.cluster.object_store,
        ObjectStoreConfig::LocalDirectory {
            path: PathBuf::from("/var/lib/bitacora/objects")
        }
    );
}

#[test]
fn example_configuration_spells_out_the_defaults_of_the_storage_table() {
    let example = parse(&read_example()).expect("the example is well-formed");

    assert_eq!(example.storage, StorageConfig::default());
}

#[test]
fn minimal_configuration_is_valid() {
    let validated = minimal_config().validate();

    assert!(validated.is_ok(), "{validated:?}");
}

#[test]
fn registration_is_renewed_every_five_seconds_by_default() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.registration_interval, Duration::from_secs(5));
}

#[test]
fn sealed_segments_are_scrubbed_weekly_by_default() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.scrub_interval, Duration::from_hours(7 * 24));
}

#[test]
fn storage_settings_default_to_the_replicas_own() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.replica, ReplicaSettings::default());
}

#[test]
fn logs_default_to_json_at_info() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(
        settings.log,
        LogSettings {
            level: LogLevel::Info,
            format: LogFormat::Json,
        }
    );
}

#[test]
fn replica_has_no_failure_domains_by_default() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.failure_domains, Vec::<String>::new());
}

#[test]
fn storage_table_overrides_the_replicas_defaults() {
    let file_text = format!("{MINIMAL_CONFIG}\n[storage]\nrepair_bytes_per_second = 1024\n");

    let settings = parse(&file_text).expect("well-formed").validate().expect("valid");

    assert_eq!(settings.replica.repair_bytes_per_second, 1024);
}

#[test]
fn listen_addresses_are_parsed() {
    let settings = minimal_config().validate().expect("valid");

    assert_eq!(settings.listen_address.port(), 7000);
}

#[test]
fn unknown_setting_is_refused_when_the_file_is_read() {
    let file_text = format!("scrub_interval = 60\n{MINIMAL_CONFIG}");

    let parsed = parse(&file_text);

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn unknown_setting_of_the_storage_table_is_refused_when_the_file_is_read() {
    let file_text = format!("{MINIMAL_CONFIG}\n[storage]\nrepair_megabytes_per_second = 64\n");

    let parsed = parse(&file_text);

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn file_without_a_cluster_table_is_refused_when_it_is_read() {
    let parsed = parse(
        "replica_id = \"replica-a\"\ndata_directory = \"/data\"\nlisten_address = \"0.0.0.0:7000\"\n\
         advertised_address = \"replica-a:7000\"\nmetrics_listen_address = \"0.0.0.0:9100\"\n",
    );

    assert!(parsed.is_err(), "{parsed:?}");
}

#[test]
fn replica_identifier_with_a_forbidden_character_is_refused() {
    let mut config = minimal_config();
    config.replica_id = "replica a".to_owned();

    assert_eq!(refused_setting(config), Some("replica_id"));
}

#[test]
fn empty_data_directory_is_refused() {
    let mut config = minimal_config();
    config.data_directory = PathBuf::new();

    assert_eq!(refused_setting(config), Some("data_directory"));
}

#[test]
fn listen_address_with_a_host_name_is_refused() {
    let mut config = minimal_config();
    config.listen_address = "localhost:7000".to_owned();

    assert_eq!(refused_setting(config), Some("listen_address"));
}

#[test]
fn metrics_address_without_a_port_is_refused() {
    let mut config = minimal_config();
    config.metrics_listen_address = "0.0.0.0".to_owned();

    assert_eq!(refused_setting(config), Some("metrics_listen_address"));
}

#[test]
fn metrics_address_equal_to_the_listen_address_is_refused() {
    let mut config = minimal_config();
    config.metrics_listen_address = config.listen_address.clone();

    assert_eq!(refused_setting(config), Some("metrics_listen_address"));
}

#[test]
fn advertised_address_without_a_port_is_refused() {
    let mut config = minimal_config();
    config.advertised_address = "replica-a.example.org".to_owned();

    assert_eq!(refused_setting(config), Some("advertised_address"));
}

#[test]
fn advertised_address_without_a_host_is_refused() {
    let mut config = minimal_config();
    config.advertised_address = ":7000".to_owned();

    assert_eq!(refused_setting(config), Some("advertised_address"));
}

#[test]
fn advertised_address_with_a_scheme_is_refused() {
    let mut config = minimal_config();
    config.advertised_address = "http://replica-a.example.org:7000".to_owned();

    assert_eq!(refused_setting(config), Some("advertised_address"));
}

#[test]
fn advertised_address_with_port_zero_is_refused() {
    let mut config = minimal_config();
    config.advertised_address = "replica-a.example.org:0".to_owned();

    assert_eq!(refused_setting(config), Some("advertised_address"));
}

#[test]
fn advertised_address_with_a_port_that_is_not_a_number_is_refused() {
    let mut config = minimal_config();
    config.advertised_address = "replica-a.example.org:grpc".to_owned();

    assert_eq!(refused_setting(config), Some("advertised_address"));
}

#[test]
fn advertised_address_may_be_an_ip_address() {
    let mut config = minimal_config();
    config.advertised_address = "10.0.0.7:7000".to_owned();

    assert_eq!(refused_setting(config), None);
}

#[test]
fn empty_failure_domain_is_refused() {
    let mut config = minimal_config();
    config.failure_domains = vec!["zone-a".to_owned(), String::new()];

    assert_eq!(refused_setting(config), Some("failure_domains"));
}

#[test]
fn failure_domain_with_whitespace_is_refused() {
    let mut config = minimal_config();
    config.failure_domains = vec!["zone a".to_owned()];

    assert_eq!(refused_setting(config), Some("failure_domains"));
}

#[test]
fn registration_interval_of_zero_is_refused() {
    let mut config = minimal_config();
    config.registration_interval_seconds = 0;

    assert_eq!(refused_setting(config), Some("registration_interval_seconds"));
}

#[test]
fn scrub_interval_of_zero_is_refused() {
    let mut config = minimal_config();
    config.scrub_interval_seconds = 0;

    assert_eq!(refused_setting(config), Some("scrub_interval_seconds"));
}

#[test]
fn preallocation_below_one_page_is_refused() {
    let mut config = minimal_config();
    config.storage.preallocated_segment_bytes = 4095;

    assert_eq!(refused_setting(config), Some("storage.preallocated_segment_bytes"));
}

#[test]
fn threshold_above_the_whole_disk_is_refused() {
    let mut config = minimal_config();
    config.storage.refuse_appends_at_used_percent = 101;

    assert_eq!(refused_setting(config), Some("storage.refuse_appends_at_used_percent"));
}

#[test]
fn threshold_of_zero_is_refused() {
    let mut config = minimal_config();
    config.storage.refuse_new_segments_at_used_percent = 0;

    assert_eq!(
        refused_setting(config),
        Some("storage.refuse_new_segments_at_used_percent")
    );
}

#[test]
fn append_threshold_below_the_new_segment_threshold_is_refused() {
    let mut config = minimal_config();
    config.storage.refuse_new_segments_at_used_percent = 90;
    config.storage.refuse_appends_at_used_percent = 80;

    assert_eq!(refused_setting(config), Some("storage.refuse_appends_at_used_percent"));
}

#[test]
fn error_names_the_setting_and_the_reason() {
    let mut config = minimal_config();
    config.advertised_address = "replica-a.example.org".to_owned();

    let message = config.validate().expect_err("the address has no port").to_string();

    assert_eq!(
        message,
        "setting `advertised_address` is invalid: \"replica-a.example.org\" has no port; \
         write `host:port`, for example `replica-a.example.org:7000`"
    );
}
