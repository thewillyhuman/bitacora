//! Tests of finding the cluster.
//!
//! # What is covered
//!
//! - Each source alone: options, environment variables, a configuration file.
//! - Their order: an option wins over the environment, and both win over the file.
//! - Every refusal: no cluster at all, a file that cannot be read, a file that is not a configuration.
//!
//! # How the fixtures work
//!
//! Configuration files are written to a temporary directory. The environment is a plain map, so no test
//! touches the environment of the test process.

// The fixtures write real configuration files, which is what the code under test reads.
#![allow(clippy::disallowed_methods)]

use std::path::Path;

use tempfile::TempDir;

use super::*;

/// The environment of a process that has none of the tool's variables set.
fn empty_environment() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// An environment with one variable set.
fn environment_with(name: &str, value: &str) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    environment.insert(name.to_owned(), value.to_owned());
    environment
}

/// Writes `content` to a file called `cluster.toml` in a new temporary directory.
fn config_file(content: &str) -> (TempDir, PathBuf) {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("cluster.toml");
    std::fs::write(&path, content).expect("the file is written");
    (directory, path)
}

/// The arguments of a command line that names `path` as its configuration file.
fn arguments_with_config(path: &Path) -> ClusterArguments {
    ClusterArguments {
        config: Some(path.to_owned()),
        ..ClusterArguments::default()
    }
}

/// A configuration file that names one endpoint, a key prefix and a local object store.
const FULL_CONFIG: &str = r#"
etcd_endpoints = ["http://file:2379"]
etcd_key_prefix = "/from-file/"

[object_store]
kind = "local_directory"
path = "/var/lib/bitacora/objects"
"#;

#[test]
fn endpoints_given_as_options_are_used() {
    let arguments = ClusterArguments {
        etcd_endpoints: vec!["http://a:2379".to_owned(), "http://b:2379".to_owned()],
        ..ClusterArguments::default()
    };

    let config = resolve_cluster_config(&arguments, &empty_environment()).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://a:2379", "http://b:2379"]);
}

#[test]
fn endpoints_alone_get_the_default_key_prefix_and_no_object_store() {
    let arguments = ClusterArguments {
        etcd_endpoints: vec!["http://a:2379".to_owned()],
        ..ClusterArguments::default()
    };

    let config = resolve_cluster_config(&arguments, &empty_environment()).expect("resolves");

    assert_eq!(config.etcd_key_prefix, DEFAULT_ETCD_KEY_PREFIX);
    assert_eq!(config.object_store, ObjectStoreConfig::None);
}

#[test]
fn endpoints_from_the_environment_are_split_at_commas() {
    let environment = environment_with(ETCD_ENDPOINTS_VARIABLE, "http://a:2379, http://b:2379,");

    let config =
        resolve_cluster_config(&ClusterArguments::default(), &environment).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://a:2379", "http://b:2379"]);
}

#[test]
fn endpoint_options_win_over_the_environment() {
    let arguments = ClusterArguments {
        etcd_endpoints: vec!["http://option:2379".to_owned()],
        ..ClusterArguments::default()
    };
    let environment = environment_with(ETCD_ENDPOINTS_VARIABLE, "http://environment:2379");

    let config = resolve_cluster_config(&arguments, &environment).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://option:2379"]);
}

#[test]
fn key_prefix_from_the_environment_is_used() {
    let mut environment = environment_with(ETCD_ENDPOINTS_VARIABLE, "http://a:2379");
    environment.insert(ETCD_KEY_PREFIX_VARIABLE.to_owned(), "/staging/".to_owned());

    let config =
        resolve_cluster_config(&ClusterArguments::default(), &environment).expect("resolves");

    assert_eq!(config.etcd_key_prefix, "/staging/");
}

#[test]
fn key_prefix_option_wins_over_the_environment() {
    let arguments = ClusterArguments {
        etcd_endpoints: vec!["http://a:2379".to_owned()],
        etcd_key_prefix: Some("/option/".to_owned()),
        ..ClusterArguments::default()
    };
    let environment = environment_with(ETCD_KEY_PREFIX_VARIABLE, "/environment/");

    let config = resolve_cluster_config(&arguments, &environment).expect("resolves");

    assert_eq!(config.etcd_key_prefix, "/option/");
}

#[test]
fn configuration_file_is_read() {
    let (_directory, path) = config_file(FULL_CONFIG);

    let config = resolve_cluster_config(&arguments_with_config(&path), &empty_environment())
        .expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://file:2379"]);
    assert_eq!(config.etcd_key_prefix, "/from-file/");
    assert_eq!(
        config.object_store,
        ObjectStoreConfig::LocalDirectory {
            path: PathBuf::from("/var/lib/bitacora/objects")
        }
    );
}

#[test]
fn configuration_file_named_by_the_environment_is_read() {
    let (_directory, path) = config_file(FULL_CONFIG);
    let environment = environment_with(CONFIG_VARIABLE, path.to_str().expect("a text path"));

    let config =
        resolve_cluster_config(&ClusterArguments::default(), &environment).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://file:2379"]);
}

#[test]
fn config_option_wins_over_the_config_variable() {
    let (_directory, path) = config_file(FULL_CONFIG);
    let environment = environment_with(CONFIG_VARIABLE, "/no/such/file.toml");

    let config =
        resolve_cluster_config(&arguments_with_config(&path), &environment).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://file:2379"]);
}

#[test]
fn endpoint_option_wins_over_the_file_and_keeps_its_object_store() {
    let (_directory, path) = config_file(FULL_CONFIG);
    let arguments = ClusterArguments {
        etcd_endpoints: vec!["http://option:2379".to_owned()],
        ..arguments_with_config(&path)
    };

    let config = resolve_cluster_config(&arguments, &empty_environment()).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://option:2379"]);
    assert_ne!(config.object_store, ObjectStoreConfig::None);
}

#[test]
fn no_source_at_all_is_refused() {
    let resolved = resolve_cluster_config(&ClusterArguments::default(), &empty_environment());

    assert_eq!(resolved, Err(ClusterConfigError::NoClusterGiven));
}

#[test]
fn environment_list_of_only_commas_is_refused() {
    let environment = environment_with(ETCD_ENDPOINTS_VARIABLE, " , ,");

    let resolved = resolve_cluster_config(&ClusterArguments::default(), &environment);

    assert_eq!(resolved, Err(ClusterConfigError::NoClusterGiven));
}

#[test]
fn missing_configuration_file_is_refused() {
    let arguments = arguments_with_config(Path::new("/no/such/file.toml"));

    let resolved = resolve_cluster_config(&arguments, &empty_environment());

    assert!(matches!(
        resolved,
        Err(ClusterConfigError::UnreadableFile { .. })
    ));
}

#[test]
fn file_that_is_not_toml_is_refused() {
    let (_directory, path) = config_file("etcd_endpoints = [");

    let resolved = resolve_cluster_config(&arguments_with_config(&path), &empty_environment());

    assert!(matches!(
        resolved,
        Err(ClusterConfigError::InvalidFile { .. })
    ));
}

#[test]
fn file_with_a_misspelled_setting_is_refused() {
    let (_directory, path) = config_file("etcd_endpoints = []\netcd_key_prefx = \"/x/\"\n");

    let resolved = resolve_cluster_config(&arguments_with_config(&path), &empty_environment());

    assert!(matches!(
        resolved,
        Err(ClusterConfigError::InvalidFile { .. })
    ));
}

#[test]
fn file_without_endpoints_and_no_other_source_is_refused() {
    let (_directory, path) = config_file("etcd_endpoints = []\n");

    let resolved = resolve_cluster_config(&arguments_with_config(&path), &empty_environment());

    assert_eq!(resolved, Err(ClusterConfigError::NoClusterGiven));
}
