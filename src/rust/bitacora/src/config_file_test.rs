//! Tests of working out which cluster a tool is to use.
//!
//! # What is covered
//!
//! Endpoints as options, a configuration file, options replacing the file's values, and every refusal.
//!
//! # How the fixtures work
//!
//! Configuration files are written to a temporary directory.

// The fixtures write real configuration files, which is what the code under test reads.
#![allow(clippy::disallowed_methods)]

use tempfile::TempDir;

use super::*;

/// Writes `content` to a file called `cluster.toml` in a new temporary directory.
fn config_file(content: &str) -> (TempDir, PathBuf) {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("cluster.toml");
    std::fs::write(&path, content).expect("the file is written");
    (directory, path)
}

/// A configuration file that names one endpoint and a key prefix.
const FILE_CONFIG: &str = "etcd_endpoints = [\"http://file:2379\"]\netcd_key_prefix = \"/from-file/\"\n";

#[test]
fn endpoints_given_as_options_are_used_with_the_default_prefix() {
    let endpoints = ["http://a:2379".to_owned()];

    let config = resolve_cluster_config(None, &endpoints, None).expect("resolves");

    assert_eq!(config.etcd_endpoints, endpoints);
    assert_eq!(config.etcd_key_prefix, DEFAULT_ETCD_KEY_PREFIX);
}

#[test]
fn configuration_file_is_read() {
    let (_directory, path) = config_file(FILE_CONFIG);

    let config = resolve_cluster_config(Some(&path), &[], None).expect("resolves");

    assert_eq!(config.etcd_endpoints, ["http://file:2379"]);
    assert_eq!(config.etcd_key_prefix, "/from-file/");
}

#[test]
fn options_replace_what_the_file_says() {
    let (_directory, path) = config_file(FILE_CONFIG);
    let endpoints = ["http://option:2379".to_owned()];

    let config = resolve_cluster_config(Some(&path), &endpoints, Some("/option/")).expect("resolves");

    assert_eq!(config.etcd_endpoints, endpoints);
    assert_eq!(config.etcd_key_prefix, "/option/");
}

#[test]
fn no_file_and_no_endpoint_is_refused() {
    let resolved = resolve_cluster_config(None, &[], None);

    assert_eq!(resolved, Err(ClusterConfigError::NoClusterGiven));
}

#[test]
fn missing_configuration_file_is_refused() {
    let resolved = resolve_cluster_config(Some(Path::new("/no/such/file.toml")), &[], None);

    assert!(matches!(resolved, Err(ClusterConfigError::UnreadableFile { .. })));
}

#[test]
fn file_with_a_misspelled_setting_is_refused() {
    let (_directory, path) = config_file("etcd_endpoints = []\netcd_key_prefx = \"/x/\"\n");

    let resolved = resolve_cluster_config(Some(&path), &[], None);

    assert!(matches!(resolved, Err(ClusterConfigError::InvalidFile { .. })));
}

#[test]
fn file_without_endpoints_and_no_option_is_refused() {
    let (_directory, path) = config_file("etcd_endpoints = []\n");

    let resolved = resolve_cluster_config(Some(&path), &[], None);

    assert_eq!(resolved, Err(ClusterConfigError::NoClusterGiven));
}
