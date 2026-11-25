//! Tests of the cluster configuration and of connecting with it.
//!
//! # What is covered
//!
//! - How a configuration is read: defaults, each kind of object store, and refusal of unknown fields.
//! - Which configurations are refused when connecting, and that an etcd that is merely down is not one of
//!   them.
//! - That the configured object store is the one that gets opened.
//!
//! # How the fixtures work
//!
//! Configurations are parsed from JSON, because the test only needs some `serde` format and the workspace
//! already has that one; the binaries read the same structure from TOML. No etcd runs here: the backend
//! has its own tests against a real etcd, and connecting sends nothing.

use bytes::Bytes;
use tempfile::TempDir;

use super::*;

/// Parses a configuration from JSON.
fn parse(json: &str) -> Result<ClusterConfig, serde_json::Error> {
    serde_json::from_str(json)
}

/// A configuration that names one etcd endpoint on this machine and no object store.
fn local_config() -> ClusterConfig {
    ClusterConfig {
        etcd_endpoints: vec!["http://127.0.0.1:2379".to_owned()],
        etcd_key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
        object_store: ObjectStoreConfig::None,
    }
}

#[test]
fn configuration_with_endpoints_alone_gets_the_defaults() {
    let config = parse(r#"{"etcd_endpoints": ["http://127.0.0.1:2379"]}"#);

    assert_eq!(config.ok(), Some(local_config()));
}

#[test]
fn configuration_can_name_its_key_prefix() {
    let config = parse(r#"{"etcd_endpoints": ["http://127.0.0.1:2379"], "etcd_key_prefix": "/staging/"}"#)
        .expect("valid configuration");

    assert_eq!(config.etcd_key_prefix, "/staging/");
}

#[test]
fn configuration_can_name_a_local_directory_as_object_store() {
    let config = parse(
        r#"{
            "etcd_endpoints": ["http://127.0.0.1:2379"],
            "object_store": {"kind": "local_directory", "path": "/var/lib/bitacora/objects"}
        }"#,
    )
    .expect("valid configuration");

    assert_eq!(
        config.object_store,
        ObjectStoreConfig::LocalDirectory {
            path: PathBuf::from("/var/lib/bitacora/objects")
        }
    );
}

#[test]
fn configuration_can_name_an_s3_bucket_with_the_defaults_of_the_rest() {
    let config = parse(
        r#"{
            "etcd_endpoints": ["http://127.0.0.1:2379"],
            "object_store": {"kind": "s3", "bucket": "bitacora", "region": "eu-west-1"}
        }"#,
    )
    .expect("valid configuration");

    assert_eq!(
        config.object_store,
        ObjectStoreConfig::S3 {
            bucket: "bitacora".to_owned(),
            region: "eu-west-1".to_owned(),
            endpoint: None,
            key_prefix: String::new(),
            force_path_style: true,
            allow_http: false,
        }
    );
}

#[test]
fn configuration_can_name_an_s3_compatible_service() {
    let config = parse(
        r#"{
            "etcd_endpoints": ["http://127.0.0.1:2379"],
            "object_store": {
                "kind": "s3",
                "bucket": "bitacora",
                "region": "us-east-1",
                "endpoint": "http://127.0.0.1:9000",
                "key_prefix": "staging/",
                "force_path_style": true,
                "allow_http": true
            }
        }"#,
    )
    .expect("valid configuration");

    assert_eq!(
        config.object_store,
        ObjectStoreConfig::S3 {
            bucket: "bitacora".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: Some("http://127.0.0.1:9000".to_owned()),
            key_prefix: "staging/".to_owned(),
            force_path_style: true,
            allow_http: true,
        }
    );
}

#[test]
fn s3_configuration_without_a_bucket_is_refused() {
    let config = parse(r#"{"etcd_endpoints": [], "object_store": {"kind": "s3", "region": "eu-west-1"}}"#);

    assert!(config.is_err(), "{config:?}");
}

#[test]
fn configuration_can_say_explicitly_that_there_is_no_object_store() {
    let config = parse(r#"{"etcd_endpoints": ["http://127.0.0.1:2379"], "object_store": {"kind": "none"}}"#)
        .expect("valid configuration");

    assert_eq!(config.object_store, ObjectStoreConfig::None);
}

#[test]
fn configuration_without_endpoints_is_refused() {
    let config = parse("{}");

    assert!(config.is_err(), "{config:?}");
}

#[test]
fn misspelled_setting_is_refused_instead_of_ignored() {
    let config = parse(r#"{"etcd_endpoints": [], "etcd_key_prefx": "/staging/"}"#);

    assert!(config.is_err(), "{config:?}");
}

#[test]
fn unknown_kind_of_object_store_is_refused() {
    let config = parse(r#"{"etcd_endpoints": [], "object_store": {"kind": "tape_library"}}"#);

    assert!(config.is_err(), "{config:?}");
}

#[test]
fn local_directory_without_a_path_is_refused() {
    let config = parse(r#"{"etcd_endpoints": [], "object_store": {"kind": "local_directory"}}"#);

    assert!(config.is_err(), "{config:?}");
}

#[tokio::test]
async fn connecting_without_any_endpoint_is_refused() {
    let config = ClusterConfig {
        etcd_endpoints: Vec::new(),
        ..local_config()
    };

    let connected = connect(&config, ClientSettings::default()).await;

    assert!(matches!(connected.err(), Some(ConnectError::InvalidMetadataStore(_))));
}

#[tokio::test]
async fn key_value_store_without_any_endpoint_is_refused() {
    let config = ClusterConfig {
        etcd_endpoints: Vec::new(),
        ..local_config()
    };

    let connected = connect_key_value_store(&config).await;

    assert!(matches!(connected.err(), Some(ConnectError::InvalidMetadataStore(_))));
}

/// A daemon that instruments its metadata store builds it on this store. Like a client, it must start
/// while etcd is down.
#[tokio::test]
async fn key_value_store_is_handed_out_while_etcd_is_down() {
    let nobody_listens_here = ClusterConfig {
        etcd_endpoints: vec!["http://127.0.0.1:1".to_owned()],
        ..local_config()
    };

    let connected = connect_key_value_store(&nobody_listens_here).await;

    assert!(connected.is_ok());
}

/// A client must start while etcd is down and work once it is back: connecting sends nothing.
#[tokio::test]
async fn connecting_succeeds_while_etcd_is_down() {
    let nobody_listens_here = ClusterConfig {
        etcd_endpoints: vec!["http://127.0.0.1:1".to_owned()],
        ..local_config()
    };

    let connected = connect(&nobody_listens_here, ClientSettings::default()).await;

    assert!(connected.is_ok());
}

#[tokio::test]
async fn cluster_without_object_store_opens_none() {
    let opened = open_object_store(&ObjectStoreConfig::None)
        .await
        .expect("nothing to open");

    assert!(opened.is_none());
}

#[tokio::test]
async fn local_directory_object_store_keeps_objects_in_that_directory() {
    let directory = TempDir::new().expect("temporary directory");
    let config = ObjectStoreConfig::LocalDirectory {
        path: directory.path().to_owned(),
    };
    let first_handle = open_object_store(&config)
        .await
        .expect("the directory opens")
        .expect("a store is configured");
    first_handle
        .put_object("streams/1/segments/1", Bytes::from_static(b"segment"))
        .await
        .expect("put");

    let second_handle = open_object_store(&config)
        .await
        .expect("the directory opens")
        .expect("a store is configured");

    assert_eq!(
        second_handle.get_object("streams/1/segments/1").await,
        Ok(Bytes::from_static(b"segment"))
    );
}

/// An S3 service that is down must not keep a process from starting: opening sends nothing.
#[tokio::test]
async fn s3_object_store_opens_without_reaching_the_service() {
    let nobody_listens_here = ObjectStoreConfig::S3 {
        bucket: "bitacora".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint: Some("http://127.0.0.1:1".to_owned()),
        key_prefix: String::new(),
        force_path_style: true,
        allow_http: true,
    };

    let opened = open_object_store(&nobody_listens_here).await;

    assert!(matches!(opened, Ok(Some(_))));
}

#[tokio::test]
async fn s3_endpoint_over_http_is_refused_unless_allowed() {
    let plain_http_without_consent = ObjectStoreConfig::S3 {
        bucket: "bitacora".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint: Some("http://127.0.0.1:9000".to_owned()),
        key_prefix: String::new(),
        force_path_style: true,
        allow_http: false,
    };

    let opened = open_object_store(&plain_http_without_consent).await;

    assert!(matches!(opened.err(), Some(ConnectError::InvalidObjectStore(_))));
}

#[tokio::test]
async fn object_store_directory_that_cannot_be_created_is_refused() {
    let directory = TempDir::new().expect("temporary directory");
    LocalArtifactStore::open(directory.path())
        .await
        .expect("the directory opens")
        .put_object("placeholder", Bytes::new())
        .await
        .expect("put");
    // The store keeps the object as a regular file, and a path below a regular file can never become
    // a directory.
    let below_a_file = directory.path().join("objects").join("placeholder").join("store");

    let opened = open_object_store(&ObjectStoreConfig::LocalDirectory { path: below_a_file }).await;

    assert!(matches!(opened.err(), Some(ConnectError::ObjectStore(_))));
}
