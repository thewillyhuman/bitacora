//! Tests of the parts of `client.rs` that need no server.
//!
//! Addresses, the channel cache and the description of a failed call are
//! checked here without a connection: a channel dials nothing until its
//! first call, so creating one is free of I/O. Everything that needs a
//! server on the other end is in `tests/replica_transport_test.rs`.

use std::io;

use super::*;

/// The identity used for every handle in these tests.
fn replica_id() -> ReplicaId {
    ReplicaId::new("replica-a").expect("a valid identifier")
}

/// How many channels `connector` holds.
fn cached_channel_count(connector: &GrpcReplicaConnector) -> usize {
    connector
        .channels
        .channels_by_address
        .lock()
        .expect("no test panicked")
        .len()
}

/// Asks `connector`'s cache for the channel to `address`, as the first
/// request of a handle would.
fn channel_to(connector: &GrpcReplicaConnector, address: &str) -> Channel {
    let endpoint = endpoint_of(address, &connector.channels.settings).expect("a valid address");
    connector.channels.channel_to(address, &endpoint)
}

#[test]
fn host_and_port_are_dialled_over_plain_http() {
    let endpoint = endpoint_of("replica-a.example:7000", &GrpcConnectorSettings::default())
        .expect("a valid address");

    assert_eq!(endpoint.uri().to_string(), "http://replica-a.example:7000/");
}

#[test]
fn endpoint_carries_the_connect_timeout_of_the_settings() {
    let settings = GrpcConnectorSettings {
        connect_timeout: Duration::from_millis(150),
    };

    let endpoint = endpoint_of("replica-a.example:7000", &settings).expect("a valid address");

    assert_eq!(
        endpoint.get_connect_timeout(),
        Some(Duration::from_millis(150))
    );
}

#[test]
fn address_with_a_space_cannot_be_dialled() {
    let endpoint = endpoint_of("not an address", &GrpcConnectorSettings::default());

    assert!(endpoint.is_err());
}

#[tokio::test]
async fn replica_at_an_address_that_cannot_be_dialled_is_unavailable() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let replica = connector.connect(&replica_id(), "not an address");
    let answer = replica.status().await;

    assert!(matches!(answer, Err(ReplicaError::Unavailable { .. })));
}

#[tokio::test]
async fn error_for_an_address_that_cannot_be_dialled_names_the_replica_and_the_address() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let replica = connector.connect(&replica_id(), "not an address");
    let error = replica.status().await.expect_err("unavailable");

    assert!(
        error
            .to_string()
            .contains("replica replica-a is registered at \"not an address\"")
    );
}

/// Connecting must not need a runtime or touch the network: callers do it
/// while they hold locks and before they know whether they will send
/// anything.
#[test]
fn connecting_opens_no_channel() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let _replica = connector.connect(&replica_id(), "replica-a.example:7000");

    assert_eq!(cached_channel_count(&connector), 0);
}

#[tokio::test]
async fn channel_to_an_address_is_created_once() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let _first_channel = channel_to(&connector, "replica-a.example:7000");
    let _second_channel = channel_to(&connector, "replica-a.example:7000");

    assert_eq!(cached_channel_count(&connector), 1);
}

#[tokio::test]
async fn each_address_gets_its_own_channel() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());

    let _first_channel = channel_to(&connector, "replica-a.example:7000");
    let _second_channel = channel_to(&connector, "replica-b.example:7000");

    assert_eq!(cached_channel_count(&connector), 2);
}

/// Clones of a connector are the same connector: a process that hands
/// clones to its writer and its controller still keeps one connection per
/// replica.
#[tokio::test]
async fn clones_of_a_connector_share_their_channels() {
    let connector = GrpcReplicaConnector::new(GrpcConnectorSettings::default());
    let cloned_connector = connector.clone();

    let _channel = channel_to(&cloned_connector, "replica-a.example:7000");

    assert_eq!(cached_channel_count(&connector), 1);
}

#[test]
fn status_is_described_by_its_code_and_message() {
    let status = Status::failed_precondition("this is replica replica-b");

    assert_eq!(
        describe_status(&status),
        "FailedPrecondition: this is replica replica-b"
    );
}

/// The transport reports what really happened as the cause of a generic
/// status; an operator needs to see it.
#[test]
fn status_description_includes_the_underlying_cause() {
    let mut status = Status::unavailable("transport error");
    status.set_source(Arc::new(io::Error::other("connection refused")));

    assert_eq!(
        describe_status(&status),
        "Unavailable: transport error: connection refused"
    );
}
