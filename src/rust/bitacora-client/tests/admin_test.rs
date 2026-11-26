//! Tests of stream administration, end to end.
//!
//! # What is covered
//!
//! - Creating a stream: it can be written at once, it waits for the controller when too few replicas are registered,
//!   and a name cannot be used twice.
//! - Describing a stream: its record, its materializers while they are registered, and a stream that does not exist.
//! - Truncation requests, which only move forward.
//! - Deleting a stream, after which it is no longer listed.
//!
//! # How the fixtures work
//!
//! `support::Cluster` is three real replicas on in-memory disks, registered in an in-memory metadata store. It comes
//! with one stream, `orders`; the tests create a second one, `payments`, through the admin handle under test.

mod support;

use bitacora_client::{StreamAdmin, StreamOptions, TakeoverPolicy};
use bitacora_meta::MetaError;
use bitacora_types::SequenceNumber;

use support::Cluster;

/// The stream the tests create.
const NEW_STREAM: &str = "payments";

/// An admin handle on `cluster` that has created [`NEW_STREAM`] with the default options.
async fn admin_with_new_stream(cluster: &Cluster) -> StreamAdmin {
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the name is free and the store is up");
    admin
}

#[tokio::test(start_paused = true)]
async fn created_stream_can_be_written_at_once() {
    let cluster = Cluster::start(3).await;
    let client = cluster.client("compute-1");
    client
        .admin()
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the name is free and the store is up");

    let writer = client
        .open_writer(NEW_STREAM, TakeoverPolicy::Force)
        .await
        .expect("the first segment was placed at creation");

    assert_eq!(writer.append("first".into()).await, Ok(SequenceNumber::FIRST));
}

#[tokio::test(start_paused = true)]
async fn stream_created_on_too_few_replicas_waits_for_the_controller() {
    let cluster = Cluster::start(3).await;
    cluster
        .metadata()
        .remove_replica(&cluster.nodes()[2].replica_id())
        .await
        .expect("the replica is registered and the store is up");

    let created = cluster
        .client("compute-1")
        .admin()
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("a stream can be created before it can be placed");

    assert_eq!(created.standby_replicas, None);
}

#[tokio::test(start_paused = true)]
async fn creating_a_stream_twice_fails() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;

    let created_again = admin.create_stream(NEW_STREAM, StreamOptions::default()).await;

    assert_eq!(
        created_again,
        Err(MetaError::StreamAlreadyExists {
            name: NEW_STREAM.to_owned()
        })
    );
}

#[tokio::test(start_paused = true)]
async fn description_holds_the_record_of_the_stream_asked_for() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;

    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream was just created");

    assert_eq!(description.stream.name, NEW_STREAM);
}

#[tokio::test(start_paused = true)]
async fn description_includes_the_registered_materializers() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;
    admin
        .register_materializer(NEW_STREAM, "pages")
        .await
        .expect("the stream exists and the name is valid");

    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream was just created");

    let mut materializers = Vec::new();
    for checkpoint in &description.checkpoints {
        materializers.push(checkpoint.materializer.as_str());
    }
    assert_eq!(materializers, ["pages"]);
}

#[tokio::test(start_paused = true)]
async fn unregistered_materializer_is_no_longer_described() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;
    admin
        .register_materializer(NEW_STREAM, "pages")
        .await
        .expect("the stream exists and the name is valid");

    admin
        .unregister_materializer(NEW_STREAM, "pages")
        .await
        .expect("the materializer is registered");

    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream was just created");
    assert_eq!(description.checkpoints, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn truncation_position_only_moves_forward() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;

    for requested in [50, 20] {
        admin
            .truncate_before(NEW_STREAM, SequenceNumber::new(requested))
            .await
            .expect("the stream exists and the store is up");
    }

    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream was just created");
    assert_eq!(description.stream.truncate_before, Some(SequenceNumber::new(50)));
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_no_longer_listed() {
    let cluster = Cluster::start(3).await;
    let admin = admin_with_new_stream(&cluster).await;

    admin
        .delete_stream(NEW_STREAM)
        .await
        .expect("the stream was just created");

    let streams = admin.list_streams().await.expect("the store is up");
    let mut names = Vec::new();
    for stream in streams {
        names.push(stream.name);
    }
    assert_eq!(names, ["orders"]);
}

#[tokio::test(start_paused = true)]
async fn describing_a_missing_stream_fails() {
    let cluster = Cluster::start(3).await;

    let description = cluster.client("compute-1").admin().describe_stream(NEW_STREAM).await;

    assert_eq!(
        description,
        Err(MetaError::StreamNotFound {
            name: NEW_STREAM.to_owned()
        })
    );
}
