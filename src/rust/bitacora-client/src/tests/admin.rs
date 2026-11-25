//! Stream administration against an in-process cluster.

use bitacora_meta::MetaError;
use bitacora_types::SequenceNumber;

use super::harness::Cluster;
use crate::{StreamOptions, TakeoverPolicy};

const NEW_STREAM: &str = "payments";

#[tokio::test(start_paused = true)]
async fn created_stream_can_be_written_at_once() {
    let cluster = Cluster::start(3).await;
    let client = cluster.client("compute-1");
    client
        .admin()
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    let writer = client
        .open_writer(NEW_STREAM, TakeoverPolicy::Force)
        .await
        .expect("the first segment was placed at creation");
    assert_eq!(
        writer.append("first".into()).await,
        Ok(SequenceNumber::FIRST)
    );
}

#[tokio::test(start_paused = true)]
async fn stream_created_on_too_few_replicas_waits_for_the_controller() {
    let cluster = Cluster::start(3).await;
    cluster
        .metadata
        .remove_replica(&cluster.nodes[2].replica_id)
        .await
        .expect("the replica is removed");
    let created = cluster
        .client("compute-1")
        .admin()
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    assert_eq!(created.standby_replicas, None);
}

#[tokio::test(start_paused = true)]
async fn creating_a_stream_twice_fails() {
    let cluster = Cluster::start(3).await;
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    assert_eq!(
        admin
            .create_stream(NEW_STREAM, StreamOptions::default())
            .await,
        Err(MetaError::StreamAlreadyExists {
            name: NEW_STREAM.to_owned()
        })
    );
}

#[tokio::test(start_paused = true)]
async fn description_includes_the_registered_materializers() {
    let cluster = Cluster::start(3).await;
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    admin
        .register_materializer(NEW_STREAM, "pages")
        .await
        .expect("registered");
    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream exists");
    let materializers: Vec<&str> = description
        .checkpoints
        .iter()
        .map(|checkpoint| checkpoint.materializer.as_str())
        .collect();
    assert_eq!(
        (description.stream.name.as_str(), materializers),
        (NEW_STREAM, vec!["pages"])
    );
}

#[tokio::test(start_paused = true)]
async fn unregistered_materializer_is_no_longer_described() {
    let cluster = Cluster::start(3).await;
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    admin
        .register_materializer(NEW_STREAM, "pages")
        .await
        .expect("registered");
    admin
        .unregister_materializer(NEW_STREAM, "pages")
        .await
        .expect("unregistered");
    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream exists");
    assert_eq!(description.checkpoints, Vec::new());
}

#[tokio::test(start_paused = true)]
async fn truncation_position_only_moves_forward() {
    let cluster = Cluster::start(3).await;
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    for requested in [50, 20] {
        admin
            .truncate_before(NEW_STREAM, SequenceNumber::new(requested))
            .await
            .expect("the position is recorded");
    }
    let description = admin
        .describe_stream(NEW_STREAM)
        .await
        .expect("the stream exists");
    assert_eq!(
        description.stream.truncate_before,
        Some(SequenceNumber::new(50))
    );
}

#[tokio::test(start_paused = true)]
async fn deleted_stream_is_no_longer_listed() {
    let cluster = Cluster::start(3).await;
    let admin = cluster.client("compute-1").admin();
    admin
        .create_stream(NEW_STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    admin
        .delete_stream(NEW_STREAM)
        .await
        .expect("the stream is deleted");
    let names: Vec<String> = admin
        .list_streams()
        .await
        .expect("lists")
        .into_iter()
        .map(|stream| stream.name)
        .collect();
    assert_eq!(names, ["orders"]);
}

#[tokio::test(start_paused = true)]
async fn describing_a_missing_stream_fails() {
    let cluster = Cluster::start(3).await;
    assert_eq!(
        cluster
            .client("compute-1")
            .admin()
            .describe_stream(NEW_STREAM)
            .await,
        Err(MetaError::StreamNotFound {
            name: NEW_STREAM.to_owned()
        })
    );
}
