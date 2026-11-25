//! The artifact store contract on a real S3 server.
//!
//! # What is covered
//!
//! `bitacora_artifact::contract::check` is the behaviour the in-memory store, the directory store and this
//! store must share: what the simulator proves about the code above only holds in production if the
//! backends agree. It runs here twice, because the key prefix changes every path the store builds.
//!
//! # How the fixtures work
//!
//! The contract wants an empty store. Each test gets one by opening the shared server's bucket under a key
//! prefix nobody else uses, and once under no prefix at all on a server of its own.

use bitacora_artifact::contract;

use crate::s3_server::{S3Server, s3_server_binary, shared_s3_server};

#[tokio::test]
async fn store_with_a_key_prefix_honours_the_artifact_store_contract() {
    let Some(server) = shared_s3_server().await else {
        return;
    };
    let store = server.store_in_fresh_namespace();

    contract::check(&store).await;
}

/// Without a key prefix the store owns the whole bucket, so this test cannot share its server.
#[tokio::test]
async fn store_without_a_key_prefix_honours_the_artifact_store_contract() {
    let Some(binary) = s3_server_binary() else {
        return;
    };
    let server = S3Server::start(&binary).await;
    let store = server.store_in("");

    contract::check(&store).await;
}
