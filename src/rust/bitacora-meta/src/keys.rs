//! Where each record lives in the metadata store.
//!
//! # Why this file exists
//!
//! The key of a record is part of the protocol: every program of a cluster, of every version, must look for a stream's
//! record in the same place (`docs/protocol/metadata.md`). Keys built in passing, with a `format!` here and another
//! there, would drift apart the first time one of them changed. So every key is built by one function in this file, and
//! so is the rule for which names may become part of a key.
//!
//! # What it contains
//!
//! - `stream_key`, `deleted_stream_key`, `checkpoint_key`, `replica_key`: the key of one record.
//! - `STREAM_KEY_PREFIX`, `DELETED_STREAM_KEY_PREFIX`, `REPLICA_KEY_PREFIX`, `checkpoints_key_prefix`: what to list to
//!   get every record of a kind.
//! - `STREAM_ID_COUNTER_KEY` and `CONTROLLER_LEADER_KEY`: the two records there is only one of.
//! - [`MAX_NAME_LENGTH_BYTES`] and `validate_name`: which stream and materializer names are accepted.
//!
//! # How it works
//!
//! ```text
//! streams/<name>                           the record of the stream called <name>
//! checkpoints/<stream id>/<materializer>   one materializer's checkpoint
//! replicas/<replica id>                    one replica's registration
//! controller/leader                        which controller instance is active
//! counters/stream-id                       the last stream identifier handed out
//! deleted-streams/<stream id>              a note that a deleted stream still has data to remove
//! ```
//!
//! A stream is found by its name, because that is what a client knows. Its checkpoints are found by its identifier,
//! because the name may be given to a new stream while the old one is still being cleaned up, and the identifier is
//! never reused.
//!
//! Names become parts of keys, so a name may only hold characters that cannot be mistaken for the structure of a key:
//! ASCII letters, digits, `.`, `_` and `-`. In particular no `/`.
//!
//! # Invariants
//!
//! - No key of one kind is a prefix of a key of another kind, so listing a prefix returns records of one kind only.
//! - A name that `validate_name` accepts cannot change which record a key points at.
//!
//! # See also
//!
//! - `docs/protocol/metadata.md`: the same layout, for a reader who does not read Rust.
//! - `store.rs` and the files under `store/`: the only code that uses these keys.

use bitacora_types::{ReplicaId, StreamId};

use crate::error::MetaError;

/// The longest stream or materializer name accepted, in bytes.
pub const MAX_NAME_LENGTH_BYTES: usize = 255;

/// The punctuation a stream or materializer name may contain, next to ASCII letters and digits. None of these
/// characters means anything in a key.
const NAME_PUNCTUATION: &str = "._-";

/// What the key of every stream record starts with.
pub(crate) const STREAM_KEY_PREFIX: &str = "streams/";

/// What the key of every deleted-stream note starts with.
pub(crate) const DELETED_STREAM_KEY_PREFIX: &str = "deleted-streams/";

/// What the key of every checkpoint record starts with.
const CHECKPOINT_KEY_PREFIX: &str = "checkpoints/";

/// What the key of every replica registration starts with.
pub(crate) const REPLICA_KEY_PREFIX: &str = "replicas/";

/// The key of the last stream identifier handed out.
pub(crate) const STREAM_ID_COUNTER_KEY: &str = "counters/stream-id";

/// The key of the record that says which controller instance is active.
pub(crate) const CONTROLLER_LEADER_KEY: &str = "controller/leader";

/// Returns the key of the record of the stream called `name`.
///
/// # Arguments
///
/// * `name` - The stream's name. The caller validates it where a new record is created.
///
/// # Returns
///
/// The key, for example `streams/orders`.
pub(crate) fn stream_key(name: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{name}")
}

/// Returns the key of the note that a deleted stream still has data to remove.
///
/// # Arguments
///
/// * `stream_id` - The identifier the stream had.
///
/// # Returns
///
/// The key, for example `deleted-streams/7`.
pub(crate) fn deleted_stream_key(stream_id: StreamId) -> String {
    format!("{DELETED_STREAM_KEY_PREFIX}{stream_id}")
}

/// Returns what the keys of all checkpoints of one stream start with.
///
/// # Arguments
///
/// * `stream_id` - The stream whose checkpoints are wanted.
///
/// # Returns
///
/// The prefix, for example `checkpoints/7/`. The trailing separator keeps stream 7 from matching the checkpoints of
/// stream 70.
pub(crate) fn checkpoints_key_prefix(stream_id: StreamId) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}{stream_id}/")
}

/// Returns the key of one materializer's checkpoint.
///
/// # Arguments
///
/// * `stream_id` - The stream the materializer follows.
/// * `materializer` - The materializer's name, unique within the stream.
///
/// # Returns
///
/// The key, for example `checkpoints/7/pages`.
pub(crate) fn checkpoint_key(stream_id: StreamId, materializer: &str) -> String {
    format!("{}{materializer}", checkpoints_key_prefix(stream_id))
}

/// Returns the key of one replica's registration.
///
/// # Arguments
///
/// * `replica_id` - The replica's identity.
///
/// # Returns
///
/// The key, for example `replicas/replica-a`.
pub(crate) fn replica_key(replica_id: &ReplicaId) -> String {
    format!("{REPLICA_KEY_PREFIX}{replica_id}")
}

/// Checks that a stream or materializer name can be part of a key.
///
/// # Arguments
///
/// * `name` - The name to check.
///
/// # Errors
///
/// Returns [`MetaError::InvalidName`] if the name is empty, longer than [`MAX_NAME_LENGTH_BYTES`], or holds anything
/// but ASCII letters, digits, `.`, `_` and `-`.
pub(crate) fn validate_name(name: &str) -> Result<(), MetaError> {
    let has_valid_length = !name.is_empty() && name.len() <= MAX_NAME_LENGTH_BYTES;
    if has_valid_length && name.chars().all(is_allowed_in_a_name) {
        Ok(())
    } else {
        Err(MetaError::InvalidName { name: name.to_owned() })
    }
}

/// Says whether a name may contain `character`.
fn is_allowed_in_a_name(character: char) -> bool {
    character.is_ascii_alphanumeric() || NAME_PUNCTUATION.contains(character)
}

#[cfg(test)]
#[path = "keys_test.rs"]
mod tests;
