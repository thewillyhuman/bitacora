//! The readers of a simulated run: clients that follow the stream and record what they are handed.
//!
//! # Why this file exists
//!
//! Three of the invariants of the log are about what readers observe: that they agree with each other, that each sees a
//! gapless prefix in order, and that what writers were told is what readers find (design, section 15, I1 to I3). They
//! can only be checked if somebody reads while the faults happen, from the tail and from sealed and tiered segments,
//! and writes down every record. This file is that somebody.
//!
//! # What it contains
//!
//! - [`run_reader`]: one reader, for the length of the chaos phase.
//!
//! # How it works
//!
//! A reader works in sessions. A session follows the stream from one position for a random time and records every
//! record it is handed. Most sessions resume where the last one stopped; one in four starts again from an earlier
//! position that is still readable, so that sealed segments, and segments that have moved to the object store, are read
//! while the cluster is under faults. A session that fails for any reason simply ends: a reader is allowed to fail, it
//! is not allowed to be handed something wrong.
//!
//! When a session is told that its position was truncated, the reader remembers the low-water mark and never starts
//! below it again.
//!
//! # Invariants
//!
//! - A session is recorded as started only once the follow call succeeded, and with the position that was asked for, so
//!   the checker can require the first record to be exactly that one.
//! - Every record a session is handed is recorded, in the order it was handed.
//! - The reader draws its random numbers in a fixed order: per session, whether to rewind, where to, then how long the
//!   session lasts; after a failed follow, how long to wait. That order is part of what a seed means.
//! - [`run_reader`] is one function of some thirty lines, because the two loops share the positions the reader
//!   remembers and their awaits must stay in this order.
//!
//! # See also
//!
//! - `checker.rs`: judges what the readers recorded.
//! - `audit.rs`: the one read of the whole stream after healing.
//! - `tests/fault_matrix_test.rs`: where the reader is tested. It has no unit tests: it is a loop around the client's
//!   reader, and every run of the fault matrix relies on what it records.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::ReadError;
use bitacora_types::{DeterministicRandom, SequenceNumber};
use tokio::time::{Instant, sleep, timeout_at};

use crate::world::{STREAM, World};

/// A session starts from an earlier position once in this many times.
const SESSIONS_PER_REWIND: u64 = 4;

/// The shortest time, in milliseconds, a reader waits after it could not start a session.
const SHORTEST_RETRY_DELAY_MILLIS: u64 = 100;

/// The longest time, in milliseconds, a reader waits after it could not start a session.
const LONGEST_RETRY_DELAY_MILLIS: u64 = 1_000;

/// The shortest time, in seconds, a session lasts.
const SHORTEST_SESSION_SECONDS: u64 = 5;

/// The longest time, in seconds, a session lasts.
const LONGEST_SESSION_SECONDS: u64 = 40;

/// Runs one reader until the task is stopped: it follows the stream in sessions, sometimes starting again from an
/// earlier position to exercise reads of sealed segments.
///
/// # Arguments
///
/// * `world` - The run the reader belongs to. The reader records into its history.
/// * `random` - The reader's own random source.
pub(crate) async fn run_reader(world: Arc<World>, mut random: DeterministicRandom) {
    let reader = world.client("reader").open_reader(STREAM);
    let mut read_through = SequenceNumber::ZERO;
    // The lowest position known to be readable still.
    let mut readable_from = SequenceNumber::FIRST;
    loop {
        let resume_at = read_through.next().max(readable_from);
        let from_sequence = if random.chance(1, SESSIONS_PER_REWIND) {
            SequenceNumber::new(random.between(readable_from.get(), resume_at.get()))
        } else {
            resume_at
        };
        let Ok(mut following) = reader.follow_from(from_sequence).await else {
            let retry_delay_millis = random.between(SHORTEST_RETRY_DELAY_MILLIS, LONGEST_RETRY_DELAY_MILLIS);
            sleep(Duration::from_millis(retry_delay_millis)).await;
            continue;
        };
        let session = world.history.reader_started(from_sequence);
        let session_seconds = random.between(SHORTEST_SESSION_SECONDS, LONGEST_SESSION_SECONDS);
        let session_ends_at = Instant::now() + Duration::from_secs(session_seconds);
        loop {
            match timeout_at(session_ends_at, following.next()).await {
                Ok(Some(Ok(record))) => {
                    world.history.record_read(session, record.sequence, record.payload);
                    read_through = read_through.max(record.sequence);
                }
                Ok(Some(Err(ReadError::Truncated { low_water_mark }))) => {
                    readable_from = readable_from.max(low_water_mark);
                    break;
                }
                // The session's time is up, the stream ended, or the read failed: the next session starts over.
                Ok(Some(Err(_)) | None) | Err(_) => break,
            }
        }
    }
}
