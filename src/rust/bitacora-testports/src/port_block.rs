//! Blocks of loopback ports that a test claims for the servers it starts.
//!
//! # Why this file exists
//!
//! A program that a test starts opens its own sockets, so the test must name a free port for it. Binding
//! port zero, reading the port the operating system chose and closing the socket again finds one, but it
//! does not keep it: until the program has bound the port itself, the operating system may hand it to the
//! next caller that asks for "any port". Tests that run in parallel are such callers, and they all ask in
//! the same instant. A test of the cluster kit failed that way in 6 of 30 parallel runs, and the S3 tests in
//! 1 of 30.
//!
//! The ports of this file cannot be lost like that. They come from a range the operating system never picks
//! from, and each of them belongs to one owner, across processes, for as long as the owner keeps its block.
//!
//! # What it contains
//!
//! - [`PortBlock`]: fifty neighbouring ports. [`PortBlock::claim`] takes a block that nobody holds, and
//!   [`PortBlock::take`] hands out its ports one at a time.
//! - [`take_shared_port`]: the next port of one block that a whole test binary shares.
//! - `guard_port_of` and `block_tried_at`: where a block is, and in which order blocks are tried.
//!
//! # How it works
//!
//! The ports from 29000 to 30849 are cut into thirty-seven blocks of fifty. The range lies below the ports
//! that Linux (32768 and up) and macOS (49152 and up) choose from when a program asks for any port, so only
//! a program that names a port can ever be in it.
//!
//! The first port of a block is its guard port. To claim a block is to listen on its guard port on the
//! loopback address, and to keep listening: the operating system lets only one socket do that, in whatever
//! process it is, so it is the operating system that keeps two owners apart. A claim tries the blocks one
//! after the other until a guard port can be bound. It starts at a block worked out from the process
//! identifier, so that test binaries that start together begin at different blocks and seldom meet. When the
//! block is dropped, or its process ends in any way, the listener closes and the block is free.
//!
//! The other forty-nine ports are handed out in order. Each is bound and closed again first, and passed over
//! if that fails: a server left behind by a test that was killed may still be listening there.
//!
//! Three test suites of the repository take ports this way, each from a range of its own, so that they need
//! no common rule for claiming:
//!
//! | Range | Used by |
//! | --- | --- |
//! | 20000 to 28999 | `development/cluster_test.sh` |
//! | 29000 to 30849 | Rust tests, through this file |
//! | 30850 to 32699 | Go tests, through `src/go/internal/testcluster/ports.go` |
//!
//! # Invariants
//!
//! - While a [`PortBlock`] lives, no other [`PortBlock`] in any process hands out one of its ports.
//! - A block hands out each of its ports at most once, and never its guard port.
//! - A port that is handed out could be bound a moment before.
//! - A server that is given a port of a block must listen on the loopback address `127.0.0.1`, as every
//!   server of the tests does. The guard only keeps out those who bind the same address.
//! - There are thirty-seven blocks for all the Rust tests that run on a machine at one moment. A claim that
//!   finds none panics; it does not wait.
//!
//! # See also
//!
//! - `bitacora-clusterkit/src/cluster.rs`: a cluster of real daemons, which keeps a block while it runs.
//! - The etcd and S3 test harnesses (`tests/etcd/etcd_process.rs`, `tests/s3/s3_server.rs`): users of the
//!   shared block.

// Production code reaches the network through the injected interfaces, which is why clippy forbids
// `TcpListener`. Claiming a port is asking the operating system for a socket, and there is no interface for
// that; nothing but tests uses this crate.
#![allow(
    clippy::disallowed_types,
    reason = "claiming a port is binding a socket, and only tests use this crate"
)]

use std::net::{Ipv4Addr, TcpListener};
use std::sync::{Mutex, PoisonError};

/// The first port of the range the blocks are cut from.
const FIRST_PORT: u16 = 29_000;

/// How many ports a block has, its guard port included. A cluster of three replica daemons, a controller
/// and an etcd needs nine.
const BLOCK_PORT_COUNT: u16 = 50;

/// How many blocks there are. The last one ends at port 30849, right below the range of the Go tests.
const BLOCK_COUNT: u16 = 37;

/// The block that the tests of this process share, once one of them has asked for a port. It is never
/// dropped: the operating system frees it when the process ends.
static SHARED_BLOCK: Mutex<Option<PortBlock>> = Mutex::new(None);

/// Fifty neighbouring loopback ports that belong to one owner.
///
/// Claim a block when a test starts a group of servers, take a port for each, and keep the block until the
/// servers are gone: as long as it lives, no other test on the machine is given one of its ports. A test
/// binary whose tests need a port or two each should use [`take_shared_port`] instead, which costs the
/// machine one block for the whole binary.
///
/// # Examples
///
/// ```
/// use std::net::TcpListener;
///
/// use bitacora_testports::PortBlock;
///
/// let mut ports = PortBlock::claim();
/// let port = ports.take();
///
/// // What a server that is told to listen on the port does.
/// let server = TcpListener::bind(("127.0.0.1", port));
///
/// assert!(server.is_ok());
/// ```
#[derive(Debug)]
pub struct PortBlock {
    /// The listener on the block's guard port. It is never accepted from; it is open so that nobody else can
    /// claim the block.
    _guard: TcpListener,
    /// Which block this is, counted from zero at the start of the range.
    block_index: u16,
    /// How many of the block's ports were looked at so far, handed out or passed over. The next one to look
    /// at lies this far above the first port after the guard port.
    ports_tried: u16,
}

impl PortBlock {
    /// Claims a block that nobody holds.
    ///
    /// Tries to listen on the guard port of one block after the other, starting at a block worked out from
    /// the process identifier, until the operating system allows it.
    ///
    /// # Returns
    ///
    /// The block, with all its forty-nine ports still to hand out. It is free again once it is dropped.
    ///
    /// # Panics
    ///
    /// Panics if every block is claimed: more tests that start servers run at once than there are blocks,
    /// or other programs listen on the guard ports.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_testports::PortBlock;
    ///
    /// let mut ports = PortBlock::claim();
    /// let client_port = ports.take();
    /// let peer_port = ports.take();
    ///
    /// assert_ne!(client_port, peer_port);
    /// ```
    #[must_use]
    pub fn claim() -> Self {
        Self::claim_starting_at(first_block_of_this_process())
    }

    /// Claims the first block that nobody holds, trying `first_block_index` first and going round the range
    /// from there. [`PortBlock::claim`] is this with a start that depends on the process; tests name the
    /// start.
    ///
    /// # Panics
    ///
    /// Panics if every block is claimed.
    fn claim_starting_at(first_block_index: u16) -> Self {
        for step in 0..BLOCK_COUNT {
            let block_index = block_tried_at(first_block_index, step);
            if let Ok(guard) = TcpListener::bind((Ipv4Addr::LOCALHOST, guard_port_of(block_index))) {
                return Self {
                    _guard: guard,
                    block_index,
                    ports_tried: 0,
                };
            }
        }
        panic!(
            "all {BLOCK_COUNT} blocks of test ports ({FIRST_PORT} to {last_port}) are claimed: more tests that \
             start servers run at once than there are blocks, or other programs listen in that range",
            last_port = guard_port_of(BLOCK_COUNT - 1) + BLOCK_PORT_COUNT - 1,
        );
    }

    /// Hands out the next port of the block that nothing is listening on.
    ///
    /// Each port is bound and closed again before it is handed out, and passed over if that fails.
    ///
    /// # Returns
    ///
    /// A loopback port that could be bound a moment ago, and that this block hands out to nobody else. No
    /// other block hands it out while this one lives.
    ///
    /// # Panics
    ///
    /// Panics once the block has no port left: each of its forty-nine was handed out or is in use by
    /// another program.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_testports::PortBlock;
    ///
    /// let mut ports = PortBlock::claim();
    ///
    /// let grpc_address = format!("127.0.0.1:{}", ports.take());
    /// let metrics_address = format!("127.0.0.1:{}", ports.take());
    ///
    /// assert_ne!(grpc_address, metrics_address);
    /// ```
    pub fn take(&mut self) -> u16 {
        let guard_port = guard_port_of(self.block_index);
        while self.ports_tried < BLOCK_PORT_COUNT - 1 {
            self.ports_tried += 1;
            let port = guard_port + self.ports_tried;
            if TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok() {
                return port;
            }
        }
        panic!(
            "the block of test ports that starts at {guard_port} has no port left: it had {port_count}, and \
             each was handed out or is in use by another program",
            port_count = BLOCK_PORT_COUNT - 1,
        );
    }
}

/// Hands out the next port of the block that the tests of this process share.
///
/// The first call claims the block, and the process keeps it until it ends. This is what a test binary
/// uses whose tests each start a server or need a port that nothing listens on: one block for all of them,
/// instead of one per test, of which the machine has only thirty-seven.
///
/// # Returns
///
/// A loopback port that could be bound a moment ago, and that no other test of this process or of any other
/// is given while this process runs.
///
/// # Panics
///
/// Panics if no block can be claimed, or once the shared block has no port left (see [`PortBlock::take`]).
/// A test binary that needs more than forty-nine ports must claim blocks of its own.
///
/// # Examples
///
/// ```
/// use bitacora_testports::take_shared_port;
///
/// let endpoint = format!("http://127.0.0.1:{}", take_shared_port());
/// let other_endpoint = format!("http://127.0.0.1:{}", take_shared_port());
///
/// assert_ne!(endpoint, other_endpoint);
/// ```
#[must_use]
pub fn take_shared_port() -> u16 {
    // A test that panicked while it took a port leaves nothing half done behind, so the others go on.
    let mut shared_block = SHARED_BLOCK.lock().unwrap_or_else(PoisonError::into_inner);
    shared_block.get_or_insert_with(PortBlock::claim).take()
}

/// The guard port of a block: its first port, which the owner of the block listens on and nobody is given.
const fn guard_port_of(block_index: u16) -> u16 {
    FIRST_PORT + block_index * BLOCK_PORT_COUNT
}

/// The block a search looks at in its step number `step`, counted from zero, when it starts at
/// `first_block_index`: the blocks in order, going on at the first one after the last.
const fn block_tried_at(first_block_index: u16, step: u16) -> u16 {
    (first_block_index % BLOCK_COUNT + step) % BLOCK_COUNT
}

/// The block at which this process starts to look for a free one. It depends on the process identifier, so
/// that test binaries that start in the same instant begin at different blocks.
fn first_block_of_this_process() -> u16 {
    let block_index = std::process::id() % u32::from(BLOCK_COUNT);
    u16::try_from(block_index).expect("a remainder below the number of blocks fits")
}

#[cfg(test)]
#[path = "port_block_test.rs"]
mod tests;
