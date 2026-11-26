//! Tests of the blocks of ports.
//!
//! # What is covered
//!
//! - The range: it is the one the documentation and the other test suites of the repository count on.
//! - Claiming: two blocks that live at once are different, a block whose guard port is held is passed over,
//!   and a block is free again once it is dropped.
//! - The order in which blocks are tried: from the one asked for, around the end of the range, each once.
//! - Taking ports: they are different, they lie inside the block, each can be bound, a port in use is
//!   passed over, and a block that has nothing left says so.
//! - The shared block: its ports are different, can be bound, and come from one block.
//!
//! # How the fixtures work
//!
//! The tests use real sockets on the loopback address, because what they check is what the operating system
//! does with them.
//!
//! Every test that claims a block holds [`ONE_AT_A_TIME`] while it runs. A test that drops a block and looks
//! at what becomes of it must not have another test of this binary claim that block in between. It also
//! keeps this binary from holding a handful of the thirty-seven blocks at once, which are shared with every
//! other test on the machine.
//!
//! Tests of other binaries cannot be kept out that way. Where one could get in between two steps of a test,
//! the test says how it deals with that.

use std::sync::{Mutex, MutexGuard, PoisonError};

use super::*;

/// Held by every test of this file that claims a block, for as long as it runs.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// How often a test tries again when a test of another binary may have claimed the block it was looking at.
const ATTEMPTS: usize = 10;

/// Waits until no other test of this file holds a block, and keeps the others waiting until the returned
/// guard is dropped. A test that panicked while it held the guard does not stop the others.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Binds `port` on the loopback address, as a server that was told to listen there would.
fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, port))
}

/// The guard port of a block, held by a plain listener instead of a [`PortBlock`]: what a claim sees when
/// another process owns the block.
struct HeldGuardPort {
    /// The block whose guard port is held.
    block_index: u16,
    /// The listener that holds it. Kept only to keep the port taken.
    _listener: TcpListener,
}

/// Holds the guard port of the first block that nobody has claimed.
fn hold_the_guard_port_of_a_free_block() -> HeldGuardPort {
    for block_index in 0..BLOCK_COUNT {
        if let Ok(listener) = bind(guard_port_of(block_index)) {
            return HeldGuardPort {
                block_index,
                _listener: listener,
            };
        }
    }
    panic!("every block is claimed, so the test has none to hold");
}

/// The block that `port` lies in.
fn block_index_of(port: u16) -> u16 {
    (port - FIRST_PORT) / BLOCK_PORT_COUNT
}

// ---- the range ----

#[test]
fn range_starts_at_the_documented_port() {
    assert_eq!(guard_port_of(0), 29_000);
}

/// The Go tests start at 30850, and Linux hands out ports from 32768 on.
#[test]
fn range_ends_at_the_documented_port() {
    let last_port = guard_port_of(BLOCK_COUNT - 1) + BLOCK_PORT_COUNT - 1;

    assert_eq!(last_port, 30_849);
}

// ---- claiming ----

#[test]
fn two_blocks_alive_at_once_are_different() {
    let _serial = one_at_a_time();
    let first_block = PortBlock::claim();

    let second_block = PortBlock::claim();

    assert_ne!(first_block.block_index, second_block.block_index);
}

#[test]
fn block_whose_guard_port_is_held_is_passed_over() {
    let _serial = one_at_a_time();
    let held = hold_the_guard_port_of_a_free_block();

    let claimed = PortBlock::claim_starting_at(held.block_index);

    assert_ne!(claimed.block_index, held.block_index);
}

/// A test of another binary can claim the block between the drop and the second claim, and then this one
/// gets another block although nothing is wrong. That cannot happen ten times in a row; a block that is not
/// released happens every time.
#[test]
fn dropped_block_can_be_claimed_again() {
    let _serial = one_at_a_time();
    let mut was_claimed_again = false;

    for _attempt in 0..ATTEMPTS {
        let block = PortBlock::claim();
        let block_index = block.block_index;
        drop(block);
        let claimed_again = PortBlock::claim_starting_at(block_index);
        if claimed_again.block_index == block_index {
            was_claimed_again = true;
            break;
        }
    }

    assert!(was_claimed_again);
}

// ---- the order in which blocks are tried ----

#[test]
fn search_starts_at_the_block_it_was_asked_to() {
    assert_eq!(block_tried_at(5, 0), 5);
}

#[test]
fn search_goes_on_at_the_first_block_after_the_last() {
    assert_eq!(block_tried_at(BLOCK_COUNT - 1, 1), 0);
}

#[test]
fn search_tries_every_block_once() {
    let mut tried_blocks = Vec::new();
    for step in 0..BLOCK_COUNT {
        tried_blocks.push(block_tried_at(20, step));
    }

    tried_blocks.sort_unstable();

    let every_block: Vec<u16> = (0..BLOCK_COUNT).collect();
    assert_eq!(tried_blocks, every_block);
}

#[test]
fn search_takes_a_start_beyond_the_range_as_if_the_range_went_round() {
    assert_eq!(block_tried_at(BLOCK_COUNT + 2, 0), 2);
}

// ---- taking ports ----

#[test]
fn ports_of_a_block_are_different_from_each_other() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();
    let mut ports = Vec::new();
    for _port_number in 0..8 {
        ports.push(block.take());
    }

    ports.sort_unstable();
    ports.dedup();

    assert_eq!(ports.len(), 8);
}

#[test]
fn port_of_a_block_lies_inside_the_block() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();

    let port = block.take();

    assert_eq!(block_index_of(port), block.block_index);
}

/// The guard port is the one port of a block that is never handed out: the block's listener holds it.
#[test]
fn guard_port_is_not_handed_out() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();

    let port = block.take();

    assert_ne!(port, guard_port_of(block.block_index));
}

#[test]
fn port_of_a_block_can_be_bound() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();

    let port = block.take();

    let bound = bind(port);
    assert!(bound.is_ok(), "{bound:?}");
}

#[test]
fn port_that_something_else_holds_is_passed_over() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();
    let first_port = guard_port_of(block.block_index) + 1;
    let _other_program = bind(first_port).expect("nothing listens inside a block that was just claimed");

    let port = block.take();

    assert_eq!(port, first_port + 1);
}

#[test]
#[should_panic(expected = "has no port left")]
fn taking_more_ports_than_a_block_holds_panics() {
    let _serial = one_at_a_time();
    let mut block = PortBlock::claim();
    for _port_number in 0..BLOCK_PORT_COUNT - 1 {
        block.take();
    }

    block.take();
}

// ---- the shared block ----

#[test]
fn shared_ports_are_different_from_each_other() {
    let first_port = take_shared_port();

    let second_port = take_shared_port();

    assert_ne!(first_port, second_port);
}

#[test]
fn shared_port_can_be_bound() {
    let port = take_shared_port();

    let bound = bind(port);

    assert!(bound.is_ok(), "{bound:?}");
}

#[test]
fn shared_ports_come_from_one_block() {
    let first_port = take_shared_port();

    let second_port = take_shared_port();

    assert_eq!(block_index_of(first_port), block_index_of(second_port));
}
