//! Tests of the used share of a disk.
//!
//! # What is covered
//!
//! [`DiskSpace::used_percent`]: an empty disk, rounding, a disk that reports no capacity, and a disk too
//! large for the arithmetic to be done in 64 bits.
//!
//! The operations of [`Disk`] and [`DiskFile`] are tested per implementation, in `memory_disk_test.rs`
//! and `local_disk_test.rs`.
//!
//! # How the fixtures work
//!
//! There are none: every test builds one [`DiskSpace`].

use super::*;

#[test]
fn empty_disk_uses_zero_percent() {
    let space = DiskSpace {
        capacity_bytes: 1_000,
        free_bytes: 1_000,
    };

    assert_eq!(space.used_percent(), 0);
}

#[test]
fn used_share_rounds_down() {
    let space = DiskSpace {
        capacity_bytes: 1_000,
        free_bytes: 149,
    };

    assert_eq!(space.used_percent(), 85);
}

#[test]
fn full_disk_uses_a_hundred_percent() {
    let space = DiskSpace {
        capacity_bytes: 1_000,
        free_bytes: 0,
    };

    assert_eq!(space.used_percent(), 100);
}

#[test]
fn disk_without_capacity_counts_as_full() {
    let space = DiskSpace {
        capacity_bytes: 0,
        free_bytes: 0,
    };

    assert_eq!(space.used_percent(), 100);
}

#[test]
fn disk_that_reports_more_free_space_than_capacity_counts_as_empty() {
    let space = DiskSpace {
        capacity_bytes: 1_000,
        free_bytes: 2_000,
    };

    assert_eq!(space.used_percent(), 0);
}

#[test]
fn huge_disk_does_not_overflow() {
    let space = DiskSpace {
        capacity_bytes: u64::MAX,
        free_bytes: 0,
    };

    assert_eq!(space.used_percent(), 100);
}
