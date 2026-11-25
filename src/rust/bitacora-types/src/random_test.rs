//! Tests of the deterministic random generator.
//!
//! # What is covered
//!
//! - The algorithm itself, against published reference values, and that a seed determines everything.
//! - Each kind of draw: that it stays inside its range, reaches all of it, and fails fast on an empty one.
//! - Forking: a fork does not replay its parent.
//!
//! # How the fixtures work
//!
//! Tests that draw many values use [`DRAW_COUNT`] draws from the generator of [`SEED`]. The seed is fixed,
//! so a test that passes once passes always: these are not statistical tests that can fail by bad luck.

use super::*;

/// The seed of every test that does not compare seeds.
const SEED: u64 = 7;

/// How many values a test draws to see a whole small range.
const DRAW_COUNT: usize = 1_000;

/// The generator most tests draw from.
fn random() -> DeterministicRandom {
    DeterministicRandom::from_seed(SEED)
}

/// Published `SplitMix64` outputs for seed 0. If this test fails the algorithm changed and every recorded
/// regression seed lost its meaning.
#[test]
fn output_matches_the_reference_sequence() {
    let mut random = DeterministicRandom::from_seed(0);

    let produced = [random.next_u64(), random.next_u64(), random.next_u64()];

    assert_eq!(
        produced,
        [0xE220_A839_7B1D_CDAF, 0x6E78_9E6A_A1B9_65F4, 0x06C4_5D18_8009_454F]
    );
}

#[test]
fn same_seed_replays_the_same_values() {
    let mut first = DeterministicRandom::from_seed(42);
    let mut second = DeterministicRandom::from_seed(42);

    assert_eq!(first.bytes(64), second.bytes(64));
}

#[test]
fn different_seeds_produce_different_values() {
    let mut first = DeterministicRandom::from_seed(1);
    let mut second = DeterministicRandom::from_seed(2);

    assert_ne!(first.next_u64(), second.next_u64());
}

#[test]
fn below_stays_under_the_bound() {
    let mut random = random();

    for _draw in 0..DRAW_COUNT {
        assert!(random.below(10) < 10);
    }
}

#[test]
fn index_reaches_every_position_of_a_small_collection() {
    let mut random = random();
    let mut was_drawn = [false; 10];

    for _draw in 0..DRAW_COUNT {
        was_drawn[random.index(10)] = true;
    }

    assert_eq!(was_drawn, [true; 10]);
}

#[test]
#[should_panic(expected = "an empty range has no value to draw")]
fn below_zero_fails_fast() {
    let _no_such_value = random().below(0);
}

#[test]
fn between_reaches_its_low_end() {
    let mut random = random();
    let mut drawn = Vec::new();

    for _draw in 0..DRAW_COUNT {
        drawn.push(random.between(3, 5));
    }

    assert!(drawn.contains(&3));
}

#[test]
fn between_reaches_its_high_end() {
    let mut random = random();
    let mut drawn = Vec::new();

    for _draw in 0..DRAW_COUNT {
        drawn.push(random.between(3, 5));
    }

    assert!(drawn.contains(&5));
}

#[test]
fn between_never_leaves_its_range() {
    let mut random = random();

    for _draw in 0..DRAW_COUNT {
        let drawn = random.between(3, 5);

        assert!((3..=5).contains(&drawn), "{drawn}");
    }
}

#[test]
fn between_of_one_value_returns_that_value() {
    let drawn = random().between(4, 4);

    assert_eq!(drawn, 4);
}

#[test]
fn between_covers_the_whole_integer_range_without_overflow() {
    let _any_value = random().between(0, u64::MAX);
}

#[test]
#[should_panic(expected = "range 5..=3 is empty")]
fn between_with_its_ends_swapped_fails_fast() {
    let _no_such_value = random().between(5, 3);
}

#[test]
fn chance_of_zero_never_happens() {
    let mut random = random();

    for _draw in 0..DRAW_COUNT {
        assert!(!random.chance(0, 100));
    }
}

#[test]
fn chance_of_one_always_happens() {
    let mut random = random();

    for _draw in 0..DRAW_COUNT {
        assert!(random.chance(100, 100));
    }
}

#[test]
fn choose_from_nothing_returns_nothing() {
    let chosen = random().choose::<u8>(&[]);

    assert_eq!(chosen, None);
}

#[test]
fn choose_returns_one_of_the_items() {
    let items = ["a", "b", "c"];

    let chosen = random().choose(&items).copied();

    assert!(chosen.is_some_and(|item| items.contains(&item)), "{chosen:?}");
}

#[test]
fn bytes_returns_exactly_the_requested_length() {
    let filled = random().bytes(13);

    assert_eq!(filled.len(), 13);
}

#[test]
fn no_bytes_is_an_empty_vector() {
    let filled = random().bytes(0);

    assert_eq!(filled, Vec::<u8>::new());
}

#[test]
fn fork_does_not_replay_its_parent() {
    let mut parent = random();
    let mut child = parent.fork();

    assert_ne!(parent.next_u64(), child.next_u64());
}

#[test]
fn two_forks_of_one_generator_differ() {
    let mut parent = random();
    let mut first_fork = parent.fork();
    let mut second_fork = parent.fork();

    assert_ne!(first_fork.next_u64(), second_fork.next_u64());
}
