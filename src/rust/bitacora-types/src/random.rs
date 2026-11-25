//! The only random source bitacora uses.
//!
//! Randomness is injected like the clock and the disk (design, section 15):
//! a component that needs it receives a [`DeterministicRandom`] built from a
//! seed, so the same seed always replays the same run. The generator lives
//! in-tree, not in a dependency, because a seed that once found a bug is kept
//! forever as a regression test and must keep producing the same values.

/// A seeded pseudo-random generator (`SplitMix64`).
///
/// It is fast and statistically sound for simulation, fault injection and
/// jitter. It is not suitable for anything that needs unpredictability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeterministicRandom {
    state: u64,
}

impl DeterministicRandom {
    const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
    const FIRST_MIX_MULTIPLIER: u64 = 0xBF58_476D_1CE4_E5B9;
    const SECOND_MIX_MULTIPLIER: u64 = 0x94D0_49BB_1331_11EB;

    /// Builds a generator whose whole output is determined by `seed`.
    #[must_use]
    pub const fn from_seed(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64 uniformly distributed bits.
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(Self::GOLDEN_GAMMA);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(Self::FIRST_MIX_MULTIPLIER);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(Self::SECOND_MIX_MULTIPLIER);
        mixed ^ (mixed >> 31)
    }

    /// Returns a value in `0..bound`.
    ///
    /// # Panics
    ///
    /// Panics if `bound` is zero, because the range would be empty.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "an empty range has no value to draw");
        // Multiply-shift maps 64 random bits onto the range without the
        // modulo bias toward small values.
        let widened = u128::from(self.next_u64()) * u128::from(bound);
        u64::try_from(widened >> 64).expect("the high half of a 128-bit product fits in 64 bits")
    }

    /// Returns a value in `low..=high`.
    ///
    /// # Panics
    ///
    /// Panics if `low` is greater than `high`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "range {low}..={high} is empty");
        match (high - low).checked_add(1) {
            Some(span) => low + self.below(span),
            None => self.next_u64(),
        }
    }

    /// Returns `true` with probability `numerator / denominator`.
    ///
    /// # Panics
    ///
    /// Panics if `denominator` is zero.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }

    /// Returns a value in `0..length`, for indexing a collection.
    ///
    /// # Panics
    ///
    /// Panics if `length` is zero, because there is nothing to index.
    pub fn index(&mut self, length: usize) -> usize {
        let bound = u64::try_from(length).expect("collection lengths fit in 64 bits");
        usize::try_from(self.below(bound)).expect("a value below a usize bound fits in usize")
    }

    /// Picks one item, or `None` if there are no items.
    pub fn choose<'items, Item>(&mut self, items: &'items [Item]) -> Option<&'items Item> {
        if items.is_empty() {
            return None;
        }
        items.get(self.index(items.len()))
    }

    /// Returns `length_bytes` random bytes.
    pub fn bytes(&mut self, length_bytes: usize) -> Vec<u8> {
        let mut filled = Vec::with_capacity(length_bytes);
        while filled.len() < length_bytes {
            let remaining = length_bytes - filled.len();
            let word = self.next_u64().to_le_bytes();
            filled.extend_from_slice(&word[..remaining.min(word.len())]);
        }
        filled
    }

    /// Splits off an independent generator.
    ///
    /// Giving each simulated component its own fork keeps one component's
    /// draws from shifting another's when the code between them changes.
    #[must_use]
    pub const fn fork(&mut self) -> Self {
        Self::from_seed(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published `SplitMix64` outputs for seed 0. If this test fails the
    /// algorithm changed and every recorded regression seed lost its meaning.
    #[test]
    fn output_matches_the_reference_sequence() {
        let mut random = DeterministicRandom::from_seed(0);
        let produced = [random.next_u64(), random.next_u64(), random.next_u64()];
        assert_eq!(
            produced,
            [
                0xE220_A839_7B1D_CDAF,
                0x6E78_9E6A_A1B9_65F4,
                0x06C4_5D18_8009_454F
            ]
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
        let mut random = DeterministicRandom::from_seed(7);
        assert!((0..1_000).all(|_| random.below(10) < 10));
    }

    #[test]
    fn below_reaches_every_value_of_a_small_range() {
        let mut random = DeterministicRandom::from_seed(7);
        let mut was_drawn = [false; 10];
        for _ in 0..1_000 {
            was_drawn[random.index(10)] = true;
        }
        assert_eq!(was_drawn, [true; 10]);
    }

    #[test]
    #[should_panic(expected = "an empty range has no value to draw")]
    fn below_zero_fails_fast() {
        let _ = DeterministicRandom::from_seed(7).below(0);
    }

    #[test]
    fn between_includes_both_ends() {
        let mut random = DeterministicRandom::from_seed(7);
        let drawn: Vec<u64> = (0..200).map(|_| random.between(3, 5)).collect();
        assert!(drawn.contains(&3) && drawn.contains(&5) && drawn.iter().all(|value| *value <= 5));
    }

    #[test]
    fn between_covers_the_whole_integer_range_without_overflow() {
        let mut random = DeterministicRandom::from_seed(7);
        let _ = random.between(0, u64::MAX);
    }

    #[test]
    fn chance_of_zero_never_happens() {
        let mut random = DeterministicRandom::from_seed(7);
        assert!((0..1_000).all(|_| !random.chance(0, 100)));
    }

    #[test]
    fn chance_of_one_always_happens() {
        let mut random = DeterministicRandom::from_seed(7);
        assert!((0..1_000).all(|_| random.chance(100, 100)));
    }

    #[test]
    fn choose_from_nothing_returns_nothing() {
        let mut random = DeterministicRandom::from_seed(7);
        assert_eq!(random.choose::<u8>(&[]), None);
    }

    #[test]
    fn choose_returns_one_of_the_items() {
        let mut random = DeterministicRandom::from_seed(7);
        let items = ["a", "b", "c"];
        assert!(items.contains(random.choose(&items).expect("items are not empty")));
    }

    #[test]
    fn bytes_returns_exactly_the_requested_length() {
        let mut random = DeterministicRandom::from_seed(7);
        assert_eq!(random.bytes(13).len(), 13);
    }

    #[test]
    fn fork_does_not_replay_its_parent() {
        let mut parent = DeterministicRandom::from_seed(7);
        let mut child = parent.fork();
        assert_ne!(parent.next_u64(), child.next_u64());
    }
}
