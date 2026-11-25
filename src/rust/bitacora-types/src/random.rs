//! The only random source bitacora uses.
//!
//! # Why this file exists
//!
//! bitacora is tested by deterministic simulation (design, section 15): the real code runs against a
//! simulated disk, network and clock, and a failing run must replay exactly from its seed. A single call to
//! the operating system's random source anywhere would break that. So randomness is injected like the clock
//! and the disk: a component that needs it receives a generator built from a seed. The generator lives here
//! and not in a dependency, because a seed that once found a bug is kept forever as a regression test, and
//! it must keep producing the same values whatever happens to a third-party crate.
//!
//! # What it contains
//!
//! - [`DeterministicRandom`]: a seeded generator with the few draws the code needs: raw bits, a value in a
//!   range, a yes or no with a given probability, an index, one item of a slice, bytes, and a fork.
//!
//! # How it works
//!
//! The generator is `SplitMix64`: one 64-bit word of state that advances by a fixed odd constant, and an
//! output function that mixes the state with two multiplications and three shifts. It is a few machine
//! instructions per draw, passes the usual statistical tests, and every seed gives a full-period sequence.
//! Ranges are drawn with a multiply-and-shift instead of a remainder, which avoids the bias toward small
//! values that a remainder has.
//!
//! A component that hands randomness to several sub-components forks one generator per sub-component.
//! Each fork is seeded from one draw of its parent, so adding a draw in one sub-component does not shift
//! the values every other one sees.
//!
//! # Invariants
//!
//! - The whole output of a generator is determined by its seed. Nothing else is read: no clock, no thread
//!   identifier, no address.
//! - The algorithm and its constants never change. `output_matches_the_reference_sequence` holds them to
//!   published values; if it fails, every recorded regression seed has lost its meaning.
//! - It is not suitable for anything that needs unpredictability, such as keys or tokens.
//!
//! # See also
//!
//! - `time.rs`: the injected clock, the other thing a deterministic run must control.
//! - `bitacora-sim`: the simulator, which seeds one generator per run.

/// A seeded pseudo-random generator (`SplitMix64`).
///
/// It is fast and statistically sound for simulation, fault injection and jitter. It is not suitable for
/// anything that needs unpredictability. Two generators built from the same seed produce the same values
/// in the same order, on every machine.
///
/// # Examples
///
/// ```
/// use bitacora_types::DeterministicRandom;
///
/// let mut first_run = DeterministicRandom::from_seed(42);
/// let mut replayed_run = DeterministicRandom::from_seed(42);
///
/// assert_eq!(first_run.between(1, 6), replayed_run.between(1, 6));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeterministicRandom {
    /// The generator's whole state. It advances by [`DeterministicRandom::GOLDEN_GAMMA`] on every draw.
    state: u64,
}

impl DeterministicRandom {
    /// What the state advances by on every draw: 2^64 divided by the golden ratio, made odd. Being odd, it
    /// makes the state visit every 64-bit value before repeating.
    const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
    /// The first multiplier of the output function, from the `SplitMix64` reference implementation.
    const FIRST_MIX_MULTIPLIER: u64 = 0xBF58_476D_1CE4_E5B9;
    /// The second multiplier of the output function, from the `SplitMix64` reference implementation.
    const SECOND_MIX_MULTIPLIER: u64 = 0x94D0_49BB_1331_11EB;
    /// How far the state is shifted before the first multiplication of the output function.
    const FIRST_MIX_SHIFT_BITS: u32 = 30;
    /// How far the intermediate value is shifted before the second multiplication.
    const SECOND_MIX_SHIFT_BITS: u32 = 27;
    /// How far the intermediate value is shifted for the final mix.
    const FINAL_MIX_SHIFT_BITS: u32 = 31;

    /// Builds a generator whose whole output is determined by `seed`.
    ///
    /// # Arguments
    ///
    /// * `seed` - Any number. Every seed is a good seed, zero included.
    ///
    /// # Returns
    ///
    /// A generator at the start of the sequence for that seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(0);
    /// assert_eq!(random.next_u64(), 0xE220_A839_7B1D_CDAF);
    /// ```
    #[must_use]
    pub const fn from_seed(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64 uniformly distributed bits.
    ///
    /// Every other draw is built on this one. It advances the state once.
    ///
    /// # Returns
    ///
    /// The next value of the sequence.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    /// let first_draw = random.next_u64();
    /// let second_draw = random.next_u64();
    ///
    /// assert_ne!(first_draw, second_draw);
    /// ```
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(Self::GOLDEN_GAMMA);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> Self::FIRST_MIX_SHIFT_BITS)).wrapping_mul(Self::FIRST_MIX_MULTIPLIER);
        mixed = (mixed ^ (mixed >> Self::SECOND_MIX_SHIFT_BITS)).wrapping_mul(Self::SECOND_MIX_MULTIPLIER);
        mixed ^ (mixed >> Self::FINAL_MIX_SHIFT_BITS)
    }

    /// Returns a value in `0..bound`.
    ///
    /// # Arguments
    ///
    /// * `bound` - One past the largest value that may be returned. Must not be zero.
    ///
    /// # Returns
    ///
    /// A value from zero up to, but not including, `bound`, each equally likely.
    ///
    /// # Panics
    ///
    /// Panics if `bound` is zero, because the range would be empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    /// assert!(random.below(10) < 10);
    /// ```
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "an empty range has no value to draw");
        // Multiply-shift maps 64 random bits onto the range without the modulo bias toward small values.
        let widened = u128::from(self.next_u64()) * u128::from(bound);
        u64::try_from(widened >> u64::BITS).expect("the high half of a 128-bit product fits in 64 bits")
    }

    /// Returns a value in `low..=high`.
    ///
    /// # Arguments
    ///
    /// * `low` - The smallest value that may be returned.
    /// * `high` - The largest value that may be returned. Must not be below `low`.
    ///
    /// # Returns
    ///
    /// A value from `low` to `high`, both included, each equally likely.
    ///
    /// # Panics
    ///
    /// Panics if `low` is greater than `high`.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    /// let pause_milliseconds = random.between(50, 200);
    ///
    /// assert!((50..=200).contains(&pause_milliseconds));
    /// ```
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "range {low}..={high} is empty");
        match (high - low).checked_add(1) {
            Some(span) => low + self.below(span),
            // The range is every 64-bit value, whose count does not fit in 64 bits.
            None => self.next_u64(),
        }
    }

    /// Returns `true` with probability `numerator / denominator`.
    ///
    /// # Arguments
    ///
    /// * `numerator` - How many outcomes out of `denominator` are `true`. Zero never happens; a value equal
    ///   to or above `denominator` always happens.
    /// * `denominator` - The number of equally likely outcomes. Must not be zero.
    ///
    /// # Returns
    ///
    /// Whether this draw came out `true`.
    ///
    /// # Panics
    ///
    /// Panics if `denominator` is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    ///
    /// assert!(random.chance(100, 100));
    /// assert!(!random.chance(0, 100));
    /// ```
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }

    /// Returns a value in `0..length`, for indexing a collection.
    ///
    /// # Arguments
    ///
    /// * `length` - The length of the collection. Must not be zero.
    ///
    /// # Returns
    ///
    /// An index that is valid for a collection of that length, each equally likely.
    ///
    /// # Panics
    ///
    /// Panics if `length` is zero, because there is nothing to index.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let replicas = ["replica-a", "replica-b", "replica-c"];
    /// let mut random = DeterministicRandom::from_seed(7);
    ///
    /// let crashed_replica = replicas[random.index(replicas.len())];
    /// assert!(replicas.contains(&crashed_replica));
    /// ```
    pub fn index(&mut self, length: usize) -> usize {
        let bound = u64::try_from(length).expect("collection lengths fit in 64 bits");
        usize::try_from(self.below(bound)).expect("a value below a usize bound fits in usize")
    }

    /// Picks one item of a slice.
    ///
    /// # Arguments
    ///
    /// * `items` - The items to pick from. May be empty.
    ///
    /// # Returns
    ///
    /// One of the items, each equally likely, or `None` if there are no items. Nothing is drawn for an
    /// empty slice, so the generator does not advance.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    ///
    /// assert!(random.choose(&["cut", "delay", "lose answers"]).is_some());
    /// assert_eq!(random.choose::<&str>(&[]), None);
    /// ```
    pub fn choose<'items, Item>(&mut self, items: &'items [Item]) -> Option<&'items Item> {
        if items.is_empty() {
            return None;
        }
        items.get(self.index(items.len()))
    }

    /// Returns random bytes.
    ///
    /// Allocates the returned vector. It draws one 64-bit word per eight bytes, rounded up.
    ///
    /// # Arguments
    ///
    /// * `length_bytes` - How many bytes to return. Zero returns an empty vector and draws nothing.
    ///
    /// # Returns
    ///
    /// Exactly `length_bytes` bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut random = DeterministicRandom::from_seed(7);
    /// let payload = random.bytes(13);
    ///
    /// assert_eq!(payload.len(), 13);
    /// ```
    pub fn bytes(&mut self, length_bytes: usize) -> Vec<u8> {
        let mut filled = Vec::with_capacity(length_bytes);
        while filled.len() < length_bytes {
            let remaining_bytes = length_bytes - filled.len();
            let word = self.next_u64().to_le_bytes();
            filled.extend_from_slice(&word[..remaining_bytes.min(word.len())]);
        }
        filled
    }

    /// Splits off an independent generator.
    ///
    /// Giving each simulated component its own fork keeps one component's draws from shifting another's
    /// when the code between them changes. The fork is seeded from one draw of this generator, which
    /// advances once.
    ///
    /// # Returns
    ///
    /// A generator whose output is determined by this generator's seed and by how many draws were made
    /// before the fork.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_types::DeterministicRandom;
    ///
    /// let mut run = DeterministicRandom::from_seed(7);
    /// let mut for_the_workload = run.fork();
    /// let mut for_the_faults = run.fork();
    ///
    /// assert_ne!(for_the_workload.next_u64(), for_the_faults.next_u64());
    /// ```
    #[must_use]
    pub const fn fork(&mut self) -> Self {
        Self::from_seed(self.next_u64())
    }
}

#[cfg(test)]
#[path = "random_test.rs"]
mod tests;
