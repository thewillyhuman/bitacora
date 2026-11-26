//! The samples of one series as the compressed bytes a block stores.
//!
//! # Why this file exists
//!
//! A block holds every sample of every series of a stretch of the log. Stored plainly a sample is sixteen
//! bytes. Real series are far more regular than that: samples arrive at a steady interval, and a value
//! changes little or not at all from one sample to the next. This file stores the samples of one series
//! the way Facebook's Gorilla paper describes and Prometheus adopted: timestamps as the change of their
//! interval, values as the bits that differ from the previous value. A steady series with a constant
//! value costs two bits a sample.
//!
//! # What it contains
//!
//! - [`encode_chunk`]: the samples of one series as bytes.
//! - [`ChunkDecoder`]: the samples back out, one at a time, without allocating.
//! - [`decode_chunk`] and [`decode_chunk_in_range`]: the whole chunk, or the part in a time range.
//! - [`DamagedChunk`]: why bytes are not a chunk.
//!
//! # How it works
//!
//! ```text
//! chunk          sample count u32, little-endian | bit stream, padded with zero bits to a whole byte
//! bit stream     first sample | later sample*
//! first sample   timestamp, 64 bits | value, 64 bits
//! later sample   timestamp code | value code
//! ```
//!
//! Bits are written most significant first. A value is the 64 bits of an IEEE 754 double.
//!
//! **Timestamp code.** Let `delta` be this timestamp minus the previous one, and `dod` this delta minus
//! the previous delta (the delta before the second sample counts as zero). All arithmetic wraps, so any
//! two timestamps can follow each other. `dod` is written as:
//!
//! | `dod` | Bits |
//! | --- | --- |
//! | 0 | `0` |
//! | -8191 to 8192 | `10`, then `dod` in 14 bits |
//! | -65535 to 65536 | `110`, then `dod` in 17 bits |
//! | -524287 to 524288 | `1110`, then `dod` in 20 bits |
//! | anything else | `1111`, then `dod` in 64 bits |
//!
//! An n-bit `dod` is its low n bits in two's complement. Reading it back, a number greater than
//! 2^(n-1) has 2^n subtracted, which is why each range reaches one further up than down.
//!
//! **Value code.** Let `xor` be the bits of this value XOR the bits of the previous one.
//!
//! | `xor` | Bits |
//! | --- | --- |
//! | 0 | `0` |
//! | fits the previous window | `10`, then the bits of the window |
//! | anything else | `11`, then leading zeros in 5 bits, window length in 6 bits, then the bits of the window |
//!
//! The window is the run of bits from the first to the last one-bit of `xor`. Its leading-zero count is
//! capped at 31 so that it fits five bits, and its length is stored modulo 64, so a length of 64 is
//! stored as 0. A later `xor` "fits the previous window" if it has at least as many leading and trailing
//! zeros as the window last written in full; it is then written in that same window, without the eleven
//! bits that describe it. There is no previous window before the first `11` code.
//!
//! This is a hot path: every sample of every block passes through here once on the way in and once per
//! query on the way out. Encoding appends bits to one growing vector; decoding walks a borrowed slice.
//! Neither allocates per sample.
//!
//! # Invariants
//!
//! - Decoding what was encoded returns the same samples, bit for bit, in the same order, whatever the
//!   timestamps and values are.
//! - Encoding is deterministic: equal samples give equal bytes.
//! - Decoding bytes that are not a chunk returns an error. It never panics and never allocates more
//!   than the bytes could hold.
//!
//! # See also
//!
//! - `block.rs`: where chunks are stored, each with its own checksum.
//! - `sample.rs`: the samples themselves.

use thiserror::Error;

use crate::sample::{Sample, TimeRange};

/// The size of the sample count in front of the bit stream.
const SAMPLE_COUNT_BYTES: usize = 4;

/// The widths of the three short timestamp codes, in bits.
const SHORT_DOD_BITS: [u8; 3] = [14, 17, 20];

/// The lengths of the prefixes `10`, `110` and `1110` that announce the three short timestamp codes.
const SHORT_DOD_PREFIX_BITS: [u8; 3] = [2, 3, 4];

/// The largest leading-zero count a value code can state: what five bits hold.
const MAX_STATED_LEADING_ZEROS: u32 = 31;

/// Why bytes are not the chunk of a series.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("chunk is damaged: {reason}")]
pub struct DamagedChunk {
    /// What is wrong with the bytes.
    pub reason: &'static str,
}

/// Appends bits to a byte vector, most significant bit first.
struct BitWriter {
    /// The bytes written so far. The last one may be partly filled.
    bytes: Vec<u8>,
    /// How many bits of the last byte are still free. Zero if there is no byte or the last one is full.
    free_bits_in_last_byte: u8,
}

impl BitWriter {
    /// Starts writing after `prefix`, which is kept as the first bytes.
    fn after(prefix: Vec<u8>) -> Self {
        Self {
            bytes: prefix,
            free_bits_in_last_byte: 0,
        }
    }

    /// Appends the low `bit_count` bits of `value`, most significant first.
    fn write_bits(&mut self, value: u64, bit_count: u8) {
        let mut bits_left = bit_count;
        while bits_left > 0 {
            if self.free_bits_in_last_byte == 0 {
                self.bytes.push(0);
                self.free_bits_in_last_byte = 8;
            }
            let taken_bits = bits_left.min(self.free_bits_in_last_byte);
            let taken = (value >> (bits_left - taken_bits)) & ((1_u64 << taken_bits) - 1);
            let taken = u8::try_from(taken).expect("at most eight bits were taken");
            let last_byte = self.bytes.last_mut().expect("a byte was just ensured");
            *last_byte |= taken << (self.free_bits_in_last_byte - taken_bits);
            self.free_bits_in_last_byte -= taken_bits;
            bits_left -= taken_bits;
        }
    }

    /// The bytes written, the last one padded with zero bits.
    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Reads bits from a byte slice, most significant bit first.
struct BitReader<'bytes> {
    /// The bytes being read.
    bytes: &'bytes [u8],
    /// How many bits were read so far.
    position_bits: usize,
}

impl BitReader<'_> {
    /// Reads the next `bit_count` bits as a number.
    ///
    /// # Returns
    ///
    /// `None` if fewer than `bit_count` bits are left; nothing is consumed then.
    fn read_bits(&mut self, bit_count: u8) -> Option<u64> {
        if self.position_bits + usize::from(bit_count) > self.bytes.len() * 8 {
            return None;
        }
        let mut value = 0_u64;
        let mut bits_left = bit_count;
        while bits_left > 0 {
            let byte = self.bytes[self.position_bits / 8];
            let offset_in_byte = u8::try_from(self.position_bits % 8).expect("less than eight");
            let available_bits = 8 - offset_in_byte;
            let taken_bits = bits_left.min(available_bits);
            let taken = (u16::from(byte) >> (available_bits - taken_bits)) & ((1_u16 << taken_bits) - 1);
            // At most eight bits are taken at a time, so the shift is always defined.
            value = (value << taken_bits) | u64::from(taken);
            self.position_bits += usize::from(taken_bits);
            bits_left -= taken_bits;
        }
        Some(value)
    }

    /// Reads one bit.
    fn read_bit(&mut self) -> Option<bool> {
        self.read_bits(1).map(|bit| bit == 1)
    }
}

/// The window of bits a value code was last written in.
#[derive(Clone, Copy)]
struct ValueWindow {
    /// How many zero bits come before the window.
    leading_zeros: u32,
    /// How many zero bits come after the window.
    trailing_zeros: u32,
}

impl ValueWindow {
    /// How many bits the window holds.
    const fn length_bits(self) -> u32 {
        64 - self.leading_zeros - self.trailing_zeros
    }
}

/// What the encoder and the decoder both remember of the previous sample.
#[derive(Clone, Copy)]
struct PreviousSample {
    /// The previous timestamp.
    timestamp_millis: i64,
    /// The previous timestamp minus the one before it; zero after the first sample.
    delta_millis: i64,
    /// The bits of the previous value.
    value_bits: u64,
    /// The window of the last value code written in full, if any.
    window: Option<ValueWindow>,
}

/// Whether `delta_of_delta` fits a short timestamp code of `bit_count` bits.
const fn fits_in_bits(delta_of_delta: i64, bit_count: u8) -> bool {
    let reach = 1_i64 << (bit_count - 1);
    -(reach - 1) <= delta_of_delta && delta_of_delta <= reach
}

/// Encodes the samples of one series as a chunk.
///
/// The samples are stored in the order given. Any order works; samples that are strictly increasing in
/// time at a steady interval compress best, and that is what the rest of the crate passes in.
///
/// # Arguments
///
/// * `samples` - The samples of one series. May be empty.
///
/// # Returns
///
/// The chunk's bytes.
pub(crate) fn encode_chunk(samples: &[Sample]) -> Vec<u8> {
    let sample_count = u32::try_from(samples.len()).expect("a series of one block has far fewer than 2^32 samples");
    let mut writer = BitWriter::after(sample_count.to_le_bytes().to_vec());
    let Some(first_sample) = samples.first() else {
        return writer.into_bytes();
    };
    #[allow(clippy::cast_sign_loss)]
    writer.write_bits(first_sample.timestamp_millis as u64, 64);
    writer.write_bits(first_sample.value.to_bits(), 64);
    let mut previous = PreviousSample {
        timestamp_millis: first_sample.timestamp_millis,
        delta_millis: 0,
        value_bits: first_sample.value.to_bits(),
        window: None,
    };
    for sample in &samples[1..] {
        write_timestamp(&mut writer, &mut previous, sample.timestamp_millis);
        write_value(&mut writer, &mut previous, sample.value.to_bits());
    }
    writer.into_bytes()
}

/// Writes the timestamp code of one sample and remembers the sample's timestamp and delta.
fn write_timestamp(writer: &mut BitWriter, previous: &mut PreviousSample, timestamp_millis: i64) {
    let delta_millis = timestamp_millis.wrapping_sub(previous.timestamp_millis);
    let delta_of_delta = delta_millis.wrapping_sub(previous.delta_millis);
    previous.timestamp_millis = timestamp_millis;
    previous.delta_millis = delta_millis;

    #[allow(clippy::cast_sign_loss)]
    let delta_of_delta_bits = delta_of_delta as u64;
    if delta_of_delta == 0 {
        writer.write_bits(0b0, 1);
        return;
    }
    for (short_bits, prefix_bits) in SHORT_DOD_BITS.into_iter().zip(SHORT_DOD_PREFIX_BITS) {
        if fits_in_bits(delta_of_delta, short_bits) {
            // The prefixes `10`, `110` and `1110`: ones, then one zero.
            writer.write_bits((1_u64 << prefix_bits) - 2, prefix_bits);
            writer.write_bits(delta_of_delta_bits & ((1_u64 << short_bits) - 1), short_bits);
            return;
        }
    }
    writer.write_bits(0b1111, 4);
    writer.write_bits(delta_of_delta_bits, 64);
}

/// Writes the value code of one sample and remembers the sample's value and window.
fn write_value(writer: &mut BitWriter, previous: &mut PreviousSample, value_bits: u64) {
    let xor = value_bits ^ previous.value_bits;
    previous.value_bits = value_bits;
    if xor == 0 {
        writer.write_bits(0b0, 1);
        return;
    }
    let leading_zeros = xor.leading_zeros().min(MAX_STATED_LEADING_ZEROS);
    let trailing_zeros = xor.trailing_zeros();
    if let Some(window) = previous.window
        && leading_zeros >= window.leading_zeros
        && trailing_zeros >= window.trailing_zeros
    {
        writer.write_bits(0b10, 2);
        writer.write_bits(xor >> window.trailing_zeros, bit_count_of(window.length_bits()));
        return;
    }
    let window = ValueWindow {
        leading_zeros,
        trailing_zeros,
    };
    writer.write_bits(0b11, 2);
    writer.write_bits(u64::from(leading_zeros), 5);
    // A window of 64 bits is stored as 0: six bits hold 0 to 63, and a window is never empty.
    writer.write_bits(u64::from(window.length_bits() % 64), 6);
    writer.write_bits(xor >> trailing_zeros, bit_count_of(window.length_bits()));
    previous.window = Some(window);
}

/// A window length as the bit count the reader and writer take.
fn bit_count_of(length_bits: u32) -> u8 {
    u8::try_from(length_bits).expect("a window is at most 64 bits")
}

/// Reads the samples of a chunk one at a time.
///
/// It borrows the chunk and allocates nothing.
pub(crate) struct ChunkDecoder<'chunk> {
    /// The bit stream after the sample count.
    reader: BitReader<'chunk>,
    /// How many samples the chunk says it holds and were not read yet.
    samples_left: u32,
    /// The previous sample, once the first one was read.
    previous: Option<PreviousSample>,
}

impl<'chunk> ChunkDecoder<'chunk> {
    /// Starts reading `chunk`.
    ///
    /// # Errors
    ///
    /// Returns [`DamagedChunk`] if the bytes are too short to hold the sample count, or the count is more
    /// than the bytes could possibly hold.
    pub(crate) fn new(chunk: &'chunk [u8]) -> Result<Self, DamagedChunk> {
        let Some((count_bytes, bit_stream)) = chunk.split_at_checked(SAMPLE_COUNT_BYTES) else {
            return Err(DamagedChunk {
                reason: "it is too short to hold its sample count",
            });
        };
        let sample_count = u32::from_le_bytes(count_bytes.try_into().expect("four bytes were split off"));
        // The first sample takes 128 bits and every later one at least two.
        let most_samples_that_fit = if bit_stream.len() < 16 {
            0
        } else {
            1 + (bit_stream.len() - 16) * 4
        };
        if sample_count as usize > most_samples_that_fit {
            return Err(DamagedChunk {
                reason: "it states more samples than its bytes can hold",
            });
        }
        Ok(Self {
            reader: BitReader {
                bytes: bit_stream,
                position_bits: 0,
            },
            samples_left: sample_count,
            previous: None,
        })
    }

    /// How many samples are still to be read.
    pub(crate) const fn samples_left(&self) -> u32 {
        self.samples_left
    }

    /// Reads the next sample.
    ///
    /// # Returns
    ///
    /// The sample, or `None` once every sample the chunk holds was read.
    ///
    /// # Errors
    ///
    /// Returns [`DamagedChunk`] if the bit stream ends before the samples do or holds a code that cannot
    /// be.
    pub(crate) fn next_sample(&mut self) -> Result<Option<Sample>, DamagedChunk> {
        if self.samples_left == 0 {
            return Ok(None);
        }
        let cut_short = DamagedChunk {
            reason: "its bit stream ends before its samples do",
        };
        let sample = match self.previous {
            None => {
                let timestamp_bits = self.reader.read_bits(64).ok_or(cut_short)?;
                let value_bits = self.reader.read_bits(64).ok_or(cut_short)?;
                #[allow(clippy::cast_possible_wrap)]
                let timestamp_millis = timestamp_bits as i64;
                self.previous = Some(PreviousSample {
                    timestamp_millis,
                    delta_millis: 0,
                    value_bits,
                    window: None,
                });
                Sample {
                    timestamp_millis,
                    value: f64::from_bits(value_bits),
                }
            }
            Some(mut previous) => {
                let timestamp_millis = read_timestamp(&mut self.reader, &mut previous).ok_or(cut_short)?;
                let value_bits = read_value(&mut self.reader, &mut previous)?;
                self.previous = Some(previous);
                Sample {
                    timestamp_millis,
                    value: f64::from_bits(value_bits),
                }
            }
        };
        self.samples_left -= 1;
        Ok(Some(sample))
    }
}

/// Reads one timestamp code and returns the timestamp it stands for. `None` if the bits run out.
fn read_timestamp(reader: &mut BitReader<'_>, previous: &mut PreviousSample) -> Option<i64> {
    let mut delta_of_delta = 0_i64;
    if reader.read_bit()? {
        // Count the further one-bits of the prefix, up to `1111`, to learn which code follows.
        let mut code = 0;
        while code < SHORT_DOD_BITS.len() && reader.read_bit()? {
            code += 1;
        }
        if let Some(short_bits) = SHORT_DOD_BITS.get(code) {
            let stored = reader.read_bits(*short_bits)?;
            let stored = i64::try_from(stored).expect("at most twenty bits");
            delta_of_delta = stored;
            if stored > (1_i64 << (short_bits - 1)) {
                delta_of_delta = stored - (1_i64 << short_bits);
            }
        } else {
            #[allow(clippy::cast_possible_wrap)]
            let wide = reader.read_bits(64)? as i64;
            delta_of_delta = wide;
        }
    }
    let delta_millis = previous.delta_millis.wrapping_add(delta_of_delta);
    let timestamp_millis = previous.timestamp_millis.wrapping_add(delta_millis);
    previous.delta_millis = delta_millis;
    previous.timestamp_millis = timestamp_millis;
    Some(timestamp_millis)
}

/// Reads one value code and returns the bits of the value it stands for.
fn read_value(reader: &mut BitReader<'_>, previous: &mut PreviousSample) -> Result<u64, DamagedChunk> {
    let cut_short = DamagedChunk {
        reason: "its bit stream ends before its samples do",
    };
    if !reader.read_bit().ok_or(cut_short)? {
        return Ok(previous.value_bits);
    }
    let window = if reader.read_bit().ok_or(cut_short)? {
        let leading_zeros = u32::try_from(reader.read_bits(5).ok_or(cut_short)?).expect("five bits");
        let stored_length = u32::try_from(reader.read_bits(6).ok_or(cut_short)?).expect("six bits");
        let length_bits = if stored_length == 0 { 64 } else { stored_length };
        if leading_zeros + length_bits > 64 {
            return Err(DamagedChunk {
                reason: "a value is stated to be wider than 64 bits",
            });
        }
        let window = ValueWindow {
            leading_zeros,
            trailing_zeros: 64 - leading_zeros - length_bits,
        };
        previous.window = Some(window);
        window
    } else {
        match previous.window {
            Some(window) => window,
            None => {
                return Err(DamagedChunk {
                    reason: "a value refers to a window that was never stated",
                });
            }
        }
    };
    let window_bits = reader.read_bits(bit_count_of(window.length_bits())).ok_or(cut_short)?;
    let value_bits = previous.value_bits ^ (window_bits << window.trailing_zeros);
    previous.value_bits = value_bits;
    Ok(value_bits)
}

/// Decodes every sample of a chunk.
///
/// # Errors
///
/// Returns [`DamagedChunk`] if the bytes are not a chunk.
pub(crate) fn decode_chunk(chunk: &[u8]) -> Result<Vec<Sample>, DamagedChunk> {
    let mut decoder = ChunkDecoder::new(chunk)?;
    let mut samples = Vec::with_capacity(decoder.samples_left() as usize);
    while let Some(sample) = decoder.next_sample()? {
        samples.push(sample);
    }
    Ok(samples)
}

/// Decodes a chunk and appends the samples that lie in `time_range` to `output`.
///
/// The whole chunk is walked, because a sample can only be decoded from the one before it. A chunk is
/// the samples of one series in one block, so this is bounded by the block size.
///
/// # Arguments
///
/// * `chunk` - The chunk's bytes.
/// * `time_range` - Only samples in this range are kept.
/// * `output` - Where the samples go, in the order the chunk holds them.
///
/// # Errors
///
/// Returns [`DamagedChunk`] if the bytes are not a chunk. `output` may then hold part of the chunk.
pub(crate) fn decode_chunk_in_range(
    chunk: &[u8],
    time_range: TimeRange,
    output: &mut Vec<Sample>,
) -> Result<(), DamagedChunk> {
    let mut decoder = ChunkDecoder::new(chunk)?;
    while let Some(sample) = decoder.next_sample()? {
        if time_range.contains(sample.timestamp_millis) {
            output.push(sample);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "chunk_test.rs"]
mod tests;
