# The metrics store: records and blocks

`bitacora-metrics` is a time-series store on one stream. This page defines
the bytes it writes: the records it appends to the log and the blocks it
keeps in the object store. A producer or a query node in another language is
written from this page.

All integers are little-endian. Checksums are CRC-32C. Text is UTF-8.

## Data model

A **sample** is a timestamp, in milliseconds since the Unix epoch as a signed
64-bit integer, and a value, an IEEE 754 double.

A **series** is identified by its **label set**: one or more labels, each a
name and a value.

- Labels are sorted by name, byte-wise ascending. No name occurs twice.
- A name and a value are 1 to 1024 bytes each. A set has at most 64 labels.
- A missing label and a label with an empty value mean the same to a query,
  so an empty value is never stored.

Label sets are ordered label by label, name before value, a shorter set
before a longer one it is the start of. Blocks keep series in that order.

A series has at most one sample per timestamp. If the log holds two samples
for the same series and timestamp, **the one in the later record wins**;
within one record, the later one in the record wins. Values are compared and
stored by their 64 bits: a NaN keeps its payload, and `-0.0` is not `0.0`.

## Record, format version 1

One record of the stream is one batch of samples.

```text
record  format version u8 = 1 | series count u32 | series*
series  label count u16 | label* | sample count u32 | sample*
label   name length u16 | name | value length u16 | value
sample  timestamp i64 | value bits u64
```

- A series may occur more than once in a record, and its samples may be in
  any order.
- A record with no series, or with series that hold no sample, is valid and
  changes nothing. The Rust producer does not write one.
- A reader refuses a record of another format version, a record that ends
  inside a field, and a record with bytes after its last series. It stops
  there: a materializer does not skip records.
- There is no checksum. The log checksums what it stores.

Example: one series `job="a"` with the sample (1, 2.0).

```text
01                       format version
01 00 00 00              one series
01 00                    one label
03 00 6a 6f 62           name "job"
01 00 61                 value "a"
01 00 00 00              one sample
01 00 00 00 00 00 00 00  timestamp 1
00 00 00 00 00 00 00 40  value 2.0
```

## Chunk

A chunk is the samples of one series in one block, in strictly increasing
order of timestamp, compressed as in the Gorilla paper.

```text
chunk         sample count u32 | bit stream, padded with zero bits to a whole byte
bit stream    first sample | later sample*
first sample  timestamp, 64 bits | value bits, 64 bits
later sample  timestamp code | value code
```

Bits are written most significant first.

**Timestamp code.** `delta` is this timestamp minus the previous one. `dod`
is this delta minus the previous delta; the delta before the second sample
counts as 0. All arithmetic wraps at 64 bits.

| `dod` | Bits |
| --- | --- |
| 0 | `0` |
| -8191 to 8192 | `10`, then `dod` in 14 bits |
| -65535 to 65536 | `110`, then `dod` in 17 bits |
| -524287 to 524288 | `1110`, then `dod` in 20 bits |
| anything else | `1111`, then `dod` in 64 bits |

An n-bit `dod` is its low n bits in two's complement. A reader subtracts 2^n
from a number greater than 2^(n-1).

**Value code.** `xor` is the bits of this value XOR the bits of the previous
one. The **window** of an `xor` is the run of bits from its first to its last
one-bit.

| `xor` | Bits |
| --- | --- |
| 0 | `0` |
| fits the window last written in full | `10`, then the bits of that window |
| anything else | `11`, leading zeros in 5 bits, window length in 6 bits, the bits of the window |

- The leading-zero count is capped at 31; the window then starts earlier and
  is longer.
- A window length of 64 is stored as 0.
- An `xor` fits the window last written in full if it has at least as many
  leading and at least as many trailing zeros as that window. Before the
  first `11` code there is no such window.

## Block, format version 1

A block is one object. It holds every sample of the records it covers.

```text
block         chunk* | series index | postings | footer
series index  series entry*
series entry  label count u16 | label* | chunk offset u64 | chunk length u32
              | chunk crc32c u32 | sample count u32 | min time i64 | max time i64
postings      pair count u32 | posting*
posting       name length u16 | name | value length u16 | value
              | series count u32 | series number u32*
footer        magic 8 "BITAMBLK" | format version u32 = 1 | series count u32
              | sample count u64 | min time i64 | max time i64
              | series index offset u64 | series index length u64
              | postings length u64 | series index crc32c u32
              | postings crc32c u32 | footer crc32c u32
```

- **Chunks.** One per series, back to back from offset 0, in the order of
  the series index. A series without a sample is not in the block.
- **Series index.** One entry per series, in ascending order of label set. A
  series' number is its position, from 0. An entry says where the chunk is,
  gives the checksum of the chunk's bytes, and the timestamps of the series'
  first and last sample in the block.
- **Postings.** For each label name and value that occurs in the block, the
  numbers of the series that have it, ascending. Pairs are in ascending
  order of name, then value. The postings start right after the series
  index.
- **Footer.** The last 76 bytes. Its checksum covers the 72 bytes before it.
  The other two checksums cover the series index and the postings. The
  minimum and maximum time of a block without samples are 0.

A block without series is valid: no chunks, an empty series index, postings
with a pair count of 0, and the footer.

### Reading a block

A reader that knows the block's size:

1. Fetches the last 76 bytes and verifies the footer. It refuses another
   magic, another format version, and a footer whose offsets do not add up
   to the block's size.
2. Fetches the series index and the postings in one range read and verifies
   both checksums.
3. Picks series. A matcher `name="value"` with a non-empty value is one
   posting list; several are intersected. The remaining matchers are checked
   against the label sets in the series index. A series whose first and last
   timestamp lie outside the query's time range is dropped.
4. Fetches the chunks of the picked series, verifies each against the
   checksum in its entry, and decodes it.

Nothing is used before the checksum that covers it has been verified.

## Blocks and the log

A block covers a range of sequence numbers: it holds what the records of
that range, applied in order, leave. The blocks of a stream cover every
record from the first to the materializer's checkpoint, without gap or
overlap, and are listed in that order in the **catalog**.

Blocks are aligned to the log, not to time. Two blocks can hold samples for
the same span of time, and even for the same series and timestamp. A reader
goes through the blocks in catalog order and lets a later block replace what
an earlier one said about a series and timestamp.

Neighbouring blocks are merged into larger ones over time. A merged block
covers the ranges of its inputs together, and the inputs are deleted once
the catalog that names the merged block is committed. A reader that finds a
block gone reads the catalog again.

### Finding the blocks

The store's materializer is registered on the stream under the name
`bitacora-metrics`.

1. Read the stream record for the stream identifier, and the checkpoint
   record of `bitacora-metrics` ([metadata.md](metadata.md)). Its
   `checkpoint` is the last record the blocks cover; its `manifest` is a
   key.
2. Read the object `materializers/<stream id>/bitacora-metrics/<manifest>`.
   It is a checkpoint manifest; its details are an artifact catalog
   ([checkpoint-manifest.md](checkpoint-manifest.md)).
3. Each catalog entry gives the range of records a block covers, the block's
   key under the same prefix, and its size.

Samples above the checkpoint are not in any block. They are in the log, and
in the memory of the store that owns the stream.
