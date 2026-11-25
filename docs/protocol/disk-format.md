# A replica's data directory

A replica stores each segment copy as one file of records plus a small file
of state. This page defines both, and the rules for reading them back after
a crash.

## Layout

```text
<data directory>/
  replica.id                        the replica's identity, as text
  <stream id>/<segment id>.seg      the records of one segment copy
  <stream id>/<segment id>.state    its fenced epoch and sealed end
```

Identifiers in paths are decimal numbers without padding. A file whose name
ends in `.tmp` is a write that was interrupted before it was renamed into
place; it is ignored and may be deleted.

`replica.id` is written once, when the replica first starts on an empty
directory, and never changes. The identity belongs to the directory, not to
the machine: a replica whose disk was wiped starts under a new identity and
can never claim copies it no longer holds.

## Segment file

```text
segment file   header | batch frame*
header         identity | zero padding to byte 64
               | flushed mark | flushed mark | zero padding to byte 128
```

The file is preallocated, so the bytes after the last batch frame are zeros.

### Identity (bytes 0 to 47)

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic: `BITASEG\0` |
| 8 | 4 | Format version |
| 12 | 8 | Stream identifier |
| 20 | 8 | Segment identifier |
| 28 | 8 | Epoch of the writer that authored the segment |
| 36 | 8 | Sequence number of the segment's first record |
| 44 | 4 | CRC-32C of bytes 0 to 43 |

The identity is written when the file is created and never changes. A file
is created atomically together with its first content, so a segment file
either does not exist or has a valid identity.

### Flushed marks (bytes 64 to 95)

Two slots of 16 bytes each, at offsets 64 and 80.

| Offset in slot | Size | Field |
| --- | --- | --- |
| 0 | 8 | Flushed bytes: every byte of the file before this offset reached the disk |
| 8 | 4 | Generation: counts the marks written to this file |
| 12 | 4 | CRC-32C of the stream identifier, the segment identifier and bytes 0 to 11 of the slot |

A mark with generation *g* is written to slot *g* mod 2, so a torn write of
one slot leaves the other intact. The current mark is the valid slot with
the higher generation. A file with no valid slot has never been flushed past
its header.

The mark is the only part of a segment file that is rewritten. It is written
without a flush of its own: it reaches the disk with the next flush of data,
or with a flush made when the stream goes idle. It may therefore lag behind
what is really flushed, and it never runs ahead of it.

### Batch frame

Records are stored in batches. A batch is the unit of checksumming, of
writing and of acknowledgement.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | Body length in bytes |
| 4 | 8 | Sequence number of the batch's first record |
| 12 | 4 | Record count, at least 1 |
| 16 | 4 | CRC-32C, see below |
| 20 | body length | Body: the records, back to back |

A record in the body is its payload length as 4 bytes followed by the
payload. Record *i* of a batch (counting from 0) has the sequence number
"first sequence + *i*".

The checksum covers, in this order: the stream identifier (8 bytes), the
segment identifier (8 bytes), bytes 0 to 15 of the frame, and the body. The
two identifiers are not stored in the frame. Mixing them in detects a frame
written to the wrong segment; covering the first sequence number detects a
frame at the wrong offset.

Limits: a record payload is at most 4 MiB, a batch body at most 8 MiB.

The same frame bytes are sent on the wire, stored on replica disks and
uploaded to the object store, so the checksum the writer computed is
verified at every hop. One consequence: a batch belongs to one segment.
A record that moves to a new segment during a roll is encoded again.

## Segment state file

33 bytes, replaced atomically (written under a `.tmp` name, flushed, renamed).

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic: `BITASTA\0` |
| 8 | 4 | Format version |
| 12 | 8 | Fenced epoch: requests from a writer below this epoch are refused |
| 20 | 1 | Sealed flag: 1 if the segment is sealed |
| 21 | 8 | Sealed end: the segment's final sequence number, if sealed |
| 29 | 4 | CRC-32C of bytes 0 to 28 |

A segment may have a state file and no segment file: a replica that is
fenced or told of a seal before it ever received a record remembers that.

## Reading a segment file after a restart

1. Verify the identity. A file that fails this check is damaged.
2. Read the current flushed mark.
3. Scan batch frames from byte 128 until a frame is invalid: too short,
   failing its checksum, not continuing the previous batch, or all zeros.
   The valid frames before it are the copy's records.
4. If the scan stopped **before** the flushed mark, the copy is **damaged**:
   bytes that were flushed, and may have been acknowledged, are gone. A
   damaged copy answers no reads and no fence requests with its length, and
   is replaced by repair.
5. If the scan stopped **at or after** the flushed mark, what follows is a
   torn tail from the crash. It was never acknowledged, because an
   acknowledgement is sent only after a flush. The file is cut there.

A copy of a sealed segment that ends before the sealed end is incomplete.
The replica keeps serving what it has; the controller compares every copy
with the sealed end recorded in the metadata store and has an incomplete or
damaged one rebuilt from a good one. A background scrub rereads sealed
copies and marks a copy that fails a checksum as damaged.

### What this does not catch

The mark lags by up to one flush while a stream is busy. Damage confined to
the records of the very last flush before a crash is therefore taken for a
torn tail, and the copy comes back shorter than it was acknowledged, without
knowing it. Recovery tolerates one such copy per segment (it takes the
longest copy among a quorum), but not the combination of that with the loss
of another copy of the same records.
