# bitacora-segment

Segment files: the stored form of a contiguous run of a stream's records, on a replica's disk and in the
object store.

## What it owns

- **The byte layouts.** The header of a segment file, the batch frame that holds records (`format`), the
  small state file next to a segment file (`SegmentState`), and the object a sealed segment becomes in the
  object store (`tiered`). The same frame bytes are sent on the wire, so this is also where a message's
  records are decoded.
- **One copy of a segment.** `SegmentFile`: create, open after a restart, append, flush, read, truncate,
  release unused space, verify.
- **The disk.** The `Disk` and `DiskFile` interfaces every file is reached through, `LocalDisk` for a
  real directory and `MemoryDisk` for tests and the simulator, with a crash model (`CrashMode`) and fault
  switches.

It owns no decision. Which records are committed, which copy is good, who may write: all of that is
decided above this crate. It stores what it is told to store and says truthfully what it holds.

## Design

**One layout for wire, disk and object store.** A batch is encoded and checksummed once, by the writer.
Replicas store the frame as received, readers get it as stored, and the tiered object carries it
unchanged. Nothing is encoded twice, and the writer's checksum is verified at every hop. The segment's
address is mixed into the checksum without being stored, so a frame written to the wrong segment fails.

**Layouts are pure functions.** Everything in `format`, `state` and `tiered` works on bytes in memory.
The formats are tested exhaustively, down to every flipped bit, without a disk.

**Files are reached through an interface.** The order of writes and flushes is what makes a copy survive
a crash, and that order can only be tested on a disk that can be crashed. `SegmentFile` therefore never
opens a file itself: it runs unchanged on `LocalDisk` and on `MemoryDisk`, which keeps for every file
what a reader sees and what a crash would leave, and can lose, tear or damage the difference.

**Recovery is a scan, not a journal.** A replica acknowledges a batch only after the whole frame is
flushed. After a restart the longest run of valid, consecutive frames is therefore everything that was
acknowledged, plus perhaps something that was not; what follows is a torn tail and is overwritten.
There is no separate log to replay.

**A flushed mark tells damage from a torn tail.** Invalid bytes at the end of a file are normal after a
crash. Invalid bytes inside what was flushed mean the copy lost acknowledged records and can no longer
vouch for what is committed. The header holds a mark of how far the file is flushed; it is written with
the next append so that it costs no flush of its own, in two alternating slots so that a torn write of
one leaves the other.

**Space is reserved ahead.** A file is preallocated and grows by whole steps, so that the flush which
acknowledges an append persists data only, never a change of size.

**Reads start from a sparse index.** About every 64 KiB the index remembers where a batch starts. It is
never stored: the scan that verifies a file when it is opened rebuilds it. The tiered object uses the
same rule for its blocks and does store the result, because there the object cannot be scanned.

**A crash is data, not code.** What a crash of `MemoryDisk` leaves behind is chosen with the `CrashMode`
enum: lose everything unflushed, keep a prefix, keep a range of an unflushed write, or tear at random
with a seeded generator. The tearing logic lives next to the disk it tears, so the simulator and the
unit tests share it.

## Invariants

- The layouts are format version 1. `tests/format_golden_test.rs` holds them to bytes written by that
  version; `docs/protocol/disk-format.md` and `docs/protocol/tiered-object.md` describe them.
- No decoder panics on any input. `tests/decoder_fuzz_test.rs` feeds them noise and damaged encodings.
- A `Batch` that exists was verified: its checksum, its structure and its limits.
- Decoding copies no payload. A decoded batch shares the memory of the buffer it came from.
- A `SegmentFile` holds whole frames, consecutive in sequence, from its header on.
- The flushed mark never runs ahead of what was flushed.
- After a failed flush a copy must not be flushed again: the kernel may have dropped the unwritten pages,
  so a second flush could falsely succeed. This crate reports the failure; the replica that owns the copy
  stops.
- A tiered object is built only from frames that are exactly the sealed segment.
- `MemoryDisk` is deterministic: files are walked in path order, and a random tear draws once per torn
  file from the generator it is given.
- Only `local_disk.rs` touches the file system. Nothing reads a clock.

## Files

| File | Holds |
| --- | --- |
| `src/lib.rs` | The crate's rationale and its public names |
| `src/format.rs` | Segment header, flushed mark, batch frame, strict decoding and the recovery scan |
| `src/file.rs` | `SegmentFile`: one copy of a segment on a disk |
| `src/index.rs` | The sparse index from sequence numbers to offsets |
| `src/state.rs` | The fencing and sealing state file |
| `src/tiered.rs` | The object of a sealed segment: frames, block index, footer |
| `src/disk.rs` | The `Disk` and `DiskFile` interfaces |
| `src/local_disk.rs` | The disk on a local directory |
| `src/memory_disk.rs` | The in-memory disk, its crash model and its fault switches |
| `src/<file>_test.rs` | The tests of the file of the same name |
| `tests/format_golden_test.rs` | The layouts, held to golden bytes |
| `tests/decoder_fuzz_test.rs` | Every decoder, fed noise |

## Dependencies

Of the bitacora crates it depends on `bitacora-types` only, and must stay that way: the replica, the
client, the controller, the wire protocol, the test kit and the simulator all depend on it. In particular
it must not depend on `bitacora-artifact`: this crate defines the bytes of a tiered object, and whoever
uploads them brings the store.
