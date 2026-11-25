# A sealed segment in the object store

Some time after a segment is sealed, one replica uploads it to the object
store as a single object. Later the replicas drop their copies, and the
object is the only place the segment's records can be read from. It is read
with range requests.

## Key

```text
streams/<stream id>/segments/<segment id, 20 digits, zero padded>
```

For example `streams/7/segments/00000000000000000003`. The key and the size
of the object are recorded in the segment's metadata record; a reader takes
them from there and never computes them.

## Layout

```text
tiered object  batch frame* | block index | footer
block index    entry*
entry          first sequence (8 bytes) | offset (8 bytes)
```

The batch frames are the bytes of the segment file after its 128-byte
header, unchanged (see [disk-format.md](disk-format.md)), from the segment's
first record to its sealed end.

A **block** starts at a batch frame and runs to the start of the next block.
The first block starts at offset 0. A new block starts at the first batch
frame that lies at least 64 KiB after the start of the previous one. Each
index entry gives the sequence number of the first record of a block and the
offset of the block in the object.

### Footer (the last 72 bytes)

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic: `BITATIER` |
| 8 | 4 | Format version |
| 12 | 8 | Stream identifier |
| 20 | 8 | Segment identifier |
| 28 | 8 | Epoch of the writer that authored the segment |
| 36 | 8 | Sequence number of the segment's first record |
| 44 | 8 | Sealed end: the segment's final sequence number |
| 52 | 8 | Frames length: how many bytes of batch frames the object starts with |
| 60 | 4 | Number of index entries |
| 64 | 4 | CRC-32C of the block index |
| 68 | 4 | CRC-32C of bytes 0 to 67 of the footer |

The block index starts at offset "frames length" and is "number of entries
× 16" bytes long. The object's size is therefore "frames length + index
length + 72".

## Reading

1. Take the object's key and size from the metadata store.
2. Fetch the last 72 bytes and verify the footer. Check that the stream
   identifier, the segment identifier, the sealed end and the computed size
   are the ones the metadata store names. If not, this is not the object of
   the segment; do not use it.
3. Fetch and verify the block index. Its entries must increase in both
   fields, the first must be the segment's first sequence number at offset 0,
   and every offset must lie inside the frames.
4. To read from sequence number *s*, find the last entry whose first
   sequence is at most *s* and fetch from its offset to the offset of a later
   entry, or to "frames length". The fetched bytes are whole batch frames.
5. Verify every batch checksum, as for bytes received from a replica.

## Writing

An object is built only from frames that verify and that are exactly the
sealed segment: the first batch starts at the segment's first sequence
number and the last record is the sealed end. After storing the object, the
uploader reads it back and compares it with what it built before it reports
success. Uploading again stores the same bytes.

Objects are immutable. An object is deleted when the stream's low-water
mark has passed its segment, or when its stream is deleted.

Compression is not used in version 1.
