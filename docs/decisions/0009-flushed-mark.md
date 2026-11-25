# 0009. A segment file records how far it was flushed

Status: accepted · 2026-11-25

## Context

After a crash a replica scans each open segment file and keeps the valid
batches at its start. Whatever follows is taken for a torn write and cut
off. That is safe for a real torn write: nothing after the last flush was
acknowledged.

It is not safe when the invalid bytes are damage to records that *were*
flushed and acknowledged. The replica then comes back with a shorter copy
and reports it, in good faith, as its local end. If a writer takes over
while another copy is unavailable, the recovery end can fall below a
committed record. The simulator produced exactly this, with one bit flipped
on one disk.

## Decision

The header of a segment file holds a **flushed mark**: an offset up to which
the file is known to have reached the disk. On restart, invalid bytes
*before* the mark mean the copy is damaged; invalid bytes *after* it are a
torn tail. A damaged copy does not answer reads or fence requests with a
length, so it cannot vouch for what is committed, and it is replaced by
repair.

The mark costs no extra flush. It is written with an append and reaches the
disk with the next flush of data, or with a flush made when the stream goes
idle. Two slots are written alternately, each with its own checksum, so a
torn write of the mark leaves the previous one readable. The header grew
from 64 to 128 bytes to hold them. The format is in
`docs/protocol/disk-format.md`.

### What was tried first

A rule without any format change: a copy whose scan stopped at an invalid
frame, instead of at unwritten space, reported its tail as suspect, and
recovery did not count it. It was removed. A crash in the middle of a write
is normal and also leaves an invalid frame, so after one node was lost for
good and another restarted with an ordinary torn tail, no quorum of
trustworthy copies existed and the stream could never be opened again.

## Consequences

- Damage to flushed data in an open segment is detected at restart.
- The mark lags by up to one flush while a stream is busy. Damage confined
  to the records of the last flush before a crash is still taken for a torn
  tail. Closing that gap would need a flush of the mark before every
  acknowledgement, which doubles the cost of an append. Recovery tolerates
  one such copy, because it takes the longest among a quorum.
- Damage that happens to look like unwritten space at the end of valid data
  is likewise not detected at restart. The scrubber and the checksums on
  every read are the remaining defences.
