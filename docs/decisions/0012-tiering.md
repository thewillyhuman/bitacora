# 0012. How segments move to the object store and leave it

Status: accepted · 2026-11-25

## Context

The design moves a sealed segment to the object store fifteen minutes after
it is sealed and fully replicated, keeps the local copies for a hot-read
window, and deletes everything once the low-water mark has passed (section
10). It leaves open who does each step and in what order, which is where
data can be lost or garbage left behind.

## Decision

**A replica uploads; the controller decides.** The bytes are on the
replicas, so sending them through the controller would put the controller
on the data path. The controller picks a replica with a good copy and asks
it to upload. The replica builds the object, stores it, reads it back and
compares before it answers.

**The controller records only what it can see.** Before it writes the
object's key into the segment record, it checks that the object is in its
own view of the object store with the size the replica reported. A replica
and a controller configured with different buckets therefore produce an
error in the log, not metadata that points nowhere.

**Local copies go only while the object is there.** After the hot-read
window the controller checks the object again, and only then empties the
segment's replica set. The ordinary sweep of copies that no record refers
to removes the files.

**Objects are deleted before their records.** When the low-water mark
passes a tiered segment, the controller deletes the object first and
removes the segment record second. A controller that stops in between finds
the record again and deletes again, which is harmless. No listing of the
object store is ever needed to find garbage, and no object is orphaned. The
price is that a truncated tiered segment stays listed for a moment; readers
are already told "truncated" by the low-water mark.

**Readers ask replicas first.** The object store is used when no replica
serves the records: after release, or while a tiered segment's replicas are
unreachable.

The object's layout is in `docs/protocol/tiered-object.md`.

## Not done

- **Compression.** The design makes it optional and lists it as an open
  question.
- **A block cache in readers.** A reader keeps the footer and index of the
  object it is reading and fetches blocks as it goes.
- **Repair from the object store.** A tiered segment that loses local
  copies during the hot-read window is repaired from the remaining replicas
  like any other, not from the object.
- **Empty segments** are not uploaded: there is nothing in them to read.

## Consequences

- A cluster that loses every replica disk keeps every record older than the
  tiering delay. A test does exactly that.
- Tiering is off unless the controller, the replicas and the readers are
  given the same object store.
- A replica uploads a segment without holding it. As first built, the
  artifact store took an object in one piece, and a replica held a whole
  segment in memory while it uploaded, twice for a moment: more than 2 GiB
  for a segment of 1 GiB. Since then the artifact store takes an object
  piece by piece (`start_upload`), and the replica sends its copy as it
  reads it, a chunk of 8 MiB at a time. An upload takes about 16 MiB
  whatever the size of the segment.
- "Reads it back and compares" no longer means comparing with an object in
  memory, because there is none. The replica reads its local copy a second
  time and compares each chunk with the same bytes of the stored object,
  and the object's index and footer with what that second reading says they
  must be. A checksum taken while sending would have saved the second read
  and been weaker in 32 bits, or a new dependency in more; the copy is
  there and a comparison is exact. The price is one more sequential read
  of a segment, once in its life.
- The object appears under its key when the upload is completed and not
  before. An upload that fails, or whose copy turns out short or damaged
  half way, is aborted and leaves nothing under the key.
- One thing is now left for the bucket to do. On S3 a segment is a
  multipart upload, and the parts of one that was neither completed nor
  aborted stay in the bucket, unseen by any listing. A replica aborts the
  uploads it gives up on; it cannot abort the one it was killed in the
  middle of. So "no listing of the object store is ever needed to find
  garbage" holds for objects, and for parts the bucket needs a lifecycle
  rule that aborts incomplete multipart uploads
  (`docs/operations/README.md`, "Before you deploy").
