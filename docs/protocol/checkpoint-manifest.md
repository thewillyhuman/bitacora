# Checkpoint manifests and the artifact catalog

A materializer's checkpoint record in the metadata store
([metadata.md](metadata.md)) names one object: the **manifest** of the
checkpoint. This page defines the manifest, and the **artifact catalog** that
a materializer built on the immutable-artifacts adapter puts inside it. A
query node in another language finds a materializer's artifacts from this
page.

All integers are little-endian. Checksums are CRC-32C. Keys are UTF-8.

## Where the objects are

Everything a materializer writes is under
`materializers/<stream id>/<materializer>/` ([object-store.md](object-store.md)).
An **artifact key** is relative to that prefix:

```text
<fencing token, 20 digits>/<artifact number, 10 digits>-<label>
```

The fencing token is the one the writing instance held. The label says what
the artifact is, for people reading a listing; it carries no meaning.

The `manifest` field of the checkpoint record is an artifact key.

## Manifest, format version 1

```text
manifest  magic 8 "BITAMANI" | format version u32 = 1 | checkpoint u64
          | artifact count u32 | artifact key*
          | details length u32 | details | crc32c u32
key       length u32 | bytes
```

- `checkpoint` is the sequence number the manifest belongs to. It equals the
  `checkpoint` of the record that names the manifest.
- The artifact keys are every artifact the checkpoint needs. The sweeper
  deletes the artifacts of the namespace that the committed manifest does
  not name.
- `details` belong to the materializer. For the immutable-artifacts adapter
  they are the catalog below.
- The checksum covers everything before it.

A reader refuses another magic, another format version, a wrong checksum,
and bytes that do not end where their lengths say.

## Artifact catalog, format version 3

```text
catalog  format version u32 = 3 | entry count u32 | entry*
         | pending flag u8 | entry, if the flag is 1
entry    first sequence u64 | last sequence u64 | size u64
         | key length u32 | key | summary length u32 | summary
```

- An entry is one artifact: the range of records it covers, both ends
  included, its size in bytes, its key, and a summary.
- The summary belongs to the materializer that wrote the artifact: what a
  reader should know before it fetches anything. It may be empty. The page
  of each materializer says what its summaries hold.
- Entries are in log order. Each starts right after the previous one ends.
- The pending flag is 0 or 1. With 1, one more entry follows: the
  **pending records**. It names an artifact that holds the records the
  materializer had applied and not yet put into an artifact of the catalog.
  Its range starts right after the last entry and ends at the checkpoint.
  It is not part of the catalog: it is replaced at every checkpoint, never
  merged, and only the materializer that wrote it says what is in it.
- The entries, and the pending records after them, cover every record from
  the first one the materializer applied to the checkpoint. Without pending
  records the entries alone do.

A reader refuses another format version, an entry that covers no record,
entries that leave a gap or overlap, pending records that do not follow the
last entry, a flag other than 0 or 1, and bytes that do not end where their
lengths say.

## Reading the catalog while it changes

The materializer replaces the manifest at every checkpoint, and merges
several neighbouring artifacts into one from time to time. The manifest and
the artifacts that the new manifest no longer names are deleted right after
the new checkpoint is committed.

- A reader that gets "not found" for the manifest named by a checkpoint
  record reads the checkpoint record again.
- A reader that gets "not found" for an artifact reads the checkpoint record
  and the manifest again. The records the artifact covered are in another
  artifact of the newer catalog.

Nothing announces a new checkpoint. A reader that wants to follow the
materializer reads the checkpoint record again from time to time.
