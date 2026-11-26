# The object store

One bucket, or one prefix of a bucket, belongs to a cluster. This page says
which keys bitacora uses in it and who owns each part. Objects are
immutable: an object is written once, under a key that is not reused for
other content, and later deleted.

| Key prefix | Contents | Written by | Deleted by |
| --- | --- | --- | --- |
| `streams/<stream id>/segments/` | Tiered segments, one object each ([tiered-object.md](tiered-object.md)) | A replica, when the controller asks | The controller: when the low-water mark passes the segment, or the stream is deleted |
| `materializers/<stream id>/<materializer>/` | What a materializer wrote: layers, artifacts, manifests | The materializer's runner | The runner's sweeper, for objects no committed checkpoint refers to; the controller, when the stream is deleted |

Stream identifiers are never reused, so the objects of a deleted stream can
never be taken for those of a new stream with the same name.

Below `materializers/<stream id>/<materializer>/`, a key starts with the
fencing token of the runner that wrote it, padded to 20 digits. A runner
that lost its token can therefore never overwrite, or be confused with, the
objects of the runner that took over, and the sweeper can tell objects that
may still be about to be committed from garbage.

Every process of a cluster must be given the same bucket and prefix. The
controller refuses to record a tiered segment that it cannot see in its own
view of the store, which catches a replica configured with another bucket.

## A local directory as the object store

A cluster on one machine, or on machines that share a file system, can use
a directory instead of a bucket: the `local_directory` kind of the cluster
configuration, with a `path`. Every program that reads objects from it must
agree on where they are:

```text
<path>/
  objects/<object key>   one file per object; the `/` of a key are directories
  scratch/               objects being written, not yet visible
```

A file below `objects/` is a whole object: it is written below `scratch/`,
flushed, and renamed into place. A reader opens `objects/<object key>` and
never looks at `scratch/`.

## Objects that are being written

An object is under its key when it is complete, and not before. A small
object is stored with one request. A large one, such as a tiered segment,
is handed to the store piece by piece, and what the store holds meanwhile
is not an object: no read finds it and no listing shows it.

| Store | What a write in progress is | What a writer that died leaves |
| --- | --- | --- |
| Local directory | A file below `scratch/` | That file. A program that opens the directory removes scratch files that were not touched for a day |
| S3 | The parts of a multipart upload, 8 MiB each | Those parts. Only the service can remove them |

In a bucket, the parts of a multipart upload that was never completed are
kept, and billed, until the upload is aborted. A writer aborts the upload
when it gives up; one that died cannot. The bucket therefore needs a
lifecycle rule that aborts incomplete multipart uploads
(`AbortIncompleteMultipartUpload`); `docs/operations/README.md` gives one.
Nothing in a cluster lists or removes them, and nothing breaks while they
are there: they are no object of any key.
