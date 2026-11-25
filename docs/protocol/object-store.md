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
