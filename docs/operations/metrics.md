# Metrics reference

Every bitacora daemon exposes Prometheus metrics at `/metrics` on its observability port, in the
OpenMetrics text format. This page lists every metric. Dashboards and alert rules are written against
these names, so a name, a label or a unit only changes together with this page and a changelog entry.
A test of each crate that registers metrics fails when it exposes one that is not listed here.

## Conventions

- Every name starts with `bitacora_`, then the subsystem, then what is measured.
- Units are base units and part of the name: `_seconds`, `_bytes`.
- Counters end in `_total`. Use `rate()` on them.
- Durations are histograms with buckets from 100 microseconds to about 105 seconds, each twice the one
  before. Use `histogram_quantile()` on the `_bucket` series.
- A label is a small closed set of values: an operation, an error kind, a direction. Stream names,
  keys and addresses are never labels, so the number of time series of a process is bounded.
- A series with labels appears once its first event happened. A missing series means zero.

## Requests through an interface

Three subsystems are measured the same way, where the daemon reaches them:

| Subsystem | What it is | Exposed by |
| --- | --- | --- |
| `replica` | Requests the replica serves, without the network | `bita-replica` |
| `metadata_store` | Requests to etcd | `bita-replica`, `bita-controller` |
| `object_store` | Requests to the object store | `bita-replica`, `bita-controller` |

For each `<subsystem>`:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `bitacora_<subsystem>_requests_total` | counter | `operation` | Requests that finished, failed ones included |
| `bitacora_<subsystem>_request_duration_seconds` | histogram | `operation` | How long they took, failed ones included |
| `bitacora_<subsystem>_request_failures_total` | counter | `operation`, `error` | Requests that returned an error |

That is nine metrics:

- `bitacora_replica_requests_total`, `bitacora_replica_request_duration_seconds`,
  `bitacora_replica_request_failures_total`
- `bitacora_metadata_store_requests_total`, `bitacora_metadata_store_request_duration_seconds`,
  `bitacora_metadata_store_request_failures_total`
- `bitacora_object_store_requests_total`, `bitacora_object_store_request_duration_seconds`,
  `bitacora_object_store_request_failures_total`

Label values:

| Subsystem | `operation` | `error` |
| --- | --- | --- |
| `replica` | `append`, `read`, `fence`, `seal`, `repair_segment`, `upload_segment`, `delete_segment`, `list_segments`, `status`, `drain` | `fenced`, `sealed`, `out_of_order`, `conflicting_seal`, `segment_not_found`, `invalid_request`, `no_capacity`, `draining`, `storage`, `unavailable` |
| `metadata_store` | `get`, `list`, `transact` | `unavailable` |
| `object_store` | `put_object`, `get_object`, `get_range`, `delete_object`, `list_prefix`, `start_upload`, `upload_write`, `upload_complete`, `upload_abort` | `not_found`, `range_out_of_bounds`, `invalid_key`, `unavailable` |

Notes:

- The duration of a replica `read` includes the time a reader that follows the tail waits for new
  records. Judge the write path by `append` and by the disk's flush histogram.
- A replica `error` is the replica's own answer. `fenced`, `sealed`, `draining` and `no_capacity` are
  how the protocol tells a writer to stop or to roll; a steady trickle is normal, a flood is not.
- A metadata store transaction whose condition did not hold was answered and is not a failure.
- For the object store only `unavailable` means the store is in trouble; the other kinds are answers a
  healthy store gives to a wrong request.
- A large object, such as a sealed segment, is stored piece by piece: one `start_upload`, one
  `upload_write` for each piece the writer hands over, and then one `upload_complete` or, if the upload
  is given up, one `upload_abort`. An `upload_write` that fills a part includes the time the store takes
  to send it; one that does not takes no time. A segment that reached the object store is one
  `upload_complete` without a failure.

## Metadata store

| Metric | Type | Meaning |
| --- | --- | --- |
| `bitacora_metadata_store_available` | gauge | 1 if the last request was answered, 0 if it failed. Starts at 1 |

## Object store

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `bitacora_object_store_transferred_bytes_total` | counter | `direction` (`uploaded`, `downloaded`) | Bytes of objects stored and of objects and ranges read, for requests that succeeded. An object stored piece by piece is counted when its upload is completed |

## Disk

Exposed by `bita-replica` for its data directory.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `bitacora_disk_flush_duration_seconds` | histogram | | How long making written data durable took, failed flushes included. An append is acknowledged only after its flush, so this is usually the cause when append latency degrades |
| `bitacora_disk_written_bytes_total` | counter | | Bytes handed to the disk |
| `bitacora_disk_operation_failures_total` | counter | `operation` | Disk operations that returned an error. A file that is absent when opened or read is not counted |
| `bitacora_disk_capacity_bytes` | gauge | | Total size of the disk, as last reported |
| `bitacora_disk_free_bytes` | gauge | | Unused part of the disk, as last reported |

`operation` is one of `create_file`, `open_file`, `write_file_atomically`, `read_file`, `remove_file`,
`rename_file`, `list_files`, `space`, `read_at`, `write_at`, `sync_data`, `set_length`, `length_bytes`.

A `sync_data` failure is serious: the replica stops, because after a failed flush the durable content
of the file is unknown.

## Replica daemon

Exposed by `bita-replica` about itself, next to the request metrics of the `replica` subsystem.

| Metric | Type | Meaning |
| --- | --- | --- |
| `bitacora_replica_segment_copies` | gauge | Segment copies the replica holds |
| `bitacora_replica_damaged_segment_copies` | gauge | Copies that failed a checksum or a flush and await replacement by the controller |
| `bitacora_replica_draining` | gauge | 1 while the replica is draining, 0 otherwise |
| `bitacora_replica_registrations_total` | counter | Renewals of the replica's registration that reached the metadata store |
| `bitacora_replica_registration_failures_total` | counter | Renewals that failed. A replica that cannot renew is presumed dead by the controller after its liveness window |
| `bitacora_replica_scrubs_total` | counter | Completed scrubs of the sealed segments |

The first three are refreshed on every registration renewal; the damaged copies also after every scrub.

## Controller daemon

Exposed by `bita-controller`. One round of its loop takes or renews the leadership and, if the
instance leads, makes one pass over every stream.

| Metric | Type | Meaning |
| --- | --- | --- |
| `bitacora_controller_leading` | gauge | 1 if the last round of this instance led, 0 if it stood by or failed |
| `bitacora_controller_rounds_total` | counter | Rounds that finished, whatever their outcome |
| `bitacora_controller_round_failures_total` | counter | Rounds that failed because the metadata store could not be used |
| `bitacora_controller_pass_duration_seconds` | histogram | How long one pass over every stream took |
| `bitacora_controller_segments_missing_copies` | gauge | Sealed segments with fewer good copies than their stream asks for, as of the last pass: durability at risk |
| `bitacora_controller_standbys_placed_total` | counter | Standby segments placed |
| `bitacora_controller_copies_confirmed_total` | counter | Copies of sealed segments confirmed complete |
| `bitacora_controller_copies_marked_bad_total` | counter | Copies of sealed segments found short, damaged or lost |
| `bitacora_controller_copies_repaired_total` | counter | Bad copies rebuilt from a good one |
| `bitacora_controller_segments_tiered_total` | counter | Sealed segments uploaded to the object store |
| `bitacora_controller_segments_released_total` | counter | Tiered segments left to the object store alone |
| `bitacora_controller_segments_deleted_total` | counter | Segments below the low-water mark deleted |
| `bitacora_controller_copies_removed_total` | counter | Leftover segment copies removed from replicas |
| `bitacora_controller_metadata_snapshots_written_total` | counter | Snapshots of the metadata store written to the object store |
| `bitacora_controller_stream_errors_total` | counter | Streams a pass could not fully handle; they are retried on the next pass |

Reading them across instances:

- `sum(bitacora_controller_leading) == 0` means nobody looks after the cluster: no instance runs, or
  none can reach the metadata store. Existing streams keep appending and reading; rolls stop when
  standby segments run out, and repair, tiering and deletion pause.
- `max(bitacora_controller_segments_missing_copies) > 0` for longer than a repair takes means
  durability is at risk. Only the leader knows the number; a standby reports 0, and an instance whose
  round failed keeps the last number it saw.

## What is not measured

- **Append latency per stream**, the user-facing number of the design. It is only known where appends
  are issued: in the client library, which runs inside the application and has no metrics. The
  application measures its own appends; `bita-bench` does so for benchmarks.
- **Replica lag behind the durable sequence.** The writer knows it; a replica does not report it.
- **Failover duration.** It is the time `open_writer` takes in the application.
- **Traces.** No daemon emits OpenTelemetry traces yet.
