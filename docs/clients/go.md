# The Go client

A native Go client of bitacora, in [`src/go`](../../src/go). It speaks the
same gRPC protocol to replicas and keeps the same records in etcd as the
Rust client, so the two work on the same cluster and on the same stream: a
stream written by one is read by the other, and a writer of one takes over
from a writer of the other. The tests in `src/go/interop` check that against
the real daemons.

It is a second implementation, written from [the protocol
documents](../protocol/README.md), not a binding to the Rust library. Why,
and what the documents had to be taught on the way, is in
[decision 0016](../decisions/0016-go-client.md).

## Install

```sh
go get github.com/thewillyhuman/bitacora/src/go@latest
```

The module is `github.com/thewillyhuman/bitacora/src/go` and the package to
import is `github.com/thewillyhuman/bitacora/src/go/bitacora`. It needs Go
1.26 or later and nothing else: the gRPC stubs are checked in, and no C
compiler is involved.

A release of bitacora is also a version of the module:
`hack/create_release.sh` tags the release commit as `src/go/vX.Y.Z`, which
is where Go looks for a module in a subdirectory. Until the first release
there is no such tag, and `@latest` is the newest commit of the main
branch.

## Example

A program that creates a stream, appends three records, reads them back and
follows the stream for a moment. It is
[`src/go/examples/quickstart/main.go`](../../src/go/examples/quickstart/main.go);
a test fails if this page shows anything else, and another runs the program
against a real cluster.

```go
// Command quickstart creates a stream, appends three records to it, reads
// them back and follows the stream for a moment. It is the example that
// docs/clients/go.md shows.
//
//	go run ./examples/quickstart http://127.0.0.1:2379
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s <etcd endpoint>", os.Args[0])
	}
	if err := run(context.Background(), os.Args[1]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, etcdEndpoint string) error {
	// Connect sends nothing yet. A cluster that is down shows on first use.
	client, err := bitacora.Connect(bitacora.Config{EtcdEndpoints: []string{etcdEndpoint}})
	if err != nil {
		return err
	}
	defer client.Close()

	// A stream is created once; a second run finds it there.
	_, err = client.CreateStream(ctx, "quickstart", bitacora.StreamOptions{})
	if err != nil && !errors.Is(err, bitacora.ErrStreamExists) {
		return err
	}

	// Become the stream's one writer. If another writer holds the stream,
	// this waits for its lease to lapse.
	writer, err := client.OpenWriter(ctx, "quickstart", bitacora.WriterOptions{Identity: "quickstart"})
	if err != nil {
		return err
	}
	// Close seals the writer's segment and gives the lease back, so that
	// the next writer starts at once. Closing twice is harmless.
	defer writer.Close(ctx)

	var first, last uint64
	for number := range 3 {
		// Append returns once the record is on a write quorum of replicas.
		// An error that wraps ErrFenced means that another writer took the
		// stream; one that wraps ErrIndeterminate, that the record may or
		// may not be in it.
		sequence, err := writer.Append(ctx, fmt.Appendf(nil, "record %d", number))
		if err != nil {
			return err
		}
		if number == 0 {
			first = sequence
		}
		last = sequence
	}
	if err := writer.Close(ctx); err != nil {
		return err
	}

	// Any number of readers read what is committed.
	reader := client.OpenReader("quickstart")
	records, err := reader.ReadRange(ctx, first, last)
	if err != nil {
		return err
	}
	for _, record := range records {
		fmt.Printf("%d %s\n", record.Sequence, record.Payload)
	}

	// Follow yields records as they commit, until its context ends. Nobody
	// is writing here, so this one waits for two seconds and sees nothing.
	following, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	for record, err := range reader.Follow(following, last+1) {
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("%d %s\n", record.Sequence, record.Payload)
	}
	return nil
}
```

```console
$ go run ./examples/quickstart http://127.0.0.1:2379
1 record 0
2 record 1
3 record 2
```

## The API

Everything is in one package. The source documents every name; `go doc
github.com/thewillyhuman/bitacora/src/go/bitacora` prints it.

### The cluster

| | |
| --- | --- |
| `Connect(config Config) (*Client, error)` | A handle to a cluster. Sends nothing |
| `(*Client) Close() error` | Releases the connections to etcd and to replicas |
| `Config.EtcdEndpoints` | The client URLs of etcd. Required |
| `Config.EtcdKeyPrefix` | The cluster's key prefix. Default `/bitacora/` |
| `Config.ObjectStore` | Where tiered segments are read from; see below. Default: none |
| `Config.RequestTimeout` | Bounds every request to etcd and to a replica. Default 5 s |

A `Client` is safe for concurrent use and is meant to be shared.

### Writing

| | |
| --- | --- |
| `(*Client) OpenWriter(ctx, stream, WriterOptions) (*Writer, error)` | Becomes the stream's writer: claims the next epoch, recovers and seals what the previous writer left open, opens a new segment |
| `(*Writer) Append(ctx, payload) (sequence, error)` | Appends one record. Returns once it is committed |
| `(*Writer) AppendBatch(ctx, payloads) (first, last, error)` | Appends several records that commit together |
| `(*Writer) Close(ctx) error` | Waits for accepted records, seals the segment, gives the lease back, stops the writer's goroutines |
| `(*Writer) Epoch() uint64` | The epoch the writer holds |
| `(*Writer) DurableSequence() uint64` | The last committed sequence number |

A stream has one writer. Opening a second one takes the stream from the
first, which finds out on its next append: bitacora does not choose who
should write, it makes sure that whoever asked last is the only one who can.
By default `OpenWriter` first waits for the previous writer's lease to
lapse, so that a healthy writer is not replaced by accident;
`WriterOptions{Takeover: bitacora.ForceTakeover}` takes the stream at once.

`Append` waits for the commit as long as its context allows. A replica that
fails does not fail the append: the writer rolls to a new segment on other
replicas and carries the record along. A `Writer` is safe for concurrent
use, and concurrent appends share requests to the replicas.

A writer runs goroutines until it is closed. Always call `Close`, also on a
writer that was fenced.

`WriterOptions` also sets the writer's name in the lease, how long opening
may take, how much may await commit before `Append` waits, when a silent or
lagging replica is given up on, the length of the lease, and the size and
age at which a segment is rolled. The zero value of each means the default
that [the replication rules](../protocol/replication.md#timings) list.

### Reading

| | |
| --- | --- |
| `(*Client) OpenReader(stream) *Reader` | A reader. Sends nothing |
| `(*Reader) ReadRange(ctx, first, last) ([]Record, error)` | The committed records of a range of sequence numbers, both ends included |
| `(*Reader) Follow(ctx, from) iter.Seq2[Record, error]` | Every committed record from a position on, as it commits, across rolls and writer changes, until `ctx` ends |
| `(*Reader) LowWaterMark(ctx) (uint64, error)` | The first sequence number that can still be read |

Readers never coordinate, with each other or with the writer. A reader
verifies the checksum of every batch it receives, whoever sent it.

`Follow` is an iterator for `range`. Trouble that can pass, such as a
replica or an etcd that is unreachable for a while, is retried without
ending the iteration. When following cannot continue, the last element
carries the error: a `*TruncatedError` if retention overtook the follower,
`ErrStreamNotFound` if the stream was deleted, or the context's error.

A record becomes readable a moment after its append returned: the replicas
serve what the writer has told them is committed, and it tells them with
its next request, at most 50 ms later.

### Administration

| | |
| --- | --- |
| `(*Client) CreateStream(ctx, name, StreamOptions) (StreamDescription, error)` | Creates a stream: by default three replicas and a write quorum of two |
| `(*Client) DescribeStream(ctx, name) (StreamDescription, error)` | The stream's epoch, writer, segments, low-water mark and materializers |
| `(*Client) DeleteStream(ctx, name) error` | Deletes a stream and all of its records. Cannot be undone |
| `(*Client) TruncateBefore(ctx, name, sequence) error` | Allows every record below a sequence number to be deleted |
| `(*Client) RegisterMaterializer(ctx, stream, materializer) error` | Makes the log keep every record above the materializer's checkpoint |
| `(*Client) UnregisterMaterializer(ctx, stream, materializer) error` | Stops a materializer from holding records back |

These change records in etcd. The controller does the work that follows
from them: it places the first segment of a new stream, deletes the
segments of a deleted one, and truncates as far as the materializers and
the retention policy allow.

### The object store

A sealed segment is uploaded to the cluster's object store, and later the
replicas drop their copies. A reader then needs the object store. Without
one, reading such a segment fails with `ErrUnavailable`.

```go
client, err := bitacora.Connect(bitacora.Config{
	EtcdEndpoints: []string{"http://10.0.0.1:2379"},
	ObjectStore:   bitacora.NewLocalObjectStore("/var/lib/bitacora/objects"),
})
```

`NewLocalObjectStore` reads the `local_directory` kind of object store; its
argument is the `path` of the cluster configuration. For any other store,
implement the one method of `ObjectStore`:

```go
type ObjectStore interface {
	// GetRange returns bytes [start, end) of the object stored under key.
	GetRange(ctx context.Context, key string, start, end uint64) ([]byte, error)
}
```

## Errors

Every error wraps one of these, so that `errors.Is` says what to do, and
keeps its cause in its text.

| Error | Meaning | What to do |
| --- | --- | --- |
| `ErrStreamNotFound` | No stream has this name | Create it, or fix the name |
| `ErrStreamExists` | A stream of this name exists | Nothing, if that is the stream that was meant |
| `ErrInvalidArgument` | A name, a range or an option that can never work | Fix the call |
| `ErrUnavailable` | etcd, the replicas that hold the records, or the object store could not be reached | Try again later |
| `ErrTruncated` | The records asked for were deleted. The error is a `*TruncatedError` with the `LowWaterMark` | Start again from the low-water mark, if losing what was below it is acceptable |
| `ErrLeaseHeld` | Another writer holds the stream, and its lease lasts longer than `OpenWriter` may wait | Wait, or force the takeover |
| `ErrFenced` | Another writer has taken the stream | Stop writing. Close the writer |
| `ErrIndeterminate` | The writer could not learn whether a record was committed | Read the stream to find out |
| `ErrClosed` | The writer was closed | Open another one |

An append has three outcomes, not two. It succeeds; it fails for certain
(`ErrInvalidArgument`, `ErrClosed`, or the context's own error when the
record was never given a sequence number); or its outcome is unknown.
`ErrIndeterminate` is the unknown one, and so is `ErrFenced` for the appends
that were in flight when the stream was taken. An append whose context ends
while the record awaits commit fails with an error that wraps both
`ErrIndeterminate` and the context's error, and returns the sequence number
the record was given: it may still commit.

Other operations that end because of their context return the context's
error, unwrapped.

## What is not implemented

- **An S3 object store.** Only the local directory comes with the client.
  The `ObjectStore` interface is one method, which an S3 client's ranged
  `GetObject` fits.
- **Placing the first segment.** `CreateStream` leaves it to the controller,
  so a stream can be opened about one controller pass after it was created.
  `OpenWriter` waits for that. The Rust tool places it at once.
- **Running a materializer.** A materializer can be registered and
  unregistered. Taking its fencing token and committing its checkpoints,
  which is what the Rust materializer framework does, is not there.
- **Replica administration.** The stubs of the `ReplicaAdmin` and
  `ReplicaTransfer` services are generated and unused; draining replicas and
  listing their segments is the `bita` tool's job.
- **Metrics.** The client exports none.
- **TLS and authentication.** The protocol has none yet.
- **A released version.** The module is not tagged; see Install.
