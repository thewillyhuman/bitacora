package interop

import (
	"errors"
	"strconv"
	"testing"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

func TestGoReadsWhatRustAppended(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	// Two Rust writers, one after the other: two epochs and two segments.
	first := appendWithRust(t, cluster, "orders", "first", "second", "third")
	second := appendWithRust(t, cluster, "orders", "fourth", "", "sixth éñ")
	if first.FirstSequence != 1 || second.LastSequence != 6 || second.Epoch <= first.Epoch {
		t.Fatalf("the Rust writers appended %+v and %+v; want sequences 1 to 6 under two epochs", first, second)
	}
	reader := connect(t, cluster).OpenReader("orders")

	records, err := reader.ReadRange(t.Context(), 1, 100)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	want := []bitacora.Record{
		{Sequence: 1, Payload: []byte("first")},
		{Sequence: 2, Payload: []byte("second")},
		{Sequence: 3, Payload: []byte("third")},
		{Sequence: 4, Payload: []byte("fourth")},
		{Sequence: 5, Payload: []byte("")},
		{Sequence: 6, Payload: []byte("sixth éñ")},
	}
	sameRecords(t, "the Go reader", records, want)
	// The Rust reader sees the same stream.
	sameRecords(t, "the Rust reader", readWithRust(t, cluster, "orders", "1", "100"), want)

	// A range inside one segment, and one across the two.
	inside, err := reader.ReadRange(t.Context(), 2, 3)
	if err != nil {
		t.Fatalf("ReadRange(2, 3): %v", err)
	}
	sameRecords(t, "ReadRange(2, 3)", inside, want[1:3])
	across, err := reader.ReadRange(t.Context(), 3, 5)
	if err != nil {
		t.Fatalf("ReadRange(3, 5): %v", err)
	}
	sameRecords(t, "ReadRange(3, 5)", across, want[2:5])

	lowWaterMark, err := reader.LowWaterMark(t.Context())
	if err != nil || lowWaterMark != 1 {
		t.Errorf("LowWaterMark = %d, %v; want 1", lowWaterMark, err)
	}
	if _, err := connect(t, cluster).OpenReader("payments").ReadRange(t.Context(), 1, 5); !errors.Is(err, bitacora.ErrStreamNotFound) {
		t.Errorf("reading a stream that does not exist = %v, want ErrStreamNotFound", err)
	}
}

func TestGoFollowerSeesRustAppendsAsTheyCommit(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	following := follow(t, connect(t, cluster), "orders", 1)

	// The stream is empty; the follower waits. A Rust writer appends and
	// steps down.
	appendWithRust(t, cluster, "orders", "one", "two")
	if record := following.next(t); record.Sequence != 1 || string(record.Payload) != "one" {
		t.Fatalf("the follower yielded %d %q, want 1 one", record.Sequence, record.Payload)
	}
	if record := following.next(t); record.Sequence != 2 || string(record.Payload) != "two" {
		t.Fatalf("the follower yielded %d %q, want 2 two", record.Sequence, record.Payload)
	}

	// A Rust writer that stays open appends at a steady pace. The follower
	// sees its records while it is still writing: the writer is killed only
	// after they arrived, so they were read from an open segment, on the
	// word of the writer's heartbeats.
	const recordBytes = 48
	writer := cluster.StartBenchmark(t, "append", "orders", "--force",
		"--rate", "200", "--records", "1000000", "--record-bytes", strconv.Itoa(recordBytes), "--appends-in-flight", "4")
	nextSequence := uint64(3)
	for range 40 {
		record := following.next(t)
		if record.Sequence != nextSequence || len(record.Payload) != recordBytes {
			t.Fatalf("the follower yielded sequence %d with %d bytes, want sequence %d with %d bytes",
				record.Sequence, len(record.Payload), nextSequence, recordBytes)
		}
		nextSequence++
	}

	// The writer crashes with its segment open. The next Rust writer
	// recovers the segment and appends; the follower crosses the recovery
	// without a gap and without a record twice.
	writer.Kill()
	last := appendWithRust(t, cluster, "orders", "after the crash")
	for {
		record := following.next(t)
		if record.Sequence != nextSequence {
			t.Fatalf("the follower yielded sequence %d, want %d", record.Sequence, nextSequence)
		}
		nextSequence++
		if record.Sequence == last.LastSequence {
			if string(record.Payload) != "after the crash" {
				t.Errorf("the last record is %q, want what the last writer appended", record.Payload)
			}
			break
		}
	}
}

func TestGoReadsASegmentTheControllerTieredAndReleased(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{IsTieringAtOnce: true})
	cluster.Bita(t, "stream", "create", "orders")
	appendWithRust(t, cluster, "orders", "first", "second", "third")

	// The controller uploads the sealed segment, verifies the object, and
	// tells the replicas to drop their copies.
	stream := waitForStream(t, cluster, "orders", "the release of the first segment", func(stream metadata.StreamRecord) bool {
		return len(stream.Segments) > 0 && stream.Segments[0].Tiered != nil && stream.Segments[0].Tiered.AreLocalCopiesDropped
	})
	if copies := stream.Segments[0].Copies; len(copies) != 0 {
		t.Fatalf("the released segment still names %d copies", len(copies))
	}
	want := []bitacora.Record{
		{Sequence: 1, Payload: []byte("first")},
		{Sequence: 2, Payload: []byte("second")},
		{Sequence: 3, Payload: []byte("third")},
	}
	client := connect(t, cluster)

	records, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 3)
	if err != nil {
		t.Fatalf("ReadRange of a tiered segment: %v", err)
	}
	sameRecords(t, "the Go reader, from the object store", records, want)
	partial, err := client.OpenReader("orders").ReadRange(t.Context(), 2, 2)
	if err != nil {
		t.Fatalf("ReadRange(2, 2) of a tiered segment: %v", err)
	}
	sameRecords(t, "ReadRange(2, 2)", partial, want[1:2])
	// The Rust reader agrees.
	sameRecords(t, "the Rust reader", readWithRust(t, cluster, "orders", "1", "100"), want)

	// A follower starts in the object store and continues on the replicas
	// of the segment a later writer opens.
	following := follow(t, client, "orders", 1)
	for _, wanted := range want {
		if record := following.next(t); record.Sequence != wanted.Sequence || string(record.Payload) != string(wanted.Payload) {
			t.Fatalf("the follower yielded %d %q, want %d %q", record.Sequence, record.Payload, wanted.Sequence, wanted.Payload)
		}
	}
	appendWithRust(t, cluster, "orders", "fourth")
	if record := following.next(t); record.Sequence != 4 || string(record.Payload) != "fourth" {
		t.Fatalf("the follower yielded %d %q, want 4 fourth", record.Sequence, record.Payload)
	}

	// A client that was given no object store says what it lacks.
	withoutObjectStore, err := bitacora.Connect(bitacora.Config{
		EtcdEndpoints: []string{cluster.EtcdEndpoint},
		EtcdKeyPrefix: testcluster.EtcdKeyPrefix,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = withoutObjectStore.Close() })
	if _, err := withoutObjectStore.OpenReader("orders").ReadRange(t.Context(), 1, 3); !errors.Is(err, bitacora.ErrUnavailable) {
		t.Errorf("reading a tiered segment without an object store = %v, want ErrUnavailable", err)
	}
}
