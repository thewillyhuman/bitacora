package interop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

// openWriter opens a Go writer of a stream. If the test does not close it,
// it is closed when the test ends.
func openWriter(t *testing.T, client *bitacora.Client, stream string, options bitacora.WriterOptions) *bitacora.Writer {
	t.Helper()
	writer, err := client.OpenWriter(t.Context(), stream, options)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = writer.Close(closeCtx)
	})
	return writer
}

// eventually polls condition until it holds.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(patience)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen in %v", what, patience)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRustReadsWhatGoAppended(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	client := connect(t, cluster)
	writer := openWriter(t, client, "orders", bitacora.WriterOptions{Identity: "go-writer"})

	want := []bitacora.Record{
		{Sequence: 1, Payload: []byte("first")},
		{Sequence: 2, Payload: []byte{0x00, 0xff, 0x10, 0x80}},
		{Sequence: 3, Payload: []byte("")},
		{Sequence: 4, Payload: []byte("fourth")},
		{Sequence: 5, Payload: []byte("fifth")},
	}
	for _, record := range want[:3] {
		sequence, err := writer.Append(t.Context(), record.Payload)
		if err != nil || sequence != record.Sequence {
			t.Fatalf("Append = %d, %v; want %d", sequence, err, record.Sequence)
		}
	}
	first, last, err := writer.AppendBatch(t.Context(), [][]byte{want[3].Payload, want[4].Payload})
	if err != nil || first != 4 || last != 5 {
		t.Fatalf("AppendBatch = %d to %d, %v; want 4 to 5", first, last, err)
	}

	// The Rust reader sees the records while the Go writer is open: the
	// real replicas serve them on the word of the Go writer's heartbeats.
	var readWhileOpen []bitacora.Record
	eventually(t, "the Rust reader to see the open segment", func() bool {
		readWhileOpen = readWithRust(t, cluster, "orders", "1", "100")
		return len(readWhileOpen) == len(want)
	})
	sameRecords(t, "the Rust reader, writer open", readWhileOpen, want)

	// The Go writer steps down: it seals its segment and gives the lease
	// back, in the records the Rust tools read.
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sameRecords(t, "the Rust reader, writer closed", readWithRust(t, cluster, "orders", "1", "100"), want)
	stream := waitForStream(t, cluster, "orders", "the seal", func(stream metadata.StreamRecord) bool {
		return len(stream.Segments) == 1 && stream.Segments[0].Sealed != nil
	})
	if stream.Segments[0].Sealed.End != 5 || stream.Writer != nil {
		t.Errorf("after Close: sealed at %d, lease %+v; want sealed at 5 and no lease", stream.Segments[0].Sealed.End, stream.Writer)
	}

	// A Rust writer that respects leases takes the stream without waiting,
	// and continues where the Go writer stopped.
	startedAt := time.Now()
	output := cluster.Bita(t, "stream", "append", "orders", "sixth")
	if waited := time.Since(startedAt); waited > 5*time.Second {
		t.Errorf("the Rust writer waited %v for a lease the Go writer had given back", waited)
	}
	if !strings.Contains(output, `"first_sequence":6`) && !strings.Contains(output, `"first_sequence": 6`) {
		t.Errorf("the Rust writer appended at %s, want sequence 6", output)
	}
	all, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 100)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	sameRecords(t, "the Go reader, after both writers", all, append(want, bitacora.Record{Sequence: 6, Payload: []byte("sixth")}))
}

func TestGoWriterTakesOverFromARustWriter(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	client := connect(t, cluster)
	reader := client.OpenReader("orders")

	// A Rust writer that stays open and keeps appending.
	rustWriter := cluster.StartBenchmark(t, "append", "orders", "--force",
		"--rate", "200", "--records", "1000000", "--record-bytes", "32", "--appends-in-flight", "4")
	eventually(t, "the Rust writer to commit some records", func() bool {
		records, err := reader.ReadRange(t.Context(), 1, 20)
		return err == nil && len(records) == 20
	})

	// The Go writer takes the stream from it: it claims the next epoch,
	// fences the replicas, recovers the Rust writer's open segment and
	// opens its own.
	goWriter := openWriter(t, client, "orders", bitacora.WriterOptions{Takeover: bitacora.ForceTakeover})
	recoveredEnd := goWriter.DurableSequence()
	if recoveredEnd < 20 {
		t.Fatalf("the Go writer recovered the stream up to %d, below what was read as committed", recoveredEnd)
	}

	// The Rust writer finds out that it was fenced, and stops. Every record
	// it was told is committed survived the Go writer's recovery.
	exitCode, printed := rustWriter.WaitForExit(t, patience)
	var report struct {
		Records uint64 `json:"records"`
		Failure string `json:"failure"`
	}
	if err := json.Unmarshal([]byte(printed), &report); err != nil {
		t.Fatalf("the Rust writer printed %q: %v", printed, err)
	}
	if exitCode != 1 || !strings.Contains(report.Failure, "another writer has taken the stream") {
		t.Errorf("the Rust writer exited with %d saying %q; want exit code 1 and that another writer took the stream", exitCode, report.Failure)
	}
	if report.Records < 20 || report.Records > recoveredEnd {
		t.Errorf("the Rust writer committed %d records, the Go writer recovered the stream up to %d", report.Records, recoveredEnd)
	}

	sequence, err := goWriter.Append(t.Context(), []byte("from the Go writer"))
	if err != nil || sequence != recoveredEnd+1 {
		t.Fatalf("the Go writer's first record got %d, %v; want %d", sequence, err, recoveredEnd+1)
	}
	if err := goWriter.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// One stream, without a gap: everything the Rust writer committed, then
	// the Go writer's record. Both readers see it.
	all, err := reader.ReadRange(t.Context(), 1, sequence+10)
	if err != nil || uint64(len(all)) != sequence {
		t.Fatalf("ReadRange = %d records, %v; want %d", len(all), err, sequence)
	}
	if last := all[len(all)-1]; string(last.Payload) != "from the Go writer" {
		t.Errorf("the last record is %q, want the Go writer's", last.Payload)
	}
	tail := readWithRust(t, cluster, "orders", fmt.Sprint(sequence-1), "10")
	sameRecords(t, "the Rust reader, across the takeover", tail, all[len(all)-2:])
	if stream := waitForStream(t, cluster, "orders", "the read of the record", func(metadata.StreamRecord) bool { return true }); stream.Epoch != goWriter.Epoch() {
		t.Errorf("the stream is at epoch %d, want the Go writer's %d", stream.Epoch, goWriter.Epoch())
	}
}

func TestRustWriterTakesOverFromAGoWriter(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	client := connect(t, cluster)
	goWriter := openWriter(t, client, "orders", bitacora.WriterOptions{})
	if _, err := goWriter.Append(t.Context(), []byte("from the Go writer")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A Rust writer takes the stream at once, appends and steps down.
	appended := appendWithRust(t, cluster, "orders", "from the Rust writer")
	if appended.Epoch != goWriter.Epoch()+1 || appended.FirstSequence != 2 {
		t.Fatalf("the Rust writer appended %+v; want sequence 2 under epoch %d", appended, goWriter.Epoch()+1)
	}

	// The Go writer is fenced: the replicas refuse its epoch, and so does
	// the metadata store.
	if sequence, err := goWriter.Append(t.Context(), []byte("too late")); !errors.Is(err, bitacora.ErrFenced) {
		t.Errorf("the fenced Go writer's Append = %d, %v; want ErrFenced", sequence, err)
	}
	if err := goWriter.Close(t.Context()); !errors.Is(err, bitacora.ErrFenced) {
		t.Errorf("the fenced Go writer's Close = %v, want ErrFenced", err)
	}

	want := []bitacora.Record{
		{Sequence: 1, Payload: []byte("from the Go writer")},
		{Sequence: 2, Payload: []byte("from the Rust writer")},
	}
	all, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 100)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	sameRecords(t, "the Go reader", all, want)
	sameRecords(t, "the Rust reader", readWithRust(t, cluster, "orders", "1", "100"), want)
}

func TestGoWriterKeepsCommittingWhenAReplicaDies(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	cluster.Bita(t, "stream", "create", "orders")
	client := connect(t, cluster)
	// A writer that gives up on a silent replica after one second instead
	// of ten, so that the test sees the roll without waiting.
	writer := openWriter(t, client, "orders", bitacora.WriterOptions{ReplicaFailureTimeout: time.Second})
	appendNext := func(sequence uint64) {
		t.Helper()
		appended, err := writer.Append(t.Context(), fmt.Appendf(nil, "record %d", sequence))
		if err != nil || appended != sequence {
			t.Fatalf("Append = %d, %v; want %d", appended, err, sequence)
		}
	}
	sequence := uint64(0)
	for range 20 {
		sequence++
		appendNext(sequence)
	}
	// The controller has placed the standby segment the writer will roll
	// to, while all three replicas were there.
	waitForStream(t, cluster, "orders", "the placement of a standby segment", func(stream metadata.StreamRecord) bool {
		return stream.StandbyReplicas != nil
	})

	// A replica is lost. Appends go on with the two that are left, which
	// are the write quorum, and the writer rolls away from the segment
	// that has a silent replica.
	cluster.KillReplica(0)
	hasRolled := func() bool {
		stream := waitForStream(t, cluster, "orders", "a read of the stream", func(metadata.StreamRecord) bool { return true })
		return len(stream.Segments) >= 2
	}
	for deadline := time.Now().Add(patience); !hasRolled(); {
		if time.Now().After(deadline) {
			t.Fatalf("the writer did not roll within %v of the loss of a replica", patience)
		}
		sequence++
		appendNext(sequence)
		time.Sleep(10 * time.Millisecond)
	}
	// And it keeps committing in the new segment.
	for range 20 {
		sequence++
		appendNext(sequence)
	}
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every record that was acknowledged is in the stream, once, in order,
	// for both readers.
	all, err := client.OpenReader("orders").ReadRange(t.Context(), 1, sequence+10)
	if err != nil || uint64(len(all)) != sequence {
		t.Fatalf("ReadRange = %d records, %v; want %d", len(all), err, sequence)
	}
	for index, record := range all {
		if want := fmt.Sprintf("record %d", index+1); record.Sequence != uint64(index+1) || string(record.Payload) != want {
			t.Fatalf("record %d is %d %q, want %q", index+1, record.Sequence, record.Payload, want)
		}
	}
	tail := readWithRust(t, cluster, "orders", fmt.Sprint(sequence-4), "100")
	sameRecords(t, "the Rust reader, after the roll", tail, all[len(all)-5:])
}
