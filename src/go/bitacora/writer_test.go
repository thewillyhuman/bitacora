package bitacora

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/wire"
)

// runController keeps a standby segment placed for a stream for the rest of
// the test, as the controller does, so that a writer can roll at any time.
func (cluster *testCluster) runController(streamName string) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(cluster.t.Context()))
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for ctx.Err() == nil {
			// A stream that does not exist yet is tried again.
			_, _ = cluster.store.UpdateStream(ctx, streamName, func(stream *metadata.StreamRecord) error {
				if stream.StandbyReplicas == nil {
					stream.StandbyReplicas = cluster.replicaIDs()
				}
				return nil
			})
			pause(ctx, 2*time.Millisecond)
		}
	}()
	cluster.t.Cleanup(func() {
		cancel()
		<-finished
	})
}

// streamRecord reads a stream's record.
func (cluster *testCluster) streamRecord(name string) metadata.StreamRecord {
	cluster.t.Helper()
	stream, err := cluster.store.GetStream(context.WithoutCancel(cluster.t.Context()), name)
	if err != nil {
		cluster.t.Fatalf("read stream %s: %v", name, err)
	}
	return stream
}

// openWriter opens a writer that is closed, if the test did not, when the
// test ends.
func (cluster *testCluster) openWriter(streamName string, options WriterOptions) *Writer {
	cluster.t.Helper()
	writer, err := cluster.client.OpenWriter(cluster.t.Context(), streamName, options)
	if err != nil {
		cluster.t.Fatalf("OpenWriter: %v", err)
	}
	cluster.t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = writer.Close(closeCtx)
	})
	return writer
}

// waitFor polls condition until it holds, and fails the test if it does not
// within a time far longer than anything here takes.
func waitFor(t testing.TB, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen in time", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// expectNoWriterGoroutines fails the test if a goroutine of a writer or of
// a takeover is still running.
func expectNoWriterGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		stacks := make([]byte, 1<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		var leaked []string
		for goroutine := range strings.SplitSeq(string(stacks), "\n\n") {
			if strings.Contains(goroutine, "bitacora.(*Writer).") || strings.Contains(goroutine, "bitacora.(*Client).fenceReplicas") {
				leaked = append(leaked, goroutine)
			}
		}
		if len(leaked) == 0 {
			return
		}
		// A goroutine that has returned may take a moment to be gone.
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines of a writer are still running:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readAll reads a whole stream once everything up to last is readable.
func (cluster *testCluster) readAll(streamName string, last uint64) []Record {
	cluster.t.Helper()
	reader := cluster.client.OpenReader(streamName)
	var records []Record
	waitFor(cluster.t, fmt.Sprintf("reading %s up to %d", streamName, last), func() bool {
		read, err := reader.ReadRange(cluster.t.Context(), 1, last)
		records = read
		return err == nil && uint64(len(read)) == last
	})
	return records
}

func TestWriterAppendsAndClosesCleanly(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{Identity: "compute-1"})
	if writer.Epoch() != 1 || writer.DurableSequence() != 0 {
		t.Fatalf("a writer of a new stream holds epoch %d at durable sequence %d, want 1 and 0", writer.Epoch(), writer.DurableSequence())
	}
	if lease := cluster.streamRecord("orders").Writer; lease == nil || lease.WriterIdentity != "compute-1" {
		t.Errorf("the lease is %+v, want one held by compute-1", lease)
	}

	for index, payload := range []string{"one", "two", "three"} {
		sequence, err := writer.Append(t.Context(), []byte(payload))
		if err != nil || sequence != uint64(index+1) {
			t.Fatalf("Append(%q) = %d, %v; want %d", payload, sequence, err, index+1)
		}
	}
	first, last, err := writer.AppendBatch(t.Context(), [][]byte{[]byte("four"), []byte("five")})
	if err != nil || first != 4 || last != 5 {
		t.Fatalf("AppendBatch = %d to %d, %v; want 4 to 5", first, last, err)
	}
	if writer.DurableSequence() != 5 {
		t.Errorf("durable sequence = %d after five committed records", writer.DurableSequence())
	}
	// Readers see the records while the writer is open, once its next
	// message has told the replicas that they are committed.
	want := []string{"one", "two", "three", "four", "five"}
	if got := payloadsOf(cluster.readAll("orders", 5)); !slices.Equal(got, want) {
		t.Errorf("read %v while the writer is open, want %v", got, want)
	}

	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The writer stepped down: the segment is sealed where the stream ends
	// and nobody holds the lease, so the next writer starts at once.
	stream := cluster.streamRecord("orders")
	if len(stream.Segments) != 1 || stream.Segments[0].Sealed == nil || stream.Segments[0].Sealed.End != 5 {
		t.Errorf("after Close the segments are %+v, want one segment sealed at 5", stream.Segments)
	}
	if stream.Writer != nil {
		t.Errorf("after Close the lease is still held: %+v", stream.Writer)
	}
	address := frame.SegmentAddress{StreamID: stream.StreamID, SegmentID: 1}
	for _, fake := range cluster.replicas {
		if sealedEnd := fake.sealedEndOf(address); sealedEnd == nil || *sealedEnd != 5 {
			t.Errorf("%s was not told the seal at 5", fake.replicaID)
		}
	}
	expectNoWriterGoroutines(t)
}

func TestAClosedWriterStaysClosed(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{})
	if _, err := writer.Append(t.Context(), []byte("one")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := writer.Close(t.Context()); err != nil {
		t.Errorf("a second Close = %v, want the result of the first", err)
	}
	if sequence, err := writer.Append(t.Context(), []byte("two")); !errors.Is(err, ErrClosed) || sequence != 0 {
		t.Errorf("Append after Close = %d, %v; want ErrClosed", sequence, err)
	}
	if _, _, err := writer.AppendBatch(t.Context(), [][]byte{[]byte("two")}); !errors.Is(err, ErrClosed) {
		t.Errorf("AppendBatch after Close = %v, want ErrClosed", err)
	}
}

func TestAppendRefusesABatchItCannotEncode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{})

	cases := []struct {
		name     string
		payloads [][]byte
	}{
		{name: "no records", payloads: nil},
		{name: "a record larger than the limit", payloads: [][]byte{make([]byte, frame.MaxRecordPayloadBytes+1)}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, _, err := writer.AppendBatch(t.Context(), testCase.payloads); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("AppendBatch = %v, want ErrInvalidArgument", err)
			}
		})
	}
	// A refused batch takes no sequence number.
	if sequence, err := writer.Append(t.Context(), []byte("one")); err != nil || sequence != 1 {
		t.Errorf("the first accepted record got sequence %d, %v; want 1", sequence, err)
	}
}

func TestConcurrentAppendsGetConsecutiveSequenceNumbers(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{})
	const appenders, appendsEach = 16, 25

	// Every record carries the name of its append; the sequence number it
	// was told must be where it is read back.
	told := make(map[uint64]string)
	var toldMutex sync.Mutex
	var appending sync.WaitGroup
	for appender := range appenders {
		appending.Go(func() {
			for index := range appendsEach {
				payload := fmt.Sprintf("appender %d record %d", appender, index)
				sequence, err := writer.Append(t.Context(), []byte(payload))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				toldMutex.Lock()
				told[sequence] = payload
				toldMutex.Unlock()
			}
		})
	}
	appending.Wait()
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	const total = appenders * appendsEach
	if len(told) != total {
		t.Fatalf("%d appends got %d distinct sequence numbers", total, len(told))
	}
	for _, record := range cluster.readAll("orders", total) {
		if told[record.Sequence] != string(record.Payload) {
			t.Fatalf("sequence %d holds %q, but its appender was told %q", record.Sequence, record.Payload, told[record.Sequence])
		}
	}
}

func TestAppendsCommitOnAQuorumAndWaitWithoutOne(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{})

	// One replica of three is down: two still make the write quorum.
	cluster.replicas[2].stop()
	if sequence, err := writer.Append(t.Context(), []byte("one")); err != nil || sequence != 1 {
		t.Fatalf("Append with one replica down = %d, %v; want 1", sequence, err)
	}

	// A second one stops answering: nothing can commit. The append waits as
	// long as its context allows, and then its outcome is unknown: the
	// record has its sequence number and may still commit.
	cluster.replicas[1].goSilent()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	sequence, err := writer.Append(ctx, []byte("two"))
	if !errors.Is(err, ErrIndeterminate) || !errors.Is(err, context.DeadlineExceeded) || sequence != 2 {
		t.Errorf("Append without a quorum = %d, %v; want sequence 2 with ErrIndeterminate from the deadline", sequence, err)
	}
	if writer.DurableSequence() != 1 {
		t.Errorf("durable sequence = %d, want 1", writer.DurableSequence())
	}

	// Closing cannot commit it either, and says so.
	closeCtx, cancelClose := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelClose()
	if err := writer.Close(closeCtx); !errors.Is(err, ErrIndeterminate) {
		t.Errorf("Close with an uncommitted record = %v, want ErrIndeterminate", err)
	}
	expectNoWriterGoroutines(t)
}

func TestAppendWaitsWhenTooMuchIsUncommitted(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{MaxUncommittedBytes: 64})
	for _, fake := range cluster.replicas {
		fake.goSilent()
	}

	// The first record fills the allowance and stays uncommitted.
	firstResult := make(chan error, 1)
	go func() {
		_, err := writer.Append(t.Context(), make([]byte, 128))
		firstResult <- err
	}()
	waitFor(t, "the first record to be assigned", func() bool {
		writer.mutex.Lock()
		defer writer.mutex.Unlock()
		return writer.state.hasUncommitted()
	})

	// The second one is not even assigned a sequence number: when its
	// context ends, nothing is in doubt, and the error is the context's.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	sequence, err := writer.Append(ctx, []byte("two"))
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrIndeterminate) || sequence != 0 {
		t.Errorf("an append that waited for room = %d, %v; want no sequence and the context's error", sequence, err)
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancelClose()
	_ = writer.Close(closeCtx)
	if err := <-firstResult; !errors.Is(err, ErrIndeterminate) {
		t.Errorf("the record that was pending when the writer closed got %v, want ErrIndeterminate", err)
	}
}

func TestWriterRollsAwayFromAReplicaThatRefuses(t *testing.T) {
	refusals := map[string]*wire.ReplicaError{
		"its disk is full": {Kind: &wire.ReplicaError_NoCapacity_{NoCapacity: &wire.ReplicaError_NoCapacity{UsedPercent: 99}}},
		"it is draining":   {Kind: &wire.ReplicaError_Draining_{Draining: &wire.ReplicaError_Draining{}}},
		"its disk failed":  {Kind: &wire.ReplicaError_Storage_{Storage: &wire.ReplicaError_Storage{Reason: "input/output error"}}},
	}
	for name, refusal := range refusals {
		t.Run(name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			cluster.createStream("orders", 2)
			writer := cluster.openWriter("orders", WriterOptions{})
			if _, err := writer.Append(t.Context(), []byte("one")); err != nil {
				t.Fatalf("Append: %v", err)
			}

			// A replica refuses appends for good. The other two keep the
			// stream committing; the writer rolls as soon as a standby
			// segment is placed.
			cluster.replicas[0].refuseAppends(refusal)
			if sequence, err := writer.Append(t.Context(), []byte("two")); err != nil || sequence != 2 {
				t.Fatalf("Append while a replica refuses = %d, %v; want 2", sequence, err)
			}
			waitFor(t, "the writer to hear the refusal", func() bool {
				writer.mutex.Lock()
				defer writer.mutex.Unlock()
				return writer.state.replicas[0].hasRefused
			})
			cluster.replicas[0].refuseAppends(nil)
			cluster.placeStandby("orders")
			waitFor(t, "the roll", func() bool { return len(cluster.streamRecord("orders").Segments) == 2 })
			if sequence, err := writer.Append(t.Context(), []byte("three")); err != nil || sequence != 3 {
				t.Fatalf("Append after the roll = %d, %v; want 3", sequence, err)
			}
			if err := writer.Close(t.Context()); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// The first segment ends where the stream was committed when the
			// writer rolled, and the stream has no gap and no repeat.
			stream := cluster.streamRecord("orders")
			if first := stream.Segments[0]; first.Sealed == nil || first.Sealed.End != 2 {
				t.Errorf("the first segment is %+v, want it sealed at 2", first)
			}
			if second := stream.Segments[1]; second.FirstSequence != 3 || second.Sealed == nil || second.Sealed.End != 3 {
				t.Errorf("the second segment is %+v, want records 3 to 3", second)
			}
			if got := payloadsOf(cluster.readAll("orders", 3)); !slices.Equal(got, []string{"one", "two", "three"}) {
				t.Errorf("read %v, want one, two, three", got)
			}
			expectNoWriterGoroutines(t)
		})
	}
}

func TestWriterRollsAwayFromASilentReplicaAndKeepsItsRecords(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{ReplicaFailureTimeout: 150 * time.Millisecond})
	if _, err := writer.Append(t.Context(), []byte("one")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A replica dies. Appends go on while the writer waits to see whether
	// it comes back, and across the roll once it has not.
	cluster.replicas[0].stop()
	cluster.placeStandby("orders")
	const total = 40
	for sequence := uint64(2); sequence <= total; sequence++ {
		appended, err := writer.Append(t.Context(), fmt.Appendf(nil, "record %d", sequence))
		if err != nil || appended != sequence {
			t.Fatalf("Append = %d, %v; want %d", appended, err, sequence)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if segments := cluster.streamRecord("orders").Segments; len(segments) != 2 {
		t.Fatalf("the stream has %d segments, want 2: one roll away from the dead replica", len(segments))
	}
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	records := cluster.readAll("orders", total)
	for index, record := range records[1:] {
		if want := fmt.Sprintf("record %d", index+2); string(record.Payload) != want {
			t.Fatalf("sequence %d holds %q, want %q", record.Sequence, record.Payload, want)
		}
	}
}

func TestWriterRollsAtTheTargetSizeAndAge(t *testing.T) {
	cases := []struct {
		name    string
		options WriterOptions
		// pace is the pause between two appends.
		pace time.Duration
	}{
		// The writer looks at the size every few milliseconds, so the
		// appends are paced for it to see each segment fill.
		{name: "size", options: WriterOptions{TargetSegmentBytes: 2048}, pace: 3 * time.Millisecond},
		{name: "age", options: WriterOptions{TargetSegmentAge: 60 * time.Millisecond}, pace: 5 * time.Millisecond},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			cluster.createStream("orders", 2)
			cluster.runController("orders")
			writer := cluster.openWriter("orders", testCase.options)

			const total = 60
			for sequence := uint64(1); sequence <= total; sequence++ {
				appended, err := writer.Append(t.Context(), make([]byte, 200))
				if err != nil || appended != sequence {
					t.Fatalf("Append = %d, %v; want %d", appended, err, sequence)
				}
				time.Sleep(testCase.pace)
			}
			if err := writer.Close(t.Context()); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Several segments, each starting where the one before ended.
			stream := cluster.streamRecord("orders")
			if len(stream.Segments) < 3 {
				t.Fatalf("the stream has %d segments, want several", len(stream.Segments))
			}
			nextFirst := uint64(1)
			for _, segment := range stream.Segments {
				if segment.FirstSequence != nextFirst || segment.Sealed == nil {
					t.Fatalf("segment %d starts at %d and is sealed %v; want it sealed and starting at %d",
						segment.SegmentID, segment.FirstSequence, segment.Sealed, nextFirst)
				}
				nextFirst = segment.Sealed.End + 1
			}
			if nextFirst != total+1 {
				t.Errorf("the segments end at %d, want %d", nextFirst-1, total)
			}
			if records := cluster.readAll("orders", total); len(records) != total {
				t.Errorf("read %d records, want %d", len(records), total)
			}
			expectNoWriterGoroutines(t)
		})
	}
}

// crashedWriter leaves a stream as a writer that died would: records one
// and two on every replica, three on replicas 0 and 1, which makes it
// committed, and four on replica 0 alone, which does not. The segment is
// open and the lease has lapsed.
func crashedWriter(cluster *testCluster) frame.SegmentAddress {
	cluster.createStream("orders", 2)
	writer := cluster.openFixtureWriter("orders")
	writer.append([]string{"one", "two"})
	writer.append([]string{"three"}, cluster.replicas[0], cluster.replicas[1])
	writer.append([]string{"four"}, cluster.replicas[0])
	cluster.placeStandby("orders")
	return writer.segment.Address
}

func TestTakeoverRecoversTheSegmentOfACrashedWriter(t *testing.T) {
	cases := []struct {
		name string
		// downReplicas are stopped before the takeover.
		downReplicas []int
		// wantSealedEnd is where the crashed writer's segment must end.
		wantSealedEnd uint64
		// wantRead is the stream after the new writer appended "next".
		wantRead []string
	}{
		{
			name:          "every replica answers: the longest copy decides",
			wantSealedEnd: 4,
			wantRead:      []string{"one", "two", "three", "four", "next"},
		},
		{
			name:          "the shortest copy is down",
			downReplicas:  []int{2},
			wantSealedEnd: 4,
			wantRead:      []string{"one", "two", "three", "four", "next"},
		},
		{
			// Record four was on one replica only, so it was never
			// committed, and a recovery that cannot see it drops it.
			name:          "the longest copy is down: what was committed is kept",
			downReplicas:  []int{0},
			wantSealedEnd: 3,
			wantRead:      []string{"one", "two", "three", "next"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			crashedSegment := crashedWriter(cluster)
			for _, replicaIndex := range testCase.downReplicas {
				cluster.replicas[replicaIndex].stop()
			}

			writer := cluster.openWriter("orders", WriterOptions{})

			if writer.Epoch() != 2 || writer.DurableSequence() != testCase.wantSealedEnd {
				t.Errorf("the new writer holds epoch %d at durable sequence %d, want epoch 2 at %d",
					writer.Epoch(), writer.DurableSequence(), testCase.wantSealedEnd)
			}
			stream := cluster.streamRecord("orders")
			if recovered := stream.Segments[0]; recovered.Sealed == nil || recovered.Sealed.End != testCase.wantSealedEnd {
				t.Fatalf("the crashed writer's segment is %+v, want it sealed at %d", recovered, testCase.wantSealedEnd)
			}
			// Every replica that answered holds the segment up to its end:
			// the tail was repaired before the seal.
			for replicaIndex, fake := range cluster.replicas {
				if slices.Contains(testCase.downReplicas, replicaIndex) {
					continue
				}
				if localEnd, _ := fake.localEndOf(crashedSegment); localEnd != testCase.wantSealedEnd {
					t.Errorf("%s holds the recovered segment up to %d, want %d", fake.replicaID, localEnd, testCase.wantSealedEnd)
				}
			}

			sequence, err := writer.Append(t.Context(), []byte("next"))
			if err != nil || sequence != testCase.wantSealedEnd+1 {
				t.Fatalf("the new writer's first record got %d, %v; want %d", sequence, err, testCase.wantSealedEnd+1)
			}
			if err := writer.Close(t.Context()); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := payloadsOf(cluster.readAll("orders", sequence)); !slices.Equal(got, testCase.wantRead) {
				t.Errorf("read %v, want %v", got, testCase.wantRead)
			}
			expectNoWriterGoroutines(t)
		})
	}
}

func TestTakeoverNeedsEnoughReplicasOfTheOldSegment(t *testing.T) {
	cluster := newTestCluster(t, 3)
	crashedWriter(cluster)
	// With a write quorum of two of three, two answers are needed to be
	// sure of seeing every committed record. One is not enough.
	cluster.replicas[0].stop()
	cluster.replicas[1].stop()

	writer, err := cluster.client.OpenWriter(t.Context(), "orders", WriterOptions{OpenTimeout: 300 * time.Millisecond})

	if !errors.Is(err, ErrUnavailable) || writer != nil {
		t.Fatalf("OpenWriter = %v, %v; want ErrUnavailable", writer, err)
	}
	stream := cluster.streamRecord("orders")
	if stream.Segments[0].Sealed != nil {
		t.Errorf("the segment was sealed at %d on the word of one replica", stream.Segments[0].Sealed.End)
	}
	// The candidate that gave up does not keep the next one waiting.
	if stream.Writer != nil {
		t.Errorf("the failed takeover still holds the lease: %+v", stream.Writer)
	}
	expectNoWriterGoroutines(t)
}

func TestANewWriterFencesTheOldOne(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	cluster.runController("orders")
	oldWriter := cluster.openWriter("orders", WriterOptions{Identity: "old"})
	if _, err := oldWriter.Append(t.Context(), []byte("from the old writer")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	newWriter := cluster.openWriter("orders", WriterOptions{Identity: "new", Takeover: ForceTakeover})

	if newWriter.Epoch() != oldWriter.Epoch()+1 {
		t.Fatalf("the new writer holds epoch %d, want %d", newWriter.Epoch(), oldWriter.Epoch()+1)
	}
	// The old writer learns that it lost the stream, at the latest from its
	// next heartbeat, and every append from then on says so.
	waitFor(t, "the old writer to notice the fence", func() bool {
		_, err := oldWriter.Append(t.Context(), []byte("too late"))
		return errors.Is(err, ErrFenced)
	})
	if _, err := oldWriter.Append(t.Context(), []byte("too late")); !errors.Is(err, ErrFenced) {
		t.Errorf("a fenced writer's append = %v, want ErrFenced", err)
	}
	if err := oldWriter.Close(t.Context()); !errors.Is(err, ErrFenced) {
		t.Errorf("a fenced writer's Close = %v, want ErrFenced", err)
	}

	sequence, err := newWriter.Append(t.Context(), []byte("from the new writer"))
	if err != nil {
		t.Fatalf("the new writer's Append: %v", err)
	}
	if err := newWriter.Close(t.Context()); err != nil {
		t.Fatalf("the new writer's Close: %v", err)
	}
	// Whatever the old writer got in before the fence is in the stream
	// once; nothing it tried afterwards is.
	records := cluster.readAll("orders", sequence)
	if string(records[0].Payload) != "from the old writer" || string(records[len(records)-1].Payload) != "from the new writer" {
		t.Errorf("the stream is %q", payloadsOf(records))
	}
	if stream := cluster.streamRecord("orders"); stream.Epoch != newWriter.Epoch() {
		t.Errorf("the stream is at epoch %d, want the new writer's %d", stream.Epoch, newWriter.Epoch())
	}
	expectNoWriterGoroutines(t)
}

func TestOpenWriterRespectsALiveLease(t *testing.T) {
	// holdLease makes a writer that never comes back hold the stream until
	// lapsesIn from now.
	holdLease := func(cluster *testCluster, lapsesIn time.Duration) {
		expiresAt := uint64(time.Now().Add(lapsesIn).UnixMilli())
		if _, err := cluster.store.ClaimEpoch(cluster.t.Context(), "orders", 0, "holder", expiresAt); err != nil {
			cluster.t.Fatalf("claim the epoch: %v", err)
		}
	}

	t.Run("it gives up at once if the lease outlasts the timeout", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		cluster.createStream("orders", 2)
		holdLease(cluster, time.Hour)
		startedAt := time.Now()

		writer, err := cluster.client.OpenWriter(t.Context(), "orders", WriterOptions{OpenTimeout: 5 * time.Second})

		if !errors.Is(err, ErrLeaseHeld) || writer != nil {
			t.Fatalf("OpenWriter = %v, %v; want ErrLeaseHeld", writer, err)
		}
		if waited := time.Since(startedAt); waited > 2*time.Second {
			t.Errorf("OpenWriter waited %v for a lease it could see would not lapse in time", waited)
		}
		if stream := cluster.streamRecord("orders"); stream.Epoch != 1 || stream.Writer == nil || stream.Writer.WriterIdentity != "holder" {
			t.Errorf("the stream changed hands although the lease was respected: epoch %d, lease %+v", stream.Epoch, stream.Writer)
		}
	})

	t.Run("it waits for a lease that lapses in time", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		cluster.createStream("orders", 2)
		holdLease(cluster, 250*time.Millisecond)
		startedAt := time.Now()

		writer := cluster.openWriter("orders", WriterOptions{})

		if waited := time.Since(startedAt); waited < 200*time.Millisecond {
			t.Errorf("OpenWriter took the stream after %v, before the lease lapsed", waited)
		}
		if writer.Epoch() != 2 {
			t.Errorf("the writer holds epoch %d, want 2", writer.Epoch())
		}
	})

	t.Run("forcing takes the stream at once", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		cluster.createStream("orders", 2)
		holdLease(cluster, time.Hour)

		writer := cluster.openWriter("orders", WriterOptions{Takeover: ForceTakeover})

		if writer.Epoch() != 2 {
			t.Errorf("the writer holds epoch %d, want 2", writer.Epoch())
		}
	})
}

func TestWriterRenewsItsLease(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openWriter("orders", WriterOptions{LeaseDuration: 300 * time.Millisecond})
	firstExpiry := cluster.streamRecord("orders").Writer.ExpiresAt

	// Well past the first lease: without renewal it would have lapsed.
	time.Sleep(450 * time.Millisecond)

	lease := cluster.streamRecord("orders").Writer
	if lease == nil || lease.ExpiresAt <= firstExpiry {
		t.Fatalf("the lease was not renewed: %+v, first expiry %d", lease, firstExpiry)
	}
	if now := uint64(time.Now().UnixMilli()); lease.ExpiresAt <= now {
		t.Errorf("the lease of a running writer has lapsed: it expired %d ms ago", now-lease.ExpiresAt)
	}
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOpenWriterWaitsForAStandbySegment(t *testing.T) {
	replication := metadata.ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2}

	t.Run("until the controller places one", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		if _, err := cluster.store.CreateStream(t.Context(), "orders", replication, metadata.RetentionPolicy{}); err != nil {
			t.Fatalf("create the stream: %v", err)
		}
		time.AfterFunc(150*time.Millisecond, func() { cluster.runController("orders") })
		startedAt := time.Now()

		writer := cluster.openWriter("orders", WriterOptions{})

		if waited := time.Since(startedAt); waited < 100*time.Millisecond {
			t.Errorf("the writer opened after %v, before a standby segment was placed", waited)
		}
		if sequence, err := writer.Append(t.Context(), []byte("one")); err != nil || sequence != 1 {
			t.Errorf("Append = %d, %v; want 1", sequence, err)
		}
	})

	t.Run("and gives up when none is", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		if _, err := cluster.store.CreateStream(t.Context(), "orders", replication, metadata.RetentionPolicy{}); err != nil {
			t.Fatalf("create the stream: %v", err)
		}

		writer, err := cluster.client.OpenWriter(t.Context(), "orders", WriterOptions{OpenTimeout: 250 * time.Millisecond})

		if !errors.Is(err, ErrUnavailable) || writer != nil || !strings.Contains(err.Error(), "standby") {
			t.Fatalf("OpenWriter = %v, %v; want ErrUnavailable that names the standby segment", writer, err)
		}
		if lease := cluster.streamRecord("orders").Writer; lease != nil {
			t.Errorf("the failed open still holds the lease: %+v", lease)
		}
	})
}

func TestOpenWriterFailsForWhatCannotBeOpened(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)

	t.Run("a stream that does not exist", func(t *testing.T) {
		writer, err := cluster.client.OpenWriter(t.Context(), "payments", WriterOptions{})
		if !errors.Is(err, ErrStreamNotFound) || writer != nil {
			t.Errorf("OpenWriter = %v, %v; want ErrStreamNotFound", writer, err)
		}
	})

	t.Run("a context that is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		writer, err := cluster.client.OpenWriter(ctx, "orders", WriterOptions{})
		if !errors.Is(err, context.Canceled) || writer != nil {
			t.Errorf("OpenWriter = %v, %v; want context.Canceled", writer, err)
		}
	})
	expectNoWriterGoroutines(t)
}
