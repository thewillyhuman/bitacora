package bitacora

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
)

// streamOfThreeSegments creates a stream whose records 1 to 3 are in a
// sealed segment (as two batches), 4 and 5 in a second sealed segment, and
// 6 and 7 in the open one.
func streamOfThreeSegments(cluster *testCluster) *fixtureWriter {
	cluster.createStream("orders", 2)
	writer := cluster.openFixtureWriter("orders")
	writer.append([]string{"one", "two"})
	writer.append([]string{"three"})
	writer.roll()
	writer.append([]string{"four", "five"})
	writer.roll()
	writer.append([]string{"six"})
	writer.append([]string{"seven"})
	return writer
}

func TestReadRangeReadsAcrossSegments(t *testing.T) {
	cluster := newTestCluster(t, 3)
	streamOfThreeSegments(cluster)
	reader := cluster.client.OpenReader("orders")

	cases := []struct {
		name        string
		first, last uint64
		want        []string
	}{
		{name: "everything", first: 1, last: 7, want: []string{"one", "two", "three", "four", "five", "six", "seven"}},
		{name: "one record", first: 3, last: 3, want: []string{"three"}},
		{name: "from the middle of a batch", first: 2, last: 5, want: []string{"two", "three", "four", "five"}},
		{name: "only the open segment", first: 6, last: 7, want: []string{"six", "seven"}},
		{name: "past the last record", first: 6, last: 100, want: []string{"six", "seven"}},
		{name: "nothing committed there yet", first: 8, last: 9, want: []string{}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			records, err := reader.ReadRange(t.Context(), testCase.first, testCase.last)
			if err != nil {
				t.Fatalf("ReadRange(%d, %d): %v", testCase.first, testCase.last, err)
			}
			if got := payloadsOf(records); !slices.Equal(got, testCase.want) {
				t.Errorf("ReadRange(%d, %d) = %v, want %v", testCase.first, testCase.last, got, testCase.want)
			}
			for index, record := range records {
				if record.Sequence != testCase.first+uint64(index) {
					t.Errorf("record %d has sequence %d, want %d", index, record.Sequence, testCase.first+uint64(index))
				}
			}
		})
	}
}

func TestReadRangeRefusesWhatCannotBeRead(t *testing.T) {
	cluster := newTestCluster(t, 3)
	streamOfThreeSegments(cluster)

	cases := []struct {
		name        string
		stream      string
		first, last uint64
		want        error
	}{
		{name: "sequence numbers start at 1", stream: "orders", first: 0, last: 5, want: ErrInvalidArgument},
		{name: "an empty range", stream: "orders", first: 5, last: 4, want: ErrInvalidArgument},
		{name: "a stream that does not exist", stream: "payments", first: 1, last: 5, want: ErrStreamNotFound},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			records, err := cluster.client.OpenReader(testCase.stream).ReadRange(t.Context(), testCase.first, testCase.last)
			if !errors.Is(err, testCase.want) || records != nil {
				t.Errorf("ReadRange = %v, %v; want no records and %v", records, err, testCase.want)
			}
		})
	}
}

func TestReadingBelowTheLowWaterMarkIsATypedError(t *testing.T) {
	cluster := newTestCluster(t, 3)
	streamOfThreeSegments(cluster)
	_, err := cluster.store.UpdateStream(t.Context(), "orders", func(stream *metadata.StreamRecord) error {
		stream.LowWaterMark = 4
		return nil
	})
	if err != nil {
		t.Fatalf("raise the low-water mark: %v", err)
	}
	reader := cluster.client.OpenReader("orders")

	_, err = reader.ReadRange(t.Context(), 2, 6)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("ReadRange below the low-water mark = %v, want ErrTruncated", err)
	}
	truncated, isTruncated := errors.AsType[*TruncatedError](err)
	if !isTruncated || truncated.LowWaterMark != 4 {
		t.Errorf("the error is %#v, want a TruncatedError with low-water mark 4", err)
	}

	lowWaterMark, err := reader.LowWaterMark(t.Context())
	if err != nil || lowWaterMark != 4 {
		t.Errorf("LowWaterMark = %d, %v; want 4", lowWaterMark, err)
	}
	records, err := reader.ReadRange(t.Context(), lowWaterMark, 6)
	if err != nil || !slices.Equal(payloadsOf(records), []string{"four", "five", "six"}) {
		t.Errorf("ReadRange from the low-water mark = %v, %v", payloadsOf(records), err)
	}
	if _, err := cluster.client.OpenReader("payments").LowWaterMark(t.Context()); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("LowWaterMark of a missing stream = %v, want ErrStreamNotFound", err)
	}
}

func TestReaderUsesAnotherCopyWhenOneCannotBeUsed(t *testing.T) {
	everything := []string{"one", "two", "three", "four", "five", "six", "seven"}
	cases := []struct {
		name string
		// arrange builds the stream, with something wrong with replica 0.
		arrange func(cluster *testCluster)
	}{
		{
			name: "a replica is down",
			arrange: func(cluster *testCluster) {
				streamOfThreeSegments(cluster)
				cluster.replicas[0].stop()
			},
		},
		{
			name: "a replica returns damaged frames",
			arrange: func(cluster *testCluster) {
				streamOfThreeSegments(cluster)
				cluster.replicas[0].corruptReads()
			},
		},
		{
			name: "a copy is marked bad",
			arrange: func(cluster *testCluster) {
				streamOfThreeSegments(cluster)
				// Were the bad copies asked, the answer's checksum would
				// fail; the reader must not even ask.
				cluster.replicas[0].corruptReads()
				_, err := cluster.store.UpdateStream(cluster.t.Context(), "orders", func(stream *metadata.StreamRecord) error {
					for segmentIndex := range stream.Segments {
						stream.Segments[segmentIndex].Copies[0].State = metadata.CopyBad
					}
					return nil
				})
				if err != nil {
					cluster.t.Fatalf("mark copies bad: %v", err)
				}
			},
		},
		{
			name: "a replica holds less than the others",
			arrange: func(cluster *testCluster) {
				cluster.createStream("orders", 2)
				writer := cluster.openFixtureWriter("orders")
				others := cluster.replicas[1:]
				writer.append([]string{"one", "two"})
				writer.append([]string{"three"}, others...)
				writer.roll()
				writer.append([]string{"four", "five"}, others...)
				writer.roll()
				writer.append([]string{"six"})
				writer.append([]string{"seven"}, others...)
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			testCase.arrange(cluster)

			records, err := cluster.client.OpenReader("orders").ReadRange(t.Context(), 1, 7)

			if err != nil {
				t.Fatalf("ReadRange: %v", err)
			}
			if got := payloadsOf(records); !slices.Equal(got, everything) {
				t.Errorf("ReadRange = %v, want %v", got, everything)
			}
		})
	}
}

func TestReadIsUnavailableWhenNoCopyCanBeUsed(t *testing.T) {
	cases := []struct {
		name         string
		breakReplica func(fake *fakeReplica)
		wantInError  string
	}{
		{name: "every replica is down", breakReplica: (*fakeReplica).stop, wantInError: "did not answer"},
		{name: "every copy is damaged", breakReplica: (*fakeReplica).corruptReads, wantInError: "checksum"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			streamOfThreeSegments(cluster)
			for _, fake := range cluster.replicas {
				testCase.breakReplica(fake)
			}

			records, err := cluster.client.OpenReader("orders").ReadRange(t.Context(), 1, 7)

			if !errors.Is(err, ErrUnavailable) || records != nil {
				t.Fatalf("ReadRange = %v, %v; want no records and ErrUnavailable", records, err)
			}
			if !strings.Contains(err.Error(), testCase.wantInError) {
				t.Errorf("the error %q does not say %q", err, testCase.wantInError)
			}
		})
	}
}

// goldenTieredObject is the object that the Rust implementation writes for
// segment 7/3, epoch 5, holding records 100 to 102: "alpha", "" and
// "gamma!". The same bytes are pinned in
// src/rust/bitacora-segment/tests/format_golden_test.rs.
const goldenTieredObject = "" +
	"17000000640000000000000003000000" +
	"668c9d1f05000000616c706861000000" +
	"000600000067616d6d61216400000000" +
	"00000000000000000000004249544154" +
	"49455201000000070000000000000003" +
	"00000000000000050000000000000064" +
	"0000000000000066000000000000002b" +
	"000000000000000100000083c5a0f034" +
	"ccd63b"

// tieredStream creates a stream whose only segment is the golden object in
// the object store, and returns the object's bytes. If areCopiesDropped is
// false, the record still names replicas, none of which holds the segment.
func tieredStream(cluster *testCluster, areCopiesDropped bool) []byte {
	cluster.t.Helper()
	object, err := hex.DecodeString(goldenTieredObject)
	if err != nil {
		cluster.t.Fatalf("the golden text is not hexadecimal: %v", err)
	}
	const objectKey = "segments/7/3"
	objectPath := filepath.Join(cluster.objectStoreDirectory, "objects", filepath.FromSlash(objectKey))
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		cluster.t.Fatalf("create the object's directory: %v", err)
	}
	if err := os.WriteFile(objectPath, object, 0o644); err != nil {
		cluster.t.Fatalf("write the object: %v", err)
	}

	cluster.createStream("archive", 2)
	_, err = cluster.store.UpdateStream(cluster.t.Context(), "archive", func(stream *metadata.StreamRecord) error {
		copies := []metadata.CopyRecord{}
		if !areCopiesDropped {
			for _, replicaID := range cluster.replicaIDs() {
				copies = append(copies, metadata.CopyRecord{ReplicaID: replicaID, State: metadata.CopyGood})
			}
		}
		stream.StreamID = 7
		stream.Epoch = 5
		stream.LowWaterMark = 100
		stream.NextSegmentID = 4
		stream.Segments = []metadata.SegmentRecord{{
			SegmentID:     3,
			Epoch:         5,
			FirstSequence: 100,
			Sealed:        &metadata.SealRecord{End: 102, At: 1},
			Copies:        copies,
			Tiered: &metadata.TieredRecord{
				ObjectKey:             objectKey,
				SizeBytes:             uint64(len(object)),
				UploadedAt:            2,
				AreLocalCopiesDropped: areCopiesDropped,
			},
		}}
		return nil
	})
	if err != nil {
		cluster.t.Fatalf("record the tiered segment: %v", err)
	}
	return object
}

func TestReaderFetchesATieredSegmentFromTheObjectStore(t *testing.T) {
	for _, areCopiesDropped := range []bool{true, false} {
		name := "replicas dropped their copies"
		if !areCopiesDropped {
			name = "replicas are named but do not hold it"
		}
		t.Run(name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			tieredStream(cluster, areCopiesDropped)
			reader := cluster.client.OpenReader("archive")

			records, err := reader.ReadRange(t.Context(), 100, 102)
			if err != nil {
				t.Fatalf("ReadRange: %v", err)
			}
			if got := payloadsOf(records); !slices.Equal(got, []string{"alpha", "", "gamma!"}) {
				t.Errorf("ReadRange = %q, want alpha, an empty record and gamma!", got)
			}
			if got := sequencesOf(records); !slices.Equal(got, []uint64{100, 101, 102}) {
				t.Errorf("sequences = %v, want 100 to 102", got)
			}

			// From inside the batch, and past the end of the stream.
			records, err = reader.ReadRange(t.Context(), 102, 110)
			if err != nil || !slices.Equal(payloadsOf(records), []string{"gamma!"}) {
				t.Errorf("ReadRange(102, 110) = %q, %v; want gamma!", payloadsOf(records), err)
			}
		})
	}
}

func TestATieredSegmentThatCannotBeFetchedIsUnavailable(t *testing.T) {
	cases := []struct {
		name string
		// spoil makes the object unusable.
		spoil       func(cluster *testCluster, object []byte, objectPath string)
		wantInError string
	}{
		{
			name: "the client has no object store",
			spoil: func(cluster *testCluster, _ []byte, _ string) {
				cluster.client.objectStore = nil
			},
			wantInError: "was given none",
		},
		{
			name: "the object is gone",
			spoil: func(cluster *testCluster, _ []byte, objectPath string) {
				if err := os.Remove(objectPath); err != nil {
					cluster.t.Fatalf("remove the object: %v", err)
				}
			},
			wantInError: "does not exist",
		},
		{
			name: "a record in the object is damaged",
			spoil: func(cluster *testCluster, object []byte, objectPath string) {
				damaged := slices.Clone(object)
				damaged[24] ^= 0x01
				if err := os.WriteFile(objectPath, damaged, 0o644); err != nil {
					cluster.t.Fatalf("write the damaged object: %v", err)
				}
			},
			wantInError: "checksum",
		},
		{
			name: "the object is another segment's",
			spoil: func(cluster *testCluster, _ []byte, _ string) {
				_, err := cluster.store.UpdateStream(cluster.t.Context(), "archive", func(stream *metadata.StreamRecord) error {
					stream.StreamID = 8
					return nil
				})
				if err != nil {
					cluster.t.Fatalf("change the stream's identifier: %v", err)
				}
			},
			wantInError: "is not segment",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)
			object := tieredStream(cluster, true)
			objectPath := filepath.Join(cluster.objectStoreDirectory, "objects", "segments", "7", "3")
			testCase.spoil(cluster, object, objectPath)

			records, err := cluster.client.OpenReader("archive").ReadRange(t.Context(), 100, 102)

			if !errors.Is(err, ErrUnavailable) || records != nil {
				t.Fatalf("ReadRange = %v, %v; want no records and ErrUnavailable", records, err)
			}
			if !strings.Contains(err.Error(), testCase.wantInError) {
				t.Errorf("the error %q does not say %q", err, testCase.wantInError)
			}
		})
	}
}

// followed is what a follower running in the background has yielded.
type followed struct {
	// records receives every record the follower yields.
	records chan Record
	// ended receives the error the follower ended with.
	ended chan error
	// cancel stops the follower.
	cancel context.CancelFunc
}

// followInBackground follows a stream from a sequence number until the test
// ends or cancel is called.
func followInBackground(t *testing.T, reader *Reader, from uint64) *followed {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	follower := &followed{records: make(chan Record, 64), ended: make(chan error, 1), cancel: cancel}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for record, err := range reader.Follow(ctx, from) {
			if err != nil {
				follower.ended <- err
				return
			}
			follower.records <- record
		}
		follower.ended <- nil
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return follower
}

// expect waits for the follower to yield exactly the given payloads next.
func (follower *followed) expect(t *testing.T, payloads ...string) {
	t.Helper()
	for _, want := range payloads {
		select {
		case record := <-follower.records:
			if string(record.Payload) != want {
				t.Fatalf("the follower yielded %q (sequence %d), want %q", record.Payload, record.Sequence, want)
			}
		case err := <-follower.ended:
			t.Fatalf("the follower ended with %v while %q was expected", err, want)
		case <-time.After(10 * time.Second):
			t.Fatalf("the follower did not yield %q in time", want)
		}
	}
}

// expectEnd waits for the follower to end and returns the error it ended
// with.
func (follower *followed) expectEnd(t *testing.T) error {
	t.Helper()
	select {
	case err := <-follower.ended:
		return err
	case record := <-follower.records:
		t.Fatalf("the follower yielded %q instead of ending", record.Payload)
	case <-time.After(10 * time.Second):
		t.Fatal("the follower did not end in time")
	}
	return nil
}

func TestFollowYieldsRecordsAsTheyCommitAcrossRollsAndWriters(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openFixtureWriter("orders")
	writer.append([]string{"one", "two"})

	follower := followInBackground(t, cluster.client.OpenReader("orders"), 1)
	follower.expect(t, "one", "two")

	// A record appended while the follower waits at the tail.
	writer.append([]string{"three"})
	follower.expect(t, "three")

	// The writer rolls to a new segment.
	writer.roll()
	writer.append([]string{"four", "five"})
	follower.expect(t, "four", "five")

	// The writer steps down and another one takes the stream.
	writer.seal()
	successor := cluster.openFixtureWriter("orders")
	if successor.epoch != writer.epoch+1 {
		t.Fatalf("the successor writes under epoch %d, want %d", successor.epoch, writer.epoch+1)
	}
	successor.append([]string{"six"})
	follower.expect(t, "six")

	// The follower ends when its context does, and says so.
	follower.cancel()
	if err := follower.expectEnd(t); !errors.Is(err, context.Canceled) {
		t.Errorf("the follower ended with %v, want context.Canceled", err)
	}
}

func TestFollowStartsWhereItIsTold(t *testing.T) {
	cluster := newTestCluster(t, 3)
	writer := streamOfThreeSegments(cluster)
	reader := cluster.client.OpenReader("orders")

	// From the middle of a sealed segment.
	followInBackground(t, reader, 5).expect(t, "five", "six", "seven")
	// From a position nothing is committed at yet: it waits.
	waiting := followInBackground(t, reader, 9)
	writer.append([]string{"eight", "nine"})
	waiting.expect(t, "nine")
	// 0 is not a sequence number; following from it starts at the first.
	followInBackground(t, reader, 0).expect(t, "one")
}

func TestFollowEndsWhenTheCallerStopsRanging(t *testing.T) {
	cluster := newTestCluster(t, 3)
	streamOfThreeSegments(cluster)

	var seen []string
	for record, err := range cluster.client.OpenReader("orders").Follow(t.Context(), 1) {
		if err != nil {
			t.Fatalf("Follow: %v", err)
		}
		seen = append(seen, string(record.Payload))
		if len(seen) == 4 {
			break
		}
	}

	if want := []string{"one", "two", "three", "four"}; !slices.Equal(seen, want) {
		t.Errorf("followed %v, want %v", seen, want)
	}
}

func TestFollowEndsWithATypedErrorWhenItCannotContinue(t *testing.T) {
	t.Run("the position was truncated", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		streamOfThreeSegments(cluster)
		follower := followInBackground(t, cluster.client.OpenReader("orders"), 1)
		follower.expect(t, "one", "two", "three", "four", "five", "six", "seven")

		// Retention overtakes a follower that fell behind: here, one that
		// is asked to start again below the new low-water mark.
		_, err := cluster.store.UpdateStream(t.Context(), "orders", func(stream *metadata.StreamRecord) error {
			stream.LowWaterMark = 6
			return nil
		})
		if err != nil {
			t.Fatalf("raise the low-water mark: %v", err)
		}
		behind := followInBackground(t, cluster.client.OpenReader("orders"), 2)
		err = behind.expectEnd(t)
		truncated, isTruncated := errors.AsType[*TruncatedError](err)
		if !isTruncated || truncated.LowWaterMark != 6 {
			t.Errorf("the follower ended with %v, want a TruncatedError with low-water mark 6", err)
		}
	})

	t.Run("the stream does not exist", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		follower := followInBackground(t, cluster.client.OpenReader("payments"), 1)
		if err := follower.expectEnd(t); !errors.Is(err, ErrStreamNotFound) {
			t.Errorf("the follower ended with %v, want ErrStreamNotFound", err)
		}
	})

	t.Run("the context is done before it starts", func(t *testing.T) {
		cluster := newTestCluster(t, 3)
		streamOfThreeSegments(cluster)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		yielded := 0
		for record, err := range cluster.client.OpenReader("orders").Follow(ctx, 1) {
			yielded++
			if !errors.Is(err, context.Canceled) || record.Payload != nil {
				t.Errorf("Follow yielded %v, %v; want only context.Canceled", record, err)
			}
		}
		if yielded != 1 {
			t.Errorf("Follow yielded %d elements, want exactly the error", yielded)
		}
	})
}

func TestFollowKeepsGoingWhenAReplicaFails(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openFixtureWriter("orders")
	writer.append([]string{"one"})
	follower := followInBackground(t, cluster.client.OpenReader("orders"), 1)
	follower.expect(t, "one")

	// The replica the follower is waiting on dies; the writer continues on
	// the two that are left.
	cluster.replicas[0].stop()
	survivors := cluster.replicas[1:]
	writer.append([]string{"two"}, survivors...)
	follower.expect(t, "two")
	writer.append([]string{"three"}, survivors...)
	follower.expect(t, "three")
}
