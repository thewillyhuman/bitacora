package bitacora

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
)

func TestCreateStreamRecordsItsOptions(t *testing.T) {
	cases := []struct {
		name             string
		options          StreamOptions
		wantReplicaCount int
		wantWriteQuorum  int
		wantRetention    time.Duration
	}{
		{name: "the defaults", options: StreamOptions{}, wantReplicaCount: 3, wantWriteQuorum: 2},
		{name: "one replica commits alone", options: StreamOptions{ReplicaCount: 1}, wantReplicaCount: 1, wantWriteQuorum: 1},
		{name: "a majority of five", options: StreamOptions{ReplicaCount: 5}, wantReplicaCount: 5, wantWriteQuorum: 3},
		{name: "a quorum of everyone", options: StreamOptions{ReplicaCount: 3, WriteQuorum: 3}, wantReplicaCount: 3, wantWriteQuorum: 3},
		{
			name:    "a retention period",
			options: StreamOptions{KeepSealedFor: 36 * time.Hour}, wantReplicaCount: 3, wantWriteQuorum: 2,
			wantRetention: 36 * time.Hour,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := newTestCluster(t, 3)

			created, err := cluster.client.CreateStream(t.Context(), "orders", testCase.options)
			if err != nil {
				t.Fatalf("CreateStream: %v", err)
			}
			described, err := cluster.client.DescribeStream(t.Context(), "orders")
			if err != nil {
				t.Fatalf("DescribeStream: %v", err)
			}

			for what, stream := range map[string]StreamDescription{"created": created, "described": described} {
				if stream.Name != "orders" || stream.ID != 1 || stream.Epoch != 0 || stream.Writer != nil ||
					stream.LowWaterMark != 1 || stream.TruncateBefore != 0 || len(stream.Segments) != 0 || len(stream.Materializers) != 0 {
					t.Errorf("%s: a new stream is %+v; want it empty at epoch 0 with identifier 1", what, stream)
				}
				if stream.ReplicaCount != testCase.wantReplicaCount || stream.WriteQuorum != testCase.wantWriteQuorum ||
					stream.KeepSealedFor != testCase.wantRetention {
					t.Errorf("%s: %d replicas, quorum %d, kept %v; want %d, %d, %v", what, stream.ReplicaCount, stream.WriteQuorum,
						stream.KeepSealedFor, testCase.wantReplicaCount, testCase.wantWriteQuorum, testCase.wantRetention)
				}
			}
		})
	}
}

func TestCreateStreamRefusesWhatCannotWork(t *testing.T) {
	cluster := newTestCluster(t, 3)
	if _, err := cluster.client.CreateStream(t.Context(), "orders", StreamOptions{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	cases := []struct {
		name       string
		streamName string
		options    StreamOptions
		want       error
	}{
		{name: "a name that is taken", streamName: "orders", want: ErrStreamExists},
		{name: "an empty name", streamName: "", want: ErrInvalidArgument},
		{name: "a name with a slash", streamName: "orders/2026", want: ErrInvalidArgument},
		{name: "a name that is too long", streamName: strings.Repeat("o", 256), want: ErrInvalidArgument},
		{name: "a quorum larger than the replica set", streamName: "a", options: StreamOptions{ReplicaCount: 2, WriteQuorum: 3}, want: ErrInvalidArgument},
		{name: "a negative quorum", streamName: "a", options: StreamOptions{WriteQuorum: -1}, want: ErrInvalidArgument},
		{name: "a negative replica count", streamName: "a", options: StreamOptions{ReplicaCount: -3}, want: ErrInvalidArgument},
		{name: "more replicas than a record can say", streamName: "a", options: StreamOptions{ReplicaCount: 256}, want: ErrInvalidArgument},
		{name: "a negative retention period", streamName: "a", options: StreamOptions{KeepSealedFor: -time.Second}, want: ErrInvalidArgument},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := cluster.client.CreateStream(t.Context(), testCase.streamName, testCase.options)
			if !errors.Is(err, testCase.want) {
				t.Errorf("CreateStream = %v, want %v", err, testCase.want)
			}
		})
	}
	// Nothing but the first stream was created.
	if _, err := cluster.client.DescribeStream(t.Context(), "a"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("a refused stream exists: %v", err)
	}
}

func TestDescribeStreamShowsTheWriterAndTheSegments(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	writer := cluster.openFixtureWriter("orders")
	writer.append([]string{"one", "two", "three"})
	writer.roll()
	writer.append([]string{"four"})
	lapsesAt := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	if err := cluster.store.RenewLease(t.Context(), "orders", writer.epoch, uint64(lapsesAt.UnixMilli())); err != nil {
		t.Fatalf("renew the lease: %v", err)
	}

	stream, err := cluster.client.DescribeStream(t.Context(), "orders")
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}

	if stream.Epoch != 1 || stream.Writer == nil || stream.Writer.Identity != "fixture" || !stream.Writer.ExpiresAt.Equal(lapsesAt) {
		t.Errorf("epoch %d, writer %+v; want epoch 1 held by fixture until %v", stream.Epoch, stream.Writer, lapsesAt)
	}
	wantSegments := []SegmentDescription{
		{ID: 1, Epoch: 1, FirstSequence: 1, IsSealed: true, LastSequence: 3, Replicas: cluster.replicaIDs()},
		{ID: 2, Epoch: 1, FirstSequence: 4, Replicas: cluster.replicaIDs()},
	}
	if len(stream.Segments) != len(wantSegments) {
		t.Fatalf("%d segments, want %d", len(stream.Segments), len(wantSegments))
	}
	for index, want := range wantSegments {
		got := stream.Segments[index]
		if got.ID != want.ID || got.Epoch != want.Epoch || got.FirstSequence != want.FirstSequence || got.IsSealed != want.IsSealed ||
			got.LastSequence != want.LastSequence || got.IsTiered || !slices.Equal(got.Replicas, want.Replicas) {
			t.Errorf("segment %d is %+v, want %+v", index, got, want)
		}
	}
	if stream.StandbyReplicas != nil {
		t.Errorf("standby replicas are %v, but the open segment took them", stream.StandbyReplicas)
	}

	if _, err := cluster.client.DescribeStream(t.Context(), "payments"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("DescribeStream of a missing stream = %v, want ErrStreamNotFound", err)
	}
}

func TestDescribeStreamShowsATieredSegment(t *testing.T) {
	cluster := newTestCluster(t, 3)
	tieredStream(cluster, true)

	stream, err := cluster.client.DescribeStream(t.Context(), "archive")
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}

	if len(stream.Segments) != 1 || !stream.Segments[0].IsTiered || len(stream.Segments[0].Replicas) != 0 || stream.LowWaterMark != 100 {
		t.Errorf("the stream is %+v; want one tiered segment without replicas and low-water mark 100", stream)
	}
}

func TestTruncateBeforeOnlyMovesForward(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	steps := []struct{ request, want uint64 }{{100, 100}, {40, 100}, {250, 250}}
	for _, step := range steps {
		if err := cluster.client.TruncateBefore(t.Context(), "orders", step.request); err != nil {
			t.Fatalf("TruncateBefore(%d): %v", step.request, err)
		}
		stream, err := cluster.client.DescribeStream(t.Context(), "orders")
		if err != nil || stream.TruncateBefore != step.want {
			t.Errorf("after TruncateBefore(%d) the position is %d, %v; want %d", step.request, stream.TruncateBefore, err, step.want)
		}
		// Asking deletes nothing: that is the controller's work.
		if stream.LowWaterMark != 1 {
			t.Errorf("TruncateBefore moved the low-water mark to %d", stream.LowWaterMark)
		}
	}

	if err := cluster.client.TruncateBefore(t.Context(), "orders", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("TruncateBefore(0) = %v, want ErrInvalidArgument", err)
	}
	if err := cluster.client.TruncateBefore(t.Context(), "payments", 10); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("TruncateBefore of a missing stream = %v, want ErrStreamNotFound", err)
	}
}

func TestMaterializersAreRegisteredAndUnregistered(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.createStream("orders", 2)
	_, err := cluster.store.UpdateStream(t.Context(), "orders", func(stream *metadata.StreamRecord) error {
		stream.LowWaterMark = 41
		return nil
	})
	if err != nil {
		t.Fatalf("raise the low-water mark: %v", err)
	}
	client := cluster.client
	materializersOf := func() []MaterializerDescription {
		t.Helper()
		stream, err := client.DescribeStream(t.Context(), "orders")
		if err != nil {
			t.Fatalf("DescribeStream: %v", err)
		}
		return stream.Materializers
	}

	// A new materializer has applied nothing: its checkpoint is just below
	// the first record it can still read, so that nothing it needs goes.
	for _, materializer := range []string{"pages", "index", "pages"} {
		if err := client.RegisterMaterializer(t.Context(), "orders", materializer); err != nil {
			t.Fatalf("RegisterMaterializer(%s): %v", materializer, err)
		}
	}
	want := []MaterializerDescription{{Name: "index", Checkpoint: 40}, {Name: "pages", Checkpoint: 40}}
	if got := materializersOf(); !slices.Equal(got, want) {
		t.Errorf("materializers = %+v, want %+v", got, want)
	}

	for _, materializer := range []string{"index", "index", "never-registered"} {
		if err := client.UnregisterMaterializer(t.Context(), "orders", materializer); err != nil {
			t.Fatalf("UnregisterMaterializer(%s): %v", materializer, err)
		}
	}
	if got := materializersOf(); !slices.Equal(got, want[1:]) {
		t.Errorf("materializers = %+v, want %+v", got, want[1:])
	}

	if err := client.RegisterMaterializer(t.Context(), "orders", "pages/2"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("RegisterMaterializer with a bad name = %v, want ErrInvalidArgument", err)
	}
	if err := client.RegisterMaterializer(t.Context(), "payments", "pages"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("RegisterMaterializer on a missing stream = %v, want ErrStreamNotFound", err)
	}
	if err := client.UnregisterMaterializer(t.Context(), "payments", "pages"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("UnregisterMaterializer on a missing stream = %v, want ErrStreamNotFound", err)
	}
}

func TestDeleteStreamRemovesItForGood(t *testing.T) {
	cluster := newTestCluster(t, 3)
	streamOfThreeSegments(cluster)
	client := cluster.client
	if err := client.RegisterMaterializer(t.Context(), "orders", "pages"); err != nil {
		t.Fatalf("RegisterMaterializer: %v", err)
	}

	if err := client.DeleteStream(t.Context(), "orders"); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}

	if _, err := client.DescribeStream(t.Context(), "orders"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("DescribeStream after the delete = %v, want ErrStreamNotFound", err)
	}
	if _, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 7); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("ReadRange after the delete = %v, want ErrStreamNotFound", err)
	}
	if _, err := client.OpenWriter(t.Context(), "orders", WriterOptions{}); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("OpenWriter after the delete = %v, want ErrStreamNotFound", err)
	}
	if err := client.DeleteStream(t.Context(), "orders"); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("a second DeleteStream = %v, want ErrStreamNotFound", err)
	}
	if checkpoints, err := cluster.store.ListCheckpoints(t.Context(), 1); err != nil || len(checkpoints) != 0 {
		t.Errorf("the deleted stream left checkpoints behind: %+v, %v", checkpoints, err)
	}

	// The name is free again, and the new stream is another stream: it
	// gets an identifier of its own and starts empty.
	recreated, err := client.CreateStream(t.Context(), "orders", StreamOptions{})
	if err != nil || recreated.ID != 2 || len(recreated.Segments) != 0 || len(recreated.Materializers) != 0 {
		t.Errorf("the stream created again is %+v, %v; want identifier 2 and nothing in it", recreated, err)
	}
}
