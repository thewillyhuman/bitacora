package metadata

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// streamName is the stream every test works on.
const streamName = "orders"

// defaultReplication is three replicas with a write quorum of two.
var defaultReplication = ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2}

// newStoreWithStream returns a store that holds the test stream, freshly
// created.
func newStoreWithStream(t *testing.T) *Store {
	t.Helper()
	store := NewStore(NewMemoryStore())
	if _, err := store.CreateStream(t.Context(), streamName, defaultReplication, RetentionPolicy{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	return store
}

// placeStandby does the controller's job of placing a replica set for the
// stream's next segment.
func placeStandby(t *testing.T, store *Store) {
	t.Helper()
	_, err := store.UpdateStream(t.Context(), streamName, func(stream *StreamRecord) error {
		stream.StandbyReplicas = []string{"replica-a", "replica-b", "replica-c"}
		return nil
	})
	if err != nil {
		t.Fatalf("placing a standby: %v", err)
	}
}

// newStoreWithOpenSegment returns a store whose test stream is at epoch 1
// with segment 1 open on three replicas.
func newStoreWithOpenSegment(t *testing.T) *Store {
	t.Helper()
	store := newStoreWithStream(t)
	placeStandby(t, store)
	if _, err := store.ClaimEpoch(t.Context(), streamName, 0, "writer-1", 1_000); err != nil {
		t.Fatalf("ClaimEpoch: %v", err)
	}
	if _, _, err := store.OpenSegment(t.Context(), streamName, 1); err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	return store
}

func TestCreatedStreamStartsEmptyAtEpochZero(t *testing.T) {
	store := newStoreWithStream(t)

	stream, err := store.GetStream(t.Context(), streamName)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}

	if stream.StreamID != 1 || stream.Epoch != 0 || stream.NextSegmentID != 1 || stream.LowWaterMark != 1 {
		t.Errorf("created stream = %+v, want id 1, epoch 0, next segment 1, low-water mark 1", stream)
	}
	if len(stream.Segments) != 0 || stream.StandbyReplicas != nil || stream.Writer != nil {
		t.Errorf("created stream has segments, a standby or a writer: %+v", stream)
	}
}

func TestStreamIdentifiersAreNeverReused(t *testing.T) {
	store := newStoreWithStream(t)
	if err := store.DeleteStream(t.Context(), streamName); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}

	recreated, err := store.CreateStream(t.Context(), streamName, defaultReplication, RetentionPolicy{})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	if recreated.StreamID != 2 {
		t.Errorf("the recreated stream has identifier %d, want 2", recreated.StreamID)
	}
}

func TestCreateStreamRefusesWhatItMust(t *testing.T) {
	store := newStoreWithStream(t)
	tests := []struct {
		name       string
		streamName string
		want       error
	}{
		{"a name that exists", streamName, ErrStreamExists},
		{"an empty name", "", ErrInvalidName},
		{"a name with a slash", "a/b", ErrInvalidName},
		{"a name with a space", "a b", ErrInvalidName},
		{"a name that is too long", string(make([]byte, MaxNameBytes+1)), ErrInvalidName},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := store.CreateStream(t.Context(), test.streamName, defaultReplication, RetentionPolicy{})

			if !errors.Is(err, test.want) {
				t.Errorf("CreateStream error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestMissingStreamIsNotFound(t *testing.T) {
	store := NewStore(NewMemoryStore())

	_, err := store.GetStream(t.Context(), "no-such-stream")

	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("GetStream error = %v, want ErrStreamNotFound", err)
	}
}

func TestClaimRaisesTheEpochAndRecordsTheLease(t *testing.T) {
	store := newStoreWithStream(t)

	claimed, err := store.ClaimEpoch(t.Context(), streamName, 0, "writer-1", 5_000)
	if err != nil {
		t.Fatalf("ClaimEpoch: %v", err)
	}

	if claimed.Epoch != 1 {
		t.Errorf("epoch = %d, want 1", claimed.Epoch)
	}
	if claimed.Writer == nil || *claimed.Writer != (WriterLease{WriterIdentity: "writer-1", ExpiresAt: 5_000}) {
		t.Errorf("lease = %+v, want writer-1 until 5000", claimed.Writer)
	}
}

func TestSecondClaimOfTheSameEpochIsFenced(t *testing.T) {
	store := newStoreWithStream(t)
	if _, err := store.ClaimEpoch(t.Context(), streamName, 0, "writer-1", 5_000); err != nil {
		t.Fatalf("ClaimEpoch: %v", err)
	}

	_, err := store.ClaimEpoch(t.Context(), streamName, 0, "writer-2", 5_000)

	fenced, isFenced := errors.AsType[*FencedError](err)
	if !isFenced || fenced.CurrentEpoch != 1 || fenced.HeldEpoch != 0 {
		t.Errorf("ClaimEpoch error = %v, want a FencedError at epoch 1", err)
	}
	if !errors.Is(err, ErrFenced) {
		t.Errorf("errors.Is(%v, ErrFenced) = false", err)
	}
}

func TestOpenWithoutAStandbyAsksToTryAgain(t *testing.T) {
	store := newStoreWithStream(t)
	if _, err := store.ClaimEpoch(t.Context(), streamName, 0, "writer-1", 5_000); err != nil {
		t.Fatalf("ClaimEpoch: %v", err)
	}

	_, _, err := store.OpenSegment(t.Context(), streamName, 1)

	if !errors.Is(err, ErrNoStandby) {
		t.Errorf("OpenSegment error = %v, want ErrNoStandby", err)
	}
}

func TestOpenedSegmentTakesTheStandbyAndTheNextIdentifier(t *testing.T) {
	store := newStoreWithOpenSegment(t)

	stream, err := store.GetStream(t.Context(), streamName)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}

	opened := stream.Segments[0]
	if opened.SegmentID != 1 || opened.Epoch != 1 || opened.FirstSequence != 1 || !opened.IsOpen() {
		t.Errorf("opened segment = %+v, want segment 1 of epoch 1 from sequence 1, open", opened)
	}
	if len(opened.Copies) != 3 || opened.Copies[0] != (CopyRecord{ReplicaID: "replica-a", State: CopyUnverified}) {
		t.Errorf("copies = %+v, want the three standby replicas, unverified", opened.Copies)
	}
	if stream.StandbyReplicas != nil || stream.NextSegmentID != 2 {
		t.Errorf("standby = %v, next segment = %d; want none and 2", stream.StandbyReplicas, stream.NextSegmentID)
	}
}

func TestOpeningAgainReturnsTheSameSegment(t *testing.T) {
	store := newStoreWithOpenSegment(t)

	stream, opened, err := store.OpenSegment(t.Context(), streamName, 1)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if opened.SegmentID != 1 || len(stream.Segments) != 1 {
		t.Errorf("opened segment %d of %d, want the one segment again", opened.SegmentID, len(stream.Segments))
	}
}

func TestRollSealsAndOpensInOneWrite(t *testing.T) {
	store := newStoreWithOpenSegment(t)
	placeStandby(t, store)

	stream, opened, err := store.RollSegment(t.Context(), streamName, 1, 1, 50, 9_000)
	if err != nil {
		t.Fatalf("RollSegment: %v", err)
	}

	if sealed := stream.Segments[0].Sealed; sealed == nil || *sealed != (SealRecord{End: 50, At: 9_000}) {
		t.Errorf("first segment seal = %+v, want end 50 at 9000", sealed)
	}
	if opened.SegmentID != 2 || opened.FirstSequence != 51 || opened.Epoch != 1 {
		t.Errorf("opened = %+v, want segment 2 from sequence 51 under epoch 1", opened)
	}
}

// A roll whose answer was lost is sent again. The second time the standby is
// gone, and without this rule the writer would wait for one it does not need.
func TestRollWhoseAnswerWasLostCanBeRepeated(t *testing.T) {
	store := newStoreWithOpenSegment(t)
	placeStandby(t, store)
	if _, _, err := store.RollSegment(t.Context(), streamName, 1, 1, 50, 9_000); err != nil {
		t.Fatalf("RollSegment: %v", err)
	}

	stream, opened, err := store.RollSegment(t.Context(), streamName, 1, 1, 50, 9_500)
	if err != nil {
		t.Fatalf("repeating RollSegment: %v", err)
	}

	if opened.SegmentID != 2 || len(stream.Segments) != 2 {
		t.Errorf("repeated roll opened segment %d of %d, want segment 2 of 2", opened.SegmentID, len(stream.Segments))
	}
}

func TestRollWithoutAStandbyChangesNothing(t *testing.T) {
	store := newStoreWithOpenSegment(t)

	_, _, err := store.RollSegment(t.Context(), streamName, 1, 1, 50, 9_000)

	if !errors.Is(err, ErrNoStandby) {
		t.Fatalf("RollSegment error = %v, want ErrNoStandby", err)
	}
	stream, err := store.GetStream(t.Context(), streamName)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	if !stream.Segments[0].IsOpen() {
		t.Error("the segment was sealed by a roll that did not happen")
	}
}

func TestSealRules(t *testing.T) {
	tests := []struct {
		name      string
		segmentID uint64
		sealedEnd uint64
		epoch     uint64
		want      error
	}{
		{"at a record", 1, 50, 1, nil},
		{"empty: one below the first sequence", 1, 0, 1, nil},
		{"a segment that is not the last", 9, 50, 1, ErrInvalidState},
		{"under an epoch that was lost", 1, 50, 0, ErrFenced},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStoreWithOpenSegment(t)

			_, err := store.SealSegment(t.Context(), streamName, test.epoch, test.segmentID, test.sealedEnd, 9_000)

			if !errors.Is(err, test.want) {
				t.Errorf("SealSegment error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSealingAgainAtTheSameEndSucceedsAndElsewhereFails(t *testing.T) {
	store := newStoreWithOpenSegment(t)
	if _, err := store.SealSegment(t.Context(), streamName, 1, 1, 50, 9_000); err != nil {
		t.Fatalf("SealSegment: %v", err)
	}

	_, sameEnd := store.SealSegment(t.Context(), streamName, 1, 1, 50, 9_500)
	_, otherEnd := store.SealSegment(t.Context(), streamName, 1, 1, 60, 9_500)

	if sameEnd != nil {
		t.Errorf("sealing again at the same end: %v", sameEnd)
	}
	if !errors.Is(otherEnd, ErrInvalidState) {
		t.Errorf("sealing at another end: %v, want ErrInvalidState", otherEnd)
	}
}

func TestLeaseIsRenewedAndReleasedOnlyByTheEpochThatHoldsIt(t *testing.T) {
	store := newStoreWithOpenSegment(t)

	staleRenewal := store.RenewLease(t.Context(), streamName, 0, 7_000)
	renewal := store.RenewLease(t.Context(), streamName, 1, 7_000)
	renewed, _ := store.GetStream(t.Context(), streamName)
	release := store.ReleaseLease(t.Context(), streamName, 1)
	released, _ := store.GetStream(t.Context(), streamName)

	if !errors.Is(staleRenewal, ErrFenced) {
		t.Errorf("renewal under a lost epoch: %v, want ErrFenced", staleRenewal)
	}
	if renewal != nil || renewed.Writer == nil || renewed.Writer.ExpiresAt != 7_000 {
		t.Errorf("renewal: %v, lease %+v; want expiry 7000", renewal, renewed.Writer)
	}
	if release != nil || released.Writer != nil {
		t.Errorf("release: %v, lease %+v; want none", release, released.Writer)
	}
}

func TestRegisteredMaterializerStartsOneBelowTheLowWaterMark(t *testing.T) {
	store := newStoreWithStream(t)
	if _, err := store.UpdateStream(t.Context(), streamName, func(stream *StreamRecord) error {
		stream.LowWaterMark = 41
		return nil
	}); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}

	if err := store.RegisterMaterializer(t.Context(), streamName, "pages"); err != nil {
		t.Fatalf("RegisterMaterializer: %v", err)
	}

	checkpoints, err := store.ListCheckpoints(t.Context(), 1)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	want := CheckpointRecord{SchemaVersion: SchemaVersion, Materializer: "pages", Checkpoint: 40}
	if len(checkpoints) != 1 || checkpoints[0] != want {
		t.Errorf("checkpoints = %+v, want %+v", checkpoints, want)
	}
}

func TestMaterializersAreKeptSortedAndRegisteredOnce(t *testing.T) {
	store := newStoreWithStream(t)
	for _, materializer := range []string{"pages", "blocks", "pages"} {
		if err := store.RegisterMaterializer(t.Context(), streamName, materializer); err != nil {
			t.Fatalf("RegisterMaterializer(%s): %v", materializer, err)
		}
	}

	stream, err := store.GetStream(t.Context(), streamName)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}

	if want := []string{"blocks", "pages"}; !slices.Equal(stream.Materializers, want) {
		t.Errorf("materializers = %v, want %v", stream.Materializers, want)
	}
}

func TestUnregisteredMaterializerLosesItsCheckpoint(t *testing.T) {
	store := newStoreWithStream(t)
	if err := store.RegisterMaterializer(t.Context(), streamName, "pages"); err != nil {
		t.Fatalf("RegisterMaterializer: %v", err)
	}

	first := store.UnregisterMaterializer(t.Context(), streamName, "pages")
	again := store.UnregisterMaterializer(t.Context(), streamName, "pages")

	checkpoints, err := store.ListCheckpoints(t.Context(), 1)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if first != nil || again != nil || len(checkpoints) != 0 {
		t.Errorf("unregister: %v, again: %v, checkpoints left: %v", first, again, checkpoints)
	}
}

func TestDeletedStreamLeavesANoteAndNoCheckpoints(t *testing.T) {
	keyValues := NewMemoryStore()
	store := NewStore(keyValues)
	if _, err := store.CreateStream(t.Context(), streamName, defaultReplication, RetentionPolicy{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if err := store.RegisterMaterializer(t.Context(), streamName, "pages"); err != nil {
		t.Fatalf("RegisterMaterializer: %v", err)
	}

	if err := store.DeleteStream(t.Context(), streamName); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}

	note, _ := keyValues.Get(t.Context(), "deleted-streams/1")
	checkpoint, _ := keyValues.Get(t.Context(), "checkpoints/1/pages")
	_, getErr := store.GetStream(t.Context(), streamName)
	if note == nil || string(note.Value) != `{"stream_id":1,"name":"orders"}` {
		t.Errorf("note = %+v, want the deleted-stream record of stream 1", note)
	}
	if checkpoint != nil {
		t.Error("the checkpoint record outlived its stream")
	}
	if !errors.Is(getErr, ErrStreamNotFound) {
		t.Errorf("GetStream after deletion: %v, want ErrStreamNotFound", getErr)
	}
}

// interferingStore lets another writer change the stream record between the
// read and the write of the first attempts of an update.
type interferingStore struct {
	*MemoryStore
	// interferencesLeft is how many more transactions are preceded by a
	// write of somebody else.
	interferencesLeft int
}

// Transact implements [KeyValueStore], with interference.
func (store *interferingStore) Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (bool, error) {
	if store.interferencesLeft > 0 {
		store.interferencesLeft--
		current, _ := store.MemoryStore.Get(ctx, conditions[0].Key)
		if _, err := store.MemoryStore.Transact(ctx, nil, []Mutation{{Key: current.Key, Value: current.Value}}); err != nil {
			return false, err
		}
	}
	return store.MemoryStore.Transact(ctx, conditions, mutations)
}

func TestUpdateIsTriedAgainWhenSomeoneWroteInBetween(t *testing.T) {
	keyValues := &interferingStore{MemoryStore: NewMemoryStore()}
	store := NewStore(keyValues)
	if _, err := store.CreateStream(t.Context(), streamName, defaultReplication, RetentionPolicy{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	keyValues.interferencesLeft = 3
	attempts := 0

	updated, err := store.UpdateStream(t.Context(), streamName, func(stream *StreamRecord) error {
		attempts++
		stream.LowWaterMark = 99
		return nil
	})

	if err != nil || updated.LowWaterMark != 99 {
		t.Errorf("UpdateStream = %+v, %v; want the change applied", updated, err)
	}
	if attempts != 4 {
		t.Errorf("the change ran %d times, want 4: three lost races and the one that won", attempts)
	}
}

func TestUpdateGivesUpAfterTooManyLostRaces(t *testing.T) {
	keyValues := &interferingStore{MemoryStore: NewMemoryStore()}
	store := NewStore(keyValues)
	if _, err := store.CreateStream(t.Context(), streamName, defaultReplication, RetentionPolicy{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	keyValues.interferencesLeft = 1_000

	_, err := store.UpdateStream(t.Context(), streamName, func(*StreamRecord) error { return nil })

	if !errors.Is(err, ErrContention) {
		t.Errorf("UpdateStream error = %v, want ErrContention", err)
	}
}

func TestListReplicasReadsTheRegistry(t *testing.T) {
	keyValues := NewMemoryStore()
	if _, err := keyValues.Transact(t.Context(), nil, []Mutation{{Key: "replicas/replica-a", Value: []byte(goldenReplica)}}); err != nil {
		t.Fatalf("writing the registration: %v", err)
	}

	replicas, err := NewStore(keyValues).ListReplicas(t.Context())
	if err != nil {
		t.Fatalf("ListReplicas: %v", err)
	}

	if len(replicas) != 1 || replicas[0].Address != "10.0.0.11:7000" {
		t.Errorf("replicas = %+v, want the one registered at 10.0.0.11:7000", replicas)
	}
}

func TestCreateStreamRefusesReplicationThatCannotCommit(t *testing.T) {
	tests := []struct {
		name        string
		replication ReplicationSettings
		wantErr     error
	}{
		{"three replicas, quorum two", ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2}, nil},
		{"one replica, quorum one", ReplicationSettings{ReplicaCount: 1, WriteQuorum: 1}, nil},
		{"a quorum of everyone", ReplicationSettings{ReplicaCount: 3, WriteQuorum: 3}, nil},
		{"a quorum of nobody", ReplicationSettings{ReplicaCount: 3, WriteQuorum: 0}, ErrInvalidReplication},
		{"a quorum larger than the replica set", ReplicationSettings{ReplicaCount: 2, WriteQuorum: 3}, ErrInvalidReplication},
		{"no replicas at all", ReplicationSettings{}, ErrInvalidReplication},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewStore(NewMemoryStore())

			_, err := store.CreateStream(t.Context(), streamName, test.replication, RetentionPolicy{})

			if !errors.Is(err, test.wantErr) {
				t.Errorf("CreateStream error = %v, want %v", err, test.wantErr)
			}
			if _, readErr := store.GetStream(t.Context(), streamName); (readErr == nil) != (test.wantErr == nil) {
				t.Errorf("after CreateStream = %v the stream reads as %v", err, readErr)
			}
		})
	}
}

func TestTheTruncationPositionOnlyMovesForward(t *testing.T) {
	store := newStoreWithStream(t)
	steps := []struct {
		request uint64
		want    uint64
	}{
		{request: 100, want: 100},
		{request: 40, want: 100},
		{request: 100, want: 100},
		{request: 250, want: 250},
	}
	for _, step := range steps {
		if err := store.RequestTruncation(t.Context(), streamName, step.request); err != nil {
			t.Fatalf("RequestTruncation(%d): %v", step.request, err)
		}
		stream, err := store.GetStream(t.Context(), streamName)
		if err != nil {
			t.Fatalf("GetStream: %v", err)
		}
		if stream.TruncateBefore == nil || *stream.TruncateBefore != step.want {
			t.Errorf("after a request to truncate before %d the position is %v, want %d", step.request, stream.TruncateBefore, step.want)
		}
		// Asking is all it does: records are deleted by the controller.
		if stream.LowWaterMark != 1 {
			t.Errorf("the request moved the low-water mark to %d", stream.LowWaterMark)
		}
	}

	if err := store.RequestTruncation(t.Context(), "payments", 10); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("RequestTruncation of a missing stream = %v, want ErrStreamNotFound", err)
	}
}
