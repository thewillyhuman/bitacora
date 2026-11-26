package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
)

const (
	streamKeyPrefix        = "streams/"
	deletedStreamKeyPrefix = "deleted-streams/"
	checkpointKeyPrefix    = "checkpoints/"
	replicaKeyPrefix       = "replicas/"
	streamIDCounterKey     = "counters/stream-id"

	// maxUpdateAttempts bounds how often a read-modify-write is tried again
	// when someone else wrote in between.
	maxUpdateAttempts = 32
	// MaxNameBytes is the longest name a stream or a materializer may have.
	MaxNameBytes = 255
)

var (
	// ErrUnavailable says the store could not be reached or did not answer.
	// The operation may or may not have been applied.
	ErrUnavailable = errors.New("metadata store is unavailable")
	// ErrStreamNotFound says no stream has the given name.
	ErrStreamNotFound = errors.New("stream does not exist")
	// ErrStreamExists says a stream of the given name exists already.
	ErrStreamExists = errors.New("stream already exists")
	// ErrInvalidName says a name is empty, too long or has a character
	// other than an ASCII letter, a digit, '.', '_' or '-'.
	ErrInvalidName = errors.New("name is not valid")
	// ErrInvalidReplication says the write quorum is not between one and
	// the number of replicas.
	ErrInvalidReplication = errors.New("replication settings are not valid")
	// ErrFenced says the stream's epoch is not the one the caller holds:
	// another writer has taken the stream.
	ErrFenced = errors.New("stream was taken by another writer")
	// ErrNoStandby says no replica set is placed for the next segment yet.
	// The controller places one; the caller tries again.
	ErrNoStandby = errors.New("no standby segment is placed")
	// ErrInvalidState says the record does not allow the transition, for
	// example sealing a segment that is not the last one.
	ErrInvalidState = errors.New("stream is not in a state that allows this")
	// ErrCorruptRecord says a stored record cannot be read by this client.
	ErrCorruptRecord = errors.New("metadata record cannot be read")
	// ErrContention says a read-modify-write lost to concurrent writers
	// every time it was tried.
	ErrContention = errors.New("too many concurrent updates")
)

// FencedError is the [ErrFenced] of a particular attempt.
type FencedError struct {
	// HeldEpoch is the epoch the caller holds.
	HeldEpoch uint64
	// CurrentEpoch is the stream's epoch now.
	CurrentEpoch uint64
}

// Error implements error.
func (fenced *FencedError) Error() string {
	return fmt.Sprintf("%v: the stream is at epoch %d, not %d", ErrFenced, fenced.CurrentEpoch, fenced.HeldEpoch)
}

// Is makes errors.Is(err, ErrFenced) true for a FencedError.
func (fenced *FencedError) Is(target error) bool { return target == ErrFenced }

// Store reads and writes the metadata records. Every change is one
// transaction conditional on what was read.
type Store struct {
	// keyValues is the store below.
	keyValues KeyValueStore
}

// NewStore returns a metadata store over keyValues.
func NewStore(keyValues KeyValueStore) *Store {
	return &Store{keyValues: keyValues}
}

// GetStream reads the record of the stream called name. The error wraps
// [ErrStreamNotFound] if there is none.
func (store *Store) GetStream(ctx context.Context, name string) (StreamRecord, error) {
	stream, _, err := store.readStream(ctx, name)
	return stream, err
}

// readStream reads a stream record and the revision it is at.
func (store *Store) readStream(ctx context.Context, name string) (StreamRecord, int64, error) {
	key := streamKeyPrefix + name
	stored, err := store.keyValues.Get(ctx, key)
	if err != nil {
		return StreamRecord{}, 0, err
	}
	if stored == nil {
		return StreamRecord{}, 0, fmt.Errorf("%w: %q", ErrStreamNotFound, name)
	}
	stream, err := decodeStream(key, stored.Value)
	return stream, stored.Revision, err
}

// UpdateStream reads the stream record, lets change modify it, and writes
// it back only if nobody wrote in between; otherwise it reads again and
// calls change again. It returns the record as written.
//
// change must be free of side effects, because it may run several times. If
// it returns an error, nothing is written and that error is returned.
func (store *Store) UpdateStream(ctx context.Context, name string, change func(*StreamRecord) error) (StreamRecord, error) {
	key := streamKeyPrefix + name
	for range maxUpdateAttempts {
		stream, revision, err := store.readStream(ctx, name)
		if err != nil {
			return StreamRecord{}, err
		}
		if err := change(&stream); err != nil {
			return StreamRecord{}, err
		}
		encoded, err := encodeStream(stream)
		if err != nil {
			return StreamRecord{}, err
		}
		applied, err := store.keyValues.Transact(ctx,
			[]Condition{{Key: key, Revision: revision}},
			[]Mutation{{Key: key, Value: encoded}})
		if err != nil {
			return StreamRecord{}, err
		}
		if applied {
			return stream, nil
		}
	}
	return StreamRecord{}, fmt.Errorf("%w: %s", ErrContention, key)
}

// CreateStream creates a stream without a first segment: it has no standby
// replica set, which the controller places as soon as it can. The stream's
// identifier is the next value of the cluster's counter, taken in the same
// transaction. The error wraps [ErrStreamExists], [ErrInvalidName] or
// [ErrInvalidReplication].
func (store *Store) CreateStream(ctx context.Context, name string, replication ReplicationSettings, retention RetentionPolicy) (StreamRecord, error) {
	if err := validateName(name); err != nil {
		return StreamRecord{}, err
	}
	if replication.WriteQuorum < 1 || replication.WriteQuorum > replication.ReplicaCount {
		return StreamRecord{}, fmt.Errorf("%w: a write quorum of %d with %d replicas",
			ErrInvalidReplication, replication.WriteQuorum, replication.ReplicaCount)
	}
	key := streamKeyPrefix + name
	for range maxUpdateAttempts {
		existing, err := store.keyValues.Get(ctx, key)
		if err != nil {
			return StreamRecord{}, err
		}
		if existing != nil {
			return StreamRecord{}, fmt.Errorf("%w: %q", ErrStreamExists, name)
		}
		counter, err := store.keyValues.Get(ctx, streamIDCounterKey)
		if err != nil {
			return StreamRecord{}, err
		}
		var lastStreamID uint64
		counterCondition := Condition{Key: streamIDCounterKey}
		if counter != nil {
			if err := json.Unmarshal(counter.Value, &lastStreamID); err != nil {
				return StreamRecord{}, fmt.Errorf("%w: %s: %v", ErrCorruptRecord, streamIDCounterKey, err)
			}
			counterCondition.Revision = counter.Revision
		}
		stream := StreamRecord{
			SchemaVersion: SchemaVersion,
			StreamID:      lastStreamID + 1,
			Name:          name,
			Replication:   replication,
			Retention:     retention,
			NextSegmentID: 1,
			LowWaterMark:  1,
		}
		encoded, err := encodeStream(stream)
		if err != nil {
			return StreamRecord{}, err
		}
		applied, err := store.keyValues.Transact(ctx,
			[]Condition{counterCondition, {Key: key}},
			[]Mutation{
				{Key: streamIDCounterKey, Value: strconv.AppendUint(nil, stream.StreamID, 10)},
				{Key: key, Value: encoded},
			})
		if err != nil {
			return StreamRecord{}, err
		}
		if applied {
			return stream, nil
		}
	}
	return StreamRecord{}, fmt.Errorf("%w: %s", ErrContention, key)
}

// DeleteStream removes the stream record and the checkpoint records of its
// materializers and leaves a note for the controller, in one transaction.
func (store *Store) DeleteStream(ctx context.Context, name string) error {
	key := streamKeyPrefix + name
	for range maxUpdateAttempts {
		stream, revision, err := store.readStream(ctx, name)
		if err != nil {
			return err
		}
		note, err := json.Marshal(DeletedStreamRecord{StreamID: stream.StreamID, Name: stream.Name})
		if err != nil {
			return err
		}
		mutations := []Mutation{
			{Key: key, Delete: true},
			{Key: deletedStreamKeyPrefix + strconv.FormatUint(stream.StreamID, 10), Value: note},
		}
		for _, materializer := range stream.Materializers {
			mutations = append(mutations, Mutation{Key: checkpointKey(stream.StreamID, materializer), Delete: true})
		}
		applied, err := store.keyValues.Transact(ctx, []Condition{{Key: key, Revision: revision}}, mutations)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrContention, key)
}

// ClaimEpoch raises the stream's epoch from expectedEpoch by one and records
// the new writer's lease. The error is a [FencedError] if the epoch is no
// longer expectedEpoch: another candidate is ahead.
func (store *Store) ClaimEpoch(ctx context.Context, name string, expectedEpoch uint64, writerIdentity string, leaseExpiresAt uint64) (StreamRecord, error) {
	return store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, expectedEpoch); err != nil {
			return err
		}
		stream.Epoch = expectedEpoch + 1
		stream.Writer = &WriterLease{WriterIdentity: writerIdentity, ExpiresAt: leaseExpiresAt}
		return nil
	})
}

// RenewLease moves the expiry of the writer's lease, if the stream is still
// at epoch.
func (store *Store) RenewLease(ctx context.Context, name string, epoch uint64, leaseExpiresAt uint64) error {
	_, err := store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, epoch); err != nil {
			return err
		}
		if stream.Writer != nil {
			stream.Writer = &WriterLease{WriterIdentity: stream.Writer.WriterIdentity, ExpiresAt: leaseExpiresAt}
		}
		return nil
	})
	return err
}

// ReleaseLease clears the writer's lease, if the stream is still at epoch.
func (store *Store) ReleaseLease(ctx context.Context, name string, epoch uint64) error {
	_, err := store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, epoch); err != nil {
			return err
		}
		stream.Writer = nil
		return nil
	})
	return err
}

// SealSegment records that the stream's last segment, segmentID, ends at
// sealedEnd. Sealing a segment that is already sealed there succeeds.
func (store *Store) SealSegment(ctx context.Context, name string, epoch, segmentID, sealedEnd, sealedAt uint64) (StreamRecord, error) {
	return store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, epoch); err != nil {
			return err
		}
		return sealLastSegment(stream, segmentID, sealedEnd, sealedAt)
	})
}

// OpenSegment opens the next segment on the standby replica set and returns
// it with the stream record. If the stream already has an open segment of
// epoch, that segment is returned: opening is safe to repeat. The error
// wraps [ErrNoStandby] if no replica set is placed.
func (store *Store) OpenSegment(ctx context.Context, name string, epoch uint64) (StreamRecord, SegmentRecord, error) {
	var opened SegmentRecord
	stream, err := store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, epoch); err != nil {
			return err
		}
		var err error
		opened, err = openNextSegment(stream, epoch)
		return err
	})
	return stream, opened, err
}

// RollSegment seals segmentID at sealedEnd and opens the next segment on
// the standby replica set, in one write. If the stream's last segment is
// already the one after segmentID, open under epoch, the roll has happened
// and its result is returned: a roll whose answer was lost is safe to
// repeat.
func (store *Store) RollSegment(ctx context.Context, name string, epoch, segmentID, sealedEnd, sealedAt uint64) (StreamRecord, SegmentRecord, error) {
	var opened SegmentRecord
	stream, err := store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if err := requireEpoch(stream, epoch); err != nil {
			return err
		}
		last := stream.LastSegment()
		wasRolledAlready := last != nil && last.SegmentID == segmentID+1 && last.Epoch == epoch && last.IsOpen()
		if !wasRolledAlready {
			if err := sealLastSegment(stream, segmentID, sealedEnd, sealedAt); err != nil {
				return err
			}
		}
		var err error
		opened, err = openNextSegment(stream, epoch)
		return err
	})
	return stream, opened, err
}

// RequestTruncation records that the stream's owner allows every record
// below sequence to be deleted. The position only moves forward: a request
// below the recorded one changes nothing. Deleting is the controller's
// work, which also honours the materializers and the retention policy.
func (store *Store) RequestTruncation(ctx context.Context, name string, sequence uint64) error {
	_, err := store.UpdateStream(ctx, name, func(stream *StreamRecord) error {
		if stream.TruncateBefore == nil || *stream.TruncateBefore < sequence {
			stream.TruncateBefore = new(sequence)
		}
		return nil
	})
	return err
}

// RegisterMaterializer adds materializer to the stream and creates its
// checkpoint record, one below the low-water mark, in one transaction.
// Registering a materializer that is registered already changes nothing.
func (store *Store) RegisterMaterializer(ctx context.Context, name, materializer string) error {
	if err := validateName(materializer); err != nil {
		return err
	}
	key := streamKeyPrefix + name
	for range maxUpdateAttempts {
		stream, revision, err := store.readStream(ctx, name)
		if err != nil {
			return err
		}
		if slices.Contains(stream.Materializers, materializer) {
			return nil
		}
		checkpoint, err := json.Marshal(CheckpointRecord{
			SchemaVersion: SchemaVersion,
			Materializer:  materializer,
			Checkpoint:    stream.LowWaterMark - 1,
		})
		if err != nil {
			return err
		}
		stream.Materializers = append(stream.Materializers, materializer)
		slices.Sort(stream.Materializers)
		encoded, err := encodeStream(stream)
		if err != nil {
			return err
		}
		applied, err := store.keyValues.Transact(ctx,
			[]Condition{{Key: key, Revision: revision}},
			[]Mutation{
				{Key: key, Value: encoded},
				{Key: checkpointKey(stream.StreamID, materializer), Value: checkpoint},
			})
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrContention, key)
}

// UnregisterMaterializer removes materializer from the stream and deletes
// its checkpoint record, in one transaction. Unregistering a materializer
// that is not registered changes nothing.
func (store *Store) UnregisterMaterializer(ctx context.Context, name, materializer string) error {
	key := streamKeyPrefix + name
	for range maxUpdateAttempts {
		stream, revision, err := store.readStream(ctx, name)
		if err != nil {
			return err
		}
		position := slices.Index(stream.Materializers, materializer)
		if position < 0 {
			return nil
		}
		stream.Materializers = slices.Delete(stream.Materializers, position, position+1)
		encoded, err := encodeStream(stream)
		if err != nil {
			return err
		}
		applied, err := store.keyValues.Transact(ctx,
			[]Condition{{Key: key, Revision: revision}},
			[]Mutation{
				{Key: key, Value: encoded},
				{Key: checkpointKey(stream.StreamID, materializer), Delete: true},
			})
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrContention, key)
}

// ListCheckpoints reads the checkpoint record of every materializer of the
// stream with the given identifier, sorted by materializer name.
func (store *Store) ListCheckpoints(ctx context.Context, streamID uint64) ([]CheckpointRecord, error) {
	prefix := checkpointKeyPrefix + strconv.FormatUint(streamID, 10) + "/"
	listed, err := store.keyValues.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	checkpoints := make([]CheckpointRecord, 0, len(listed))
	for _, stored := range listed {
		var checkpoint CheckpointRecord
		if err := json.Unmarshal(stored.Value, &checkpoint); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrCorruptRecord, stored.Key, err)
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, nil
}

// ListReplicas reads every replica registration, sorted by replica
// identifier.
func (store *Store) ListReplicas(ctx context.Context) ([]ReplicaRecord, error) {
	listed, err := store.keyValues.List(ctx, replicaKeyPrefix)
	if err != nil {
		return nil, err
	}
	replicas := make([]ReplicaRecord, 0, len(listed))
	for _, stored := range listed {
		var replica ReplicaRecord
		if err := json.Unmarshal(stored.Value, &replica); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrCorruptRecord, stored.Key, err)
		}
		replicas = append(replicas, replica)
	}
	return replicas, nil
}

// checkpointKey returns the key of a materializer's checkpoint record.
func checkpointKey(streamID uint64, materializer string) string {
	return checkpointKeyPrefix + strconv.FormatUint(streamID, 10) + "/" + materializer
}

// validateName checks that name is 1 to [MaxNameBytes] ASCII letters,
// digits, '.', '_' or '-'.
func validateName(name string) error {
	if name == "" || len(name) > MaxNameBytes {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	for _, character := range name {
		isLetter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		isDigit := character >= '0' && character <= '9'
		if !isLetter && !isDigit && character != '.' && character != '_' && character != '-' {
			return fmt.Errorf("%w: %q", ErrInvalidName, name)
		}
	}
	return nil
}

// requireEpoch checks that the stream is still at the epoch the caller
// holds.
func requireEpoch(stream *StreamRecord, heldEpoch uint64) error {
	if stream.Epoch != heldEpoch {
		return &FencedError{HeldEpoch: heldEpoch, CurrentEpoch: stream.Epoch}
	}
	return nil
}

// sealLastSegment seals the stream's last segment, which must be
// segmentID, at sealedEnd.
func sealLastSegment(stream *StreamRecord, segmentID, sealedEnd, sealedAt uint64) error {
	last := stream.LastSegment()
	if last == nil || last.SegmentID != segmentID {
		return fmt.Errorf("%w: segment %d is not the stream's last segment", ErrInvalidState, segmentID)
	}
	switch {
	case last.Sealed != nil && last.Sealed.End == sealedEnd:
		return nil
	case last.Sealed != nil:
		return fmt.Errorf("%w: segment %d is already sealed at %d, not %d",
			ErrInvalidState, segmentID, last.Sealed.End, sealedEnd)
	case sealedEnd+1 < last.FirstSequence:
		return fmt.Errorf("%w: segment %d starts at %d and cannot end at %d",
			ErrInvalidState, segmentID, last.FirstSequence, sealedEnd)
	}
	last.Sealed = &SealRecord{End: sealedEnd, At: sealedAt}
	return nil
}

// openNextSegment appends a new open segment on the standby replica set.
func openNextSegment(stream *StreamRecord, epoch uint64) (SegmentRecord, error) {
	if open := stream.OpenSegment(); open != nil {
		if open.Epoch == epoch {
			return *open, nil
		}
		return SegmentRecord{}, fmt.Errorf("%w: segment %d is still open under epoch %d",
			ErrInvalidState, open.SegmentID, open.Epoch)
	}
	if stream.StandbyReplicas == nil {
		return SegmentRecord{}, fmt.Errorf("%w: %q", ErrNoStandby, stream.Name)
	}
	copies := make([]CopyRecord, 0, len(stream.StandbyReplicas))
	for _, replicaID := range stream.StandbyReplicas {
		copies = append(copies, CopyRecord{ReplicaID: replicaID, State: CopyUnverified})
	}
	opened := SegmentRecord{
		SegmentID:     stream.NextSegmentID,
		Epoch:         epoch,
		FirstSequence: stream.NextSegmentFirstSequence(),
		Copies:        copies,
	}
	stream.StandbyReplicas = nil
	stream.NextSegmentID = opened.SegmentID + 1
	stream.Segments = append(stream.Segments, opened)
	return opened, nil
}
