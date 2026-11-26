package bitacora

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
)

// StreamOptions say how a new stream is replicated and how long its records
// are kept. The zero value is three replicas with a write quorum of two and
// no minimum retention.
type StreamOptions struct {
	// ReplicaCount is how many replicas hold each segment. Zero means 3.
	ReplicaCount int
	// WriteQuorum is how many replicas must store a record before it is
	// committed. Zero means a majority of ReplicaCount.
	WriteQuorum int
	// KeepSealedFor keeps every sealed segment at least this long, whatever
	// truncation allows. Zero means no minimum. It is recorded in whole
	// milliseconds.
	KeepSealedFor time.Duration
}

// StreamDescription is what the metadata store knows about a stream.
type StreamDescription struct {
	// Name is the stream's name.
	Name string
	// ID is the stream's identifier, which is never given to another
	// stream, not even after this one is deleted.
	ID uint64
	// Epoch counts the writers the stream has had.
	Epoch uint64
	// Writer is the lease of the writer that holds the stream, or nil if
	// the last writer stepped down or there never was one. A lease that
	// has expired is still shown.
	Writer *WriterLease
	// ReplicaCount is how many replicas hold each segment.
	ReplicaCount int
	// WriteQuorum is how many replicas must store a record before it is
	// committed.
	WriteQuorum int
	// KeepSealedFor is how long sealed segments are kept at least, or zero
	// if there is no minimum.
	KeepSealedFor time.Duration
	// LowWaterMark is the first sequence number that can still be read.
	LowWaterMark uint64
	// TruncateBefore is the position the owner allowed truncation up to, or
	// zero if it never did.
	TruncateBefore uint64
	// Segments are the stream's segments, oldest first.
	Segments []SegmentDescription
	// StandbyReplicas are the replicas placed for the next segment, or nil
	// if the controller has not placed them yet.
	StandbyReplicas []string
	// Materializers are the registered materializers, sorted by name.
	Materializers []MaterializerDescription
}

// WriterLease says who holds a stream and until when.
type WriterLease struct {
	// Identity is the name the writer gave itself.
	Identity string
	// ExpiresAt is when the lease lapses unless it is renewed.
	ExpiresAt time.Time
}

// SegmentDescription is one segment of a stream.
type SegmentDescription struct {
	// ID is the segment's identifier within its stream.
	ID uint64
	// Epoch is the epoch of the writer that wrote the segment.
	Epoch uint64
	// FirstSequence is the sequence number of the segment's first record.
	FirstSequence uint64
	// IsSealed says whether the segment is closed for good. At most the
	// last segment of a stream is not.
	IsSealed bool
	// LastSequence is the sequence number of the segment's last record, for
	// a sealed segment. A segment sealed empty has FirstSequence - 1 here.
	LastSequence uint64
	// Replicas are the identities of the replicas that hold a copy. It is
	// empty once the copies were dropped in favour of the object store.
	Replicas []string
	// IsTiered says whether the segment is in the object store.
	IsTiered bool
}

// MaterializerDescription is a registered materializer and its progress.
type MaterializerDescription struct {
	// Name is the materializer's name.
	Name string
	// Checkpoint is the last sequence number the materializer has durably
	// applied. The log keeps every record above it.
	Checkpoint uint64
	// FencingToken counts the instances that have taken the materializer.
	FencingToken uint64
}

// CreateStream creates a stream. A writer can open it as soon as the
// controller has placed its first segment, which takes about one of the
// controller's passes; [Client.OpenWriter] waits for that.
//
// The error wraps [ErrStreamExists], or [ErrInvalidArgument] if the name is
// not 1 to 255 ASCII letters, digits, '.', '_' or '-' or the options cannot
// work.
func (client *Client) CreateStream(ctx context.Context, name string, options StreamOptions) (StreamDescription, error) {
	replicaCount := options.ReplicaCount
	if replicaCount == 0 {
		replicaCount = 3
	}
	writeQuorum := options.WriteQuorum
	if writeQuorum == 0 {
		writeQuorum = replicaCount/2 + 1
	}
	if replicaCount < 1 || replicaCount > math.MaxUint8 || writeQuorum < 1 || writeQuorum > replicaCount {
		return StreamDescription{}, fmt.Errorf("%w: a write quorum of %d with %d replicas", ErrInvalidArgument, writeQuorum, replicaCount)
	}
	if options.KeepSealedFor < 0 {
		return StreamDescription{}, fmt.Errorf("%w: sealed segments cannot be kept for %v", ErrInvalidArgument, options.KeepSealedFor)
	}
	var retention metadata.RetentionPolicy
	if options.KeepSealedFor > 0 {
		retention.KeepSealedForMillis = new(uint64(options.KeepSealedFor.Milliseconds()))
	}
	replication := metadata.ReplicationSettings{ReplicaCount: uint8(replicaCount), WriteQuorum: uint8(writeQuorum)}
	stream, err := client.metadata.CreateStream(ctx, name, replication, retention)
	if err != nil {
		return StreamDescription{}, fromMetadata(err)
	}
	return describe(stream, nil), nil
}

// DescribeStream returns what the metadata store knows about a stream and
// the progress of its materializers. The error wraps [ErrStreamNotFound].
func (client *Client) DescribeStream(ctx context.Context, name string) (StreamDescription, error) {
	stream, err := client.metadata.GetStream(ctx, name)
	if err != nil {
		return StreamDescription{}, fromMetadata(err)
	}
	checkpoints, err := client.metadata.ListCheckpoints(ctx, stream.StreamID)
	if err != nil {
		return StreamDescription{}, fromMetadata(err)
	}
	return describe(stream, checkpoints), nil
}

// DeleteStream deletes a stream with all of its records. The controller
// then removes its segments from the replicas and the object store. This
// cannot be undone. The error wraps [ErrStreamNotFound].
func (client *Client) DeleteStream(ctx context.Context, name string) error {
	return fromMetadata(client.metadata.DeleteStream(ctx, name))
}

// TruncateBefore allows every record below sequence to be deleted. The
// position only moves forward: a call with a lower one changes nothing.
//
// Records are deleted by the controller, whole segments at a time, and only
// as far as the registered materializers' checkpoints and the retention
// policy permit. [Reader.LowWaterMark] says how far it got.
//
// The error wraps [ErrStreamNotFound], or [ErrInvalidArgument] if sequence
// is 0, which is not a sequence number.
func (client *Client) TruncateBefore(ctx context.Context, name string, sequence uint64) error {
	if sequence == 0 {
		return fmt.Errorf("%w: sequence numbers start at 1", ErrInvalidArgument)
	}
	return fromMetadata(client.metadata.RequestTruncation(ctx, name, sequence))
}

// RegisterMaterializer registers a materializer on a stream. From then on
// the log keeps every record above the materializer's checkpoint: one that
// stops checkpointing pins the log until it is unregistered. Registering a
// materializer that is registered already changes nothing.
//
// The error wraps [ErrStreamNotFound], or [ErrInvalidArgument] if the
// materializer's name is not one a stream could have.
func (client *Client) RegisterMaterializer(ctx context.Context, streamName, materializer string) error {
	return fromMetadata(client.metadata.RegisterMaterializer(ctx, streamName, materializer))
}

// UnregisterMaterializer unregisters a materializer, so that it no longer
// holds records back, and forgets its checkpoint. Unregistering one that is
// not registered changes nothing. The error wraps [ErrStreamNotFound].
func (client *Client) UnregisterMaterializer(ctx context.Context, streamName, materializer string) error {
	return fromMetadata(client.metadata.UnregisterMaterializer(ctx, streamName, materializer))
}

// describe turns a stream record and its checkpoint records into what the
// client shows of them.
func describe(stream metadata.StreamRecord, checkpoints []metadata.CheckpointRecord) StreamDescription {
	description := StreamDescription{
		Name:            stream.Name,
		ID:              stream.StreamID,
		Epoch:           stream.Epoch,
		ReplicaCount:    int(stream.Replication.ReplicaCount),
		WriteQuorum:     int(stream.Replication.WriteQuorum),
		LowWaterMark:    stream.LowWaterMark,
		StandbyReplicas: stream.StandbyReplicas,
	}
	if stream.Writer != nil {
		description.Writer = &WriterLease{
			Identity:  stream.Writer.WriterIdentity,
			ExpiresAt: time.UnixMilli(int64(stream.Writer.ExpiresAt)),
		}
	}
	if stream.Retention.KeepSealedForMillis != nil {
		description.KeepSealedFor = time.Duration(*stream.Retention.KeepSealedForMillis) * time.Millisecond
	}
	if stream.TruncateBefore != nil {
		description.TruncateBefore = *stream.TruncateBefore
	}
	for _, segment := range stream.Segments {
		described := SegmentDescription{
			ID:            segment.SegmentID,
			Epoch:         segment.Epoch,
			FirstSequence: segment.FirstSequence,
			IsTiered:      segment.Tiered != nil,
		}
		if segment.Sealed != nil {
			described.IsSealed = true
			described.LastSequence = segment.Sealed.End
		}
		for _, copyRecord := range segment.Copies {
			described.Replicas = append(described.Replicas, copyRecord.ReplicaID)
		}
		description.Segments = append(description.Segments, described)
	}
	for _, checkpoint := range checkpoints {
		description.Materializers = append(description.Materializers, MaterializerDescription{
			Name:         checkpoint.Materializer,
			Checkpoint:   checkpoint.Checkpoint,
			FencingToken: checkpoint.FencingToken,
		})
	}
	return description
}
