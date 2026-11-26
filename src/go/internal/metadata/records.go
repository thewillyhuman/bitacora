// Package metadata reads and writes the records of the bitacora metadata
// store.
//
// The keys, the records and every transition are defined in
// docs/protocol/metadata.md. Records are JSON objects; this file holds them
// as Go structs whose encoding is byte for byte what the Rust implementation
// writes, which the tests check against the same golden texts.
//
// Every change to a record is a transaction conditional on the revision at
// which the record was read. There are no blind writes. The store
// (store.go) is built on a small key-value interface (kv.go) with an etcd
// implementation (etcd.go) and an in-memory one for tests.
package metadata

import (
	"encoding/json"
	"fmt"
	"slices"
)

// SchemaVersion is the version of the records this package reads and
// writes. A record of a newer version is refused: this code does not know
// what a newer writer put in it and would drop it on the next update.
const SchemaVersion = 1

// CopyState says what is known about one replica's copy of a sealed
// segment.
type CopyState string

const (
	// CopyUnverified means nobody has checked the copy since the seal.
	CopyUnverified CopyState = "unverified"
	// CopyGood means the copy holds the whole segment and passes its checksums.
	CopyGood CopyState = "good"
	// CopyBad means the copy is short, damaged or lost.
	CopyBad CopyState = "bad"
)

// ReplicationSettings say how many replicas a segment has and how many
// acknowledgements commit a record.
type ReplicationSettings struct {
	// ReplicaCount is N, the number of replicas per segment.
	ReplicaCount uint8 `json:"replica_count"`
	// WriteQuorum is W, the number of acknowledgements that commit a record.
	WriteQuorum uint8 `json:"write_quorum"`
}

// RecoveryQuorum returns N − W + 1: how many replicas of a segment a
// recovering writer must hear from so that at least one of them holds every
// committed record.
func (settings ReplicationSettings) RecoveryQuorum() int {
	return int(settings.ReplicaCount) - int(settings.WriteQuorum) + 1
}

// RetentionPolicy says how long sealed segments are kept regardless of
// checkpoints.
type RetentionPolicy struct {
	// KeepSealedForMillis is the minimum age of a sealed segment before it
	// may be deleted, or nil for no minimum.
	KeepSealedForMillis *uint64 `json:"keep_sealed_for_millis"`
}

// WriterLease names the current writer of a stream. It is a courtesy that
// keeps a second writer from taking a healthy stream by accident;
// correctness depends on the epoch alone.
type WriterLease struct {
	// WriterIdentity is the name the writer gave itself.
	WriterIdentity string `json:"writer_identity"`
	// ExpiresAt is when the lease lapses unless renewed, in milliseconds
	// since the Unix epoch.
	ExpiresAt uint64 `json:"expires_at"`
}

// CopyRecord is one replica's copy of a segment.
type CopyRecord struct {
	// ReplicaID is the identity of the replica that holds the copy.
	ReplicaID string `json:"replica_id"`
	// State is what is known about the copy.
	State CopyState `json:"state"`
}

// SealRecord says where and when a segment ended.
type SealRecord struct {
	// End is the segment's final sequence number: one below its first
	// sequence number if it was sealed empty.
	End uint64 `json:"end"`
	// At is when the seal was written, in milliseconds since the Unix epoch.
	At uint64 `json:"at"`
}

// TieredRecord says where a sealed segment lives in the object store.
type TieredRecord struct {
	// ObjectKey is the key of the segment's object.
	ObjectKey string `json:"object_key"`
	// SizeBytes is the size of the object, which locates its footer.
	SizeBytes uint64 `json:"size_bytes"`
	// UploadedAt is when the upload was verified, in milliseconds since the
	// Unix epoch.
	UploadedAt uint64 `json:"uploaded_at"`
	// AreLocalCopiesDropped is true once the object is the only copy.
	AreLocalCopiesDropped bool `json:"are_local_copies_dropped"`
}

// SegmentRecord is one segment of a stream.
type SegmentRecord struct {
	// SegmentID is the segment's position within the stream.
	SegmentID uint64 `json:"segment_id"`
	// Epoch is the only epoch that authors records in the segment.
	Epoch uint64 `json:"epoch"`
	// FirstSequence is the sequence number of the segment's first record.
	FirstSequence uint64 `json:"first_sequence"`
	// Sealed is set once the segment is sealed.
	Sealed *SealRecord `json:"sealed"`
	// Copies is the replica set, fixed while the segment is open.
	Copies []CopyRecord `json:"copies"`
	// Tiered is set once the segment is in the object store.
	Tiered *TieredRecord `json:"tiered"`
}

// IsOpen reports whether the segment is not sealed yet.
func (segment SegmentRecord) IsOpen() bool { return segment.Sealed == nil }

// Contains reports whether the segment holds sequence: at or after its
// first record and, if it is sealed, at or before its sealed end.
func (segment SegmentRecord) Contains(sequence uint64) bool {
	if sequence < segment.FirstSequence {
		return false
	}
	return segment.Sealed == nil || sequence <= segment.Sealed.End
}

// StreamRecord is everything the metadata store knows about one stream,
// except its materializers' checkpoints.
type StreamRecord struct {
	// SchemaVersion is the version the record was written with.
	SchemaVersion uint32 `json:"schema_version"`
	// StreamID is the stream's permanent identifier.
	StreamID uint64 `json:"stream_id"`
	// Name is the stream's name.
	Name string `json:"name"`
	// Replication says how the stream's segments are replicated.
	Replication ReplicationSettings `json:"replication"`
	// Retention says how long sealed segments are kept.
	Retention RetentionPolicy `json:"retention"`
	// Epoch is the current epoch. Only the writer that raised it to this
	// value may change the segment list for writing.
	Epoch uint64 `json:"epoch"`
	// Writer is the current writer's lease, if any.
	Writer *WriterLease `json:"writer"`
	// Segments are the stream's segments, oldest first. At most the last
	// one is open.
	Segments []SegmentRecord `json:"segments"`
	// NextSegmentID is the identifier the next segment gets.
	NextSegmentID uint64 `json:"next_segment_id"`
	// StandbyReplicas is the replica set placed in advance for the next
	// segment, or nil if none is placed.
	StandbyReplicas []string `json:"standby_replicas"`
	// TruncateBefore is a position the stream's owner asked to truncate
	// before, or nil.
	TruncateBefore *uint64 `json:"truncate_before"`
	// LowWaterMark is the first sequence number that can still be read.
	LowWaterMark uint64 `json:"low_water_mark"`
	// Materializers are the registered materializers, sorted.
	Materializers []string `json:"materializers"`
}

// LastSegment returns the stream's newest segment, or nil if it has none.
func (stream *StreamRecord) LastSegment() *SegmentRecord {
	if len(stream.Segments) == 0 {
		return nil
	}
	return &stream.Segments[len(stream.Segments)-1]
}

// OpenSegment returns the stream's open segment, or nil if the last segment
// is sealed or there is none.
func (stream *StreamRecord) OpenSegment() *SegmentRecord {
	if last := stream.LastSegment(); last != nil && last.IsOpen() {
		return last
	}
	return nil
}

// SegmentContaining returns the segment that holds sequence, or nil.
func (stream *StreamRecord) SegmentContaining(sequence uint64) *SegmentRecord {
	for index := range stream.Segments {
		if stream.Segments[index].Contains(sequence) {
			return &stream.Segments[index]
		}
	}
	return nil
}

// Segment returns the segment with the given identifier, or nil.
func (stream *StreamRecord) Segment(segmentID uint64) *SegmentRecord {
	for index := range stream.Segments {
		if stream.Segments[index].SegmentID == segmentID {
			return &stream.Segments[index]
		}
	}
	return nil
}

// NextSegmentFirstSequence returns the sequence number the next segment
// starts at: one past the last sealed end, the start of an open last
// segment, or the low-water mark if the stream has no segment.
func (stream *StreamRecord) NextSegmentFirstSequence() uint64 {
	last := stream.LastSegment()
	switch {
	case last == nil:
		return stream.LowWaterMark
	case last.Sealed == nil:
		return last.FirstSequence
	default:
		return last.Sealed.End + 1
	}
}

// CheckpointRecord is a materializer's durable statement that everything up
// to Checkpoint is in its artifacts.
type CheckpointRecord struct {
	// SchemaVersion is the version the record was written with.
	SchemaVersion uint32 `json:"schema_version"`
	// Materializer is the materializer's name.
	Materializer string `json:"materializer"`
	// Checkpoint is the last sequence number covered by the artifacts.
	Checkpoint uint64 `json:"checkpoint"`
	// FencingToken is raised by one by each instance that takes over.
	FencingToken uint64 `json:"fencing_token"`
	// Manifest is opaque to the log, or nil before the first checkpoint.
	Manifest *string `json:"manifest"`
}

// ReplicaRecord is a replica's registration.
type ReplicaRecord struct {
	// SchemaVersion is the version the record was written with.
	SchemaVersion uint32 `json:"schema_version"`
	// ReplicaID is the identity of the replica's data directory.
	ReplicaID string `json:"replica_id"`
	// Address is where the replica is reached, as "host:port".
	Address string `json:"address"`
	// FailureDomains are the replica's labels, widest first.
	FailureDomains []string `json:"failure_domains"`
	// CapacityBytes is the size of the replica's disk.
	CapacityBytes uint64 `json:"capacity_bytes"`
	// FreeBytes is how much of the disk is unused.
	FreeBytes uint64 `json:"free_bytes"`
	// IsDraining is true while the replica is drained for maintenance.
	IsDraining bool `json:"is_draining"`
	// HeartbeatAt is when the registration was last renewed, in
	// milliseconds since the Unix epoch.
	HeartbeatAt uint64 `json:"heartbeat_at"`
}

// DeletedStreamRecord is the note a deleted stream leaves for the
// controller, which removes what the stream left on replicas and in the
// object store.
type DeletedStreamRecord struct {
	// StreamID is the deleted stream's identifier.
	StreamID uint64 `json:"stream_id"`
	// Name is the name the stream had.
	Name string `json:"name"`
}

// encodeStream writes a stream record as the JSON the store holds. Lists
// that are empty are written as [], never as null: null is not a list to
// the other implementations.
func encodeStream(stream StreamRecord) ([]byte, error) {
	if stream.Segments == nil {
		stream.Segments = []SegmentRecord{}
	} else {
		stream.Segments = slices.Clone(stream.Segments)
	}
	for index := range stream.Segments {
		if stream.Segments[index].Copies == nil {
			stream.Segments[index].Copies = []CopyRecord{}
		}
	}
	if stream.Materializers == nil {
		stream.Materializers = []string{}
	}
	return json.Marshal(stream)
}

// decodeStream reads a stream record and refuses a schema version this
// package does not know.
func decodeStream(key string, value []byte) (StreamRecord, error) {
	var stream StreamRecord
	if err := json.Unmarshal(value, &stream); err != nil {
		return StreamRecord{}, fmt.Errorf("%w: %s: %v", ErrCorruptRecord, key, err)
	}
	if stream.SchemaVersion != SchemaVersion {
		return StreamRecord{}, fmt.Errorf("%w: %s has schema version %d, this client knows %d",
			ErrCorruptRecord, key, stream.SchemaVersion, SchemaVersion)
	}
	return stream, nil
}
