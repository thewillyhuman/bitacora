package bitacora

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
	"github.com/thewillyhuman/bitacora/src/go/internal/tiered"
)

const (
	// readerMaxResponseBytes is about how many bytes one read asks a
	// replica or the object store for.
	readerMaxResponseBytes = 1024 * 1024
	// readerTailWait is how long a follower lets a replica hold a request
	// when nothing new is committed.
	readerTailWait = time.Second
	// readerRetryInterval is the pause between two rounds of asking when
	// nothing could serve a read.
	readerRetryInterval = 100 * time.Millisecond
	// readerMaxAttempts is how many rounds a range read makes before it
	// reports the records as unavailable.
	readerMaxAttempts = 3
)

// Reader reads the committed records of one stream. Any number of readers
// can read the same stream; they never coordinate. A Reader is safe for
// concurrent use.
type Reader struct {
	// client is the cluster the stream lives in.
	client *Client
	// streamName is the stream that is read.
	streamName string
}

// OpenReader returns a reader of the stream called streamName. It sends
// nothing; a stream that does not exist shows up on the first read.
func (client *Client) OpenReader(streamName string) *Reader {
	return &Reader{client: client, streamName: streamName}
}

// ReadRange reads the committed records with sequence numbers first to
// last, both included, in order. The result is shorter than the range only
// if the range extends past the last record the replicas know to be
// committed. They learn that from the writer's next message, so a record is
// readable a moment after its append returned, not at that instant.
//
// The error is a [*TruncatedError] if the range starts below the low-water
// mark, and wraps [ErrUnavailable] if nothing that holds the records could
// be reached, [ErrStreamNotFound] if the stream does not exist, or
// [ErrInvalidArgument] if the range is empty or starts at 0.
func (reader *Reader) ReadRange(ctx context.Context, first, last uint64) ([]Record, error) {
	if first == 0 || last < first {
		return nil, fmt.Errorf("%w: a range from %d to %d", ErrInvalidArgument, first, last)
	}
	cursor, err := reader.openCursor(ctx, first)
	if err != nil {
		return nil, err
	}
	var records []Record
	for cursor.nextSequence <= last {
		fetched, err := cursor.fetch(ctx, 0)
		if err != nil {
			return nil, err
		}
		if len(fetched) == 0 {
			break
		}
		for _, record := range fetched {
			if record.Sequence <= last {
				records = append(records, record)
			}
		}
	}
	return records, nil
}

// Follow yields every committed record from sequence number from on, as
// soon as it commits, and keeps going across segment rolls and writer
// changes until ctx is done or the caller stops ranging. Sequence numbers
// start at 1, and from 0 means the same.
//
// Trouble that can pass, such as an unreachable replica or metadata store,
// is retried without bothering the caller. An error is yielded, as the last
// element, only when following cannot continue: a [*TruncatedError] if the
// position was truncated, [ErrStreamNotFound] if the stream was deleted, or
// the context's error once ctx is done.
func (reader *Reader) Follow(ctx context.Context, from uint64) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		var cursor *cursor
		for {
			if err := ctx.Err(); err != nil {
				yield(Record{}, err)
				return
			}
			if cursor == nil {
				opened, err := reader.openCursor(ctx, max(from, 1))
				if err != nil {
					if isFatalForAFollower(err) {
						yield(Record{}, err)
						return
					}
					pause(ctx, readerRetryInterval)
					continue
				}
				cursor = opened
			}
			askedAt := time.Now()
			records, err := cursor.fetch(ctx, readerTailWait)
			switch {
			case err != nil && isFatalForAFollower(err):
				yield(Record{}, err)
				return
			case err != nil:
				pause(ctx, readerRetryInterval)
			case len(records) == 0:
				// Usually a replica held the request for the tail wait.
				// When there was nothing to ask, nothing has waited yet:
				// pause here, or the follower would poll the metadata
				// store in a loop.
				pause(ctx, readerRetryInterval-time.Since(askedAt))
			}
			for _, record := range records {
				if !yield(record, nil) {
					return
				}
			}
		}
	}
}

// LowWaterMark returns the first sequence number that can still be read.
// Everything below it has been truncated.
func (reader *Reader) LowWaterMark(ctx context.Context) (uint64, error) {
	stream, err := reader.client.metadata.GetStream(ctx, reader.streamName)
	if err != nil {
		return 0, fromMetadata(err)
	}
	return stream.LowWaterMark, nil
}

// isFatalForAFollower reports whether following cannot go on after err.
func isFatalForAFollower(err error) bool {
	return errors.Is(err, ErrTruncated) || errors.Is(err, ErrStreamNotFound) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// pause waits for duration or until ctx is done, whichever comes first.
func pause(ctx context.Context, duration time.Duration) {
	if duration <= 0 {
		return
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// cursor is a position in a stream, with what is needed to read on from it.
type cursor struct {
	reader *Reader
	// nextSequence is the sequence number of the next record to read.
	nextSequence uint64
	// stream is the stream's record as last read. Sealed segments never
	// change, so only what it says about the open segment can be stale.
	stream metadata.StreamRecord
	// connectedSegmentID is the segment that connected talks to, or 0.
	connectedSegmentID uint64
	// connected are the replicas of that segment, in the order of its
	// copies.
	connected []*replica.Client
	// preferredReplica is the replica that is asked first.
	preferredReplica int
	// openTieredSegmentID is the segment whose object openTiered is, or 0.
	openTieredSegmentID uint64
	// openTiered is the index of the tiered object being read, kept so
	// that its footer and index are fetched once.
	openTiered tiered.Index
}

// openCursor reads the stream's record and returns a cursor at
// fromSequence.
func (reader *Reader) openCursor(ctx context.Context, fromSequence uint64) (*cursor, error) {
	stream, err := reader.client.metadata.GetStream(ctx, reader.streamName)
	if err != nil {
		return nil, fromMetadata(err)
	}
	return &cursor{reader: reader, nextSequence: fromSequence, stream: stream}, nil
}

// replicasAnswer is what the replicas of a segment answered, together.
type replicasAnswer int

const (
	// gotRecords: one of them had records.
	gotRecords replicasAnswer = iota
	// nothingNewAtTheTail: a follower's replica of the open segment has
	// nothing new yet.
	nothingNewAtTheTail
	// segmentFinished: the segment is sealed and the cursor is past its end.
	segmentFinished
	// noRecords: none of them had records.
	noRecords
)

// fetch reads the records at the cursor's position and moves past them. It
// returns no records and no error if nothing is committed at the position
// yet. tailWait is how long a replica may hold the request when nothing is
// committed; zero asks every replica once without waiting.
func (cursor *cursor) fetch(ctx context.Context, tailWait time.Duration) ([]Record, error) {
	hasRefreshed := false
	failedRounds := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cursor.nextSequence < cursor.stream.LowWaterMark {
			return nil, &TruncatedError{LowWaterMark: cursor.stream.LowWaterMark}
		}
		found := cursor.stream.SegmentContaining(cursor.nextSequence)
		if found == nil {
			// No known segment holds the position: either the stream ends
			// here for now, or a segment was opened since.
			if hasRefreshed {
				return nil, nil
			}
			if err := cursor.refresh(ctx); err != nil {
				return nil, err
			}
			hasRefreshed = true
			continue
		}
		segment := *found

		answer, records, lastFailure, err := cursor.askReplicas(ctx, segment, tailWait)
		if err != nil {
			return nil, err
		}
		switch answer {
		case gotRecords:
			return records, nil
		case nothingNewAtTheTail:
			// The refresh notices a seal.
			return nil, cursor.refresh(ctx)
		case segmentFinished:
			// A replica knows the seal and the record read does not: read
			// it again, once. The seal is written to the metadata store
			// before any replica hears of it, so once is enough.
			if hasRefreshed {
				return nil, nil
			}
			if err := cursor.refresh(ctx); err != nil {
				return nil, err
			}
			hasRefreshed = true
			continue
		}

		// No replica produced records. A segment that was uploaded can
		// still be read from the object store.
		if segment.Tiered != nil {
			records, err := cursor.fetchTiered(ctx, segment)
			if err == nil {
				return records, nil
			}
			lastFailure = err.Error()
		}

		// Look at the metadata again: the segment may have been sealed,
		// repaired, tiered or followed by another.
		if err := cursor.refresh(ctx); err != nil {
			return nil, err
		}
		if lastFailure == "" {
			current := cursor.stream.SegmentContaining(cursor.nextSequence)
			if current != nil && current.IsOpen() {
				return nil, nil
			}
			// A sealed segment holds the position but every copy asked
			// was short: wait for repair like for a failed replica.
			lastFailure = "every reachable copy of the sealed segment is incomplete"
		}
		failedRounds++
		if failedRounds >= readerMaxAttempts {
			address := frame.SegmentAddress{StreamID: cursor.stream.StreamID, SegmentID: segment.SegmentID}
			return nil, fmt.Errorf("%w: nothing could serve segment %s from sequence %d: %s",
				ErrUnavailable, address, cursor.nextSequence, lastFailure)
		}
		pause(ctx, readerRetryInterval)
	}
}

// askReplicas asks the replicas of segment for the records at the cursor's
// position, starting with the preferred one, until one has them. It moves
// the cursor past the records it returns. lastFailure is why the last
// replica that failed did, or empty if they all answered and held nothing.
func (cursor *cursor) askReplicas(ctx context.Context, segment metadata.SegmentRecord, tailWait time.Duration) (answer replicasAnswer, records []Record, lastFailure string, err error) {
	replicas, err := cursor.replicasOf(ctx, segment)
	if err != nil {
		return noRecords, nil, "", fromMetadata(err)
	}
	for attempt := range len(replicas) {
		replicaIndex := (cursor.preferredReplica + attempt) % len(replicas)
		if segment.Copies[replicaIndex].State == metadata.CopyBad {
			continue
		}
		records, isFinished, failure := cursor.ask(ctx, replicas[replicaIndex], segment, tailWait)
		switch {
		case failure != "":
			lastFailure = failure
		case len(records) > 0:
			cursor.preferredReplica = replicaIndex
			cursor.nextSequence = records[len(records)-1].Sequence + 1
			return gotRecords, records, "", nil
		case isFinished:
			return segmentFinished, nil, "", nil
		case segment.IsOpen() && tailWait > 0:
			// A follower asks one replica per wait. If this one is cut off
			// from the writer, the next call asks its neighbour.
			cursor.preferredReplica = (replicaIndex + 1) % len(replicas)
			return nothingNewAtTheTail, nil, "", nil
		}
		if ctx.Err() != nil {
			return noRecords, nil, "", ctx.Err()
		}
	}
	return noRecords, nil, lastFailure, nil
}

// ask sends one read to one replica. It returns the records at the cursor's
// position, or isFinished if the segment is sealed and the cursor is past
// its end, or a failure if the replica failed or sent something invalid.
// None of the three means the replica holds nothing at the position.
func (cursor *cursor) ask(ctx context.Context, replicaClient *replica.Client, segment metadata.SegmentRecord, tailWait time.Duration) (records []Record, isFinished bool, failure string) {
	address := frame.SegmentAddress{StreamID: cursor.stream.StreamID, SegmentID: segment.SegmentID}
	// A sealed segment has nothing to wait for.
	wait := tailWait
	if !segment.IsOpen() {
		wait = 0
	}
	requestCtx, cancel := context.WithTimeout(ctx, cursor.reader.client.requestTimeout+wait)
	defer cancel()
	response, err := replicaClient.Read(requestCtx, address, cursor.nextSequence, readerMaxResponseBytes, wait, nil)
	if err != nil {
		return nil, false, err.Error()
	}
	// The reader verifies every batch checksum itself: the replica is not
	// trusted to have done it.
	records, err = recordsFrom(address, response.Frames, cursor.nextSequence)
	if err != nil {
		return nil, false, fmt.Sprintf("replica %s: %v", replicaClient.ReplicaID(), err)
	}
	if len(records) > 0 {
		return records, false, ""
	}
	sealedEnd := response.SealedEnd
	if segment.Sealed != nil {
		sealedEnd = &segment.Sealed.End
	}
	return nil, sealedEnd != nil && cursor.nextSequence > *sealedEnd, ""
}

// recordsFrom verifies frames and returns their records from fromSequence
// on. It is an error if records are returned that do not start exactly
// there.
func recordsFrom(address frame.SegmentAddress, frames []byte, fromSequence uint64) ([]Record, error) {
	batches, err := frame.DecodeAll(address, frames)
	if err != nil {
		return nil, err
	}
	var records []Record
	for _, batch := range batches {
		for _, decoded := range batch.Records() {
			if decoded.Sequence >= fromSequence {
				// The payload aliases the response; copy it so that the
				// caller owns what it is given.
				records = append(records, Record{Sequence: decoded.Sequence, Payload: slices.Clone(decoded.Payload)})
			}
		}
	}
	if len(records) > 0 && records[0].Sequence != fromSequence {
		return nil, fmt.Errorf("the answer starts at %d instead of %d", records[0].Sequence, fromSequence)
	}
	return records, nil
}

// replicasOf returns clients for the replicas of segment, connecting when
// the cursor moves to another segment or the segment's replica set changed.
func (cursor *cursor) replicasOf(ctx context.Context, segment metadata.SegmentRecord) ([]*replica.Client, error) {
	isSameSet := cursor.connectedSegmentID == segment.SegmentID && len(cursor.connected) == len(segment.Copies)
	if isSameSet {
		for index, copyRecord := range segment.Copies {
			if cursor.connected[index].ReplicaID() != copyRecord.ReplicaID {
				isSameSet = false
			}
		}
	}
	if isSameSet {
		return cursor.connected, nil
	}
	if cursor.connectedSegmentID != segment.SegmentID {
		cursor.preferredReplica = 0
	}
	connected, err := cursor.reader.client.connectCopies(ctx, segment.Copies)
	if err != nil {
		return nil, err
	}
	cursor.connectedSegmentID = segment.SegmentID
	cursor.connected = connected
	return connected, nil
}

// fetchTiered reads the records at the cursor's position from the object
// store and moves past them.
func (cursor *cursor) fetchTiered(ctx context.Context, segment metadata.SegmentRecord) ([]Record, error) {
	objectStore := cursor.reader.client.objectStore
	if objectStore == nil {
		return nil, errors.New("the segment is in the object store, and this client was given none")
	}
	if segment.Sealed == nil {
		return nil, errors.New("the segment is recorded as tiered but not as sealed")
	}
	address := frame.SegmentAddress{StreamID: cursor.stream.StreamID, SegmentID: segment.SegmentID}
	requestCtx, cancel := context.WithTimeout(ctx, cursor.reader.client.requestTimeout)
	defer cancel()
	if cursor.openTieredSegmentID != segment.SegmentID {
		index, err := openTieredObject(requestCtx, objectStore, address, segment.Sealed.End, *segment.Tiered)
		if err != nil {
			return nil, err
		}
		cursor.openTieredSegmentID = segment.SegmentID
		cursor.openTiered = index
	}
	start, end := cursor.openTiered.BlocksFrom(cursor.nextSequence, readerMaxResponseBytes)
	var records []Record
	if start < end {
		blocks, err := objectStore.GetRange(requestCtx, segment.Tiered.ObjectKey, start, end)
		if err == nil {
			records, err = recordsFrom(address, blocks, cursor.nextSequence)
		}
		if err != nil {
			// Open the object afresh next time: it may have been replaced.
			cursor.openTieredSegmentID = 0
			return nil, fmt.Errorf("object %q: %w", segment.Tiered.ObjectKey, err)
		}
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("object %q holds no record at %d", segment.Tiered.ObjectKey, cursor.nextSequence)
	}
	cursor.nextSequence = records[len(records)-1].Sequence + 1
	return records, nil
}

// openTieredObject fetches and verifies the footer and the block index of a
// tiered object, and checks that it is the segment the metadata store says
// it is.
func openTieredObject(ctx context.Context, objectStore ObjectStore, address frame.SegmentAddress, sealedEnd uint64, record metadata.TieredRecord) (tiered.Index, error) {
	if record.SizeBytes < tiered.FooterBytes {
		return tiered.Index{}, fmt.Errorf("object %q is recorded with %d bytes, less than a footer", record.ObjectKey, record.SizeBytes)
	}
	footerBytes, err := objectStore.GetRange(ctx, record.ObjectKey, record.SizeBytes-tiered.FooterBytes, record.SizeBytes)
	if err != nil {
		return tiered.Index{}, err
	}
	footer, err := tiered.DecodeFooter(footerBytes)
	if err != nil {
		return tiered.Index{}, fmt.Errorf("object %q: %w", record.ObjectKey, err)
	}
	if footer.Segment != address || footer.SealedEnd != sealedEnd || footer.ObjectBytes() != record.SizeBytes {
		return tiered.Index{}, fmt.Errorf("object %q is not segment %s sealed at %d", record.ObjectKey, address, sealedEnd)
	}
	indexBytes, err := objectStore.GetRange(ctx, record.ObjectKey, footer.IndexStart(), footer.IndexEnd())
	if err != nil {
		return tiered.Index{}, err
	}
	index, err := tiered.DecodeIndex(footer, indexBytes)
	if err != nil {
		return tiered.Index{}, fmt.Errorf("object %q: %w", record.ObjectKey, err)
	}
	return index, nil
}

// refresh reads the stream's record again.
func (cursor *cursor) refresh(ctx context.Context) error {
	stream, err := cursor.reader.client.metadata.GetStream(ctx, cursor.reader.streamName)
	if err != nil {
		return fromMetadata(err)
	}
	cursor.stream = stream
	return nil
}
