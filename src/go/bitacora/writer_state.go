package bitacora

import (
	"slices"
	"sort"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

// writerState is the bookkeeping of a writer: which records were assigned,
// what each replica of the open segment has acknowledged, and what is
// committed. It does no I/O and takes the time as an argument, so the rules
// of docs/protocol/replication.md ("A writer") can be tested on their own.
// The writer guards it with its mutex.
//
// Every acknowledgement of every replica goes through here, so nothing in
// it scans the pending batches: positions are found by binary search and
// sizes are differences of running totals.
type writerState struct {
	// writeQuorum is W: how many acknowledgements commit a record.
	writeQuorum int
	// segment is the open segment that batches are encoded for.
	segment replica.Descriptor
	// generation counts the segments this writer has opened. A sender that
	// belongs to an older generation must stop.
	generation uint64
	// replicas are the replicas of the open segment.
	replicas []replicaProgress
	// pending are the batches some replica still has to acknowledge, in
	// sequence order. The first reportedCount of them are committed.
	pending []pendingBatch
	// reportedCount is how many leading batches of pending have had their
	// outcome reported.
	reportedCount int
	// nextSequence is the sequence number the next record gets.
	nextSequence uint64
	// durableSequence is the last committed sequence number.
	durableSequence uint64
	// enqueuedBytes is the total size of every batch ever enqueued. Sizes
	// of ranges of batches are differences of such totals.
	enqueuedBytes uint64
	// durableBytes is the total size of every batch up to durableSequence.
	durableBytes uint64
	// segmentStartBytes is the value of durableBytes when the open segment
	// was opened, for the size limit.
	segmentStartBytes uint64
	// segmentOpenedAt is when the open segment was opened, for the age
	// limit.
	segmentOpenedAt time.Time
	// isRolling is set from the moment the sealed end of the segment is
	// chosen until the next segment is open. Nothing commits meanwhile.
	isRolling bool
}

// replicaProgress is what the writer knows about one replica of the open
// segment.
type replicaProgress struct {
	// replicaID is the replica's identity.
	replicaID string
	// acknowledgedEnd is the last sequence number the replica has
	// acknowledged as flushed. It never decreases.
	acknowledgedEnd uint64
	// acknowledgedBytes is the total size of every batch up to
	// acknowledgedEnd.
	acknowledgedBytes uint64
	// lastContact is when the replica last answered.
	lastContact time.Time
	// hasRefused is set when the replica refuses appends for good: it is
	// full, draining, damaged, or holds less than it acknowledged.
	hasRefused bool
	// isLagging is set when the replica is so far behind that the writer
	// would have to keep more than its limit of batches for it.
	isLagging bool
}

// isAbandoned reports whether the writer has given up on the replica for
// this segment and is waiting to roll away from it.
func (progress replicaProgress) isAbandoned() bool {
	return progress.hasRefused || progress.isLagging
}

// pendingBatch is a batch that is not acknowledged by every replica yet.
type pendingBatch struct {
	// batch is the encoded frame, for the open segment.
	batch frame.Batch
	// endBytes is the total size of every batch up to and including this
	// one.
	endBytes uint64
	// outcome receives nil when the batch commits, or the reason its
	// outcome will never be known. It is buffered, so reporting never
	// blocks.
	outcome chan error
}

// rollLimits are the settings that decide when a writer rolls.
type rollLimits struct {
	// replicaFailureTimeout is how long a replica may be silent.
	replicaFailureTimeout time.Duration
	// maxReplicaLagBytes is how many bytes of committed batches the writer
	// keeps for a replica that is behind.
	maxReplicaLagBytes uint64
	// targetSegmentBytes is the size at which a segment is rolled.
	targetSegmentBytes uint64
	// targetSegmentAge is the age at which a segment is rolled.
	targetSegmentAge time.Duration
}

// newWriterState returns the state of a writer that has just opened segment
// on the replicas with the given identities.
func newWriterState(writeQuorum int, segment replica.Descriptor, replicaIDs []string, now time.Time) *writerState {
	state := &writerState{
		writeQuorum:     writeQuorum,
		nextSequence:    segment.FirstSequence,
		durableSequence: segment.FirstSequence - 1,
	}
	state.openSegment(segment, replicaIDs, now)
	return state
}

// openSegment makes segment the open segment. Every replica starts with
// everything up to the durable sequence acknowledged and nothing after it.
func (state *writerState) openSegment(segment replica.Descriptor, replicaIDs []string, now time.Time) {
	state.segment = segment
	state.replicas = make([]replicaProgress, 0, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		state.replicas = append(state.replicas, replicaProgress{
			replicaID:         replicaID,
			acknowledgedEnd:   state.durableSequence,
			acknowledgedBytes: state.durableBytes,
			lastContact:       now,
		})
	}
	state.segmentStartBytes = state.durableBytes
	state.segmentOpenedAt = now
}

// enqueue assigns the next sequence numbers to payloads, as one batch, and
// returns the batch's first and last sequence number and the channel its
// outcome arrives on.
func (state *writerState) enqueue(payloads [][]byte) (first, last uint64, outcome <-chan error, err error) {
	batch, err := frame.Encode(state.segment.Address, state.nextSequence, payloads)
	if err != nil {
		return 0, 0, nil, err
	}
	state.enqueuedBytes += uint64(len(batch.Frame))
	reported := make(chan error, 1)
	state.pending = append(state.pending, pendingBatch{batch: batch, endBytes: state.enqueuedBytes, outcome: reported})
	state.nextSequence = batch.LastSequence() + 1
	return batch.FirstSequence, batch.LastSequence(), reported, nil
}

// uncommittedBytes returns the size of the batches that are assigned and
// not committed, for flow control.
func (state *writerState) uncommittedBytes() uint64 {
	return state.enqueuedBytes - state.durableBytes
}

// hasUncommitted reports whether a record was assigned that is not
// committed yet.
func (state *writerState) hasUncommitted() bool {
	return state.durableSequence+1 < state.nextSequence
}

// hasBatchesFor reports whether there is something to send to the replica:
// it has not acknowledged every assigned record and was not abandoned.
func (state *writerState) hasBatchesFor(replicaIndex int) bool {
	progress := state.replicas[replicaIndex]
	return !progress.isAbandoned() && progress.acknowledgedEnd+1 < state.nextSequence
}

// firstPendingAfter returns the position in pending of the first batch that
// ends after sequence.
func (state *writerState) firstPendingAfter(sequence uint64) int {
	return sort.Search(len(state.pending), func(position int) bool {
		return state.pending[position].batch.LastSequence() > sequence
	})
}

// bytesUpTo returns the total size of every batch up to and including the
// one that ends at sequence.
func (state *writerState) bytesUpTo(sequence uint64) uint64 {
	position := state.firstPendingAfter(sequence)
	if position > 0 {
		return state.pending[position-1].endBytes
	}
	if len(state.pending) > 0 {
		// Everything pending is after the sequence.
		return state.pending[0].endBytes - uint64(len(state.pending[0].batch.Frame))
	}
	return state.enqueuedBytes
}

// planRequest returns the frames to send to a replica next: the batches
// after what it acknowledged, whole, up to about maxBytes and at least one
// if there is any. No frames means a heartbeat.
func (state *writerState) planRequest(replicaIndex int, maxBytes int) []byte {
	progress := state.replicas[replicaIndex]
	if progress.isAbandoned() {
		return nil
	}
	var frames []byte
	for position := state.firstPendingAfter(progress.acknowledgedEnd); position < len(state.pending); position++ {
		encoded := state.pending[position].batch.Frame
		if len(frames) > 0 && len(frames)+len(encoded) > maxBytes {
			break
		}
		frames = append(frames, encoded...)
	}
	return frames
}

// recordAcknowledgement notes that a replica answered at now and holds the
// segment up to localEnd. It returns false if the replica holds less than
// it acknowledged before, or more than was ever assigned: its copy is not
// this writer's segment any more, and the writer gives up on it.
func (state *writerState) recordAcknowledgement(replicaIndex int, localEnd uint64, now time.Time, limits rollLimits) bool {
	progress := &state.replicas[replicaIndex]
	progress.lastContact = now
	if localEnd < progress.acknowledgedEnd || localEnd >= state.nextSequence {
		progress.hasRefused = true
		state.advance(limits)
		return false
	}
	if localEnd > progress.acknowledgedEnd {
		progress.acknowledgedEnd = localEnd
		progress.acknowledgedBytes = state.bytesUpTo(localEnd)
		state.advance(limits)
	}
	return true
}

// recordRefusal notes that a replica answered at now and refuses appends
// for good.
func (state *writerState) recordRefusal(replicaIndex int, now time.Time, limits rollLimits) {
	state.replicas[replicaIndex].lastContact = now
	state.replicas[replicaIndex].hasRefused = true
	state.advance(limits)
}

// advance moves the durable sequence to the W-th highest acknowledged end,
// reports the batches it passed as committed, forgets the batches nobody
// needs again, and marks a replica as lagging if keeping batches for it
// would exceed the limit. While a roll is in progress the durable sequence
// stands still.
func (state *writerState) advance(limits rollLimits) {
	if !state.isRolling {
		acknowledgedEnds := make([]uint64, 0, len(state.replicas))
		for _, progress := range state.replicas {
			acknowledgedEnds = append(acknowledgedEnds, progress.acknowledgedEnd)
		}
		slices.Sort(acknowledgedEnds)
		// The W-th highest of N values, sorted ascending, is at N - W.
		quorumEnd := acknowledgedEnds[len(acknowledgedEnds)-state.writeQuorum]
		if quorumEnd > state.durableSequence {
			state.durableSequence = quorumEnd
			state.durableBytes = state.bytesUpTo(quorumEnd)
		}
		for state.reportedCount < len(state.pending) &&
			state.pending[state.reportedCount].batch.LastSequence() <= state.durableSequence {
			state.pending[state.reportedCount].outcome <- nil
			state.reportedCount++
		}
	}
	for replicaIndex := range state.replicas {
		progress := &state.replicas[replicaIndex]
		if state.durableBytes > progress.acknowledgedBytes &&
			state.durableBytes-progress.acknowledgedBytes > limits.maxReplicaLagBytes {
			progress.isLagging = true
		}
	}
	state.forgetAcknowledgedByAll()
}

// forgetAcknowledgedByAll drops the committed batches that every replica
// the writer still sends to has acknowledged: nobody needs them again.
func (state *writerState) forgetAcknowledgedByAll() {
	lowestAcknowledgedEnd := state.durableSequence
	for _, progress := range state.replicas {
		if !progress.isAbandoned() {
			lowestAcknowledgedEnd = min(lowestAcknowledgedEnd, progress.acknowledgedEnd)
		}
	}
	forgottenCount := min(state.firstPendingAfter(lowestAcknowledgedEnd), state.reportedCount)
	// Zero what is dropped, so that the frames are freed although the
	// slice still shares its array.
	clear(state.pending[:forgottenCount])
	state.pending = state.pending[forgottenCount:]
	state.reportedCount -= forgottenCount
}

// rollReason says why the writer should roll now, or returns the empty
// string if it should not.
func (state *writerState) rollReason(now time.Time, limits rollLimits) string {
	if state.isRolling {
		return ""
	}
	for _, progress := range state.replicas {
		switch {
		case progress.hasRefused:
			return "replica " + progress.replicaID + " refuses appends"
		case progress.isLagging:
			return "replica " + progress.replicaID + " is too far behind"
		case now.Sub(progress.lastContact) >= limits.replicaFailureTimeout:
			return "replica " + progress.replicaID + " does not answer"
		}
	}
	if state.durableBytes-state.segmentStartBytes >= limits.targetSegmentBytes {
		return "the segment is full"
	}
	if now.Sub(state.segmentOpenedAt) >= limits.targetSegmentAge {
		return "the segment is old"
	}
	return ""
}

// beginRoll freezes the durable sequence and returns it: the sequence
// number the open segment will be sealed at.
func (state *writerState) beginRoll() uint64 {
	state.isRolling = true
	return state.durableSequence
}

// abandonRoll resumes committing on the open segment after a roll that did
// not happen.
func (state *writerState) abandonRoll(limits rollLimits) {
	state.isRolling = false
	state.advance(limits)
}

// completeRoll makes segment the open segment: the batches at or below the
// sealed end are forgotten, and every batch above it is encoded again for
// the new segment under the same sequence numbers, because a frame belongs
// to one segment.
func (state *writerState) completeRoll(segment replica.Descriptor, replicaIDs []string, now time.Time) error {
	uncommitted := state.pending[state.firstPendingAfter(state.durableSequence):]
	reencoded := make([]pendingBatch, 0, len(uncommitted))
	for _, pending := range uncommitted {
		records := pending.batch.Records()
		payloads := make([][]byte, 0, len(records))
		for _, record := range records {
			payloads = append(payloads, record.Payload)
		}
		batch, err := frame.Encode(segment.Address, pending.batch.FirstSequence, payloads)
		if err != nil {
			return err
		}
		pending.batch = batch
		reencoded = append(reencoded, pending)
	}
	state.pending = reencoded
	state.reportedCount = 0
	state.openSegment(segment, replicaIDs, now)
	state.generation++
	state.isRolling = false
	return nil
}

// failUncommitted reports reason to every batch whose outcome is not known
// yet. The writer has stopped; nothing more will commit through it.
func (state *writerState) failUncommitted(reason error) {
	for _, pending := range state.pending[state.reportedCount:] {
		pending.outcome <- reason
	}
	state.pending = nil
	state.reportedCount = 0
	state.durableBytes = state.enqueuedBytes
}
