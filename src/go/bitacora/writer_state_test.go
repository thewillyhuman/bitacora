package bitacora

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

// The tests of the writer's bookkeeping give it a clock of their own.
var testStart = time.Date(2026, time.November, 25, 12, 0, 0, 0, time.UTC)

// generousLimits never ask for a roll on their own.
var generousLimits = rollLimits{
	replicaFailureTimeout: time.Hour,
	maxReplicaLagBytes:    1 << 30,
	targetSegmentBytes:    1 << 30,
	targetSegmentAge:      24 * time.Hour,
}

// firstSegment is the first segment of stream 7, starting at sequence 1.
var firstSegment = replica.Descriptor{
	Address:       frame.SegmentAddress{StreamID: 7, SegmentID: 1},
	Epoch:         1,
	FirstSequence: 1,
}

// stateOfThree returns the state of a writer of a segment on three
// replicas with a write quorum of two.
func stateOfThree() *writerState {
	return newWriterState(2, firstSegment, []string{"replica-a", "replica-b", "replica-c"}, testStart)
}

// enqueueOne assigns one record of the given size and returns the channel
// its outcome arrives on.
func enqueueOne(t *testing.T, state *writerState, payloadBytes int) <-chan error {
	t.Helper()
	_, _, outcome, err := state.enqueue([][]byte{make([]byte, payloadBytes)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return outcome
}

// outcomeOf returns what a batch's outcome channel holds: the error, and
// whether an outcome has been reported at all.
func outcomeOf(outcome <-chan error) (err error, isReported bool) {
	select {
	case err := <-outcome:
		return err, true
	default:
		return nil, false
	}
}

func TestEnqueueAssignsConsecutiveSequenceNumbers(t *testing.T) {
	state := stateOfThree()

	first, last, _, err := state.enqueue([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil || first != 1 || last != 3 {
		t.Fatalf("the first batch got %d to %d, %v; want 1 to 3", first, last, err)
	}
	first, last, _, err = state.enqueue([][]byte{[]byte("d")})
	if err != nil || first != 4 || last != 4 {
		t.Fatalf("the second batch got %d to %d, %v; want 4", first, last, err)
	}
	if !state.hasUncommitted() || state.uncommittedBytes() == 0 {
		t.Error("assigned records must count as uncommitted")
	}

	// A batch that cannot be encoded takes no sequence number.
	if _, _, _, err := state.enqueue(nil); err == nil {
		t.Error("an empty batch was accepted")
	}
	first, _, _, _ = state.enqueue([][]byte{[]byte("e")})
	if first != 5 {
		t.Errorf("after a refused batch the next record got %d, want 5", first)
	}
}

func TestARecordCommitsOnTheWriteQuorum(t *testing.T) {
	cases := []struct {
		name string
		// writeQuorum of replicaCount replicas.
		writeQuorum, replicaCount int
		// acknowledged is the last sequence number each replica flushed.
		acknowledged []uint64
		wantDurable  uint64
	}{
		{name: "nobody acknowledged", writeQuorum: 2, replicaCount: 3, acknowledged: []uint64{0, 0, 0}, wantDurable: 0},
		{name: "one of three is not a quorum of two", writeQuorum: 2, replicaCount: 3, acknowledged: []uint64{5, 0, 0}, wantDurable: 0},
		{name: "two of three are", writeQuorum: 2, replicaCount: 3, acknowledged: []uint64{5, 0, 5}, wantDurable: 5},
		{name: "the second highest decides", writeQuorum: 2, replicaCount: 3, acknowledged: []uint64{5, 3, 1}, wantDurable: 3},
		{name: "a quorum of three needs everyone", writeQuorum: 3, replicaCount: 3, acknowledged: []uint64{5, 3, 1}, wantDurable: 1},
		{name: "a quorum of one needs anyone", writeQuorum: 1, replicaCount: 3, acknowledged: []uint64{2, 5, 0}, wantDurable: 5},
		{name: "three of five", writeQuorum: 3, replicaCount: 5, acknowledged: []uint64{5, 4, 3, 2, 1}, wantDurable: 3},
		{name: "one replica alone", writeQuorum: 1, replicaCount: 1, acknowledged: []uint64{4}, wantDurable: 4},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			replicaIDs := make([]string, testCase.replicaCount)
			state := newWriterState(testCase.writeQuorum, firstSegment, replicaIDs, testStart)
			outcomes := make([]<-chan error, 0, 5)
			for range 5 {
				outcomes = append(outcomes, enqueueOne(t, state, 10))
			}

			for replicaIndex, localEnd := range testCase.acknowledged {
				if !state.recordAcknowledgement(replicaIndex, localEnd, testStart, generousLimits) {
					t.Fatalf("the acknowledgement of replica %d was refused", replicaIndex)
				}
			}

			if state.durableSequence != testCase.wantDurable {
				t.Errorf("durable sequence = %d, want %d", state.durableSequence, testCase.wantDurable)
			}
			for index, outcome := range outcomes {
				sequence := uint64(index + 1)
				err, isReported := outcomeOf(outcome)
				if wantReported := sequence <= testCase.wantDurable; isReported != wantReported || err != nil {
					t.Errorf("record %d: reported %v with %v, want reported %v as committed", sequence, isReported, err, wantReported)
				}
			}
		})
	}
}

func TestTheDurableSequenceNeverMovesBack(t *testing.T) {
	state := stateOfThree()
	for range 3 {
		enqueueOne(t, state, 10)
	}
	state.recordAcknowledgement(0, 3, testStart, generousLimits)
	state.recordAcknowledgement(1, 3, testStart, generousLimits)
	if state.durableSequence != 3 {
		t.Fatalf("durable sequence = %d, want 3", state.durableSequence)
	}

	// A replica that now reports less than it acknowledged has lost
	// records: what it acknowledged stays counted, and it is given up on.
	if state.recordAcknowledgement(1, 1, testStart, generousLimits) {
		t.Error("an acknowledgement that moves back was accepted")
	}
	if state.durableSequence != 3 {
		t.Errorf("durable sequence = %d after a replica moved back, want 3", state.durableSequence)
	}
	if reason := state.rollReason(testStart, generousLimits); reason == "" {
		t.Error("the writer does not roll away from a replica that lost records")
	}

	// A replica that claims more than was ever assigned is no better.
	if state.recordAcknowledgement(2, 4, testStart, generousLimits) {
		t.Error("an acknowledgement of a record that was never assigned was accepted")
	}
}

func TestPlanRequestSendsWhatAReplicaLacks(t *testing.T) {
	state := stateOfThree()
	var frames [][]byte
	for range 4 {
		enqueueOne(t, state, 100)
		frames = append(frames, state.pending[len(state.pending)-1].batch.Frame)
	}
	frameBytes := len(frames[0])

	cases := []struct {
		name         string
		acknowledged uint64
		maxBytes     int
		wantFrames   [][]byte
	}{
		{name: "everything fits", acknowledged: 0, maxBytes: 1 << 20, wantFrames: frames},
		{name: "only what was not acknowledged", acknowledged: 2, maxBytes: 1 << 20, wantFrames: frames[2:]},
		{name: "whole batches up to the limit", acknowledged: 0, maxBytes: 2*frameBytes + 1, wantFrames: frames[:2]},
		{name: "at least one batch, however small the limit", acknowledged: 1, maxBytes: 1, wantFrames: frames[1:2]},
		{name: "nothing: a heartbeat", acknowledged: 4, maxBytes: 1 << 20, wantFrames: nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			state.replicas[2].acknowledgedEnd = testCase.acknowledged

			planned := state.planRequest(2, testCase.maxBytes)

			if want := slices.Concat(testCase.wantFrames...); !slices.Equal(planned, want) {
				t.Errorf("planned %d bytes, want %d bytes (%d batches)", len(planned), len(want), len(testCase.wantFrames))
			}
			if wantMore := testCase.acknowledged < 4; state.hasBatchesFor(2) != wantMore {
				t.Errorf("hasBatchesFor = %v, want %v", state.hasBatchesFor(2), wantMore)
			}
		})
	}
}

func TestBatchesAreKeptUntilEveryReplicaHasThem(t *testing.T) {
	state := stateOfThree()
	for range 3 {
		enqueueOne(t, state, 10)
	}

	state.recordAcknowledgement(0, 3, testStart, generousLimits)
	state.recordAcknowledgement(1, 3, testStart, generousLimits)
	if len(state.pending) != 3 {
		t.Fatalf("%d batches are kept while the third replica has none, want 3", len(state.pending))
	}
	if state.uncommittedBytes() != 0 {
		t.Errorf("%d bytes count as uncommitted although everything is committed", state.uncommittedBytes())
	}

	state.recordAcknowledgement(2, 2, testStart, generousLimits)
	if len(state.pending) != 1 {
		t.Errorf("%d batches are kept after the third replica acknowledged two, want 1", len(state.pending))
	}
	state.recordAcknowledgement(2, 3, testStart, generousLimits)
	if len(state.pending) != 0 {
		t.Errorf("%d batches are kept although every replica has everything", len(state.pending))
	}
}

func TestTheWriterRollsWhenItShould(t *testing.T) {
	limits := rollLimits{
		replicaFailureTimeout: 10 * time.Second,
		maxReplicaLagBytes:    300,
		targetSegmentBytes:    1000,
		targetSegmentAge:      time.Hour,
	}
	cases := []struct {
		name string
		// arrange brings the state to the situation and returns the time.
		arrange    func(t *testing.T, state *writerState) time.Time
		wantReason string
	}{
		{
			name:       "a healthy segment stays",
			arrange:    func(_ *testing.T, _ *writerState) time.Time { return testStart.Add(time.Second) },
			wantReason: "",
		},
		{
			name: "a replica refuses appends",
			arrange: func(_ *testing.T, state *writerState) time.Time {
				state.recordRefusal(1, testStart, limits)
				return testStart
			},
			wantReason: "replica replica-b refuses appends",
		},
		{
			name: "a replica is silent for too long",
			arrange: func(_ *testing.T, state *writerState) time.Time {
				later := testStart.Add(10 * time.Second)
				state.recordAcknowledgement(0, 0, later, limits)
				state.recordAcknowledgement(1, 0, later, limits)
				return later
			},
			wantReason: "replica replica-c does not answer",
		},
		{
			name: "a replica is silent, but not for long enough",
			arrange: func(_ *testing.T, state *writerState) time.Time {
				later := testStart.Add(10*time.Second - time.Millisecond)
				state.recordAcknowledgement(0, 0, later, limits)
				state.recordAcknowledgement(1, 0, later, limits)
				return later
			},
			wantReason: "",
		},
		{
			name: "a replica falls too far behind",
			arrange: func(t *testing.T, state *writerState) time.Time {
				for range 4 {
					enqueueOne(t, state, 100)
				}
				state.recordAcknowledgement(0, 4, testStart, limits)
				state.recordAcknowledgement(1, 4, testStart, limits)
				return testStart
			},
			wantReason: "replica replica-c is too far behind",
		},
		{
			name: "the segment is full",
			arrange: func(t *testing.T, state *writerState) time.Time {
				// Every replica keeps up, so none is behind.
				for sequence := range uint64(10) {
					enqueueOne(t, state, 100)
					for replicaIndex := range 3 {
						state.recordAcknowledgement(replicaIndex, sequence+1, testStart, limits)
					}
				}
				return testStart
			},
			wantReason: "the segment is full",
		},
		{
			name: "the segment is old",
			arrange: func(_ *testing.T, state *writerState) time.Time {
				return keepInContact(state, testStart.Add(time.Hour), limits)
			},
			wantReason: "the segment is old",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			state := stateOfThree()
			now := testCase.arrange(t, state)
			if reason := state.rollReason(now, limits); reason != testCase.wantReason {
				t.Errorf("rollReason = %q, want %q", reason, testCase.wantReason)
			}
		})
	}
}

// keepInContact notes an answer of every replica at now, and returns now.
func keepInContact(state *writerState, now time.Time, limits rollLimits) time.Time {
	for replicaIndex := range state.replicas {
		state.recordAcknowledgement(replicaIndex, state.replicas[replicaIndex].acknowledgedEnd, now, limits)
	}
	return now
}

func TestNothingCommitsWhileARollIsInFlight(t *testing.T) {
	state := stateOfThree()
	committed := enqueueOne(t, state, 10)
	state.recordAcknowledgement(0, 1, testStart, generousLimits)
	state.recordAcknowledgement(1, 1, testStart, generousLimits)
	inFlight := enqueueOne(t, state, 10)

	if sealedEnd := state.beginRoll(); sealedEnd != 1 {
		t.Fatalf("the roll seals at %d, want the durable sequence 1", sealedEnd)
	}
	if reason := state.rollReason(testStart.Add(48*time.Hour), generousLimits); reason != "" {
		t.Errorf("a writer that is rolling wants to roll again: %q", reason)
	}

	// Acknowledgements of the old segment arrive, but the segment's end is
	// decided: record 2 must not commit there.
	state.recordAcknowledgement(0, 2, testStart, generousLimits)
	state.recordAcknowledgement(1, 2, testStart, generousLimits)
	if state.durableSequence != 1 {
		t.Fatalf("durable sequence = %d during a roll, want 1", state.durableSequence)
	}
	if _, isReported := outcomeOf(inFlight); isReported {
		t.Fatal("a record committed while the roll was in flight")
	}
	if err, isReported := outcomeOf(committed); !isReported || err != nil {
		t.Errorf("the record committed before the roll: reported %v with %v", isReported, err)
	}

	// The roll did not happen after all: the acknowledgements count.
	state.abandonRoll(generousLimits)
	if state.durableSequence != 2 {
		t.Errorf("durable sequence = %d after the roll was abandoned, want 2", state.durableSequence)
	}
	if err, isReported := outcomeOf(inFlight); !isReported || err != nil {
		t.Errorf("the record in flight: reported %v with %v, want committed", isReported, err)
	}
}

func TestARollMovesUncommittedRecordsToTheNewSegment(t *testing.T) {
	state := stateOfThree()
	enqueueOne(t, state, 10)
	state.recordAcknowledgement(0, 1, testStart, generousLimits)
	state.recordAcknowledgement(1, 1, testStart, generousLimits)
	_, _, secondOutcome, _ := state.enqueue([][]byte{[]byte("two"), []byte("three")})
	_, _, thirdOutcome, _ := state.enqueue([][]byte{[]byte("four")})
	// Replica a already holds the second batch in the old segment; that
	// does not count for the new one.
	state.recordAcknowledgement(0, 3, testStart, generousLimits)

	sealedEnd := state.beginRoll()
	nextSegment := replica.Descriptor{
		Address:       frame.SegmentAddress{StreamID: 7, SegmentID: 2},
		Epoch:         1,
		FirstSequence: sealedEnd + 1,
	}
	rolledAt := testStart.Add(time.Minute)
	if err := state.completeRoll(nextSegment, []string{"replica-d", "replica-e", "replica-f"}, rolledAt); err != nil {
		t.Fatalf("completeRoll: %v", err)
	}

	if state.generation != 1 || state.segment != nextSegment || state.isRolling {
		t.Errorf("after the roll: generation %d, segment %+v, rolling %v", state.generation, state.segment, state.isRolling)
	}
	if state.durableSequence != 1 || state.nextSequence != 5 {
		t.Errorf("after the roll: durable %d, next %d; want 1 and 5", state.durableSequence, state.nextSequence)
	}
	// Every record above the sealed end is encoded for the new segment,
	// under the sequence number it already had.
	planned := state.planRequest(0, 1<<20)
	batches, err := frame.DecodeAll(nextSegment.Address, planned)
	if err != nil {
		t.Fatalf("the batches for the new segment do not decode under its address: %v", err)
	}
	var payloads []string
	for _, batch := range batches {
		for _, record := range batch.Records() {
			payloads = append(payloads, string(record.Payload))
		}
	}
	if len(batches) != 2 || batches[0].FirstSequence != 2 || batches[1].FirstSequence != 4 ||
		!slices.Equal(payloads, []string{"two", "three", "four"}) {
		t.Errorf("the new segment gets %d batches holding %v; want records two to four in two batches from sequence 2", len(batches), payloads)
	}
	// They commit on the new replicas like any others.
	state.recordAcknowledgement(1, 3, rolledAt, generousLimits)
	if _, isReported := outcomeOf(secondOutcome); isReported {
		t.Error("a record committed on one replica of the new segment")
	}
	state.recordAcknowledgement(2, 4, rolledAt, generousLimits)
	if err, isReported := outcomeOf(secondOutcome); !isReported || err != nil {
		t.Errorf("the second batch: reported %v with %v, want committed", isReported, err)
	}
	if _, isReported := outcomeOf(thirdOutcome); isReported {
		t.Error("the third batch committed on one replica")
	}
	// The size and age limits count from the roll.
	limits := rollLimits{replicaFailureTimeout: time.Hour, maxReplicaLagBytes: 1 << 30, targetSegmentBytes: 1 << 30, targetSegmentAge: time.Hour}
	if reason := state.rollReason(keepInContact(state, rolledAt.Add(59*time.Minute), limits), limits); reason != "" {
		t.Errorf("the new segment is rolled %q although it is younger than the limit", reason)
	}
}

func TestAStoppedWriterFailsWhatIsNotCommitted(t *testing.T) {
	state := stateOfThree()
	committed := enqueueOne(t, state, 10)
	state.recordAcknowledgement(0, 1, testStart, generousLimits)
	state.recordAcknowledgement(1, 1, testStart, generousLimits)
	pending := enqueueOne(t, state, 10)
	reason := errors.New("the writer stopped")

	state.failUncommitted(reason)

	if err, isReported := outcomeOf(committed); !isReported || err != nil {
		t.Errorf("the committed record: reported %v with %v, want committed", isReported, err)
	}
	if err, isReported := outcomeOf(pending); !isReported || !errors.Is(err, reason) {
		t.Errorf("the pending record: reported %v with %v, want the reason the writer stopped", isReported, err)
	}
	if state.uncommittedBytes() != 0 || len(state.pending) != 0 {
		t.Errorf("a stopped writer still holds %d batches and %d uncommitted bytes", len(state.pending), state.uncommittedBytes())
	}
}
