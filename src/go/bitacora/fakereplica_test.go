package bitacora

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/wire"
)

// fakeReplica is a replica in memory that follows the rules of
// docs/protocol/replication.md ("A replica") closely enough to test the
// client against: it fences, seals, refuses gaps, serves what is committed,
// and can be told to fail in the ways a real one does. The interoperability
// tests check the client against the real daemon; this one makes the same
// behaviour testable in milliseconds and lets a test break things at will.
type fakeReplica struct {
	wire.UnimplementedReplicaDataServer
	// replicaID is the identity the replica answers to.
	replicaID string
	// address is the "host:port" the replica listens on.
	address string
	// server is the gRPC server, stopped when the test ends or by stop.
	server *grpc.Server

	mutex sync.Mutex
	// segments are the copies the replica holds.
	segments map[frame.SegmentAddress]*fakeSegment
	// changed is closed, and replaced, when a copy changes, to wake reads
	// that wait for records.
	changed chan struct{}
	// appendRefusal, if set, is what every append is refused with.
	appendRefusal *wire.ReplicaError
	// isCorrupting makes every read flip one bit of what it returns.
	isCorrupting bool
	// isSilent makes every append hang until its caller gives up.
	isSilent bool
	// appendCount counts the appends that carried batches.
	appendCount int
}

// fakeSegment is one copy of a segment.
type fakeSegment struct {
	// epoch is the epoch of the writer that authored the segment.
	epoch uint64
	// firstSequence is the sequence number of the segment's first record.
	firstSequence uint64
	// batches are the stored batches, in order.
	batches []frame.Batch
	// localEnd is the last sequence number stored.
	localEnd uint64
	// durableSequence is the last sequence number known to be committed.
	durableSequence uint64
	// fencedEpoch is the lowest writer epoch the copy accepts.
	fencedEpoch uint64
	// sealedEnd is the segment's final sequence number, once known.
	sealedEnd *uint64
}

// hasCopy reports whether the replica holds a copy, and not only the memory
// of a fence. Sequence numbers start at 1, so a copy never starts at 0.
func (segment *fakeSegment) hasCopy() bool { return segment.firstSequence != 0 }

// startFakeReplica starts a replica on a loopback port for the length of
// the test.
func startFakeReplica(t testing.TB, replicaID string) *fakeReplica {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &fakeReplica{
		replicaID: replicaID,
		address:   listener.Addr().String(),
		server:    grpc.NewServer(),
		segments:  make(map[frame.SegmentAddress]*fakeSegment),
		changed:   make(chan struct{}),
	}
	wire.RegisterReplicaDataServer(fake.server, fake)
	go func() { _ = fake.server.Serve(listener) }()
	t.Cleanup(fake.server.Stop)
	return fake
}

// stop makes the replica unreachable, as a crash would.
func (fake *fakeReplica) stop() { fake.server.Stop() }

// refuseAppends makes every append fail with refusal; nil accepts again.
func (fake *fakeReplica) refuseAppends(refusal *wire.ReplicaError) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.appendRefusal = refusal
}

// corruptReads makes every read return frames with one bit flipped.
func (fake *fakeReplica) corruptReads() {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.isCorrupting = true
}

// goSilent makes every append hang until its caller gives up, like a
// replica behind a network that drops packets.
func (fake *fakeReplica) goSilent() {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	fake.isSilent = true
}

// localEndOf returns the last sequence number the replica holds of a
// segment, and whether it holds the segment at all.
func (fake *fakeReplica) localEndOf(address frame.SegmentAddress) (uint64, bool) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	segment, isKnown := fake.segments[address]
	if !isKnown || !segment.hasCopy() {
		return 0, false
	}
	return segment.localEnd, true
}

// sealedEndOf returns the sealed end the replica knows for a segment.
func (fake *fakeReplica) sealedEndOf(address frame.SegmentAddress) *uint64 {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	if segment, isKnown := fake.segments[address]; isKnown {
		return segment.sealedEnd
	}
	return nil
}

// announceChange wakes the reads that wait. The caller holds the mutex.
func (fake *fakeReplica) announceChange() {
	close(fake.changed)
	fake.changed = make(chan struct{})
}

// checkIdentity refuses a request that is not addressed to this replica,
// the way docs/protocol/wire.md says.
func (fake *fakeReplica) checkIdentity(ctx context.Context) error {
	incoming, _ := grpcmetadata.FromIncomingContext(ctx)
	if identities := incoming.Get("bitacora-replica-id"); len(identities) != 1 || identities[0] != fake.replicaID {
		return status.Errorf(codes.FailedPrecondition, "this is replica %s, the request is for %v", fake.replicaID, identities)
	}
	return nil
}

func addressOf(address *wire.SegmentAddress) frame.SegmentAddress {
	return frame.SegmentAddress{StreamID: address.GetStreamId(), SegmentID: address.GetSegmentId()}
}

func (fake *fakeReplica) Append(ctx context.Context, request *wire.AppendRequest) (*wire.AppendResponse, error) {
	if err := fake.checkIdentity(ctx); err != nil {
		return nil, err
	}
	refused := func(refusal *wire.ReplicaError) (*wire.AppendResponse, error) {
		return &wire.AppendResponse{Outcome: &wire.AppendResponse_Error{Error: refusal}}, nil
	}
	fake.mutex.Lock()
	if fake.isSilent {
		fake.mutex.Unlock()
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	defer fake.mutex.Unlock()
	if fake.appendRefusal != nil {
		return refused(fake.appendRefusal)
	}
	address := addressOf(request.GetSegment().GetAddress())
	segment, isKnown := fake.segments[address]
	if !isKnown {
		segment = &fakeSegment{}
		fake.segments[address] = segment
	}
	if request.GetWriterEpoch() < segment.fencedEpoch {
		return refused(&wire.ReplicaError{Kind: &wire.ReplicaError_Fenced_{Fenced: &wire.ReplicaError_Fenced{
			RequestEpoch: request.GetWriterEpoch(), FencedEpoch: segment.fencedEpoch,
		}}})
	}
	if segment.sealedEnd != nil {
		return refused(&wire.ReplicaError{Kind: &wire.ReplicaError_Sealed_{Sealed: &wire.ReplicaError_Sealed{
			SealedEnd: *segment.sealedEnd,
		}}})
	}
	if !segment.hasCopy() {
		// The first append creates the copy from the descriptor.
		segment.epoch = request.GetSegment().GetEpoch()
		segment.firstSequence = request.GetSegment().GetFirstSequence()
		segment.localEnd = segment.firstSequence - 1
	}
	batches, err := frame.DecodeAll(address, request.GetBatchFrames())
	if err != nil {
		return refused(&wire.ReplicaError{Kind: &wire.ReplicaError_InvalidRequest_{InvalidRequest: &wire.ReplicaError_InvalidRequest{
			Reason: err.Error(),
		}}})
	}
	if len(batches) > 0 {
		fake.appendCount++
	}
	for _, batch := range batches {
		if batch.LastSequence() <= segment.localEnd {
			// A batch the copy already holds: a resend.
			continue
		}
		if batch.FirstSequence != segment.localEnd+1 {
			return refused(&wire.ReplicaError{Kind: &wire.ReplicaError_OutOfOrder_{OutOfOrder: &wire.ReplicaError_OutOfOrder{
				FirstSequence: batch.FirstSequence, LocalEnd: segment.localEnd,
			}}})
		}
		// The frame aliases the request, which gRPC may reuse.
		batch.Frame = append([]byte(nil), batch.Frame...)
		segment.batches = append(segment.batches, batch)
		segment.localEnd = batch.LastSequence()
	}
	// A replica never serves past what it holds, whatever the writer says.
	segment.durableSequence = max(segment.durableSequence, min(request.GetDurableSequence(), segment.localEnd))
	fake.announceChange()
	return &wire.AppendResponse{Outcome: &wire.AppendResponse_Success_{
		Success: &wire.AppendResponse_Success{LocalEnd: segment.localEnd},
	}}, nil
}

func (fake *fakeReplica) Read(ctx context.Context, request *wire.ReadRequest) (*wire.ReadResponse, error) {
	if err := fake.checkIdentity(ctx); err != nil {
		return nil, err
	}
	address := addressOf(request.GetSegment())
	waitEnds := time.After(time.Duration(request.GetWaitForRecordsNanoseconds()))
	for {
		fake.mutex.Lock()
		segment, isKnown := fake.segments[address]
		if !isKnown || !segment.hasCopy() {
			fake.mutex.Unlock()
			return &wire.ReadResponse{Outcome: &wire.ReadResponse_Error{Error: &wire.ReplicaError{
				Kind: &wire.ReplicaError_SegmentNotFound_{SegmentNotFound: &wire.ReplicaError_SegmentNotFound{}},
			}}}, nil
		}
		readableEnd := segment.durableSequence
		switch {
		case segment.sealedEnd != nil:
			readableEnd = *segment.sealedEnd
		case request.RecoveryEpoch != nil && request.GetRecoveryEpoch() >= segment.fencedEpoch:
			// The recovering writer reads to the end of the copy.
			readableEnd = segment.localEnd
		}
		var frames []byte
		for _, batch := range segment.batches {
			if batch.LastSequence() < request.GetFromSequence() || batch.LastSequence() > readableEnd {
				continue
			}
			if len(frames) > 0 && uint64(len(frames)+len(batch.Frame)) > request.GetMaxBytes() {
				break
			}
			frames = append(frames, batch.Frame...)
		}
		if len(frames) > 0 && fake.isCorrupting {
			frames[len(frames)-1] ^= 0x01
		}
		response := &wire.ReadResponse{Outcome: &wire.ReadResponse_Success_{Success: &wire.ReadResponse_Success{
			BatchFrames: frames, ReadableEnd: readableEnd, SealedEnd: segment.sealedEnd,
		}}}
		changed := fake.changed
		mustAnswerNow := len(frames) > 0 || segment.sealedEnd != nil || request.WaitForRecordsNanoseconds == nil
		fake.mutex.Unlock()
		if mustAnswerNow {
			return response, nil
		}
		select {
		case <-changed:
		case <-waitEnds:
			return response, nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (fake *fakeReplica) Fence(ctx context.Context, request *wire.FenceRequest) (*wire.FenceResponse, error) {
	if err := fake.checkIdentity(ctx); err != nil {
		return nil, err
	}
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	address := addressOf(request.GetSegment())
	segment, isKnown := fake.segments[address]
	if !isKnown {
		// The fence is remembered although there is no copy, so that a late
		// append of the old writer cannot create one.
		segment = &fakeSegment{}
		fake.segments[address] = segment
	}
	if request.GetEpoch() < segment.fencedEpoch {
		return &wire.FenceResponse{Outcome: &wire.FenceResponse_Error{Error: &wire.ReplicaError{
			Kind: &wire.ReplicaError_Fenced_{Fenced: &wire.ReplicaError_Fenced{
				RequestEpoch: request.GetEpoch(), FencedEpoch: segment.fencedEpoch,
			}},
		}}}, nil
	}
	segment.fencedEpoch = request.GetEpoch()
	success := &wire.FenceResponse_Success{SealedEnd: segment.sealedEnd}
	if segment.hasCopy() {
		success.LocalEnd = new(segment.localEnd)
	}
	return &wire.FenceResponse{Outcome: &wire.FenceResponse_Success_{Success: success}}, nil
}

func (fake *fakeReplica) Seal(ctx context.Context, request *wire.SealRequest) (*wire.SealResponse, error) {
	if err := fake.checkIdentity(ctx); err != nil {
		return nil, err
	}
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	address := addressOf(request.GetSegment())
	segment, isKnown := fake.segments[address]
	if !isKnown || !segment.hasCopy() {
		return &wire.SealResponse{Outcome: &wire.SealResponse_Error{Error: &wire.ReplicaError{
			Kind: &wire.ReplicaError_SegmentNotFound_{SegmentNotFound: &wire.ReplicaError_SegmentNotFound{}},
		}}}, nil
	}
	if segment.sealedEnd != nil && *segment.sealedEnd != request.GetSealedEnd() {
		return &wire.SealResponse{Outcome: &wire.SealResponse_Error{Error: &wire.ReplicaError{
			Kind: &wire.ReplicaError_ConflictingSeal_{ConflictingSeal: &wire.ReplicaError_ConflictingSeal{
				ExistingSealedEnd: *segment.sealedEnd, RequestedSealedEnd: request.GetSealedEnd(),
			}},
		}}}, nil
	}
	// Whatever the copy holds past the sealed end was never committed.
	keptBatches := segment.batches[:0]
	for _, batch := range segment.batches {
		if batch.LastSequence() <= request.GetSealedEnd() {
			keptBatches = append(keptBatches, batch)
		}
	}
	segment.batches = keptBatches
	segment.localEnd = min(segment.localEnd, request.GetSealedEnd())
	segment.sealedEnd = new(request.GetSealedEnd())
	fake.announceChange()
	return &wire.SealResponse{Outcome: &wire.SealResponse_Success_{Success: &wire.SealResponse_Success{}}}, nil
}
