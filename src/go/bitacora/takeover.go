package bitacora

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

// stragglerGrace is how long a recovering writer waits for more answers
// once it has the quorum it needs. Every extra answer can only make the
// recovery end higher, which keeps more of the old writer's tail.
const stragglerGrace = 100 * time.Millisecond

// openedStream is a stream this writer now owns, with its open segment.
type openedStream struct {
	// stream is the stream's record after the segment was opened.
	stream metadata.StreamRecord
	// segment is the open segment that only this writer appends to.
	segment metadata.SegmentRecord
	// replicas are the replicas of that segment, in the order of its copies.
	replicas []*replica.Client
}

// takeOver runs the takeover protocol of docs/protocol/replication.md
// ("Taking over a stream") and returns the stream with a fresh open segment.
// ctx bounds the whole takeover.
func (client *Client) takeOver(ctx context.Context, streamName string, options writerOptions) (openedStream, error) {
	streamBefore, err := client.waitForLease(ctx, streamName, options.takeover)
	if err != nil {
		return openedStream{}, err
	}

	// Step 1: claim the epoch. If the swap fails, someone else is ahead.
	leaseExpiresAt := nowMillis() + uint64(options.leaseDuration.Milliseconds())
	stream, err := client.metadata.ClaimEpoch(ctx, streamName, streamBefore.Epoch, options.identity, leaseExpiresAt)
	if err != nil {
		return openedStream{}, fromMetadata(err)
	}
	opened, err := client.recoverAndOpen(ctx, stream, options)
	if err != nil && !errors.Is(err, ErrFenced) {
		// The stream is claimed but has no writer. Give the lease back, so
		// that the next candidate does not wait for it to lapse. This is a
		// courtesy like the lease itself: if it fails, the lease lapses.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), client.requestTimeout)
		defer cancel()
		_ = client.metadata.ReleaseLease(releaseCtx, streamName, stream.Epoch)
	}
	return opened, err
}

// recoverAndOpen is steps 2 to 8 of the takeover, for a stream whose epoch
// this writer has just claimed.
func (client *Client) recoverAndOpen(ctx context.Context, claimed metadata.StreamRecord, options writerOptions) (openedStream, error) {
	epoch := claimed.Epoch

	// Step 2: the record the claim returned reflects every roll the old
	// writer completed before it lost the epoch. Steps 3 to 7 apply only
	// if its last segment was left open.
	if open := claimed.OpenSegment(); open != nil {
		if err := client.recoverSegment(ctx, claimed, *open, epoch, options); err != nil {
			return openedStream{}, err
		}
	}

	// Step 8: open the next segment on the standby replica set. If none is
	// placed yet, the controller places one; wait for it.
	for {
		stream, segment, err := client.metadata.OpenSegment(ctx, claimed.Name, epoch)
		if err == nil {
			replicas, err := client.connectCopies(ctx, segment.Copies)
			if err != nil {
				return openedStream{}, fromMetadata(err)
			}
			return openedStream{stream: stream, segment: segment, replicas: replicas}, nil
		}
		if !errors.Is(err, metadata.ErrNoStandby) {
			return openedStream{}, fromMetadata(err)
		}
		if ctx.Err() != nil {
			return openedStream{}, fmt.Errorf("%w: no standby segment was placed for stream %q in time; is a controller running?",
				ErrUnavailable, claimed.Name)
		}
		pause(ctx, options.retryInterval)
	}
}

// waitForLease waits for the previous writer's lease to lapse, unless the
// policy is to force the takeover. The lease is a courtesy: correctness
// depends only on the epoch.
func (client *Client) waitForLease(ctx context.Context, streamName string, policy TakeoverPolicy) (metadata.StreamRecord, error) {
	for {
		stream, err := client.metadata.GetStream(ctx, streamName)
		if err != nil {
			return metadata.StreamRecord{}, fromMetadata(err)
		}
		now := nowMillis()
		isLeaseLive := stream.Writer != nil && stream.Writer.ExpiresAt > now
		if policy == ForceTakeover || !isLeaseLive {
			return stream, nil
		}
		lapsesIn := time.Duration(stream.Writer.ExpiresAt-now) * time.Millisecond
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline && time.Now().Add(lapsesIn).After(deadline) {
			return metadata.StreamRecord{}, fmt.Errorf("%w: writer %q holds stream %q for another %v",
				ErrLeaseHeld, stream.Writer.WriterIdentity, streamName, lapsesIn)
		}
		pause(ctx, lapsesIn)
		if err := ctx.Err(); err != nil {
			return metadata.StreamRecord{}, err
		}
	}
}

// responder is one replica's answer to a fence.
type responder struct {
	// replica is the replica that answered.
	replica *replica.Client
	// localEnd is the end of the replica's copy: the position before the
	// segment's first record if it has no copy.
	localEnd uint64
	// sealedEnd is the segment's sealed end, if the replica knows one.
	sealedEnd *uint64
}

// recoverSegment is steps 3 to 7: fence the previous writer's segment, find
// its end, repair the tail, and seal it.
func (client *Client) recoverSegment(ctx context.Context, stream metadata.StreamRecord, segment metadata.SegmentRecord, epoch uint64, options writerOptions) error {
	address := frame.SegmentAddress{StreamID: stream.StreamID, SegmentID: segment.SegmentID}
	replicas, err := client.connectCopies(ctx, segment.Copies)
	if err != nil {
		return fromMetadata(err)
	}
	// N - W + 1 of the segment's own replica set.
	needed := max(len(replicas)-int(stream.Replication.WriteQuorum), 0) + 1
	emptyEnd := segment.FirstSequence - 1

	var recoveryEnd uint64
	for {
		// Steps 3 and 4: fence, and wait for N - W + 1 answers. Any
		// committed record is on at least W replicas, so at least one
		// responder holds it.
		responders, err := client.fenceReplicas(ctx, replicas, address, epoch, needed, emptyEnd)
		if err != nil {
			return err
		}
		whyNot := fmt.Sprintf("only %d of the %d replicas needed answered the fence", len(responders), needed)
		if len(responders) >= needed {
			// Step 5: the recovery end is the highest local end among the
			// responders, unless one of them already knows the segment's
			// sealed end from an earlier, interrupted recovery.
			recoveryEnd = emptyEnd
			var knownSealedEnd *uint64
			for _, answered := range responders {
				recoveryEnd = max(recoveryEnd, answered.localEnd)
				if answered.sealedEnd != nil && knownSealedEnd == nil {
					knownSealedEnd = answered.sealedEnd
				}
			}
			if knownSealedEnd != nil {
				recoveryEnd = *knownSealedEnd
			}
			// Step 6: every responder must hold the segment up to the
			// recovery end before it is sealed there.
			err := client.repairTail(ctx, stream, segment, responders, epoch, recoveryEnd)
			if err == nil {
				break
			}
			whyNot = "the tail could not be repaired: " + err.Error()
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: segment %s of the previous writer could not be recovered in time: %s",
				ErrUnavailable, address, whyNot)
		}
		pause(ctx, options.retryInterval)
	}

	// Step 7: seal with one conditional write, then tell the replicas.
	if _, err := client.metadata.SealSegment(ctx, stream.Name, epoch, segment.SegmentID, recoveryEnd, nowMillis()); err != nil {
		return fromMetadata(err)
	}
	client.announceSeal(ctx, replicas, address, recoveryEnd)
	return nil
}

// fenceReplicas sends the fence to every replica at once and collects the
// answers until needed have arrived plus a short grace for the rest. It
// fails with [ErrFenced] if a replica is already fenced at a higher epoch:
// another writer is ahead.
func (client *Client) fenceReplicas(ctx context.Context, replicas []*replica.Client, address frame.SegmentAddress, epoch uint64, needed int, emptyEnd uint64) ([]responder, error) {
	type fenceAnswer struct {
		replica *replica.Client
		result  replica.FenceResult
		err     error
	}
	requestCtx, cancel := context.WithTimeout(ctx, client.requestTimeout)
	// The channel holds every answer, so no request waits to hand its in.
	answers := make(chan fenceAnswer, len(replicas))
	var requests sync.WaitGroup
	// Requests that have not answered when this returns are cancelled and
	// waited for: nothing of a takeover outlives it.
	defer func() {
		cancel()
		requests.Wait()
	}()
	for _, replicaClient := range replicas {
		requests.Go(func() {
			result, err := replicaClient.Fence(requestCtx, address, epoch)
			answers <- fenceAnswer{replica: replicaClient, result: result, err: err}
		})
	}

	var responders []responder
	var graceEnds <-chan time.Time
	for range replicas {
		var answer fenceAnswer
		select {
		case answer = <-answers:
		case <-graceEnds:
			return responders, nil
		}
		if replicaError, isReplicaError := errors.AsType[*replica.Error](answer.err); isReplicaError && replicaError.Kind == replica.Fenced {
			return nil, fmt.Errorf("%w: segment %s is fenced at epoch %d", ErrFenced, address, replicaError.FencedEpoch)
		}
		if answer.err != nil {
			continue
		}
		localEnd := emptyEnd
		if answer.result.LocalEnd != nil {
			localEnd = *answer.result.LocalEnd
		}
		responders = append(responders, responder{replica: answer.replica, localEnd: localEnd, sealedEnd: answer.result.SealedEnd})
		if len(responders) >= needed && graceEnds == nil {
			graceEnds = time.After(stragglerGrace)
		}
	}
	return responders, nil
}

// repairTail copies missing records from the longest responder to the
// others, until every responder holds the segment up to recoveryEnd.
func (client *Client) repairTail(ctx context.Context, stream metadata.StreamRecord, segment metadata.SegmentRecord, responders []responder, epoch uint64, recoveryEnd uint64) error {
	var longest *responder
	for index := range responders {
		if responders[index].localEnd >= recoveryEnd {
			longest = &responders[index]
			break
		}
	}
	if longest == nil {
		// Only possible when the end came from a replica that knows the
		// seal but lost the tail: the seal already happened, and repairing
		// sealed segments is the controller's job.
		return nil
	}
	address := frame.SegmentAddress{StreamID: stream.StreamID, SegmentID: segment.SegmentID}
	descriptor := replica.Descriptor{Address: address, Epoch: segment.Epoch, FirstSequence: segment.FirstSequence}
	for _, short := range responders {
		copiedEnd := short.localEnd
		for copiedEnd < recoveryEnd {
			requestCtx, cancel := context.WithTimeout(ctx, client.requestTimeout)
			// The recovering writer may read past the durable sequence,
			// which nobody knows any more, up to the end of the copy.
			read, err := longest.replica.Read(requestCtx, address, copiedEnd+1, writerMaxRequestBytes, 0, &epoch)
			if err == nil && len(read.Frames) == 0 {
				err = fmt.Errorf("replica %s no longer holds records after %d", longest.replica.ReplicaID(), copiedEnd)
			}
			var appendedEnd uint64
			if err == nil {
				// The durable sequence is unknown to the recovering writer;
				// the seal that follows tells the replica what it may serve.
				appendedEnd, err = short.replica.Append(requestCtx, descriptor, epoch, read.Frames, 0)
			}
			cancel()
			if err != nil {
				return err
			}
			if appendedEnd <= copiedEnd {
				return fmt.Errorf("replica %s made no progress past %d", short.replica.ReplicaID(), copiedEnd)
			}
			copiedEnd = appendedEnd
		}
	}
	return nil
}

// announceSeal tells every replica of a segment its sealed end, waiting
// only briefly. A replica that misses it learns it from the controller.
func (client *Client) announceSeal(ctx context.Context, replicas []*replica.Client, address frame.SegmentAddress, sealedEnd uint64) {
	requestCtx, cancel := context.WithTimeout(ctx, stragglerGrace)
	defer cancel()
	var announcements sync.WaitGroup
	for _, replicaClient := range replicas {
		announcements.Go(func() {
			// Best effort: the controller tells whoever did not hear.
			_ = replicaClient.Seal(requestCtx, address, sealedEnd)
		})
	}
	announcements.Wait()
}
