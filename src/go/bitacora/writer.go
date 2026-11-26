package bitacora

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

const (
	// writerMaxRequestBytes is about how many bytes of batch frames one
	// request to a replica carries.
	writerMaxRequestBytes = 256 * 1024
	// writerHeartbeatInterval is how often an idle writer tells each
	// replica its durable sequence, which is also how it notices a fence
	// and a replica that stopped answering.
	writerHeartbeatInterval = 50 * time.Millisecond
	// writerDriverTick is how often the writer looks at whether to renew
	// its lease or roll.
	writerDriverTick = 20 * time.Millisecond
)

// TakeoverPolicy says what opening a writer does about a lease that another
// writer still holds.
type TakeoverPolicy int

const (
	// WaitForLeaseExpiry waits for the previous writer's lease to lapse
	// before taking the stream, so that a healthy writer is not taken over
	// by accident. This is the default.
	WaitForLeaseExpiry TakeoverPolicy = iota
	// ForceTakeover takes the stream at once. The previous writer, if it is
	// still running, is fenced and gets [ErrFenced].
	ForceTakeover
)

// WriterOptions say how a writer behaves. The zero value of every field
// means its default.
type WriterOptions struct {
	// Identity names the writer in the stream's lease, for operators.
	// Default "bitacora-go-writer".
	Identity string
	// Takeover says what to do about a lease another writer still holds.
	Takeover TakeoverPolicy
	// OpenTimeout bounds the takeover, in addition to the context given to
	// OpenWriter. Default 30 seconds.
	OpenTimeout time.Duration
	// MaxUncommittedBytes is how many bytes of records may await commit
	// before Append waits. Default 64 MiB.
	MaxUncommittedBytes uint64
	// MaxReplicaLagBytes is how far behind the durable sequence a replica
	// may fall before the writer rolls away from it. Default 64 MiB.
	MaxReplicaLagBytes uint64
	// ReplicaFailureTimeout is how long a replica may stay silent before
	// the writer rolls away from it. Default 10 seconds.
	ReplicaFailureTimeout time.Duration
	// LeaseDuration is how long the writer's lease lasts without renewal.
	// Default 10 seconds. The lease is renewed every third of it.
	LeaseDuration time.Duration
	// TargetSegmentBytes is the size at which the writer rolls to a new
	// segment. Default 1 GiB.
	TargetSegmentBytes uint64
	// TargetSegmentAge is the age at which the writer rolls to a new
	// segment. Default one hour.
	TargetSegmentAge time.Duration
}

// writerOptions are the options of a writer with every default filled in.
type writerOptions struct {
	identity            string
	takeover            TakeoverPolicy
	openTimeout         time.Duration
	maxUncommittedBytes uint64
	leaseDuration       time.Duration
	retryInterval       time.Duration
	limits              rollLimits
}

// resolve fills in the defaults of the options.
func (options WriterOptions) resolve() writerOptions {
	return writerOptions{
		identity:            cmp.Or(options.Identity, "bitacora-go-writer"),
		takeover:            options.Takeover,
		openTimeout:         cmp.Or(options.OpenTimeout, 30*time.Second),
		maxUncommittedBytes: cmp.Or(options.MaxUncommittedBytes, 64*1024*1024),
		leaseDuration:       cmp.Or(options.LeaseDuration, 10*time.Second),
		retryInterval:       100 * time.Millisecond,
		limits: rollLimits{
			replicaFailureTimeout: cmp.Or(options.ReplicaFailureTimeout, 10*time.Second),
			maxReplicaLagBytes:    cmp.Or(options.MaxReplicaLagBytes, 64*1024*1024),
			targetSegmentBytes:    cmp.Or(options.TargetSegmentBytes, 1024*1024*1024),
			targetSegmentAge:      cmp.Or(options.TargetSegmentAge, time.Hour),
		},
	}
}

// Writer is the one writer of a stream. It assigns sequence numbers, sends
// records to the replicas of the open segment, and reports each record as
// committed once a write quorum of them has flushed it.
//
// A Writer is safe for concurrent use. It runs goroutines until it is
// closed: always call [Writer.Close], also on a writer that reported
// [ErrFenced].
type Writer struct {
	client     *Client
	streamName string
	streamID   uint64
	epoch      uint64
	options    writerOptions

	// mutex guards the four fields below.
	mutex sync.Mutex
	// state is the bookkeeping of records, acknowledgements and commits.
	state *writerState
	// changed is closed, and replaced, whenever state changes, to wake
	// whoever waits for a change.
	changed chan struct{}
	// isClosing is set once Close was called: no more appends are taken and
	// no roll begins.
	isClosing bool
	// stopReason is set once the writer stopped for good.
	stopReason error

	// stopGoroutines cancels the context every goroutine of the writer
	// runs under.
	stopGoroutines context.CancelFunc
	// goroutines counts the driver and the senders.
	goroutines sync.WaitGroup
	// closeOnce makes every call of Close after the first return its
	// result, closeResult.
	closeOnce   sync.Once
	closeResult error
}

// OpenWriter becomes the writer of the stream called streamName.
//
// It runs the takeover protocol: it claims the next epoch, fences and seals
// whatever segment the previous writer left open, and opens a new one.
// bitacora does not choose which process should be the writer; it
// guarantees that whoever calls this last wins cleanly.
//
// The error wraps [ErrStreamNotFound], [ErrLeaseHeld] if a live writer
// holds the lease and the policy is to wait longer than the timeout allows,
// [ErrFenced] if another candidate took the stream first, or
// [ErrUnavailable] if the metadata store or enough replicas of the previous
// segment could not be reached in time. If ctx ends first, the error wraps
// ctx's.
func (client *Client) OpenWriter(ctx context.Context, streamName string, options WriterOptions) (*Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved := options.resolve()
	openCtx, cancel := context.WithTimeout(ctx, resolved.openTimeout)
	defer cancel()
	opened, err := client.takeOver(openCtx, streamName, resolved)
	if err != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			// The caller gave up; say so instead of blaming the cluster.
			return nil, fmt.Errorf("opening a writer of %q was abandoned: %w (%v)", streamName, callerErr, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			// The writer's own timeout ran out, not the caller's context.
			return nil, fmt.Errorf("%w: stream %q could not be opened within %v: %v",
				ErrUnavailable, streamName, resolved.openTimeout, err)
		}
		return nil, err
	}

	descriptor := replica.Descriptor{
		Address:       frame.SegmentAddress{StreamID: opened.stream.StreamID, SegmentID: opened.segment.SegmentID},
		Epoch:         opened.segment.Epoch,
		FirstSequence: opened.segment.FirstSequence,
	}
	// The goroutines outlive this call, so they do not end with ctx.
	runCtx, stopGoroutines := context.WithCancel(context.WithoutCancel(ctx))
	writer := &Writer{
		client:         client,
		streamName:     streamName,
		streamID:       opened.stream.StreamID,
		epoch:          opened.stream.Epoch,
		options:        resolved,
		state:          newWriterState(int(opened.stream.Replication.WriteQuorum), descriptor, replicaIDsOf(opened.replicas), time.Now()),
		changed:        make(chan struct{}),
		stopGoroutines: stopGoroutines,
	}
	writer.startSenders(runCtx, 0, opened.replicas)
	writer.goroutines.Go(func() { writer.drive(runCtx) })
	return writer, nil
}

// replicaIDsOf returns the identities of replicas, in order.
func replicaIDsOf(replicas []*replica.Client) []string {
	replicaIDs := make([]string, 0, len(replicas))
	for _, replicaClient := range replicas {
		replicaIDs = append(replicaIDs, replicaClient.ReplicaID())
	}
	return replicaIDs
}

// Append appends one record and returns its sequence number once the record
// is stored on a write quorum of replicas. It waits for that as long as ctx
// allows: replicas that fail are rolled away from, not given up on.
//
// The error wraps [ErrFenced] if another writer has taken the stream,
// [ErrIndeterminate] if the outcome could not be learned, or [ErrClosed].
// It is [ErrIndeterminate] too when ctx ends while the record awaits
// commit: the record keeps its sequence number, which is returned with the
// error, and may still commit.
func (writer *Writer) Append(ctx context.Context, payload []byte) (uint64, error) {
	_, last, err := writer.AppendBatch(ctx, [][]byte{payload})
	return last, err
}

// AppendBatch appends several records as one batch: they commit together,
// under consecutive sequence numbers, and travel under one checksum. It
// returns the first and the last of those sequence numbers. The errors are
// those of [Writer.Append], and [ErrInvalidArgument] if the batch is empty
// or too large.
func (writer *Writer) AppendBatch(ctx context.Context, payloads [][]byte) (first, last uint64, err error) {
	writer.mutex.Lock()
	// Flow control: wait while too much awaits commit.
	for {
		if writer.stopReason != nil {
			writer.mutex.Unlock()
			return 0, 0, writer.stopReason
		}
		if writer.isClosing {
			writer.mutex.Unlock()
			return 0, 0, ErrClosed
		}
		if writer.state.uncommittedBytes() < writer.options.maxUncommittedBytes {
			break
		}
		changed := writer.changed
		writer.mutex.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			// Nothing was assigned yet, so nothing is in doubt.
			return 0, 0, ctx.Err()
		}
		writer.mutex.Lock()
	}
	first, last, outcome, err := writer.state.enqueue(payloads)
	if err != nil {
		writer.mutex.Unlock()
		return 0, 0, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	writer.announceChange()
	writer.mutex.Unlock()

	select {
	case err := <-outcome:
		return first, last, err
	case <-ctx.Done():
		return first, last, fmt.Errorf("%w: %w", ErrIndeterminate, ctx.Err())
	}
}

// DurableSequence returns the sequence number of the last committed record.
// Until this writer commits one, that is where the stream ended when the
// writer opened it: 0 for a stream that never held a record.
func (writer *Writer) DurableSequence() uint64 {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return writer.state.durableSequence
}

// Epoch returns the epoch under which the writer owns the stream.
func (writer *Writer) Epoch() uint64 { return writer.epoch }

// Close stops taking appends, waits for the accepted ones to commit, seals
// the segment and releases the lease, so that the next writer starts
// without recovery. It stops the writer's goroutines whatever happens.
// Close may be called more than once; every call returns the result of the
// first.
//
// The error wraps [ErrFenced] if another writer took the stream,
// [ErrIndeterminate] if accepted records did not commit in time, or
// [ErrUnavailable] if they did but the segment could not be sealed. In the
// last two cases the next writer recovers the segment as after a crash.
func (writer *Writer) Close(ctx context.Context) error {
	writer.closeOnce.Do(func() { writer.closeResult = writer.close(ctx) })
	return writer.closeResult
}

// close is Close, once.
func (writer *Writer) close(ctx context.Context) error {
	writer.mutex.Lock()
	writer.isClosing = true
	result := writer.stopReason
	writer.mutex.Unlock()

	closeCtx, cancel := context.WithTimeout(ctx, writer.client.requestTimeout)
	defer cancel()
	if result == nil {
		result = writer.waitUntilDrained(closeCtx)
	}
	// The senders stop before the segment is sealed: a heartbeat that met
	// the seal on a replica would look like another writer's takeover.
	writer.stop(cmp.Or(result, ErrClosed))
	writer.goroutines.Wait()
	if result == nil {
		result = writer.sealAndRelease(closeCtx)
	}
	return result
}

// waitUntilDrained waits until every accepted record is committed and no
// roll is in flight.
func (writer *Writer) waitUntilDrained(ctx context.Context) error {
	for {
		writer.mutex.Lock()
		stopReason, changed := writer.stopReason, writer.changed
		hasUncommitted, isRolling := writer.state.hasUncommitted(), writer.state.isRolling
		writer.mutex.Unlock()
		switch {
		case stopReason != nil:
			return stopReason
		case !hasUncommitted && !isRolling:
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			if hasUncommitted {
				return fmt.Errorf("%w: accepted records did not commit before the writer closed", ErrIndeterminate)
			}
			return fmt.Errorf("%w: the segment could not be sealed: a roll did not finish in time", ErrUnavailable)
		}
	}
}

// sealAndRelease seals the open segment at the durable sequence, releases
// the lease and tells the replicas the seal.
func (writer *Writer) sealAndRelease(ctx context.Context) error {
	writer.mutex.Lock()
	segment, sealedEnd := writer.state.segment, writer.state.durableSequence
	writer.mutex.Unlock()

	store := writer.client.metadata
	stream, err := store.SealSegment(ctx, writer.streamName, writer.epoch, segment.Address.SegmentID, sealedEnd, nowMillis())
	if err == nil {
		err = store.ReleaseLease(ctx, writer.streamName, writer.epoch)
	}
	if errors.Is(err, metadata.ErrFenced) {
		return fmt.Errorf("%w: %w", ErrFenced, err)
	}
	if err != nil {
		return fmt.Errorf("%w: the segment could not be sealed: %w", ErrUnavailable, err)
	}
	if sealed := stream.Segment(segment.Address.SegmentID); sealed != nil {
		if replicas, err := writer.client.connectCopies(ctx, sealed.Copies); err == nil {
			writer.client.announceSeal(ctx, replicas, segment.Address, sealedEnd)
		}
	}
	return nil
}

// announceChange wakes whoever waits for the state to change. The caller
// holds the mutex.
func (writer *Writer) announceChange() {
	close(writer.changed)
	writer.changed = make(chan struct{})
}

// stop stops the writer for good, for the given reason, unless it has
// stopped already. Every append whose outcome is not known gets the reason.
func (writer *Writer) stop(reason error) {
	writer.mutex.Lock()
	if writer.stopReason == nil {
		writer.stopReason = reason
		writer.state.failUncommitted(reason)
		writer.announceChange()
	}
	writer.mutex.Unlock()
	writer.stopGoroutines()
}

// startSenders starts one sender per replica of the segment that the given
// generation writes to.
func (writer *Writer) startSenders(ctx context.Context, generation uint64, replicas []*replica.Client) {
	for replicaIndex, replicaClient := range replicas {
		writer.goroutines.Go(func() { writer.send(ctx, generation, replicaIndex, replicaClient) })
	}
}

// drive renews the lease and rolls the segment when it is time to.
func (writer *Writer) drive(ctx context.Context) {
	leaseRenewalInterval := writer.options.leaseDuration / 3
	renewLeaseAt := time.Now().Add(leaseRenewalInterval)
	mayRollAt := time.Now()
	ticker := time.NewTicker(writerDriverTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		if now.After(renewLeaseAt) {
			writer.renewLease(ctx)
			renewLeaseAt = time.Now().Add(leaseRenewalInterval)
		}
		writer.mutex.Lock()
		rollReason := writer.state.rollReason(now, writer.options.limits)
		writer.mutex.Unlock()
		if rollReason != "" && !now.Before(mayRollAt) && !writer.roll(ctx) {
			mayRollAt = time.Now().Add(writer.options.retryInterval)
		}
	}
}

// renewLease moves the expiry of the writer's lease. If it cannot be
// renewed the writer keeps going: the lease is a courtesy, and only the
// epoch decides who may write.
func (writer *Writer) renewLease(ctx context.Context) {
	expiresAt := nowMillis() + uint64(writer.options.leaseDuration.Milliseconds())
	err := writer.client.metadata.RenewLease(ctx, writer.streamName, writer.epoch, expiresAt)
	if errors.Is(err, metadata.ErrFenced) {
		writer.stop(fmt.Errorf("%w: %w", ErrFenced, err))
	}
}

// roll seals the open segment at the durable sequence and continues in a
// new one on the standby replica set (docs/protocol/replication.md,
// "Rolling"). It returns false if the roll did not happen; the driver then
// tries again a little later.
func (writer *Writer) roll(ctx context.Context) bool {
	store := writer.client.metadata
	// Read first, while still committing: if the store is down or no
	// standby is placed, nothing is frozen and appends continue.
	before, err := store.GetStream(ctx, writer.streamName)
	if err != nil {
		return false
	}
	if before.Epoch != writer.epoch {
		writer.stop(fmt.Errorf("%w: the stream is at epoch %d, this writer holds epoch %d", ErrFenced, before.Epoch, writer.epoch))
		return false
	}
	if before.StandbyReplicas == nil {
		return false
	}

	// Step 1: stop committing on the current segment. Close and a roll
	// exclude each other here: whichever takes the mutex first goes ahead.
	writer.mutex.Lock()
	if writer.isClosing || writer.stopReason != nil {
		writer.mutex.Unlock()
		return false
	}
	oldSegment := writer.state.segment
	sealedEnd := writer.state.beginRoll()
	writer.mutex.Unlock()

	// Step 2: one conditional write seals this segment and opens the next.
	// It is repeated until its outcome is known, because until then nothing
	// may commit on either segment.
	var stream metadata.StreamRecord
	var opened metadata.SegmentRecord
	for {
		stream, opened, err = store.RollSegment(ctx, writer.streamName, writer.epoch, oldSegment.Address.SegmentID, sealedEnd, nowMillis())
		if !errors.Is(err, metadata.ErrUnavailable) {
			break
		}
		if ctx.Err() != nil {
			return false
		}
		pause(ctx, writer.options.retryInterval)
	}
	if errors.Is(err, metadata.ErrFenced) {
		writer.stop(fmt.Errorf("%w: %w", ErrFenced, err))
		return false
	}
	if err != nil {
		// The write was refused for a reason that leaves the stream as it
		// was, such as a standby that was withdrawn: go on as before.
		writer.mutex.Lock()
		writer.state.abandonRoll(writer.options.limits)
		writer.announceChange()
		writer.mutex.Unlock()
		return false
	}

	// The new replica set must be reachable before anything can commit, so
	// its addresses are looked up until the registry answers.
	var newReplicas []*replica.Client
	for {
		newReplicas, err = writer.client.connectCopies(ctx, opened.Copies)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return false
		}
		pause(ctx, writer.options.retryInterval)
	}

	// Step 4: every record above the sealed end goes to the new segment,
	// under the same sequence number.
	descriptor := replica.Descriptor{
		Address:       frame.SegmentAddress{StreamID: writer.streamID, SegmentID: opened.SegmentID},
		Epoch:         opened.Epoch,
		FirstSequence: opened.FirstSequence,
	}
	writer.mutex.Lock()
	err = writer.state.completeRoll(descriptor, replicaIDsOf(newReplicas), time.Now())
	generation := writer.state.generation
	writer.announceChange()
	writer.mutex.Unlock()
	if err != nil {
		writer.stop(fmt.Errorf("%w: records could not be encoded for the new segment: %w", ErrIndeterminate, err))
		return false
	}
	writer.startSenders(ctx, generation, newReplicas)

	// Step 3: tell the old replicas the sealed end. Whoever misses it is
	// told by the controller.
	if sealed := stream.Segment(oldSegment.Address.SegmentID); sealed != nil {
		if oldReplicas, err := writer.client.connectCopies(ctx, sealed.Copies); err == nil {
			writer.client.announceSeal(ctx, oldReplicas, oldSegment.Address, sealedEnd)
		}
	}
	return true
}

// send keeps one replica of one generation's segment up to date: it sends
// the batches the replica has not acknowledged, and a heartbeat with the
// durable sequence when there has been nothing to send for a while. One
// request is in flight per replica at a time.
func (writer *Writer) send(ctx context.Context, generation uint64, replicaIndex int, replicaClient *replica.Client) {
	// The first request goes out at once, with or without batches: it
	// creates the replica's copy and shows that the replica is there.
	heartbeatDueAt := time.Now()
	for {
		writer.mutex.Lock()
		if writer.stopReason != nil || writer.state.generation != generation {
			writer.mutex.Unlock()
			return
		}
		if !writer.state.hasBatchesFor(replicaIndex) && time.Now().Before(heartbeatDueAt) {
			changed := writer.changed
			writer.mutex.Unlock()
			timer := time.NewTimer(time.Until(heartbeatDueAt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-changed:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		frames := writer.state.planRequest(replicaIndex, writerMaxRequestBytes)
		segment, durableSequence := writer.state.segment, writer.state.durableSequence
		acknowledgedBefore := writer.state.replicas[replicaIndex].acknowledgedEnd
		writer.mutex.Unlock()

		requestCtx, cancel := context.WithTimeout(ctx, writer.client.requestTimeout)
		localEnd, err := replicaClient.Append(requestCtx, segment, writer.epoch, frames, durableSequence)
		cancel()
		heartbeatDueAt = time.Now().Add(writerHeartbeatInterval)
		if ctx.Err() != nil {
			return
		}

		writer.mutex.Lock()
		if writer.state.generation != generation {
			// The writer rolled while the request was in flight; its answer
			// is about a segment that is sealed now.
			writer.mutex.Unlock()
			return
		}
		mustPause := false
		replicaError, _ := errors.AsType[*replica.Error](err)
		switch {
		case err == nil:
			mustPause = !writer.state.recordAcknowledgement(replicaIndex, localEnd, time.Now(), writer.options.limits)
			writer.announceChange()
		case replicaError == nil:
			// Not a replica's answer at all; treated like no answer.
			mustPause = true
		case replicaError.Kind == replica.OutOfOrder:
			// The replica's copy ends elsewhere than assumed, typically
			// after a request whose answer was lost. Resume from there, and
			// do not spin if that changes nothing.
			isAccepted := writer.state.recordAcknowledgement(replicaIndex, replicaError.LocalEnd, time.Now(), writer.options.limits)
			mustPause = !isAccepted || replicaError.LocalEnd <= acknowledgedBefore
			writer.announceChange()
		case replicaError.Kind == replica.Sealed && writer.state.isRolling:
			// This writer's own roll sealed the segment, and the controller
			// told the replica before the roll finished here.
			mustPause = true
		case replicaError.Kind == replica.Fenced || replicaError.Kind == replica.Sealed:
			// A fence, or a seal this writer did not make, on the open
			// segment can only come from a newer writer's takeover.
			writer.mutex.Unlock()
			writer.stop(fmt.Errorf("%w: %w", ErrFenced, err))
			return
		case replicaError.Kind == replica.Unavailable:
			// No answer. The driver rolls if this lasts.
			mustPause = true
		default:
			// A lasting refusal: the replica is full, draining or damaged.
			// The driver rolls away from it.
			writer.state.recordRefusal(replicaIndex, time.Now(), writer.options.limits)
			writer.announceChange()
			mustPause = true
		}
		writer.mutex.Unlock()
		if mustPause {
			pause(ctx, writer.options.retryInterval)
		}
	}
}
