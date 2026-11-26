// Package replica talks to replicas over gRPC as docs/protocol/wire.md
// says: every request names the replica it is meant for in the
// "bitacora-replica-id" header, a refusal comes back inside the response as
// a typed error, and anything else that goes wrong means that no answer
// arrived, which is reported as unavailable.
package replica

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/wire"
)

const (
	// identityHeader carries the identity of the replica a request is for.
	identityHeader = "bitacora-replica-id"
	// maxMessageBytes is the largest message either side accepts.
	maxMessageBytes = 16 * 1024 * 1024
	// reconnectDelayLimit bounds how long the transport waits before it
	// tries a replica again, so that a restarted replica is found quickly.
	reconnectDelayLimit = 2 * time.Second
)

// Kind says why a replica refused a request, or that it could not be
// reached.
type Kind int

const (
	// Fenced: a newer writer has fenced the segment.
	Fenced Kind = iota + 1
	// Sealed: the segment is sealed and accepts no more appends.
	Sealed
	// OutOfOrder: the batches leave a gap after the replica's copy; resend
	// from LocalEnd + 1.
	OutOfOrder
	// ConflictingSeal: the replica knows another sealed end.
	ConflictingSeal
	// SegmentNotFound: the replica has no copy of the segment.
	SegmentNotFound
	// InvalidRequest: the request itself is wrong; retrying cannot help.
	InvalidRequest
	// NoCapacity: the replica's disk is too full.
	NoCapacity
	// Draining: the replica is being drained for maintenance.
	Draining
	// Storage: the replica's copy is damaged or its disk failed.
	Storage
	// Unavailable: no answer arrived. The request may or may not have been
	// applied.
	Unavailable
)

// Error is a refusal by a replica, or the absence of an answer.
type Error struct {
	// Kind says what happened.
	Kind Kind
	// Reason is the replica's or the transport's explanation.
	Reason string
	// FencedEpoch is the lowest epoch the replica accepts, for [Fenced].
	FencedEpoch uint64
	// LocalEnd is the last sequence number the replica holds, for
	// [OutOfOrder].
	LocalEnd uint64
}

// Error implements error.
func (replicaError *Error) Error() string { return replicaError.Reason }

// KindOf returns the kind of a replica error, or 0 if err is not one.
func KindOf(err error) Kind {
	if replicaError, isReplicaError := errors.AsType[*Error](err); isReplicaError {
		return replicaError.Kind
	}
	return 0
}

// Descriptor is what a replica needs to create its copy of a segment.
type Descriptor struct {
	// Address identifies the segment.
	Address frame.SegmentAddress
	// Epoch is the epoch of the writer that authors the segment.
	Epoch uint64
	// FirstSequence is the sequence number of the segment's first record.
	FirstSequence uint64
}

// ReadResult is a replica's answer to a read.
type ReadResult struct {
	// Frames are whole batch frames, as stored.
	Frames []byte
	// ReadableEnd is the last sequence number the replica may serve.
	ReadableEnd uint64
	// SealedEnd is the segment's final sequence number, if the replica
	// knows the segment is sealed.
	SealedEnd *uint64
}

// FenceResult is a replica's answer to a fence.
type FenceResult struct {
	// LocalEnd is the last sequence number of the replica's copy, or nil
	// if it has no copy.
	LocalEnd *uint64
	// SealedEnd is the segment's final sequence number, if the replica
	// knows it.
	SealedEnd *uint64
}

// Connector hands out clients for replicas and keeps one lazily opened
// connection per address.
type Connector struct {
	mutex sync.Mutex
	// connections holds the open connections by address.
	connections map[string]*grpc.ClientConn
}

// NewConnector returns a connector with no connections.
func NewConnector() *Connector {
	return &Connector{connections: make(map[string]*grpc.ClientConn)}
}

// Connect returns a client for the replica with the given identity at the
// given "host:port" address. It sends nothing. A replica that cannot be
// reached, or an address that cannot be used, shows up as [Unavailable] on
// every request.
func (connector *Connector) Connect(replicaID, address string) *Client {
	connector.mutex.Lock()
	defer connector.mutex.Unlock()
	connection, isOpen := connector.connections[address]
	if !isOpen {
		var err error
		connection, err = grpc.NewClient(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageBytes),
				grpc.MaxCallSendMsgSize(maxMessageBytes),
			),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   reconnectDelayLimit,
				},
				MinConnectTimeout: reconnectDelayLimit,
			}),
		)
		if err != nil {
			return Unreachable(replicaID, fmt.Sprintf("address %q cannot be used: %v", address, err))
		}
		connector.connections[address] = connection
	}
	return &Client{replicaID: replicaID, data: wire.NewReplicaDataClient(connection)}
}

// Close closes every connection.
func (connector *Connector) Close() error {
	connector.mutex.Lock()
	defer connector.mutex.Unlock()
	var closeErrors []error
	for address, connection := range connector.connections {
		closeErrors = append(closeErrors, connection.Close())
		delete(connector.connections, address)
	}
	return errors.Join(closeErrors...)
}

// Client talks to one replica.
type Client struct {
	// replicaID is the identity every request is addressed to.
	replicaID string
	// data is the stub of the calls on the path of every record. It is nil
	// for a replica that cannot be reached at all.
	data wire.ReplicaDataClient
	// unreachableReason says why data is nil.
	unreachableReason string
}

// Unreachable returns a client whose every request fails as [Unavailable]
// with reason, for a replica that is not registered.
func Unreachable(replicaID, reason string) *Client {
	return &Client{replicaID: replicaID, unreachableReason: reason}
}

// ReplicaID returns the identity of the replica the client talks to.
func (client *Client) ReplicaID() string { return client.replicaID }

// Append sends batch frames, or none as a heartbeat, and returns the last
// sequence number of the replica's copy afterwards.
func (client *Client) Append(ctx context.Context, segment Descriptor, writerEpoch uint64, frames []byte, durableSequence uint64) (uint64, error) {
	if client.data == nil {
		return 0, client.unreachable()
	}
	response, err := client.data.Append(client.addressed(ctx), &wire.AppendRequest{
		Segment:         descriptorToWire(segment),
		WriterEpoch:     writerEpoch,
		BatchFrames:     frames,
		DurableSequence: durableSequence,
	})
	if err != nil {
		return 0, client.noAnswer(err)
	}
	switch outcome := response.GetOutcome().(type) {
	case *wire.AppendResponse_Success_:
		return outcome.Success.GetLocalEnd(), nil
	case *wire.AppendResponse_Error:
		return 0, errorFromWire(outcome.Error)
	default:
		return 0, client.unreadable("AppendResponse")
	}
}

// Read asks for batch frames from fromSequence. If wait is positive, the
// replica may hold the request that long until records are readable. If
// recoveryEpoch is not nil, the read is the recovering writer's and may go
// to the end of the copy.
func (client *Client) Read(ctx context.Context, segment frame.SegmentAddress, fromSequence uint64, maxBytes uint64, wait time.Duration, recoveryEpoch *uint64) (ReadResult, error) {
	if client.data == nil {
		return ReadResult{}, client.unreachable()
	}
	request := &wire.ReadRequest{
		Segment:       addressToWire(segment),
		FromSequence:  fromSequence,
		MaxBytes:      maxBytes,
		RecoveryEpoch: recoveryEpoch,
	}
	if wait > 0 {
		request.WaitForRecordsNanoseconds = new(uint64(wait.Nanoseconds()))
	}
	response, err := client.data.Read(client.addressed(ctx), request)
	if err != nil {
		return ReadResult{}, client.noAnswer(err)
	}
	switch outcome := response.GetOutcome().(type) {
	case *wire.ReadResponse_Success_:
		return ReadResult{
			Frames:      outcome.Success.GetBatchFrames(),
			ReadableEnd: outcome.Success.GetReadableEnd(),
			SealedEnd:   outcome.Success.SealedEnd,
		}, nil
	case *wire.ReadResponse_Error:
		return ReadResult{}, errorFromWire(outcome.Error)
	default:
		return ReadResult{}, client.unreadable("ReadResponse")
	}
}

// Fence raises the replica's fenced epoch for the segment and returns where
// its copy ends.
func (client *Client) Fence(ctx context.Context, segment frame.SegmentAddress, epoch uint64) (FenceResult, error) {
	if client.data == nil {
		return FenceResult{}, client.unreachable()
	}
	response, err := client.data.Fence(client.addressed(ctx), &wire.FenceRequest{
		Segment: addressToWire(segment),
		Epoch:   epoch,
	})
	if err != nil {
		return FenceResult{}, client.noAnswer(err)
	}
	switch outcome := response.GetOutcome().(type) {
	case *wire.FenceResponse_Success_:
		return FenceResult{LocalEnd: outcome.Success.LocalEnd, SealedEnd: outcome.Success.SealedEnd}, nil
	case *wire.FenceResponse_Error:
		return FenceResult{}, errorFromWire(outcome.Error)
	default:
		return FenceResult{}, client.unreadable("FenceResponse")
	}
}

// Seal tells the replica the segment's final sequence number.
func (client *Client) Seal(ctx context.Context, segment frame.SegmentAddress, sealedEnd uint64) error {
	if client.data == nil {
		return client.unreachable()
	}
	response, err := client.data.Seal(client.addressed(ctx), &wire.SealRequest{
		Segment:   addressToWire(segment),
		SealedEnd: sealedEnd,
	})
	if err != nil {
		return client.noAnswer(err)
	}
	switch outcome := response.GetOutcome().(type) {
	case *wire.SealResponse_Success_:
		return nil
	case *wire.SealResponse_Error:
		return errorFromWire(outcome.Error)
	default:
		return client.unreadable("SealResponse")
	}
}

// addressed returns ctx with the header that names the replica.
func (client *Client) addressed(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, identityHeader, client.replicaID)
}

// unreachable is the error of a client that has no connection.
func (client *Client) unreachable() error {
	return &Error{Kind: Unavailable, Reason: client.unreachableReason}
}

// noAnswer turns a transport failure into [Unavailable]: no answer arrived,
// so the replica may or may not have applied the request.
func (client *Client) noAnswer(cause error) error {
	return &Error{Kind: Unavailable, Reason: fmt.Sprintf("replica %s did not answer: %v", client.replicaID, cause)}
}

// unreadable is the error for a response without an outcome, which is
// treated like no answer at all.
func (client *Client) unreadable(message string) error {
	return &Error{Kind: Unavailable, Reason: fmt.Sprintf("replica %s sent a %s without an outcome", client.replicaID, message)}
}

// errorFromWire turns a refusal carried in a response into an [Error].
func errorFromWire(wireError *wire.ReplicaError) error {
	switch kind := wireError.GetKind().(type) {
	case *wire.ReplicaError_Fenced_:
		return &Error{
			Kind:        Fenced,
			FencedEpoch: kind.Fenced.GetFencedEpoch(),
			Reason: fmt.Sprintf("epoch %d is fenced: the replica accepts epoch %d or later",
				kind.Fenced.GetRequestEpoch(), kind.Fenced.GetFencedEpoch()),
		}
	case *wire.ReplicaError_Sealed_:
		return &Error{Kind: Sealed, Reason: fmt.Sprintf("segment is sealed at %d", kind.Sealed.GetSealedEnd())}
	case *wire.ReplicaError_OutOfOrder_:
		return &Error{
			Kind:     OutOfOrder,
			LocalEnd: kind.OutOfOrder.GetLocalEnd(),
			Reason: fmt.Sprintf("batches start at %d but the copy ends at %d",
				kind.OutOfOrder.GetFirstSequence(), kind.OutOfOrder.GetLocalEnd()),
		}
	case *wire.ReplicaError_ConflictingSeal_:
		return &Error{
			Kind: ConflictingSeal,
			Reason: fmt.Sprintf("segment is already sealed at %d, not %d",
				kind.ConflictingSeal.GetExistingSealedEnd(), kind.ConflictingSeal.GetRequestedSealedEnd()),
		}
	case *wire.ReplicaError_SegmentNotFound_:
		return &Error{Kind: SegmentNotFound, Reason: "replica has no copy of the segment"}
	case *wire.ReplicaError_InvalidRequest_:
		return &Error{Kind: InvalidRequest, Reason: "invalid request: " + kind.InvalidRequest.GetReason()}
	case *wire.ReplicaError_NoCapacity_:
		return &Error{
			Kind:   NoCapacity,
			Reason: fmt.Sprintf("replica is out of space: disk is %d%% full", kind.NoCapacity.GetUsedPercent()),
		}
	case *wire.ReplicaError_Draining_:
		return &Error{Kind: Draining, Reason: "replica is draining"}
	case *wire.ReplicaError_Storage_:
		return &Error{Kind: Storage, Reason: "replica storage failed: " + kind.Storage.GetReason()}
	case *wire.ReplicaError_Unavailable_:
		return &Error{Kind: Unavailable, Reason: "replica is unavailable: " + kind.Unavailable.GetReason()}
	default:
		// An error this client does not know is not a refusal it can act
		// on; it is treated like no answer.
		return &Error{Kind: Unavailable, Reason: "replica answered with an error this client does not know"}
	}
}

// addressToWire writes a segment address as its message.
func addressToWire(address frame.SegmentAddress) *wire.SegmentAddress {
	return &wire.SegmentAddress{StreamId: address.StreamID, SegmentId: address.SegmentID}
}

// descriptorToWire writes a segment descriptor as its message.
func descriptorToWire(descriptor Descriptor) *wire.SegmentDescriptor {
	return &wire.SegmentDescriptor{
		Address:       addressToWire(descriptor.Address),
		Epoch:         descriptor.Epoch,
		FirstSequence: descriptor.FirstSequence,
	}
}
