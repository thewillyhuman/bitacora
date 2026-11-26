package replica

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
	"github.com/thewillyhuman/bitacora/src/go/internal/wire"
)

// scriptedServer answers every request with what the test put in it, and
// remembers what it was asked.
type scriptedServer struct {
	wire.UnimplementedReplicaDataServer
	appendResponse *wire.AppendResponse
	readResponse   *wire.ReadResponse
	fenceResponse  *wire.FenceResponse
	sealResponse   *wire.SealResponse
	// seenIdentities are the values of the identity header of each request.
	seenIdentities []string
	// lastAppend and lastRead are the last requests of each kind.
	lastAppend *wire.AppendRequest
	lastRead   *wire.ReadRequest
}

func (server *scriptedServer) remember(ctx context.Context) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	server.seenIdentities = append(server.seenIdentities, incoming.Get(identityHeader)...)
}

func (server *scriptedServer) Append(ctx context.Context, request *wire.AppendRequest) (*wire.AppendResponse, error) {
	server.remember(ctx)
	server.lastAppend = request
	return server.appendResponse, nil
}

func (server *scriptedServer) Read(ctx context.Context, request *wire.ReadRequest) (*wire.ReadResponse, error) {
	server.remember(ctx)
	server.lastRead = request
	return server.readResponse, nil
}

func (server *scriptedServer) Fence(ctx context.Context, _ *wire.FenceRequest) (*wire.FenceResponse, error) {
	server.remember(ctx)
	return server.fenceResponse, nil
}

func (server *scriptedServer) Seal(ctx context.Context, _ *wire.SealRequest) (*wire.SealResponse, error) {
	server.remember(ctx)
	return server.sealResponse, nil
}

// serve starts server on a loopback port and returns a client for it.
func serve(t *testing.T, server *scriptedServer) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	wire.RegisterReplicaDataServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	connector := NewConnector()
	t.Cleanup(func() { _ = connector.Close() })
	return connector.Connect("replica-a", listener.Addr().String())
}

var testSegment = Descriptor{
	Address:       frame.SegmentAddress{StreamID: 7, SegmentID: 3},
	Epoch:         4,
	FirstSequence: 100,
}

func TestRequestsCarryTheReplicaIdentityAndTheirFields(t *testing.T) {
	server := &scriptedServer{
		appendResponse: &wire.AppendResponse{Outcome: &wire.AppendResponse_Success_{
			Success: &wire.AppendResponse_Success{LocalEnd: 102},
		}},
		readResponse: &wire.ReadResponse{Outcome: &wire.ReadResponse_Success_{
			Success: &wire.ReadResponse_Success{BatchFrames: []byte("frames"), ReadableEnd: 102, SealedEnd: new(uint64(102))},
		}},
		fenceResponse: &wire.FenceResponse{Outcome: &wire.FenceResponse_Success_{
			Success: &wire.FenceResponse_Success{LocalEnd: new(uint64(102))},
		}},
		sealResponse: &wire.SealResponse{Outcome: &wire.SealResponse_Success_{Success: &wire.SealResponse_Success{}}},
	}
	client := serve(t, server)
	ctx := t.Context()

	localEnd, err := client.Append(ctx, testSegment, 5, []byte("batch"), 99)
	if err != nil || localEnd != 102 {
		t.Fatalf("Append = %d, %v; want 102", localEnd, err)
	}
	sent := server.lastAppend
	if sent.GetSegment().GetAddress().GetStreamId() != 7 || sent.GetSegment().GetAddress().GetSegmentId() != 3 ||
		sent.GetSegment().GetEpoch() != 4 || sent.GetSegment().GetFirstSequence() != 100 ||
		sent.GetWriterEpoch() != 5 || string(sent.GetBatchFrames()) != "batch" || sent.GetDurableSequence() != 99 {
		t.Errorf("the append request does not carry what was given: %v", sent)
	}

	read, err := client.Read(ctx, testSegment.Address, 100, 4096, 250*time.Millisecond, new(uint64(5)))
	if err != nil || string(read.Frames) != "frames" || read.ReadableEnd != 102 || read.SealedEnd == nil || *read.SealedEnd != 102 {
		t.Fatalf("Read = %+v, %v", read, err)
	}
	if asked := server.lastRead; asked.GetFromSequence() != 100 || asked.GetMaxBytes() != 4096 ||
		asked.GetWaitForRecordsNanoseconds() != 250_000_000 || asked.GetRecoveryEpoch() != 5 {
		t.Errorf("the read request does not carry what was given: %v", asked)
	}
	if _, err := client.Read(ctx, testSegment.Address, 100, 4096, 0, nil); err != nil {
		t.Fatalf("Read without waiting: %v", err)
	}
	if asked := server.lastRead; asked.WaitForRecordsNanoseconds != nil || asked.RecoveryEpoch != nil {
		t.Errorf("a read that neither waits nor recovers must leave both fields out: %v", asked)
	}

	fenced, err := client.Fence(ctx, testSegment.Address, 5)
	if err != nil || fenced.LocalEnd == nil || *fenced.LocalEnd != 102 || fenced.SealedEnd != nil {
		t.Fatalf("Fence = %+v, %v", fenced, err)
	}
	if err := client.Seal(ctx, testSegment.Address, 102); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if len(server.seenIdentities) != 5 {
		t.Fatalf("got %d identity headers for 5 requests", len(server.seenIdentities))
	}
	for _, identity := range server.seenIdentities {
		if identity != "replica-a" {
			t.Errorf("identity header = %q, want replica-a", identity)
		}
	}
}

func TestEveryRefusalBecomesItsKind(t *testing.T) {
	cases := []struct {
		name            string
		refusal         *wire.ReplicaError
		wantKind        Kind
		wantFencedEpoch uint64
		wantLocalEnd    uint64
		wantInReason    string
	}{
		{
			name: "fenced",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_Fenced_{
				Fenced: &wire.ReplicaError_Fenced{RequestEpoch: 4, FencedEpoch: 6},
			}},
			wantKind: Fenced, wantFencedEpoch: 6, wantInReason: "epoch 6",
		},
		{
			name: "sealed",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_Sealed_{
				Sealed: &wire.ReplicaError_Sealed{SealedEnd: 120},
			}},
			wantKind: Sealed, wantInReason: "120",
		},
		{
			name: "out of order",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_OutOfOrder_{
				OutOfOrder: &wire.ReplicaError_OutOfOrder{FirstSequence: 110, LocalEnd: 104},
			}},
			wantKind: OutOfOrder, wantLocalEnd: 104, wantInReason: "104",
		},
		{
			name: "conflicting seal",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_ConflictingSeal_{
				ConflictingSeal: &wire.ReplicaError_ConflictingSeal{ExistingSealedEnd: 120, RequestedSealedEnd: 121},
			}},
			wantKind: ConflictingSeal, wantInReason: "120",
		},
		{
			name: "segment not found",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_SegmentNotFound_{
				SegmentNotFound: &wire.ReplicaError_SegmentNotFound{},
			}},
			wantKind: SegmentNotFound, wantInReason: "no copy",
		},
		{
			name: "invalid request",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_InvalidRequest_{
				InvalidRequest: &wire.ReplicaError_InvalidRequest{Reason: "no segment"},
			}},
			wantKind: InvalidRequest, wantInReason: "no segment",
		},
		{
			name: "no capacity",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_NoCapacity_{
				NoCapacity: &wire.ReplicaError_NoCapacity{UsedPercent: 97},
			}},
			wantKind: NoCapacity, wantInReason: "97%",
		},
		{
			name: "draining",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_Draining_{
				Draining: &wire.ReplicaError_Draining{},
			}},
			wantKind: Draining, wantInReason: "draining",
		},
		{
			name: "storage",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_Storage_{
				Storage: &wire.ReplicaError_Storage{Reason: "checksum"},
			}},
			wantKind: Storage, wantInReason: "checksum",
		},
		{
			name: "unavailable",
			refusal: &wire.ReplicaError{Kind: &wire.ReplicaError_Unavailable_{
				Unavailable: &wire.ReplicaError_Unavailable{Reason: "another replica"},
			}},
			wantKind: Unavailable, wantInReason: "another replica",
		},
		{
			name:     "an error this client does not know",
			refusal:  &wire.ReplicaError{},
			wantKind: Unavailable, wantInReason: "does not know",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			server := &scriptedServer{
				appendResponse: &wire.AppendResponse{Outcome: &wire.AppendResponse_Error{Error: testCase.refusal}},
				readResponse:   &wire.ReadResponse{Outcome: &wire.ReadResponse_Error{Error: testCase.refusal}},
				fenceResponse:  &wire.FenceResponse{Outcome: &wire.FenceResponse_Error{Error: testCase.refusal}},
				sealResponse:   &wire.SealResponse{Outcome: &wire.SealResponse_Error{Error: testCase.refusal}},
			}
			client := serve(t, server)
			ctx := t.Context()

			_, appendErr := client.Append(ctx, testSegment, 4, nil, 0)
			_, readErr := client.Read(ctx, testSegment.Address, 100, 1, 0, nil)
			_, fenceErr := client.Fence(ctx, testSegment.Address, 5)
			sealErr := client.Seal(ctx, testSegment.Address, 100)
			for call, err := range map[string]error{"Append": appendErr, "Read": readErr, "Fence": fenceErr, "Seal": sealErr} {
				if KindOf(err) != testCase.wantKind {
					t.Errorf("%s: kind = %d, want %d (%v)", call, KindOf(err), testCase.wantKind, err)
					continue
				}
				replicaError := err.(*Error)
				if replicaError.FencedEpoch != testCase.wantFencedEpoch || replicaError.LocalEnd != testCase.wantLocalEnd {
					t.Errorf("%s: fenced epoch %d, local end %d; want %d, %d", call,
						replicaError.FencedEpoch, replicaError.LocalEnd, testCase.wantFencedEpoch, testCase.wantLocalEnd)
				}
				if !strings.Contains(err.Error(), testCase.wantInReason) {
					t.Errorf("%s: reason %q does not mention %q", call, err, testCase.wantInReason)
				}
			}
		})
	}
}

func TestAResponseWithoutAnOutcomeIsNoAnswer(t *testing.T) {
	client := serve(t, &scriptedServer{
		appendResponse: &wire.AppendResponse{},
		readResponse:   &wire.ReadResponse{},
		fenceResponse:  &wire.FenceResponse{},
		sealResponse:   &wire.SealResponse{},
	})
	ctx := t.Context()
	_, appendErr := client.Append(ctx, testSegment, 4, nil, 0)
	_, readErr := client.Read(ctx, testSegment.Address, 100, 1, 0, nil)
	_, fenceErr := client.Fence(ctx, testSegment.Address, 5)
	sealErr := client.Seal(ctx, testSegment.Address, 100)
	for call, err := range map[string]error{"Append": appendErr, "Read": readErr, "Fence": fenceErr, "Seal": sealErr} {
		if KindOf(err) != Unavailable {
			t.Errorf("%s: kind = %d, want unavailable (%v)", call, KindOf(err), err)
		}
	}
}

func TestAReplicaThatCannotBeReachedIsUnavailable(t *testing.T) {
	// An address nobody listens on, and that no other test is given.
	address := testcluster.FreeAddress(t)

	connector := NewConnector()
	t.Cleanup(func() { _ = connector.Close() })
	clients := map[string]*Client{
		"nobody listens":  connector.Connect("replica-a", address),
		"not registered":  Unreachable("replica-b", "replica replica-b is not registered"),
		"unusable target": connector.Connect("replica-c", "bad\x00address"),
	}
	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, appendErr := client.Append(ctx, testSegment, 4, nil, 0)
			_, readErr := client.Read(ctx, testSegment.Address, 100, 1, 0, nil)
			_, fenceErr := client.Fence(ctx, testSegment.Address, 5)
			sealErr := client.Seal(ctx, testSegment.Address, 100)
			for call, err := range map[string]error{"Append": appendErr, "Read": readErr, "Fence": fenceErr, "Seal": sealErr} {
				if KindOf(err) != Unavailable {
					t.Errorf("%s: kind = %d, want unavailable (%v)", call, KindOf(err), err)
				}
			}
		})
	}
}

func TestOneConnectionPerAddress(t *testing.T) {
	connector := NewConnector()
	t.Cleanup(func() { _ = connector.Close() })
	connector.Connect("replica-a", "127.0.0.1:1")
	connector.Connect("replica-a", "127.0.0.1:1")
	connector.Connect("replica-b", "127.0.0.1:2")
	if len(connector.connections) != 2 {
		t.Errorf("%d connections for 2 addresses", len(connector.connections))
	}
	if err := connector.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if len(connector.connections) != 0 {
		t.Errorf("Close left %d connections", len(connector.connections))
	}
}

func TestKindOfAnotherErrorIsZero(t *testing.T) {
	if kind := KindOf(context.Canceled); kind != 0 {
		t.Errorf("KindOf(context.Canceled) = %d, want 0", kind)
	}
}
