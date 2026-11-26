package bitacora

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

func TestConnectNeedsAnEtcdEndpoint(t *testing.T) {
	client, err := Connect(Config{})
	if !errors.Is(err, ErrInvalidArgument) || client != nil {
		t.Errorf("Connect without endpoints = %v, %v; want ErrInvalidArgument", client, err)
	}
}

// connectToNothing returns a client for an etcd endpoint nobody listens on.
func connectToNothing(t *testing.T) *Client {
	t.Helper()
	// An address nobody listens on, and that no other test is given.
	endpoint := "http://" + testcluster.FreeAddress(t)

	client, err := Connect(Config{EtcdEndpoints: []string{endpoint}, RequestTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("Connect sends nothing and must not fail: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestAnEtcdThatIsDownShowsOnTheFirstOperation(t *testing.T) {
	client := connectToNothing(t)

	if _, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 10); !errors.Is(err, ErrUnavailable) {
		t.Errorf("ReadRange without etcd = %v, want ErrUnavailable", err)
	}
	if _, err := client.OpenReader("orders").LowWaterMark(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Errorf("LowWaterMark without etcd = %v, want ErrUnavailable", err)
	}
}

func TestAnOperationThatEndsWithItsContextReturnsTheContextsError(t *testing.T) {
	client := connectToNothing(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, readErr := client.OpenReader("orders").ReadRange(ctx, 1, 10)
	_, markErr := client.OpenReader("orders").LowWaterMark(ctx)
	_, describeErr := client.DescribeStream(ctx, "orders")
	_, createErr := client.CreateStream(ctx, "orders", StreamOptions{})
	_, openErr := client.OpenWriter(ctx, "orders", WriterOptions{})
	calls := map[string]error{
		"ReadRange":              readErr,
		"LowWaterMark":           markErr,
		"DescribeStream":         describeErr,
		"CreateStream":           createErr,
		"OpenWriter":             openErr,
		"DeleteStream":           client.DeleteStream(ctx, "orders"),
		"TruncateBefore":         client.TruncateBefore(ctx, "orders", 5),
		"RegisterMaterializer":   client.RegisterMaterializer(ctx, "orders", "pages"),
		"UnregisterMaterializer": client.UnregisterMaterializer(ctx, "orders", "pages"),
	}
	for call, err := range calls {
		// The caller gave up; nothing says that the cluster is at fault.
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) {
			t.Errorf("%s with a cancelled context = %v, want context.Canceled and not ErrUnavailable", call, err)
		}
	}
}

func TestTheClientPrintsNothingOfItsOwn(t *testing.T) {
	// The etcd client logs to standard error unless told otherwise, and it
	// has something to say when its endpoint is down.
	captured, err := os.CreateTemp(t.TempDir(), "standard-error")
	if err != nil {
		t.Fatalf("create a file to capture standard error: %v", err)
	}
	standardError := os.Stderr
	os.Stderr = captured
	t.Cleanup(func() { os.Stderr = standardError })

	client := connectToNothing(t)
	if _, err := client.OpenReader("orders").LowWaterMark(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Errorf("LowWaterMark without etcd = %v, want ErrUnavailable", err)
	}
	_ = client.Close()

	os.Stderr = standardError
	printed, err := os.ReadFile(captured.Name())
	if err != nil {
		t.Fatalf("read what was captured: %v", err)
	}
	if len(printed) != 0 {
		t.Errorf("the client printed to standard error; it must return its errors and print nothing:\n%s", printed)
	}
}

func TestAReplicaThatIsNotRegisteredIsTreatedAsDown(t *testing.T) {
	cluster := newTestCluster(t, 2)
	copies := []metadata.CopyRecord{
		{ReplicaID: "replica-1", State: metadata.CopyGood},
		{ReplicaID: "replica-gone", State: metadata.CopyGood},
		{ReplicaID: "replica-0", State: metadata.CopyGood},
	}

	connected, err := cluster.client.connectCopies(t.Context(), copies)

	if err != nil || len(connected) != 3 {
		t.Fatalf("connectCopies = %d clients, %v; want one per copy", len(connected), err)
	}
	for index, replicaClient := range connected {
		if replicaClient.ReplicaID() != copies[index].ReplicaID {
			t.Errorf("client %d is for %s, want %s: the order of the copies must be kept",
				index, replicaClient.ReplicaID(), copies[index].ReplicaID)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = connected[1].Fence(ctx, frame.SegmentAddress{StreamID: 1, SegmentID: 1}, 1)
	if replica.KindOf(err) != replica.Unavailable {
		t.Errorf("a request to an unregistered replica = %v, want unavailable", err)
	}
	if _, err := connected[0].Fence(ctx, frame.SegmentAddress{StreamID: 1, SegmentID: 1}, 1); err != nil {
		t.Errorf("a request to a registered replica: %v", err)
	}
}
