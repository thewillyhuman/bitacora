package bitacora

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

func TestConnectNeedsAnEtcdEndpoint(t *testing.T) {
	client, err := Connect(Config{})
	if !errors.Is(err, ErrInvalidArgument) || client != nil {
		t.Errorf("Connect without endpoints = %v, %v; want ErrInvalidArgument", client, err)
	}
}

func TestAnEtcdThatIsDownShowsOnTheFirstOperation(t *testing.T) {
	// A port nobody listens on: take one and give it back.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	_ = listener.Close()

	client, err := Connect(Config{EtcdEndpoints: []string{endpoint}, RequestTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("Connect sends nothing and must not fail: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 10); !errors.Is(err, ErrUnavailable) {
		t.Errorf("ReadRange without etcd = %v, want ErrUnavailable", err)
	}
	if _, err := client.OpenReader("orders").LowWaterMark(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Errorf("LowWaterMark without etcd = %v, want ErrUnavailable", err)
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
