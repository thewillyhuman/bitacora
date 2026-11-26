package metadata

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

// connectToEtcd starts an etcd and returns a client for it.
func connectToEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	endpoint := testcluster.StartEtcd(t)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("connect to etcd: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestEtcdStoreMapsKeysValuesAndRevisions(t *testing.T) {
	client := connectToEtcd(t)
	ctx := t.Context()
	store := NewEtcdStore(client, "/cluster-a/", 5*time.Second)
	other := NewEtcdStore(client, "/cluster-b/", 5*time.Second)

	if stored, err := store.Get(ctx, "streams/orders"); err != nil || stored != nil {
		t.Fatalf("Get of an absent key = %v, %v; want nil, nil", stored, err)
	}

	// A condition with revision 0 asks for absence: it holds once.
	absent := []Condition{{Key: "streams/orders", Revision: 0}}
	applied, err := store.Transact(ctx, absent, []Mutation{{Key: "streams/orders", Value: []byte("first")}})
	if err != nil || !applied {
		t.Fatalf("create = %v, %v; want applied", applied, err)
	}
	applied, err = store.Transact(ctx, absent, []Mutation{{Key: "streams/orders", Value: []byte("again")}})
	if err != nil || applied {
		t.Fatalf("create of a key that exists = %v, %v; want not applied", applied, err)
	}

	// The key in etcd is the prefix followed by the key, the value is the
	// bytes as given, and the revision is the key's mod revision.
	raw, err := client.Get(ctx, "/cluster-a/streams/orders")
	if err != nil || len(raw.Kvs) != 1 {
		t.Fatalf("the key is not at prefix + key in etcd: %v, %v", raw, err)
	}
	stored, err := store.Get(ctx, "streams/orders")
	if err != nil || stored == nil {
		t.Fatalf("Get = %v, %v", stored, err)
	}
	if stored.Key != "streams/orders" || string(stored.Value) != "first" || stored.Revision != raw.Kvs[0].ModRevision {
		t.Errorf("Get = %+v; want the key without prefix, the value, and mod revision %d", stored, raw.Kvs[0].ModRevision)
	}

	// Another cluster's prefix sees nothing of it.
	if elsewhere, err := other.Get(ctx, "streams/orders"); err != nil || elsewhere != nil {
		t.Errorf("another prefix sees the key: %v, %v", elsewhere, err)
	}

	// A write conditional on the revision read applies once; the same
	// revision is stale afterwards.
	atRevision := []Condition{{Key: "streams/orders", Revision: stored.Revision}}
	applied, err = store.Transact(ctx, atRevision, []Mutation{{Key: "streams/orders", Value: []byte("second")}})
	if err != nil || !applied {
		t.Fatalf("update at the revision read = %v, %v; want applied", applied, err)
	}
	applied, err = store.Transact(ctx, atRevision, []Mutation{{Key: "streams/orders", Value: []byte("third")}})
	if err != nil || applied {
		t.Fatalf("update at a stale revision = %v, %v; want not applied", applied, err)
	}
	updated, _ := store.Get(ctx, "streams/orders")
	if string(updated.Value) != "second" || updated.Revision <= stored.Revision {
		t.Errorf("after the update: %+v; want value second and a higher revision than %d", updated, stored.Revision)
	}

	// Several writes apply together, and a list is sorted and limited to
	// its prefix.
	applied, err = store.Transact(ctx, nil, []Mutation{
		{Key: "streams/zeta", Value: []byte("z")},
		{Key: "streams/alpha", Value: []byte("a")},
		{Key: "replicas/replica-a", Value: []byte("r")},
	})
	if err != nil || !applied {
		t.Fatalf("unconditional transaction = %v, %v", applied, err)
	}
	listed, err := store.List(ctx, "streams/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var keys []string
	for _, entry := range listed {
		keys = append(keys, entry.Key)
	}
	if want := []string{"streams/alpha", "streams/orders", "streams/zeta"}; !slices.Equal(keys, want) {
		t.Errorf("List(streams/) = %v, want %v", keys, want)
	}

	// A delete is a mutation like any other, and a key that is created
	// again never gets an old revision back.
	applied, err = store.Transact(ctx, []Condition{{Key: "streams/orders", Revision: updated.Revision}},
		[]Mutation{{Key: "streams/orders", Delete: true}})
	if err != nil || !applied {
		t.Fatalf("delete = %v, %v", applied, err)
	}
	if deleted, err := store.Get(ctx, "streams/orders"); err != nil || deleted != nil {
		t.Fatalf("Get after delete = %v, %v", deleted, err)
	}
	if applied, err = store.Transact(ctx, absent, []Mutation{{Key: "streams/orders", Value: []byte("new")}}); err != nil || !applied {
		t.Fatalf("create after delete = %v, %v", applied, err)
	}
	recreated, _ := store.Get(ctx, "streams/orders")
	if recreated.Revision <= updated.Revision {
		t.Errorf("a key created again has revision %d, not above %d", recreated.Revision, updated.Revision)
	}
}

func TestOnlyOneOfTwoWritersClaimsAnEpochOnEtcd(t *testing.T) {
	client := connectToEtcd(t)
	ctx := t.Context()
	store := NewStore(NewEtcdStore(client, "/bitacora/", 5*time.Second))
	created, err := store.CreateStream(ctx, "orders", ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2}, RetentionPolicy{})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if created.StreamID != 1 || created.Epoch != 0 {
		t.Fatalf("created stream = %+v; want id 1 at epoch 0", created)
	}

	// Many candidates read epoch 0 and all try to claim epoch 1. The
	// conditional write lets exactly one through.
	const candidates = 8
	results := make([]error, candidates)
	var claims sync.WaitGroup
	for candidate := range candidates {
		claims.Go(func() {
			_, results[candidate] = store.ClaimEpoch(ctx, "orders", 0, "candidate", 1)
		})
	}
	claims.Wait()
	winners := 0
	for _, result := range results {
		switch {
		case result == nil:
			winners++
		case !errors.Is(result, ErrFenced):
			t.Errorf("a losing candidate got %v, want a fenced error", result)
		}
	}
	if winners != 1 {
		t.Errorf("%d candidates claimed epoch 1, want exactly 1", winners)
	}
	stream, err := store.GetStream(ctx, "orders")
	if err != nil || stream.Epoch != 1 {
		t.Errorf("stream after the claims = epoch %d, %v; want epoch 1", stream.Epoch, err)
	}

	// The second stream gets the next identifier from the counter.
	second, err := store.CreateStream(ctx, "payments", ReplicationSettings{ReplicaCount: 3, WriteQuorum: 2}, RetentionPolicy{})
	if err != nil || second.StreamID != 2 {
		t.Errorf("second stream = id %d, %v; want id 2", second.StreamID, err)
	}
}

func TestAnEtcdThatCannotBeReachedIsUnavailable(t *testing.T) {
	// A port nobody listens on: take one and give it back.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	_ = listener.Close()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	store := NewEtcdStore(client, "/bitacora/", 200*time.Millisecond)

	ctx := t.Context()
	if _, err := store.Get(ctx, "streams/orders"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Get = %v, want unavailable", err)
	}
	if _, err := store.List(ctx, "streams/"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("List = %v, want unavailable", err)
	}
	if _, err := store.Transact(ctx, nil, []Mutation{{Key: "streams/orders", Value: []byte("x")}}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Transact = %v, want unavailable", err)
	}

	// A context that is done is the caller's own doing, not the store's:
	// the caller gets its context's error back, and no "unavailable" that
	// would send it looking for a fault in the cluster.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, getErr := store.Get(cancelled, "streams/orders")
	_, listErr := store.List(cancelled, "streams/")
	_, transactErr := store.Transact(cancelled, nil, []Mutation{{Key: "streams/orders", Value: []byte("x")}})
	for call, err := range map[string]error{"Get": getErr, "List": listErr, "Transact": transactErr} {
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) {
			t.Errorf("%s with a cancelled context = %v, want context.Canceled and not unavailable", call, err)
		}
	}
}
