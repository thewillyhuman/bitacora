package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdStore is the [KeyValueStore] of a real cluster: etcd.
//
// The mapping is the one docs/protocol/metadata.md gives: a key is the
// cluster's prefix followed by the key, a value is the record's JSON bytes,
// the revision of a key is its etcd mod revision, and "the key is absent"
// is "its create revision is 0".
type EtcdStore struct {
	// client is the etcd client. The store does not own it.
	client *clientv3.Client
	// prefix is put in front of every key and removed from every key that
	// comes back.
	prefix string
	// requestTimeout bounds every request.
	requestTimeout time.Duration
}

// NewEtcdStore returns a store over client that keeps its keys under
// prefix and gives every request at most requestTimeout.
func NewEtcdStore(client *clientv3.Client, prefix string, requestTimeout time.Duration) *EtcdStore {
	return &EtcdStore{client: client, prefix: prefix, requestTimeout: requestTimeout}
}

// Get implements [KeyValueStore].
func (store *EtcdStore) Get(ctx context.Context, key string) (*StoredValue, error) {
	ctx, cancel := context.WithTimeout(ctx, store.requestTimeout)
	defer cancel()
	response, err := store.client.Get(ctx, store.prefix+key)
	if err != nil {
		return nil, unavailable("read "+key, err)
	}
	if len(response.Kvs) == 0 {
		return nil, nil
	}
	return &StoredValue{Key: key, Value: response.Kvs[0].Value, Revision: response.Kvs[0].ModRevision}, nil
}

// List implements [KeyValueStore].
func (store *EtcdStore) List(ctx context.Context, prefix string) ([]StoredValue, error) {
	ctx, cancel := context.WithTimeout(ctx, store.requestTimeout)
	defer cancel()
	response, err := store.client.Get(ctx, store.prefix+prefix,
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, unavailable("list "+prefix, err)
	}
	listed := make([]StoredValue, 0, len(response.Kvs))
	for _, stored := range response.Kvs {
		listed = append(listed, StoredValue{
			Key:      strings.TrimPrefix(string(stored.Key), store.prefix),
			Value:    stored.Value,
			Revision: stored.ModRevision,
		})
	}
	return listed, nil
}

// Transact implements [KeyValueStore].
func (store *EtcdStore) Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, store.requestTimeout)
	defer cancel()
	comparisons := make([]clientv3.Cmp, 0, len(conditions))
	for _, condition := range conditions {
		key := store.prefix + condition.Key
		if condition.Revision == 0 {
			comparisons = append(comparisons, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
		} else {
			comparisons = append(comparisons, clientv3.Compare(clientv3.ModRevision(key), "=", condition.Revision))
		}
	}
	operations := make([]clientv3.Op, 0, len(mutations))
	for _, mutation := range mutations {
		key := store.prefix + mutation.Key
		if mutation.Delete {
			operations = append(operations, clientv3.OpDelete(key))
		} else {
			operations = append(operations, clientv3.OpPut(key, string(mutation.Value)))
		}
	}
	response, err := store.client.Txn(ctx).If(comparisons...).Then(operations...).Commit()
	if err != nil {
		return false, unavailable("transact", err)
	}
	return response.Succeeded, nil
}

// unavailable wraps a failure of the store below as [ErrUnavailable].
func unavailable(operation string, cause error) error {
	return fmt.Errorf("%w: cannot %s: %v", ErrUnavailable, operation, cause)
}
