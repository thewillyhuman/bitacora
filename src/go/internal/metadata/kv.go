package metadata

import (
	"context"
	"slices"
	"strings"
	"sync"
)

// StoredValue is a value of the key-value store with the revision at which
// it was last written.
type StoredValue struct {
	// Key is the key, without the cluster's prefix.
	Key string
	// Value is the stored bytes.
	Value []byte
	// Revision changes on every write of the key and is never repeated,
	// not even after the key is deleted and created again. It is never 0.
	Revision int64
}

// Condition is one thing that must hold for a transaction to apply.
type Condition struct {
	// Key is the key the condition is about.
	Key string
	// Revision is the revision the key must be at, or 0 if the key must be
	// absent.
	Revision int64
}

// Mutation is one write of a transaction.
type Mutation struct {
	// Key is the key to write or delete.
	Key string
	// Value is the new value. Ignored when Delete is set.
	Value []byte
	// Delete removes the key instead of writing it.
	Delete bool
}

// KeyValueStore is what the metadata store needs from the store below it
// (docs/protocol/metadata.md, "What is required of the store").
type KeyValueStore interface {
	// Get reads one key with a linearizable read. It returns nil if the key
	// is absent.
	Get(ctx context.Context, key string) (*StoredValue, error)
	// List reads every key that starts with prefix, sorted by key, as one
	// consistent snapshot.
	List(ctx context.Context, prefix string) ([]StoredValue, error)
	// Transact applies every mutation atomically if and only if every
	// condition holds, and reports whether it did. After an error the
	// transaction may or may not have been applied.
	Transact(ctx context.Context, conditions []Condition, mutations []Mutation) (applied bool, err error)
}

// MemoryStore is a [KeyValueStore] in memory, for tests.
type MemoryStore struct {
	mutex sync.Mutex
	// values holds the stored values by key.
	values map[string]StoredValue
	// lastRevision is the revision of the latest write.
	lastRevision int64
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{values: make(map[string]StoredValue)}
}

// Get implements [KeyValueStore].
func (store *MemoryStore) Get(_ context.Context, key string) (*StoredValue, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	stored, isPresent := store.values[key]
	if !isPresent {
		return nil, nil
	}
	return &stored, nil
}

// List implements [KeyValueStore].
func (store *MemoryStore) List(_ context.Context, prefix string) ([]StoredValue, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	var listed []StoredValue
	for key, stored := range store.values {
		if strings.HasPrefix(key, prefix) {
			listed = append(listed, stored)
		}
	}
	slices.SortFunc(listed, func(left, right StoredValue) int { return strings.Compare(left.Key, right.Key) })
	return listed, nil
}

// Transact implements [KeyValueStore].
func (store *MemoryStore) Transact(_ context.Context, conditions []Condition, mutations []Mutation) (bool, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	for _, condition := range conditions {
		// An absent key reads as revision 0, which is also how a condition
		// asks for absence.
		if store.values[condition.Key].Revision != condition.Revision {
			return false, nil
		}
	}
	store.lastRevision++
	for _, mutation := range mutations {
		if mutation.Delete {
			delete(store.values, mutation.Key)
			continue
		}
		store.values[mutation.Key] = StoredValue{
			Key:      mutation.Key,
			Value:    slices.Clone(mutation.Value),
			Revision: store.lastRevision,
		}
	}
	return true, nil
}
