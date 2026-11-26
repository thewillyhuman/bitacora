package bitacora

import (
	"cmp"
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

// DefaultEtcdKeyPrefix is the etcd key prefix of a cluster whose
// configuration names none.
const DefaultEtcdKeyPrefix = "/bitacora/"

// Config says where a cluster is.
type Config struct {
	// EtcdEndpoints are the client URLs of the etcd members that hold the
	// cluster's metadata, for example "http://10.0.0.1:2379". At least one
	// is required.
	EtcdEndpoints []string
	// EtcdKeyPrefix is the text in front of every key of the cluster in
	// etcd. Empty means [DefaultEtcdKeyPrefix].
	EtcdKeyPrefix string
	// ObjectStore is the cluster's object store, from which readers fetch
	// segments that replicas no longer hold. Nil means the client can only
	// read segments that replicas still hold.
	ObjectStore ObjectStore
	// RequestTimeout bounds every request to etcd and to a replica. Zero
	// means five seconds.
	RequestTimeout time.Duration
}

// Record is one record of a stream.
type Record struct {
	// Sequence is the record's position in its stream, starting at 1.
	Sequence uint64
	// Payload is the record's content.
	Payload []byte
}

// Client is a handle to a bitacora cluster. It is safe for concurrent use.
// Close it when done.
type Client struct {
	// etcd is the connection to the metadata store, owned by the client.
	etcd *clientv3.Client
	// metadata reads and writes the cluster's records.
	metadata *metadata.Store
	// replicas hands out connections to replicas.
	replicas *replica.Connector
	// objectStore is where tiered segments are read from, or nil.
	objectStore ObjectStore
	// requestTimeout bounds every request to a replica.
	requestTimeout time.Duration
}

// Connect returns a client for the cluster that config describes.
//
// Nothing is sent yet: an etcd that is down shows up as [ErrUnavailable] on
// the first operation, not here. The error wraps [ErrInvalidArgument] if
// the configuration can never work.
func Connect(config Config) (*Client, error) {
	if len(config.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("%w: at least one etcd endpoint is required", ErrInvalidArgument)
	}
	requestTimeout := cmp.Or(config.RequestTimeout, 5*time.Second)
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   config.EtcdEndpoints,
		DialTimeout: requestTimeout,
		// Left alone, the etcd client writes warnings to standard error.
		// A library returns its errors and prints nothing.
		Logger: zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: cannot use the etcd endpoints: %w", ErrInvalidArgument, err)
	}
	keyPrefix := cmp.Or(config.EtcdKeyPrefix, DefaultEtcdKeyPrefix)
	return &Client{
		etcd:           etcd,
		metadata:       metadata.NewStore(metadata.NewEtcdStore(etcd, keyPrefix, requestTimeout)),
		replicas:       replica.NewConnector(),
		objectStore:    config.ObjectStore,
		requestTimeout: requestTimeout,
	}, nil
}

// Close releases the client's connections. Writers and readers opened from
// the client must not be used afterwards.
func (client *Client) Close() error {
	replicasErr := client.replicas.Close()
	if client.etcd == nil {
		return replicasErr
	}
	if err := client.etcd.Close(); err != nil {
		return err
	}
	return replicasErr
}

// connectCopies returns a client for the replica of every copy, in the
// same order. A replica that is no longer registered gets a client that
// fails every request, so callers treat it like any replica that is down.
func (client *Client) connectCopies(ctx context.Context, copies []metadata.CopyRecord) ([]*replica.Client, error) {
	registered, err := client.metadata.ListReplicas(ctx)
	if err != nil {
		return nil, err
	}
	addresses := make(map[string]string, len(registered))
	for _, registration := range registered {
		addresses[registration.ReplicaID] = registration.Address
	}
	connected := make([]*replica.Client, 0, len(copies))
	for _, copyRecord := range copies {
		address, isRegistered := addresses[copyRecord.ReplicaID]
		if !isRegistered {
			connected = append(connected, replica.Unreachable(copyRecord.ReplicaID,
				fmt.Sprintf("replica %s is not registered", copyRecord.ReplicaID)))
			continue
		}
		connected = append(connected, client.replicas.Connect(copyRecord.ReplicaID, address))
	}
	return connected, nil
}

// nowMillis returns the wall-clock time in milliseconds since the Unix
// epoch, the unit of every timestamp in the metadata store.
func nowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}
