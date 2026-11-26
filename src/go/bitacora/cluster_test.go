package bitacora

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/replica"
)

// testCluster is a client wired to a metadata store in memory and to fake
// replicas, so that the client's behaviour can be tested in milliseconds
// and with faults a test chooses.
type testCluster struct {
	t testing.TB
	// client is the client under test.
	client *Client
	// store is the metadata store the client uses.
	store *metadata.Store
	// replicas are the fake replicas, all registered.
	replicas []*fakeReplica
	// objectStoreDirectory is the root of the client's object store.
	objectStoreDirectory string
}

// newTestCluster starts replicaCount fake replicas, registers them, and
// returns a client for them.
func newTestCluster(t testing.TB, replicaCount int) *testCluster {
	t.Helper()
	keyValues := metadata.NewMemoryStore()
	cluster := &testCluster{
		t:                    t,
		store:                metadata.NewStore(keyValues),
		objectStoreDirectory: t.TempDir(),
	}
	for index := range replicaCount {
		fake := startFakeReplica(t, fmt.Sprintf("replica-%d", index))
		cluster.replicas = append(cluster.replicas, fake)
		registration, err := json.Marshal(metadata.ReplicaRecord{
			SchemaVersion: metadata.SchemaVersion,
			ReplicaID:     fake.replicaID,
			Address:       fake.address,
		})
		if err != nil {
			t.Fatalf("encode a registration: %v", err)
		}
		mutation := metadata.Mutation{Key: "replicas/" + fake.replicaID, Value: registration}
		if _, err := keyValues.Transact(t.Context(), nil, []metadata.Mutation{mutation}); err != nil {
			t.Fatalf("register %s: %v", fake.replicaID, err)
		}
	}
	cluster.client = &Client{
		metadata:       cluster.store,
		replicas:       replica.NewConnector(),
		objectStore:    NewLocalObjectStore(cluster.objectStoreDirectory),
		requestTimeout: 2 * time.Second,
	}
	t.Cleanup(func() { _ = cluster.client.Close() })
	return cluster
}

// replicaIDs returns the identities of every replica.
func (cluster *testCluster) replicaIDs() []string {
	identities := make([]string, 0, len(cluster.replicas))
	for _, fake := range cluster.replicas {
		identities = append(identities, fake.replicaID)
	}
	return identities
}

// createStream creates a stream replicated on every replica with the given
// write quorum, and places its standby segment, as the controller would.
func (cluster *testCluster) createStream(name string, writeQuorum uint8) metadata.StreamRecord {
	cluster.t.Helper()
	replication := metadata.ReplicationSettings{ReplicaCount: uint8(len(cluster.replicas)), WriteQuorum: writeQuorum}
	if _, err := cluster.store.CreateStream(cluster.t.Context(), name, replication, metadata.RetentionPolicy{}); err != nil {
		cluster.t.Fatalf("create stream %s: %v", name, err)
	}
	return cluster.placeStandby(name)
}

// placeStandby names every replica as the stream's standby replica set, as
// the controller does after each segment is opened.
func (cluster *testCluster) placeStandby(name string) metadata.StreamRecord {
	cluster.t.Helper()
	stream, err := cluster.store.UpdateStream(context.WithoutCancel(cluster.t.Context()), name, func(stream *metadata.StreamRecord) error {
		if stream.StandbyReplicas == nil {
			stream.StandbyReplicas = cluster.replicaIDs()
		}
		return nil
	})
	if err != nil {
		cluster.t.Fatalf("place a standby for %s: %v", name, err)
	}
	return stream
}

// fixtureWriter writes a stream the way a writer does, one step at a time
// and with nothing running in the background, so that a test of the reader
// decides exactly what is in the stream and on which replica.
type fixtureWriter struct {
	cluster    *testCluster
	streamName string
	streamID   uint64
	epoch      uint64
	// segment is the open segment.
	segment replica.Descriptor
	// nextSequence is the sequence number the next record gets.
	nextSequence uint64
}

// openFixtureWriter claims the stream's next epoch and opens a segment.
func (cluster *testCluster) openFixtureWriter(streamName string) *fixtureWriter {
	cluster.t.Helper()
	ctx := cluster.t.Context()
	before, err := cluster.store.GetStream(ctx, streamName)
	if err != nil {
		cluster.t.Fatalf("read stream: %v", err)
	}
	claimed, err := cluster.store.ClaimEpoch(ctx, streamName, before.Epoch, "fixture", 0)
	if err != nil {
		cluster.t.Fatalf("claim the epoch: %v", err)
	}
	writer := &fixtureWriter{cluster: cluster, streamName: streamName, streamID: claimed.StreamID, epoch: claimed.Epoch}
	writer.openSegment()
	return writer
}

// openSegment opens the next segment on the standby replica set.
func (writer *fixtureWriter) openSegment() {
	writer.cluster.t.Helper()
	writer.cluster.placeStandby(writer.streamName)
	_, opened, err := writer.cluster.store.OpenSegment(writer.cluster.t.Context(), writer.streamName, writer.epoch)
	if err != nil {
		writer.cluster.t.Fatalf("open a segment: %v", err)
	}
	writer.segment = replica.Descriptor{
		Address:       frame.SegmentAddress{StreamID: writer.streamID, SegmentID: opened.SegmentID},
		Epoch:         opened.Epoch,
		FirstSequence: opened.FirstSequence,
	}
	writer.nextSequence = opened.FirstSequence
}

// append stores one batch of records on the given replicas, all of them if
// none is given, and tells them that it is committed.
func (writer *fixtureWriter) append(payloads []string, onReplicas ...*fakeReplica) {
	writer.cluster.t.Helper()
	if len(onReplicas) == 0 {
		onReplicas = writer.cluster.replicas
	}
	encoded := make([][]byte, 0, len(payloads))
	for _, payload := range payloads {
		encoded = append(encoded, []byte(payload))
	}
	batch, err := frame.Encode(writer.segment.Address, writer.nextSequence, encoded)
	if err != nil {
		writer.cluster.t.Fatalf("encode a batch: %v", err)
	}
	writer.nextSequence = batch.LastSequence() + 1
	for _, fake := range onReplicas {
		client := writer.cluster.client.replicas.Connect(fake.replicaID, fake.address)
		if _, err := client.Append(writer.cluster.t.Context(), writer.segment, writer.epoch, batch.Frame, batch.LastSequence()); err != nil {
			writer.cluster.t.Fatalf("append to %s: %v", fake.replicaID, err)
		}
	}
}

// roll seals the open segment at the last record and opens the next.
func (writer *fixtureWriter) roll() {
	writer.cluster.t.Helper()
	writer.seal()
	writer.openSegment()
}

// seal seals the open segment at the last record, in the metadata store
// and on every replica that holds it.
func (writer *fixtureWriter) seal() {
	writer.cluster.t.Helper()
	ctx := writer.cluster.t.Context()
	sealedEnd := writer.nextSequence - 1
	if _, err := writer.cluster.store.SealSegment(ctx, writer.streamName, writer.epoch, writer.segment.Address.SegmentID, sealedEnd, 1); err != nil {
		writer.cluster.t.Fatalf("seal: %v", err)
	}
	for _, fake := range writer.cluster.replicas {
		if _, isHeld := fake.localEndOf(writer.segment.Address); !isHeld {
			continue
		}
		client := writer.cluster.client.replicas.Connect(fake.replicaID, fake.address)
		if err := client.Seal(ctx, writer.segment.Address, sealedEnd); err != nil {
			writer.cluster.t.Fatalf("seal on %s: %v", fake.replicaID, err)
		}
	}
}

// payloadsOf returns the payloads of records as strings, for comparisons.
func payloadsOf(records []Record) []string {
	payloads := make([]string, 0, len(records))
	for _, record := range records {
		payloads = append(payloads, string(record.Payload))
	}
	return payloads
}

// sequencesOf returns the sequence numbers of records.
func sequencesOf(records []Record) []uint64 {
	sequences := make([]uint64, 0, len(records))
	for _, record := range records {
		sequences = append(sequences, record.Sequence)
	}
	return sequences
}
