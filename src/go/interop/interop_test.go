// Package interop tests the Go client against the Rust implementation: a
// cluster of the real daemons, written and read by the Rust tools on one
// side and by the Go client on the other. What passes here is what makes
// the two clients interchangeable; the unit tests of each package cannot
// show that.
package interop

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

// patience is how long a test waits for something the cluster does on its
// own, such as placing a segment or yielding a record. It is far longer
// than the cluster needs, so that only a real failure exhausts it.
const patience = 30 * time.Second

// connect returns a Go client for the cluster, closed when the test ends.
func connect(t *testing.T, cluster *testcluster.Cluster) *bitacora.Client {
	t.Helper()
	client, err := bitacora.Connect(bitacora.Config{
		EtcdEndpoints: []string{cluster.EtcdEndpoint},
		EtcdKeyPrefix: testcluster.EtcdKeyPrefix,
		ObjectStore:   bitacora.NewLocalObjectStore(cluster.ObjectStoreDirectory),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// rustRecord is one record as `bita stream read --output json` prints it.
type rustRecord struct {
	Sequence   uint64 `json:"sequence"`
	PayloadHex string `json:"payload_hex"`
}

// readWithRust reads a range of a stream with the Rust reader, through the
// bita tool, and returns the records as the Go client would.
func readWithRust(t *testing.T, cluster *testcluster.Cluster, stream string, from, count string) []bitacora.Record {
	t.Helper()
	var printed struct {
		Records []rustRecord `json:"records"`
	}
	output := cluster.Bita(t, "stream", "read", stream, "--from", from, "--count", count)
	if err := json.Unmarshal([]byte(output), &printed); err != nil {
		t.Fatalf("bita stream read printed %q: %v", output, err)
	}
	records := make([]bitacora.Record, 0, len(printed.Records))
	for _, record := range printed.Records {
		payload, err := hex.DecodeString(record.PayloadHex)
		if err != nil {
			t.Fatalf("payload_hex %q: %v", record.PayloadHex, err)
		}
		records = append(records, bitacora.Record{Sequence: record.Sequence, Payload: payload})
	}
	return records
}

// rustAppend is what `bita stream append --output json` prints.
type rustAppend struct {
	Epoch         uint64 `json:"epoch"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
}

// appendWithRust appends records to a stream with the Rust writer, through
// the bita tool, taking the stream from whoever holds it.
func appendWithRust(t *testing.T, cluster *testcluster.Cluster, stream string, payloads ...string) rustAppend {
	t.Helper()
	arguments := append([]string{"stream", "append", stream, "--force"}, payloads...)
	var appended rustAppend
	output := cluster.Bita(t, arguments...)
	if err := json.Unmarshal([]byte(output), &appended); err != nil {
		t.Fatalf("bita stream append printed %q: %v", output, err)
	}
	return appended
}

// sameRecords fails the test unless got and want hold the same records.
func sameRecords(t *testing.T, what string, got, want []bitacora.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d records, want %d", what, len(got), len(want))
	}
	for index := range got {
		if got[index].Sequence != want[index].Sequence || string(got[index].Payload) != string(want[index].Payload) {
			t.Errorf("%s: record %d is %d %q, want %d %q", what, index,
				got[index].Sequence, got[index].Payload, want[index].Sequence, want[index].Payload)
		}
	}
}

// waitForStream reads a stream's record from etcd until it satisfies
// condition, and returns it.
func waitForStream(t *testing.T, cluster *testcluster.Cluster, name string, what string, condition func(metadata.StreamRecord) bool) metadata.StreamRecord {
	t.Helper()
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{cluster.EtcdEndpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("connect to etcd: %v", err)
	}
	defer etcd.Close()
	store := metadata.NewStore(metadata.NewEtcdStore(etcd, testcluster.EtcdKeyPrefix, 5*time.Second))
	deadline := time.Now().Add(patience)
	for {
		stream, err := store.GetStream(t.Context(), name)
		if err == nil && condition(stream) {
			return stream
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s: %s did not happen in %v (last read: %+v, %v)", name, what, patience, stream, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// follower is a Go reader that follows a stream in the background.
type follower struct {
	// records receives every record the follower yields.
	records chan bitacora.Record
	// ended receives the error the follower ended with.
	ended chan error
}

// follow starts following a stream from a sequence number. The follower
// stops when the test ends.
func follow(t *testing.T, client *bitacora.Client, stream string, from uint64) *follower {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	following := &follower{records: make(chan bitacora.Record, 4096), ended: make(chan error, 1)}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for record, err := range client.OpenReader(stream).Follow(ctx, from) {
			if err != nil {
				following.ended <- err
				return
			}
			following.records <- record
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return following
}

// next waits for the follower's next record.
func (following *follower) next(t *testing.T) bitacora.Record {
	t.Helper()
	select {
	case record := <-following.records:
		return record
	case err := <-following.ended:
		t.Fatalf("the follower ended: %v", err)
	case <-time.After(patience):
		t.Fatalf("the follower yielded nothing in %v", patience)
	}
	return bitacora.Record{}
}
