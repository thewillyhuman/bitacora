package interop

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

// rustDescription is what `bita stream describe --output json` prints, as
// far as these tests look at it.
type rustDescription struct {
	Name                string   `json:"name"`
	StreamID            uint64   `json:"stream_id"`
	Epoch               uint64   `json:"epoch"`
	Replicas            int      `json:"replicas"`
	WriteQuorum         int      `json:"write_quorum"`
	KeepSealedForSecond *uint64  `json:"keep_sealed_for_seconds"`
	LowWaterMark        uint64   `json:"low_water_mark"`
	TruncateBefore      *uint64  `json:"truncate_before"`
	Segments            int      `json:"segments"`
	StandbyReplicas     []string `json:"standby_replicas"`
	Materializers       []struct {
		Name         string `json:"name"`
		Checkpoint   uint64 `json:"checkpoint"`
		FencingToken uint64 `json:"fencing_token"`
	} `json:"materializers"`
}

// describeWithRust describes a stream through the bita tool.
func describeWithRust(t *testing.T, cluster *testcluster.Cluster, stream string) rustDescription {
	t.Helper()
	var description rustDescription
	output := cluster.Bita(t, "stream", "describe", stream)
	if err := json.Unmarshal([]byte(output), &description); err != nil {
		t.Fatalf("bita stream describe printed %q: %v", output, err)
	}
	return description
}

func TestRustAndTheControllerUnderstandWhatGoAdministers(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	client := connect(t, cluster)

	// Go creates the stream. It places no segment itself; the controller
	// reads the record Go wrote and places one.
	created, err := client.CreateStream(t.Context(), "orders", bitacora.StreamOptions{KeepSealedFor: 2 * time.Hour})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	if _, err := client.CreateStream(t.Context(), "orders", bitacora.StreamOptions{}); !errors.Is(err, bitacora.ErrStreamExists) {
		t.Errorf("creating the stream again = %v, want ErrStreamExists", err)
	}
	waitForStream(t, cluster, "orders", "the placement of the first segment", func(stream metadata.StreamRecord) bool {
		return len(stream.StandbyReplicas) == 3
	})
	rustView := describeWithRust(t, cluster, "orders")
	if rustView.Name != "orders" || rustView.StreamID != created.ID || rustView.Replicas != 3 || rustView.WriteQuorum != 2 ||
		rustView.KeepSealedForSecond == nil || *rustView.KeepSealedForSecond != 7200 || rustView.LowWaterMark != 1 {
		t.Errorf("the Rust tool describes the stream Go created as %+v", rustView)
	}

	// Both writers use the stream Go created, and a second stream that Rust
	// creates gets the next identifier from the counter Go advanced.
	appendWithRust(t, cluster, "orders", "one", "two", "three")
	writer := openWriter(t, client, "orders", bitacora.WriterOptions{Takeover: bitacora.ForceTakeover})
	if sequence, err := writer.Append(t.Context(), []byte("four")); err != nil || sequence != 4 {
		t.Fatalf("Append = %d, %v; want 4", sequence, err)
	}
	if err := writer.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cluster.Bita(t, "stream", "create", "payments")
	payments, err := client.DescribeStream(t.Context(), "payments")
	if err != nil || payments.ID != created.ID+1 {
		t.Errorf("the stream Rust created is %+v, %v; want identifier %d", payments, err, created.ID+1)
	}

	// Go describes what both wrote.
	described, err := client.DescribeStream(t.Context(), "orders")
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}
	if described.Epoch != 2 || described.Writer != nil || len(described.Segments) != 2 || described.KeepSealedFor != 2*time.Hour {
		t.Errorf("Go describes the stream as %+v; want epoch 2, no writer, two segments, kept two hours", described)
	}
	if first, second := described.Segments[0], described.Segments[1]; !first.IsSealed || first.LastSequence != 3 ||
		!second.IsSealed || second.FirstSequence != 4 || second.LastSequence != 4 || len(first.Replicas) != 3 {
		t.Errorf("Go describes the segments as %+v", described.Segments)
	}

	// A materializer Go registers is one the Rust tool lists, with the
	// checkpoint record Go wrote; one the Rust tool registers, Go lists.
	if err := client.RegisterMaterializer(t.Context(), "orders", "pages"); err != nil {
		t.Fatalf("RegisterMaterializer: %v", err)
	}
	cluster.Bita(t, "stream", "register-materializer", "orders", "index")
	rustView = describeWithRust(t, cluster, "orders")
	if len(rustView.Materializers) != 2 || rustView.Materializers[0].Name != "index" || rustView.Materializers[1].Name != "pages" ||
		rustView.Materializers[1].Checkpoint != 0 || rustView.Materializers[1].FencingToken != 0 {
		t.Errorf("the Rust tool lists the materializers as %+v; want index and pages at checkpoint 0", rustView.Materializers)
	}
	described, err = client.DescribeStream(t.Context(), "orders")
	wantMaterializers := []bitacora.MaterializerDescription{{Name: "index"}, {Name: "pages"}}
	if err != nil || !slices.Equal(described.Materializers, wantMaterializers) {
		t.Errorf("Go lists the materializers as %+v, %v; want %+v", described.Materializers, err, wantMaterializers)
	}
	cluster.Bita(t, "stream", "unregister-materializer", "orders", "pages")
	if err := client.UnregisterMaterializer(t.Context(), "orders", "index"); err != nil {
		t.Fatalf("UnregisterMaterializer: %v", err)
	}
	if rustView = describeWithRust(t, cluster, "orders"); len(rustView.Materializers) != 0 {
		t.Errorf("after both were unregistered the Rust tool lists %+v", rustView.Materializers)
	}

	// Go deletes the stream; the Rust tool no longer finds it, and the
	// controller, which reads the note Go left, cleans up and removes it.
	if err := client.DeleteStream(t.Context(), "orders"); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}
	if _, err := cluster.BitaWithInput(t, "", "stream", "describe", "orders"); err == nil || !strings.Contains(err.Error(), "orders") {
		t.Errorf("the Rust tool describing the deleted stream = %v, want a failure that names it", err)
	}
	if _, err := client.OpenReader("orders").ReadRange(t.Context(), 1, 4); !errors.Is(err, bitacora.ErrStreamNotFound) {
		t.Errorf("reading the deleted stream = %v, want ErrStreamNotFound", err)
	}
	eventually(t, "the controller to finish the deletion Go asked for", func() bool {
		return !etcdHasKey(t, cluster, "deleted-streams/1")
	})
}

func TestTheControllerTruncatesWhereGoAsked(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})
	client := connect(t, cluster)
	if _, err := client.CreateStream(t.Context(), "orders", bitacora.StreamOptions{}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	// Three sealed segments of two records each.
	for _, payloads := range [][]string{{"one", "two"}, {"three", "four"}, {"five", "six"}} {
		appendWithRust(t, cluster, "orders", payloads...)
	}
	reader := client.OpenReader("orders")

	// Go allows everything below record 3 to go: the first segment. The
	// controller reads the position Go recorded and deletes the segment.
	if err := client.TruncateBefore(t.Context(), "orders", 3); err != nil {
		t.Fatalf("TruncateBefore: %v", err)
	}
	if rustView := describeWithRust(t, cluster, "orders"); rustView.TruncateBefore == nil || *rustView.TruncateBefore != 3 {
		t.Errorf("the Rust tool shows the truncation position as %v, want 3", rustView.TruncateBefore)
	}
	eventually(t, "the controller to truncate", func() bool {
		lowWaterMark, err := reader.LowWaterMark(t.Context())
		return err == nil && lowWaterMark > 1
	})
	lowWaterMark, err := reader.LowWaterMark(t.Context())
	if err != nil || lowWaterMark != 3 {
		t.Fatalf("LowWaterMark = %d, %v; want 3", lowWaterMark, err)
	}

	// What is gone is refused with the mark, never read as an empty range;
	// what is left reads as before.
	_, err = reader.ReadRange(t.Context(), 1, 6)
	if truncated, isTruncated := errors.AsType[*bitacora.TruncatedError](err); !isTruncated || truncated.LowWaterMark != 3 {
		t.Errorf("reading below the low-water mark = %v, want a TruncatedError at 3", err)
	}
	records, err := reader.ReadRange(t.Context(), lowWaterMark, 6)
	if err != nil {
		t.Fatalf("ReadRange from the low-water mark: %v", err)
	}
	sameRecords(t, "the Go reader, after truncation", records, []bitacora.Record{
		{Sequence: 3, Payload: []byte("three")},
		{Sequence: 4, Payload: []byte("four")},
		{Sequence: 5, Payload: []byte("five")},
		{Sequence: 6, Payload: []byte("six")},
	})
}
