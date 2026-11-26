package interop

import (
	"testing"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
	"github.com/thewillyhuman/bitacora/src/go/internal/testcluster"
)

// TestTheExampleOfThePageRuns runs the program that docs/clients/go.md
// shows, against a real cluster, twice: what a reader of the page copies
// must work, also the second time, when the stream is already there.
func TestTheExampleOfThePageRuns(t *testing.T) {
	t.Parallel()
	cluster := testcluster.Start(t, testcluster.Options{})

	for run, want := range []string{
		"1 record 0\n2 record 1\n3 record 2\n",
		"4 record 0\n5 record 1\n6 record 2\n",
	} {
		printed, err := cluster.Quickstart(t)
		if err != nil {
			t.Fatalf("run %d of the example failed: %v", run+1, err)
		}
		if printed != want {
			t.Errorf("run %d of the example printed %q, want %q", run+1, printed, want)
		}
	}

	// The Rust reader sees what the example wrote.
	sameRecords(t, "the Rust reader", readWithRust(t, cluster, "quickstart", "5", "1"),
		[]bitacora.Record{{Sequence: 5, Payload: []byte("record 1")}})
}
