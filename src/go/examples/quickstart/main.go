// Command quickstart creates a stream, appends three records to it, reads
// them back and follows the stream for a moment. It is the example that
// docs/clients/go.md shows.
//
//	go run ./examples/quickstart http://127.0.0.1:2379
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thewillyhuman/bitacora/src/go/bitacora"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s <etcd endpoint>", os.Args[0])
	}
	if err := run(context.Background(), os.Args[1]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, etcdEndpoint string) error {
	// Connect sends nothing yet. A cluster that is down shows on first use.
	client, err := bitacora.Connect(bitacora.Config{EtcdEndpoints: []string{etcdEndpoint}})
	if err != nil {
		return err
	}
	defer client.Close()

	// A stream is created once; a second run finds it there.
	_, err = client.CreateStream(ctx, "quickstart", bitacora.StreamOptions{})
	if err != nil && !errors.Is(err, bitacora.ErrStreamExists) {
		return err
	}

	// Become the stream's one writer. If another writer holds the stream,
	// this waits for its lease to lapse.
	writer, err := client.OpenWriter(ctx, "quickstart", bitacora.WriterOptions{Identity: "quickstart"})
	if err != nil {
		return err
	}
	// Close seals the writer's segment and gives the lease back, so that
	// the next writer starts at once. Closing twice is harmless.
	defer writer.Close(ctx)

	var first, last uint64
	for number := range 3 {
		// Append returns once the record is on a write quorum of replicas.
		// An error that wraps ErrFenced means that another writer took the
		// stream; one that wraps ErrIndeterminate, that the record may or
		// may not be in it.
		sequence, err := writer.Append(ctx, fmt.Appendf(nil, "record %d", number))
		if err != nil {
			return err
		}
		if number == 0 {
			first = sequence
		}
		last = sequence
	}
	if err := writer.Close(ctx); err != nil {
		return err
	}

	// Any number of readers read what is committed.
	reader := client.OpenReader("quickstart")
	records, err := reader.ReadRange(ctx, first, last)
	if err != nil {
		return err
	}
	for _, record := range records {
		fmt.Printf("%d %s\n", record.Sequence, record.Payload)
	}

	// Follow yields records as they commit, until its context ends. Nobody
	// is writing here, so this one waits for two seconds and sees nothing.
	following, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	for record, err := range reader.Follow(following, last+1) {
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("%d %s\n", record.Sequence, record.Payload)
	}
	return nil
}
