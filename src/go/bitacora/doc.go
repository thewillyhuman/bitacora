// Package bitacora is the Go client of bitacora, a replicated log.
//
// A stream is one continuous sequence of records, numbered from 1 without
// gaps. One writer at a time appends to it; an append returns once the
// record is stored on a write quorum of replicas. Any number of readers
// read it back, or follow it as records commit.
//
// The client finds a cluster through its metadata store, etcd, and talks to
// replicas over gRPC. It is a native implementation of the protocol in
// docs/protocol of the bitacora repository, not a binding to the Rust
// client, and the two interoperate: a stream written by one is read by the
// other, and a writer of one takes over from a writer of the other.
//
//	client, err := bitacora.Connect(bitacora.Config{
//		EtcdEndpoints: []string{"http://127.0.0.1:2379"},
//	})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	writer, err := client.OpenWriter(ctx, "orders", bitacora.WriterOptions{Identity: "compute-1"})
//	if err != nil {
//		return err
//	}
//	defer writer.Close(ctx)
//	sequence, err := writer.Append(ctx, []byte("first record"))
//
//	for record, err := range client.OpenReader("orders").Follow(ctx, sequence) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(record.Sequence, string(record.Payload))
//	}
//
// # Outcomes of an append
//
// An append has three outcomes. It succeeds: the record is on a write
// quorum and every reader will see it. It fails with [ErrFenced]: another
// writer has taken the stream and this one must stop. Or it fails with
// [ErrIndeterminate]: the writer could not learn the outcome, and the
// record may or may not be in the stream. Indeterminate outcomes are
// unavoidable in a replicated log; the application reads the stream to find
// out, as after a crash between writing and acknowledging.
//
// # Errors
//
// Every error wraps one of the Err values of this package, so that callers
// decide with errors.Is what to do, and keeps its cause for the log. An
// operation that was given a context and ends because of it returns the
// context's error.
//
// # What is not implemented
//
// There is no S3 object store, only a local directory. There is no TLS,
// because the protocol has none yet.
package bitacora
