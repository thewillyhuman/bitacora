// Package bitacora is the Go client of bitacora, a replicated log.
//
// A stream is one continuous sequence of records, numbered from 1 without
// gaps. One writer at a time appends to it; any number of readers read it
// back, or follow it as records commit.
//
// The client finds a cluster through its metadata store, etcd, and talks to
// replicas over gRPC. It is a native implementation of the protocol in
// docs/protocol of the bitacora repository, not a binding to the Rust
// client, and the two interoperate on the same cluster and the same stream.
//
//	client, err := bitacora.Connect(bitacora.Config{
//		EtcdEndpoints: []string{"http://127.0.0.1:2379"},
//	})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	for record, err := range client.OpenReader("orders").Follow(ctx, 1) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(record.Sequence, string(record.Payload))
//	}
//
// # Errors
//
// Every error wraps one of the Err values of this package, so that callers
// decide with errors.Is what to do, and keeps its cause for the log.
//
// # What is not implemented
//
// There is no S3 object store, only a local directory. There is no TLS,
// because the protocol has none yet.
package bitacora
