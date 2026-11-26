package testcluster

import "testing"

// StartEtcd starts a one-member etcd that lives as long as the test, and
// returns its client URL. It skips the test if no etcd program is named
// (see the package documentation).
func StartEtcd(t testing.TB) string {
	t.Helper()
	program := binary(t, EtcdBinaryVariable)
	directory := t.TempDir()
	clientURL := "http://" + FreeAddress(t)
	peerURL := "http://" + FreeAddress(t)
	process := start(t, directory, "etcd", program,
		"--name", "test",
		"--data-dir", directory+"/data",
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "test="+peerURL,
		// The data is thrown away with the test; waiting for the disk
		// would only make every test slower.
		"--unsafe-no-fsync",
		"--log-level", "error",
	)
	waitUntilHealthy(t, process, clientURL+"/health")
	return clientURL
}
