package testcluster

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// EtcdKeyPrefix is the etcd key prefix of every cluster this package
// starts.
const EtcdKeyPrefix = "/bitacora/"

// wholeDiskPercent switches off the daemons' disk-usage limits, so that a
// nearly full disk on the machine does not fail a test.
const wholeDiskPercent = 100

// Options say how a cluster differs from the usual one.
type Options struct {
	// IsTieringAtOnce makes the controller upload a segment to the object
	// store as soon as it is sealed and fully replicated, and release the
	// replicas' copies right after. Without it, a test never sees tiering.
	IsTieringAtOnce bool
}

// Cluster is a cluster of real processes: one etcd, three replica daemons
// and one controller daemon, as built from the Rust sources.
type Cluster struct {
	// EtcdEndpoint is the client URL of the cluster's etcd.
	EtcdEndpoint string
	// ObjectStoreDirectory is the root of the cluster's object store, of
	// the local-directory kind.
	ObjectStoreDirectory string
	// directory holds every file of the cluster.
	directory string
	// commandLineConfigPath is the configuration file that tells the bita
	// tools where the cluster is.
	commandLineConfigPath string
	// commandLine is the bita program.
	commandLine string
	// replicas are the replica daemons, by number.
	replicas []*Process
}

// Start starts a cluster that lives as long as the test. It skips the test
// if the programs are not named (see the package documentation).
func Start(t testing.TB, options Options) *Cluster {
	t.Helper()
	replicaProgram := binary(t, ReplicaBinaryVariable)
	controllerProgram := binary(t, ControllerBinaryVariable)
	commandLine := binary(t, CommandLineBinaryVariable)
	directory := t.TempDir()
	cluster := &Cluster{
		EtcdEndpoint:          StartEtcd(t),
		ObjectStoreDirectory:  filepath.Join(directory, "object-store"),
		directory:             directory,
		commandLineConfigPath: filepath.Join(directory, "bita.toml"),
		commandLine:           commandLine,
	}
	// The tools take the cluster table at the top level of their file; the
	// daemons take it under [cluster].
	writeFile(t, cluster.commandLineConfigPath, fmt.Sprintf(
		"etcd_endpoints = [%q]\netcd_key_prefix = %q\n\n[object_store]\nkind = \"local_directory\"\npath = %q\n",
		cluster.EtcdEndpoint, EtcdKeyPrefix, cluster.ObjectStoreDirectory))
	clusterTable := fmt.Sprintf(
		"[cluster]\netcd_endpoints = [%q]\netcd_key_prefix = %q\n\n[cluster.object_store]\nkind = \"local_directory\"\npath = %q\n",
		cluster.EtcdEndpoint, EtcdKeyPrefix, cluster.ObjectStoreDirectory)

	const replicaCount = 3
	readinessURLs := make([]string, 0, replicaCount)
	for number := range replicaCount {
		name := fmt.Sprintf("replica-%d", number)
		serviceAddress := freeAddress(t)
		observabilityAddress := freeAddress(t)
		configPath := filepath.Join(directory, name+".toml")
		// What an operator would write, with a registration every second
		// and small segment files.
		writeFile(t, configPath, fmt.Sprintf(
			"replica_id = %q\ndata_directory = %q\nlisten_address = %q\nadvertised_address = %q\n"+
				"failure_domains = [%q]\nmetrics_listen_address = %q\nregistration_interval_seconds = 1\n\n"+
				"[storage]\npreallocated_segment_bytes = 1048576\n"+
				"refuse_new_segments_at_used_percent = %d\nrefuse_appends_at_used_percent = %d\n\n%s",
			name, filepath.Join(directory, name), serviceAddress, serviceAddress,
			"host-of-"+name, observabilityAddress, wholeDiskPercent, wholeDiskPercent, clusterTable))
		cluster.replicas = append(cluster.replicas, start(t, directory, name, replicaProgram, "--config", configPath))
		readinessURLs = append(readinessURLs, "http://"+observabilityAddress+"/readyz")
	}

	controllerObservabilityAddress := freeAddress(t)
	tiering := ""
	if options.IsTieringAtOnce {
		tiering = "tiering_delay_seconds = 0\nhot_read_window_seconds = 0\n"
	}
	controllerConfigPath := filepath.Join(directory, "controller.toml")
	// A pass five times a second, so that a test does not wait long for a
	// standby segment or an upload.
	writeFile(t, controllerConfigPath, fmt.Sprintf(
		"identity = \"controller\"\nmetrics_listen_address = %q\npass_interval_milliseconds = 200\n"+
			"replica_lost_after_seconds = 60\n%s\n"+
			"[placement]\nliveness_window_seconds = 3\nrefuse_at_used_percent = %d\n\n%s",
		controllerObservabilityAddress, tiering, wholeDiskPercent, clusterTable))
	controller := start(t, directory, "controller", controllerProgram, "--config", controllerConfigPath)

	for number, readinessURL := range readinessURLs {
		waitUntilHealthy(t, cluster.replicas[number], readinessURL)
	}
	waitUntilHealthy(t, controller, "http://"+controllerObservabilityAddress+"/readyz")
	return cluster
}

// Bita runs the bita command-line tool against the cluster with JSON
// output and returns what it printed. It fails the test if the tool does.
func (cluster *Cluster) Bita(t testing.TB, arguments ...string) string {
	t.Helper()
	printed, err := cluster.BitaWithInput(t, "", arguments...)
	if err != nil {
		t.Fatalf("bita %s: %v", strings.Join(arguments, " "), err)
	}
	return printed
}

// BitaWithInput runs the bita command-line tool with input on its standard
// input, and returns what it printed. The error, if the tool failed, holds
// what it wrote to standard error.
func (cluster *Cluster) BitaWithInput(t testing.TB, input string, arguments ...string) (string, error) {
	t.Helper()
	return run(cluster.commandLine, input, cluster.toolArguments(arguments)...)
}

// StartBenchmark starts the bita-bench tool against the cluster and returns
// while it runs. Tests use `bita-bench append` as a Rust writer that stays
// open, which the bita tool, appending and stepping down at once, is not.
func (cluster *Cluster) StartBenchmark(t testing.TB, arguments ...string) *Process {
	t.Helper()
	program := binary(t, BenchmarkBinaryVariable)
	return start(t, cluster.directory, "bita-bench", program, cluster.toolArguments(arguments)...)
}

// toolArguments puts in front of arguments what tells a bita tool where the
// cluster is and to print JSON.
func (cluster *Cluster) toolArguments(arguments []string) []string {
	return append([]string{"--config", cluster.commandLineConfigPath, "--output", "json"}, arguments...)
}

// run runs a program to its end and returns its standard output. If it
// exits with a failure, the error holds its exit status and what it wrote
// to standard error.
func run(program, input string, arguments ...string) (string, error) {
	command := exec.Command(program, arguments...)
	command.Stdin = strings.NewReader(input)
	var standardOutput, standardError bytes.Buffer
	command.Stdout = &standardOutput
	command.Stderr = &standardError
	if err := command.Run(); err != nil {
		return standardOutput.String(), fmt.Errorf("%w: %s", err, bytes.TrimSpace(standardError.Bytes()))
	}
	return standardOutput.String(), nil
}

// writeFile writes a file of the cluster.
func writeFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("cannot write %s: %v", path, err)
	}
}
