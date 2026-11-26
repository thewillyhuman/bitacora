// Package testcluster starts the real programs that tests of the Go client
// talk to: etcd, the replica daemon, the controller daemon and the bita
// command-line tools. Everything listens on loopback ports and keeps its
// files in the test's temporary directory, and is stopped when the test
// ends.
//
// The programs are found through environment variables that the Bazel test
// targets set. Under plain `go test` they are not set and the test is
// skipped with a message that says how to run it.
package testcluster

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The environment variables that name the programs.
const (
	// EtcdBinaryVariable names the etcd server.
	EtcdBinaryVariable = "BITA_ETCD_BINARY"
	// ReplicaBinaryVariable names the replica daemon, bita-replica.
	ReplicaBinaryVariable = "BITA_REPLICA_BINARY"
	// ControllerBinaryVariable names the controller daemon, bita-controller.
	ControllerBinaryVariable = "BITA_CONTROLLER_BINARY"
	// CommandLineBinaryVariable names the command-line tool, bita.
	CommandLineBinaryVariable = "BITA_CLI_BINARY"
	// BenchmarkBinaryVariable names the benchmark tool, bita-bench, which
	// tests use as a Rust writer that stays open.
	BenchmarkBinaryVariable = "BITA_BENCH_BINARY"
)

// startupTimeout is how long a program may take until it answers.
const startupTimeout = 30 * time.Second

// binary returns the absolute path of the program that an environment
// variable names. If the variable is not set, the test is skipped, except
// under Bazel, which always provides the programs: there a missing one
// means the build file is wrong, and a test that silently does nothing must
// not look green.
func binary(t testing.TB, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		if os.Getenv("BAZEL_TEST") != "" {
			t.Fatalf("%s is not set: the test target must name the program in its env", variable)
		}
		t.Skipf("%s is not set: run the test with bazel, or set the variable to the program's path", variable)
	}
	if !filepath.IsAbs(path) {
		// Bazel gives paths relative to the root of the runfiles tree, and
		// a Go test runs in its package's directory, not there.
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), path)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s does not name a program: %v", variable, err)
	}
	return path
}

// Process is a program started for a test.
type Process struct {
	// name says what the program is, in messages.
	name string
	// command is the running program.
	command *exec.Cmd
	// exited is closed when the program has ended.
	exited chan struct{}
	// logPath is the file that holds what the program printed.
	logPath string
}

// start runs a program until the test ends. What it prints goes to a file
// in directory, and is shown if the test fails.
func start(t testing.TB, directory, name, program string, arguments ...string) *Process {
	t.Helper()
	logPath := filepath.Join(directory, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("%s: cannot create its log: %v", name, err)
	}
	command := exec.Command(program, arguments...)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		t.Fatalf("%s: cannot start %s: %v", name, program, err)
	}
	// The child has its own copy of the file.
	_ = logFile.Close()
	process := &Process{name: name, command: command, exited: make(chan struct{}), logPath: logPath}
	go func() {
		_ = command.Wait()
		close(process.exited)
	}()
	t.Cleanup(func() {
		process.Kill()
		if t.Failed() {
			t.Logf("---- %s ----\n%s", name, process.logTail())
		}
	})
	return process
}

// Kill stops the program at once, as a crash would, and waits until it is
// gone. Killing a program that has ended does nothing.
func (process *Process) Kill() {
	_ = process.command.Process.Kill()
	<-process.exited
}

// logTail returns the end of what the program printed.
func (process *Process) logTail() []byte {
	const tailBytes = 8 * 1024
	content, err := os.ReadFile(process.logPath)
	if err != nil {
		return fmt.Appendf(nil, "(cannot read the log: %v)", err)
	}
	if len(content) > tailBytes {
		content = content[len(content)-tailBytes:]
	}
	return bytes.TrimSpace(content)
}

// freeAddress returns a loopback "host:port" that nothing listens on now.
// Another process could take the port before the caller does; tests accept
// that small chance, as there is no way to hand a listening port to a
// program that opens its own.
func freeAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot find a free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// waitUntilHealthy waits until an HTTP GET of url answers 200, which is how
// etcd and the daemons say that they are ready. It fails the test if the
// program ends first or does not get there in time.
func waitUntilHealthy(t testing.TB, process *Process, url string) {
	t.Helper()
	httpClient := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(startupTimeout)
	for {
		response, err := httpClient.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-process.exited:
			t.Fatalf("%s ended before it was ready:\n%s", process.name, process.logTail())
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not ready after %v:\n%s", process.name, startupTimeout, process.logTail())
		}
	}
}
