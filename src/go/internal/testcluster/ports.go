package testcluster

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
)

// This file hands out the loopback ports that the programs of a test
// listen on.
//
// A program that opens its own sockets must be told a free port. Listening
// on port 0, reading the port the operating system chose and closing the
// socket again finds one, but does not keep it: until the program has bound
// the port itself, the operating system may hand it to the next caller that
// asks for "any port", and tests that run in parallel all ask at once. The
// Rust tests that found their ports that way failed in up to 6 of 30
// parallel runs.
//
// The ports of this file cannot be lost like that. They come from a range
// below the ports that Linux (32768 and up) and macOS (49152 and up) choose
// from, so only a program that names a port can be in it. The range is cut
// into blocks of fifty. The first port of a block is its guard port: to
// claim a block is to listen on its guard port on 127.0.0.1 and to keep
// listening, which only one socket on the machine can do, so it is the
// operating system that keeps two test processes apart. The other
// forty-nine ports are handed out in order, each after a check that it can
// be bound now; one that cannot, because a program left behind by a killed
// test still listens there, is passed over.
//
// A test process keeps the blocks it claims until it ends, when the
// operating system closes their listeners. It claims a second block only
// when the first has no port left.
//
// Three test suites of the repository take ports this way, each from a
// range of its own, so that they need no common rule for claiming:
//
//	20000 to 28999  development/cluster_test.sh
//	29000 to 30849  Rust tests, src/rust/bitacora-testports
//	30850 to 32699  Go tests, this file

const (
	// firstTestPort is the first port of the range the blocks are cut from.
	firstTestPort = 30850
	// blockPortCount is how many ports a block has, its guard port
	// included. A cluster of three replicas, a controller and an etcd
	// needs nine.
	blockPortCount = 50
	// blockCount is how many blocks there are. The last one ends at port
	// 32699, below the ports Linux hands out by itself.
	blockCount = 37
)

// portBlock is fifty neighbouring loopback ports that belong to this
// process for as long as guard is open.
type portBlock struct {
	// guard listens on the block's first port. Nothing is ever accepted
	// from it; it is open so that no other process can claim the block.
	guard net.Listener
	// index says which block this is, counted from zero at the start of
	// the range.
	index int
	// portsTried counts the ports of the block that were looked at so far,
	// handed out or passed over.
	portsTried int
}

// sharedPorts holds the blocks this test process has claimed. The last one
// is the one ports are taken from.
var sharedPorts struct {
	mutex  sync.Mutex
	blocks []*portBlock
}

// FreeAddress returns a loopback "host:port" that nothing listens on, for a
// program the test starts or for a test that needs an address nobody
// answers at. No other test, in this process or in any other, is given the
// same address while this process runs. It fails the test if every block of
// ports on the machine is claimed.
func FreeAddress(t testing.TB) string {
	t.Helper()
	sharedPorts.mutex.Lock()
	defer sharedPorts.mutex.Unlock()
	if count := len(sharedPorts.blocks); count > 0 {
		if port, ok := sharedPorts.blocks[count-1].take(); ok {
			return loopbackAddress(port)
		}
	}
	block, err := claimPortBlock(os.Getpid())
	if err != nil {
		t.Fatalf("cannot find a free port: %v", err)
	}
	sharedPorts.blocks = append(sharedPorts.blocks, block)
	port, ok := block.take()
	if !ok {
		t.Fatalf("cannot find a free port: every port of the block at %d is in use by another program",
			guardPortOf(block.index))
	}
	return loopbackAddress(port)
}

// claimPortBlock claims the first block that nobody holds. It tries the
// block firstIndex first, taken round the range, and goes on from there.
// FreeAddress passes the process identifier, so that test processes that
// start together begin at different blocks; tests name the block.
func claimPortBlock(firstIndex int) (*portBlock, error) {
	for step := range blockCount {
		index := blockTriedAt(firstIndex, step)
		guard, err := net.Listen("tcp", loopbackAddress(guardPortOf(index)))
		if err == nil {
			return &portBlock{guard: guard, index: index}, nil
		}
	}
	return nil, fmt.Errorf("all %d blocks of test ports (%d to %d) are claimed: "+
		"more tests that start programs run at once than there are blocks, or other programs listen in that range",
		blockCount, firstTestPort, guardPortOf(blockCount-1)+blockPortCount-1)
}

// take hands out the next port of the block that nothing listens on. It
// reports false once the block has no port left: each was handed out or is
// in use by another program.
func (block *portBlock) take() (port int, ok bool) {
	for block.portsTried < blockPortCount-1 {
		block.portsTried++
		port := guardPortOf(block.index) + block.portsTried
		listener, err := net.Listen("tcp", loopbackAddress(port))
		if err != nil {
			continue
		}
		_ = listener.Close()
		return port, true
	}
	return 0, false
}

// guardPortOf returns the guard port of a block: its first port, which the
// owner of the block listens on and nobody is given.
func guardPortOf(index int) int {
	return firstTestPort + index*blockPortCount
}

// blockTriedAt returns the block a search looks at in its step number
// step, counted from zero, when it starts at firstIndex: the blocks in
// order, going on at the first one after the last.
func blockTriedAt(firstIndex, step int) int {
	return (firstIndex%blockCount + step) % blockCount
}

// loopbackAddress returns "127.0.0.1:port".
func loopbackAddress(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}
