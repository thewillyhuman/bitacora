package testcluster

import (
	"fmt"
	"net"
	"slices"
	"testing"
)

// The tests in this file use real sockets on the loopback address, because
// what they check is what the operating system does with them. They do not
// run in parallel: a test that looks at one block must not have another
// test of this package claim it in between.

// claimForTest claims a block and gives it back when the test ends.
func claimForTest(t *testing.T, firstIndex int) *portBlock {
	t.Helper()
	block, err := claimPortBlock(firstIndex)
	if err != nil {
		t.Fatalf("claim a block: %v", err)
	}
	t.Cleanup(func() { _ = block.guard.Close() })
	return block
}

// listen binds a loopback port for the length of the test, as a program
// that was told to listen there would.
func listen(t *testing.T, port int) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("listen on port %d: %v", port, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestTheRangeStartsAtTheDocumentedPort(t *testing.T) {
	if first := guardPortOf(0); first != 30850 {
		t.Errorf("the first port of the range = %d, want 30850", first)
	}
}

// The Rust tests end at 30849, and Linux hands out ports from 32768 on.
func TestTheRangeEndsAtTheDocumentedPort(t *testing.T) {
	if last := guardPortOf(blockCount-1) + blockPortCount - 1; last != 32699 {
		t.Errorf("the last port of the range = %d, want 32699", last)
	}
}

func TestTwoBlocksClaimedAtOnceAreDifferent(t *testing.T) {
	first := claimForTest(t, 0)

	second := claimForTest(t, 0)

	if first.index == second.index {
		t.Errorf("both claims got block %d", first.index)
	}
}

// holdTheGuardPortOfAFreeBlock listens, for the length of the test, on the
// guard port of the first block that nobody has claimed, and returns the
// block's index. A plain listener holds the port, not a portBlock: that is
// what a claim sees when another process owns the block.
func holdTheGuardPortOfAFreeBlock(t *testing.T) int {
	t.Helper()
	for index := range blockCount {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", guardPortOf(index)))
		if err == nil {
			t.Cleanup(func() { _ = listener.Close() })
			return index
		}
	}
	t.Fatalf("every block is claimed, so the test has none to hold")
	return 0
}

func TestABlockWhoseGuardPortIsHeldIsPassedOver(t *testing.T) {
	held := holdTheGuardPortOfAFreeBlock(t)

	claimed := claimForTest(t, held)

	if claimed.index == held {
		t.Errorf("the claim got block %d although its guard port was held", held)
	}
}

func TestTheSearchGoesOnAtTheFirstBlockAfterTheLast(t *testing.T) {
	if tried := blockTriedAt(blockCount-1, 1); tried != 0 {
		t.Errorf("the block after the last = %d, want 0", tried)
	}
}

func TestTheSearchTriesEveryBlockOnce(t *testing.T) {
	tried := make([]int, 0, blockCount)
	for step := range blockCount {
		tried = append(tried, blockTriedAt(20, step))
	}

	slices.Sort(tried)

	every := make([]int, 0, blockCount)
	for index := range blockCount {
		every = append(every, index)
	}
	if !slices.Equal(tried, every) {
		t.Errorf("the search tried %v, want every block once", tried)
	}
}

func TestPortsOfABlockAreDifferentFromEachOther(t *testing.T) {
	block := claimForTest(t, 0)
	ports := make([]int, 0, 8)
	for range 8 {
		port, _ := block.take()
		ports = append(ports, port)
	}

	slices.Sort(ports)

	if different := slices.Compact(ports); len(different) != 8 {
		t.Errorf("eight takes gave %d different ports: %v", len(different), different)
	}
}

func TestAPortOfABlockCanBeBound(t *testing.T) {
	block := claimForTest(t, 0)

	port, ok := block.take()

	if !ok {
		t.Fatalf("a block that was just claimed has no port")
	}
	listen(t, port)
}

// The guard port is the one port of a block that is never handed out: the
// block's own listener holds it.
func TestTheGuardPortIsNotHandedOut(t *testing.T) {
	block := claimForTest(t, 0)

	port, _ := block.take()

	if port == guardPortOf(block.index) {
		t.Errorf("the block handed out its guard port %d", port)
	}
}

func TestAPortThatSomethingElseHoldsIsPassedOver(t *testing.T) {
	block := claimForTest(t, 0)
	firstPort := guardPortOf(block.index) + 1
	listen(t, firstPort)

	port, _ := block.take()

	if port != firstPort+1 {
		t.Errorf("take = %d, want %d, the port after the one that is held", port, firstPort+1)
	}
}

func TestABlockWithNoPortLeftSaysSo(t *testing.T) {
	block := claimForTest(t, 0)
	for range blockPortCount - 1 {
		block.take()
	}

	_, ok := block.take()

	if ok {
		t.Errorf("a block handed out more ports than it has")
	}
}

func TestFreeAddressesAreDifferentFromEachOther(t *testing.T) {
	first := FreeAddress(t)

	second := FreeAddress(t)

	if first == second {
		t.Errorf("FreeAddress gave %s twice", first)
	}
}

func TestAFreeAddressCanBeListenedOn(t *testing.T) {
	address := FreeAddress(t)

	listener, err := net.Listen("tcp", address)

	if err != nil {
		t.Fatalf("listen on %s: %v", address, err)
	}
	_ = listener.Close()
}

// One test binary may need more ports than a block has: the interop tests
// start ten clusters of nine ports each.
func TestFreeAddressesStayDifferentBeyondOneBlock(t *testing.T) {
	addresses := make([]string, 0, blockPortCount+10)
	for range blockPortCount + 10 {
		addresses = append(addresses, FreeAddress(t))
	}

	slices.Sort(addresses)

	if different := slices.Compact(addresses); len(different) != blockPortCount+10 {
		t.Errorf("%d addresses, of which %d different", blockPortCount+10, len(different))
	}
}
