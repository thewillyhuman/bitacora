package tiered

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
)

// goldenObject is the tiered object that format version 1 of the Rust
// implementation writes for segment 7/3, epoch 5, holding records 100 to 102
// in one batch. The same bytes are pinned in
// src/rust/bitacora-segment/tests/format_golden_test.rs.
const goldenObject = `
	17000000640000000000000003000000
	668c9d1f05000000616c706861000000
	000600000067616d6d61216400000000
	00000000000000000000004249544154
	49455201000000070000000000000003
	00000000000000050000000000000064
	0000000000000066000000000000002b
	000000000000000100000083c5a0f034
	ccd63b`

// goldenSegment is the segment the golden object holds.
var goldenSegment = frame.SegmentAddress{StreamID: 7, SegmentID: 3}

// bytesOf turns hexadecimal text, with any white space in it, into bytes.
func bytesOf(t *testing.T, hexadecimal string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.Join(strings.Fields(hexadecimal), ""))
	if err != nil {
		t.Fatalf("the golden text is not hexadecimal: %v", err)
	}
	return decoded
}

func TestDecodeFooterReadsTheGoldenObject(t *testing.T) {
	object := bytesOf(t, goldenObject)

	footer, err := DecodeFooter(object)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}

	want := Footer{
		Segment:         goldenSegment,
		Epoch:           5,
		FirstSequence:   100,
		SealedEnd:       102,
		FramesBytes:     43,
		IndexEntryCount: 1,
		indexChecksum:   footer.indexChecksum,
	}
	if footer != want {
		t.Errorf("footer = %+v, want %+v", footer, want)
	}
	if footer.ObjectBytes() != uint64(len(object)) {
		t.Errorf("ObjectBytes = %d, want %d", footer.ObjectBytes(), len(object))
	}
}

func TestDecodeIndexReadsTheGoldenObject(t *testing.T) {
	object := bytesOf(t, goldenObject)
	footer, err := DecodeFooter(object)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}

	index, err := DecodeIndex(footer, object[footer.IndexStart():footer.IndexEnd()])
	if err != nil {
		t.Fatalf("DecodeIndex: %v", err)
	}

	start, end := index.BlocksFrom(100, 1)
	if start != 0 || end != 43 {
		t.Errorf("BlocksFrom(100) = [%d, %d), want [0, 43)", start, end)
	}
	batches, err := frame.DecodeAll(goldenSegment, object[start:end])
	if err != nil {
		t.Fatalf("the block does not decode: %v", err)
	}
	if batches[0].LastSequence() != footer.SealedEnd {
		t.Errorf("the block ends at %d, want the sealed end %d", batches[0].LastSequence(), footer.SealedEnd)
	}
}

func TestEveryFlippedBitInIndexOrFooterIsDetected(t *testing.T) {
	object := bytesOf(t, goldenObject)
	intactFooter, err := DecodeFooter(object)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	for position := int(intactFooter.FramesBytes); position < len(object); position++ {
		damaged := bytes.Clone(object)
		damaged[position] ^= 0x04

		footer, err := DecodeFooter(damaged)
		if err == nil && footer.IndexStart() <= footer.IndexEnd() && footer.IndexEnd() <= uint64(len(damaged)) {
			_, err = DecodeIndex(footer, damaged[footer.IndexStart():footer.IndexEnd()])
		}

		if err == nil && footer == intactFooter {
			t.Fatalf("damage at byte %d went unnoticed", position)
		}
	}
}

func TestDecodeFooterReportsWhatIsWrong(t *testing.T) {
	object := bytesOf(t, goldenObject)
	newerVersion := bytes.Clone(object)
	newerVersion[len(object)-FooterBytes+8] = 2
	tests := []struct {
		name      string
		objectEnd []byte
	}{
		{"too few bytes", object[len(object)-FooterBytes+1:]},
		{"not a footer", bytes.Repeat([]byte{'x'}, 200)},
		{"newer format", newerVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeFooter(test.objectEnd)

			if !errors.Is(err, ErrDamaged) {
				t.Errorf("DecodeFooter error = %v, want ErrDamaged", err)
			}
		})
	}
}

// indexOf builds an index over blocks that start at the given offsets, each
// holding ten records, in an object with framesBytes of frames.
func indexOf(framesBytes uint64, offsets ...uint64) Index {
	blocks := make([]block, 0, len(offsets))
	for position, offset := range offsets {
		blocks = append(blocks, block{firstSequence: uint64(1 + 10*position), offset: offset})
	}
	return Index{blocks: blocks, framesBytes: framesBytes}
}

func TestBlocksFrom(t *testing.T) {
	// Four blocks of 100 bytes: records 1-10, 11-20, 21-30, 31-40.
	index := indexOf(400, 0, 100, 200, 300)
	tests := []struct {
		name      string
		sequence  uint64
		maxBytes  uint64
		wantStart uint64
		wantEnd   uint64
	}{
		{"first record, one block", 1, 1, 0, 100},
		{"inside the second block", 15, 1, 100, 200},
		{"exactly at a block start", 21, 1, 200, 300},
		{"last block ends where the frames end", 40, 1, 300, 400},
		{"past the sealed end reads the last block", 99, 1, 300, 400},
		{"below the segment reads from the start", 0, 1, 0, 100},
		{"limit of two and a half blocks fetches two", 1, 250, 0, 200},
		{"limit of exactly two blocks fetches two", 11, 200, 100, 300},
		{"limit beyond the segment fetches all of it", 1, 1 << 40, 0, 400},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, end := index.BlocksFrom(test.sequence, test.maxBytes)

			if start != test.wantStart || end != test.wantEnd {
				t.Errorf("BlocksFrom(%d, %d) = [%d, %d), want [%d, %d)",
					test.sequence, test.maxBytes, start, end, test.wantStart, test.wantEnd)
			}
		})
	}
}

func TestBlocksFromOfAnEmptySegmentIsEmpty(t *testing.T) {
	start, end := indexOf(0).BlocksFrom(1, 1)

	if start != 0 || end != 0 {
		t.Errorf("BlocksFrom = [%d, %d), want an empty range", start, end)
	}
}
