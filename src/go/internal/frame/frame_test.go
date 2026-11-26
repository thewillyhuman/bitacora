package frame

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// goldenSegment is the segment the golden frame was written for.
var goldenSegment = SegmentAddress{StreamID: 7, SegmentID: 3}

// goldenFrame is the frame that format version 1 of the Rust implementation
// writes for records 100 to 102 of goldenSegment with the payloads "alpha",
// nothing and "gamma!". The same bytes are pinned in
// src/rust/bitacora-segment/tests/format_golden_test.rs.
const goldenFrame = `
	17000000640000000000000003000000
	668c9d1f05000000616c706861000000
	000600000067616d6d6121`

// goldenPayloads are the payloads of the golden frame.
func goldenPayloads() [][]byte {
	return [][]byte{[]byte("alpha"), {}, []byte("gamma!")}
}

// bytesOf turns hexadecimal text, with any white space in it, into bytes.
func bytesOf(t *testing.T, hexadecimal string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.Join(strings.Fields(hexadecimal), ""))
	if err != nil {
		t.Fatalf("the golden text is not hexadecimal: %v", err)
	}
	return decoded
}

func TestEncodeWritesTheGoldenFrame(t *testing.T) {
	batch, err := Encode(goldenSegment, 100, goldenPayloads())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if want := bytesOf(t, goldenFrame); !bytes.Equal(batch.Frame, want) {
		t.Errorf("frame = %x, want %x", batch.Frame, want)
	}
}

func TestDecodeReadsTheGoldenFrame(t *testing.T) {
	batch, err := Decode(goldenSegment, bytesOf(t, goldenFrame))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	records := batch.Records()
	if len(records) != 3 {
		t.Fatalf("decoded %d records, want 3", len(records))
	}
	for index, wantPayload := range goldenPayloads() {
		if wantSequence := uint64(100 + index); records[index].Sequence != wantSequence {
			t.Errorf("record %d has sequence %d, want %d", index, records[index].Sequence, wantSequence)
		}
		if !bytes.Equal(records[index].Payload, wantPayload) {
			t.Errorf("record %d has payload %q, want %q", index, records[index].Payload, wantPayload)
		}
	}
	if batch.LastSequence() != 102 {
		t.Errorf("LastSequence = %d, want 102", batch.LastSequence())
	}
}

func TestDecodeRefusesAFrameOfAnotherSegment(t *testing.T) {
	otherSegment := SegmentAddress{StreamID: 7, SegmentID: 4}

	_, err := Decode(otherSegment, bytesOf(t, goldenFrame))

	if !errors.Is(err, ErrChecksum) {
		t.Errorf("Decode error = %v, want ErrChecksum", err)
	}
}

func TestDecodeRefusesEveryFlippedBit(t *testing.T) {
	golden := bytesOf(t, goldenFrame)
	for position := range golden {
		for bit := range 8 {
			damaged := bytes.Clone(golden)
			damaged[position] ^= 1 << bit

			batch, err := Decode(goldenSegment, damaged)

			if err == nil && bytes.Equal(batch.Frame, damaged) {
				t.Fatalf("a frame with bit %d of byte %d flipped was accepted", bit, position)
			}
		}
	}
}

func TestDecodeReportsWhatIsWrong(t *testing.T) {
	golden := bytesOf(t, goldenFrame)
	oversizedBody := bytes.Clone(golden)
	copy(oversizedBody, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	tests := []struct {
		name   string
		source []byte
		want   error
	}{
		{"no bytes", nil, ErrNoFrame},
		{"unwritten space", make([]byte, 64), ErrNoFrame},
		{"cut header", golden[:10], ErrIncomplete},
		{"cut body", golden[:len(golden)-1], ErrIncomplete},
		{"body length beyond the limit", oversizedBody, ErrMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Decode(goldenSegment, test.source)

			if !errors.Is(err, test.want) {
				t.Errorf("Decode error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestEncodeRefusesWhatCannotBeAFrame(t *testing.T) {
	tests := []struct {
		name          string
		firstSequence uint64
		payloads      [][]byte
	}{
		{"no records", 1, nil},
		{"sequence zero", 0, [][]byte{[]byte("a")}},
		{"record over the limit", 1, [][]byte{make([]byte, MaxRecordPayloadBytes+1)}},
		{"body over the limit", 1, [][]byte{
			make([]byte, MaxRecordPayloadBytes), make([]byte, MaxRecordPayloadBytes), make([]byte, 1),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Encode(goldenSegment, test.firstSequence, test.payloads)

			if !errors.Is(err, ErrMalformed) {
				t.Errorf("Encode error = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestEncodeAcceptsARecordOfExactlyTheLimit(t *testing.T) {
	_, err := Encode(goldenSegment, 1, [][]byte{make([]byte, MaxRecordPayloadBytes)})

	if err != nil {
		t.Errorf("Encode: %v", err)
	}
}

// consecutiveFrames returns three back-to-back frames holding records 1 to 6.
func consecutiveFrames(t *testing.T) []byte {
	t.Helper()
	var frames []byte
	for firstSequence := uint64(1); firstSequence <= 5; firstSequence += 2 {
		batch, err := Encode(goldenSegment, firstSequence, [][]byte{[]byte("one"), []byte("two")})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		frames = append(frames, batch.Frame...)
	}
	return frames
}

func TestDecodeAllReadsEveryFrameInOrder(t *testing.T) {
	batches, err := DecodeAll(goldenSegment, consecutiveFrames(t))
	if err != nil {
		t.Fatalf("DecodeAll: %v", err)
	}

	var firstSequences []uint64
	for _, batch := range batches {
		firstSequences = append(firstSequences, batch.FirstSequence)
	}
	if want := []uint64{1, 3, 5}; !slices.Equal(firstSequences, want) {
		t.Errorf("first sequences = %v, want %v", firstSequences, want)
	}
}

func TestDecodeAllOfNothingIsNothing(t *testing.T) {
	batches, err := DecodeAll(goldenSegment, nil)

	if err != nil || len(batches) != 0 {
		t.Errorf("DecodeAll(nil) = %v, %v; want no batches and no error", batches, err)
	}
}

func TestDecodeAllRefusesAGap(t *testing.T) {
	first, _ := Encode(goldenSegment, 1, [][]byte{[]byte("a")})
	third, _ := Encode(goldenSegment, 3, [][]byte{[]byte("c")})

	_, err := DecodeAll(goldenSegment, append(bytes.Clone(first.Frame), third.Frame...))

	if !errors.Is(err, ErrSequenceGap) {
		t.Errorf("DecodeAll error = %v, want ErrSequenceGap", err)
	}
}

func TestDecodeAllRefusesTrailingBytes(t *testing.T) {
	frames := append(consecutiveFrames(t), 0x01)

	_, err := DecodeAll(goldenSegment, frames)

	if err == nil {
		t.Error("DecodeAll accepted bytes that belong to no frame")
	}
}

// TestNoDecoderPanicsOnDamagedInput is the property a fuzzer checks: bytes
// from a socket or an object store never make a decoder panic or allocate
// what a corrupted length field asks for.
func TestNoDecoderPanicsOnDamagedInput(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	valid := consecutiveFrames(t)
	for range 5_000 {
		damaged := bytes.Clone(valid)
		switch random.IntN(3) {
		case 0:
			damaged = damaged[:random.IntN(len(damaged)+1)]
		case 1:
			for range 1 + random.IntN(8) {
				damaged[random.IntN(len(damaged))] ^= 1 << random.IntN(8)
			}
		default:
			damaged = make([]byte, random.IntN(200))
			for index := range damaged {
				damaged[index] = byte(random.Uint32())
			}
		}

		// Returning at all is the assertion.
		_, _ = Decode(goldenSegment, damaged)
		_, _ = DecodeAll(goldenSegment, damaged)
	}
}
