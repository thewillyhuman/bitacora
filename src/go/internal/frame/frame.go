// Package frame encodes and decodes batch frames, the unit in which records
// travel on the wire, sit on replica disks and are stored in the object
// store.
//
// The layout is defined in docs/protocol/disk-format.md. All integers are
// little-endian:
//
//	offset  size  field
//	0       4     body length in bytes
//	4       8     sequence number of the batch's first record
//	12      4     record count, at least 1
//	16      4     CRC-32C
//	20      n     body: for each record, payload length (4 bytes), payload
//
// The checksum covers the stream identifier and the segment identifier
// (8 bytes each, not stored in the frame), bytes 0 to 15 of the frame, and
// the body. A frame therefore belongs to one segment: the same records
// encoded for another segment have another checksum.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// HeaderBytes is the size of a frame before its body.
	HeaderBytes = 20
	// recordHeaderBytes is the size of the length that precedes each payload.
	recordHeaderBytes = 4
	// MaxRecordPayloadBytes is the largest payload one record may carry.
	MaxRecordPayloadBytes = 4 * 1024 * 1024
	// MaxBatchBodyBytes is the largest body one frame may have.
	MaxBatchBodyBytes = 8 * 1024 * 1024

	firstSequenceOffset = 4
	recordCountOffset   = 12
	checksumOffset      = 16
)

// castagnoli is the CRC-32C table. Every checksum of the formats uses it.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrNoFrame says that the bytes hold no frame: they are empty, or the
	// body length is zero, which is what unwritten space looks like.
	ErrNoFrame = errors.New("no frame")
	// ErrIncomplete says that the bytes end in the middle of a frame.
	ErrIncomplete = errors.New("frame is incomplete")
	// ErrChecksum says that a frame does not match its checksum for the
	// segment it was decoded for.
	ErrChecksum = errors.New("frame checksum mismatch")
	// ErrMalformed says that a frame passes its checksum but breaks a rule
	// of the format, or that records cannot be encoded as one frame.
	ErrMalformed = errors.New("frame is malformed")
	// ErrSequenceGap says that a frame does not continue the one before it.
	ErrSequenceGap = errors.New("frames are not consecutive")
)

// SegmentAddress identifies one segment across the cluster.
type SegmentAddress struct {
	// StreamID is the stream the segment belongs to.
	StreamID uint64
	// SegmentID is the segment's position within the stream.
	SegmentID uint64
}

// String renders the address as "stream/segment", as the Rust tools do.
func (address SegmentAddress) String() string {
	return fmt.Sprintf("%d/%d", address.StreamID, address.SegmentID)
}

// Record is one record of a stream.
type Record struct {
	// Sequence is the record's position in its stream, starting at 1.
	Sequence uint64
	// Payload is the record's content. After a decode it aliases the frame
	// it was decoded from.
	Payload []byte
}

// Batch is a frame whose checksum and structure were verified, or that was
// just encoded.
type Batch struct {
	// FirstSequence is the sequence number of the batch's first record.
	FirstSequence uint64
	// RecordCount is how many records the batch holds, at least one.
	RecordCount uint32
	// Frame is the whole encoded frame, header included.
	Frame []byte
}

// LastSequence returns the sequence number of the batch's last record.
func (batch Batch) LastSequence() uint64 {
	return batch.FirstSequence + uint64(batch.RecordCount) - 1
}

// Records returns the batch's records in order. Their payloads alias the
// frame.
func (batch Batch) Records() []Record {
	records := make([]Record, 0, batch.RecordCount)
	body := batch.Frame[HeaderBytes:]
	for index := range uint64(batch.RecordCount) {
		payloadBytes := binary.LittleEndian.Uint32(body)
		payloadEnd := recordHeaderBytes + int(payloadBytes)
		records = append(records, Record{
			Sequence: batch.FirstSequence + index,
			Payload:  body[recordHeaderBytes:payloadEnd:payloadEnd],
		})
		body = body[payloadEnd:]
	}
	return records
}

// Encode builds the frame that holds payloads as consecutive records of
// segment, numbered from firstSequence.
//
// It returns an error wrapping [ErrMalformed] if there are no payloads, if
// firstSequence is 0, or if a size limit is exceeded.
func Encode(segment SegmentAddress, firstSequence uint64, payloads [][]byte) (Batch, error) {
	if len(payloads) == 0 {
		return Batch{}, fmt.Errorf("%w: a batch needs at least one record", ErrMalformed)
	}
	if firstSequence == 0 {
		return Batch{}, fmt.Errorf("%w: a batch cannot start at sequence 0", ErrMalformed)
	}
	bodyBytes := 0
	for _, payload := range payloads {
		if len(payload) > MaxRecordPayloadBytes {
			return Batch{}, fmt.Errorf("%w: a record of %d bytes exceeds the limit of %d",
				ErrMalformed, len(payload), MaxRecordPayloadBytes)
		}
		bodyBytes += recordHeaderBytes + len(payload)
	}
	if bodyBytes > MaxBatchBodyBytes {
		return Batch{}, fmt.Errorf("%w: a body of %d bytes exceeds the limit of %d",
			ErrMalformed, bodyBytes, MaxBatchBodyBytes)
	}

	encoded := make([]byte, HeaderBytes, HeaderBytes+bodyBytes)
	binary.LittleEndian.PutUint32(encoded, uint32(bodyBytes))
	binary.LittleEndian.PutUint64(encoded[firstSequenceOffset:], firstSequence)
	binary.LittleEndian.PutUint32(encoded[recordCountOffset:], uint32(len(payloads)))
	for _, payload := range payloads {
		encoded = binary.LittleEndian.AppendUint32(encoded, uint32(len(payload)))
		encoded = append(encoded, payload...)
	}
	binary.LittleEndian.PutUint32(encoded[checksumOffset:], checksum(segment, encoded))
	return Batch{FirstSequence: firstSequence, RecordCount: uint32(len(payloads)), Frame: encoded}, nil
}

// Decode verifies and decodes the one frame that source starts with. Bytes
// after the frame are ignored; the frame's length is len(batch.Frame).
//
// The error wraps [ErrNoFrame], [ErrIncomplete], [ErrChecksum] or
// [ErrMalformed].
func Decode(segment SegmentAddress, source []byte) (Batch, error) {
	if len(source) == 0 {
		return Batch{}, ErrNoFrame
	}
	if len(source) < HeaderBytes {
		return Batch{}, fmt.Errorf("%w: a header needs %d bytes, found %d", ErrIncomplete, HeaderBytes, len(source))
	}
	bodyBytes := int(binary.LittleEndian.Uint32(source))
	if bodyBytes == 0 {
		return Batch{}, ErrNoFrame
	}
	if bodyBytes > MaxBatchBodyBytes {
		return Batch{}, fmt.Errorf("%w: a body of %d bytes exceeds the limit of %d",
			ErrMalformed, bodyBytes, MaxBatchBodyBytes)
	}
	frameBytes := HeaderBytes + bodyBytes
	if len(source) < frameBytes {
		return Batch{}, fmt.Errorf("%w: the frame needs %d bytes, found %d", ErrIncomplete, frameBytes, len(source))
	}
	encoded := source[:frameBytes:frameBytes]
	stored := binary.LittleEndian.Uint32(encoded[checksumOffset:])
	if computed := checksum(segment, encoded); stored != computed {
		return Batch{}, fmt.Errorf("%w: stored %#010x, computed %#010x", ErrChecksum, stored, computed)
	}

	firstSequence := binary.LittleEndian.Uint64(encoded[firstSequenceOffset:])
	if firstSequence == 0 {
		return Batch{}, fmt.Errorf("%w: the batch starts at sequence 0", ErrMalformed)
	}
	recordCount := binary.LittleEndian.Uint32(encoded[recordCountOffset:])
	if err := validateBody(encoded[HeaderBytes:], recordCount); err != nil {
		return Batch{}, err
	}
	if firstSequence+uint64(recordCount) < firstSequence {
		return Batch{}, fmt.Errorf("%w: the batch runs past the end of the sequence space", ErrMalformed)
	}
	return Batch{FirstSequence: firstSequence, RecordCount: recordCount, Frame: encoded}, nil
}

// DecodeAll decodes a run of back-to-back frames in which every byte must
// belong to a valid frame and every frame must continue the one before it,
// as in an append request, a read response or a block of a tiered object.
//
// The error wraps the errors of [Decode], or [ErrSequenceGap].
func DecodeAll(segment SegmentAddress, frames []byte) ([]Batch, error) {
	var batches []Batch
	for offset := 0; offset < len(frames); {
		batch, err := Decode(segment, frames[offset:])
		if err != nil {
			return nil, fmt.Errorf("frame at byte %d: %w", offset, err)
		}
		if count := len(batches); count > 0 {
			if expected := batches[count-1].LastSequence() + 1; batch.FirstSequence != expected {
				return nil, fmt.Errorf("frame at byte %d: %w: it starts at sequence %d where %d was expected",
					offset, ErrSequenceGap, batch.FirstSequence, expected)
			}
		}
		batches = append(batches, batch)
		offset += len(batch.Frame)
	}
	return batches, nil
}

// validateBody checks that body is exactly recordCount records.
func validateBody(body []byte, recordCount uint32) error {
	if recordCount == 0 {
		return fmt.Errorf("%w: the batch holds no records", ErrMalformed)
	}
	remaining := body
	for range recordCount {
		if len(remaining) < recordHeaderBytes {
			return fmt.Errorf("%w: the body ends before its last record", ErrMalformed)
		}
		payloadBytes := int(binary.LittleEndian.Uint32(remaining))
		if payloadBytes > MaxRecordPayloadBytes {
			return fmt.Errorf("%w: a record of %d bytes exceeds the limit of %d",
				ErrMalformed, payloadBytes, MaxRecordPayloadBytes)
		}
		if len(remaining)-recordHeaderBytes < payloadBytes {
			return fmt.Errorf("%w: a record runs past the end of the body", ErrMalformed)
		}
		remaining = remaining[recordHeaderBytes+payloadBytes:]
	}
	if len(remaining) != 0 {
		return fmt.Errorf("%w: %d bytes follow the last record", ErrMalformed, len(remaining))
	}
	return nil
}

// checksum computes the CRC-32C of an encoded frame for segment, skipping
// the checksum field itself.
func checksum(segment SegmentAddress, encoded []byte) uint32 {
	var identifiers [16]byte
	binary.LittleEndian.PutUint64(identifiers[:8], segment.StreamID)
	binary.LittleEndian.PutUint64(identifiers[8:], segment.SegmentID)
	sum := crc32.Update(0, castagnoli, identifiers[:])
	sum = crc32.Update(sum, castagnoli, encoded[:checksumOffset])
	return crc32.Update(sum, castagnoli, encoded[HeaderBytes:])
}
