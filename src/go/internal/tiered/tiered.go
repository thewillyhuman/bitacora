// Package tiered reads the object a sealed segment becomes in the object
// store.
//
// The layout is defined in docs/protocol/tiered-object.md. All integers are
// little-endian:
//
//	tiered object  batch frame* | block index | footer
//	block index    entry*
//	entry          first sequence (8 bytes) | offset (8 bytes)
//	footer         magic "BITATIER" | format version u32 | stream id u64
//	               | segment id u64 | epoch u64 | first sequence u64
//	               | sealed end u64 | frames length u64 | index entry count u32
//	               | index crc32c u32 | footer crc32c u32
//
// A reader that knows the object's size fetches the footer, then the index,
// then whole blocks with range requests. This package is the pure part of
// that: it decodes the footer and the index and says which bytes to fetch.
package tiered

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/thewillyhuman/bitacora/src/go/internal/frame"
)

const (
	// FooterBytes is the size of the footer at the end of every object.
	FooterBytes = 72
	// formatVersion is the object format this package reads.
	formatVersion = 1
	// indexEntryBytes is the size of one block index entry.
	indexEntryBytes = 16
	// footerChecksumOffset is where the footer's own checksum starts.
	footerChecksumOffset = FooterBytes - 4
)

// magic is how the footer of every tiered object starts.
var magic = [8]byte{'B', 'I', 'T', 'A', 'T', 'I', 'E', 'R'}

// castagnoli is the CRC-32C table.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrDamaged says that bytes are not a valid footer or block index.
var ErrDamaged = errors.New("tiered object is damaged")

// Footer is what the last bytes of a tiered object say about it.
type Footer struct {
	// Segment is the segment the object holds.
	Segment frame.SegmentAddress
	// Epoch is the epoch of the writer that authored the segment.
	Epoch uint64
	// FirstSequence is the sequence number of the segment's first record.
	FirstSequence uint64
	// SealedEnd is the segment's final sequence number.
	SealedEnd uint64
	// FramesBytes is how many bytes of batch frames the object starts with.
	FramesBytes uint64
	// IndexEntryCount is how many entries the block index has.
	IndexEntryCount uint32
	// indexChecksum is the CRC-32C of the block index.
	indexChecksum uint32
}

// IndexStart returns the offset in the object at which the block index
// starts.
func (footer Footer) IndexStart() uint64 { return footer.FramesBytes }

// IndexEnd returns the offset in the object at which the block index ends.
func (footer Footer) IndexEnd() uint64 {
	return footer.FramesBytes + uint64(footer.IndexEntryCount)*indexEntryBytes
}

// ObjectBytes returns the size of the whole object the footer describes.
func (footer Footer) ObjectBytes() uint64 { return footer.IndexEnd() + FooterBytes }

// DecodeFooter verifies and decodes a footer from the last [FooterBytes]
// bytes of objectEnd. The error wraps [ErrDamaged].
func DecodeFooter(objectEnd []byte) (Footer, error) {
	if len(objectEnd) < FooterBytes {
		return Footer{}, fmt.Errorf("%w: a footer is %d bytes, only %d were read", ErrDamaged, FooterBytes, len(objectEnd))
	}
	encoded := objectEnd[len(objectEnd)-FooterBytes:]
	if [8]byte(encoded[:8]) != magic {
		return Footer{}, fmt.Errorf("%w: it does not end with a tiered segment footer", ErrDamaged)
	}
	if version := binary.LittleEndian.Uint32(encoded[8:]); version != formatVersion {
		return Footer{}, fmt.Errorf("%w: format version %d is not supported", ErrDamaged, version)
	}
	stored := binary.LittleEndian.Uint32(encoded[footerChecksumOffset:])
	if computed := crc32.Checksum(encoded[:footerChecksumOffset], castagnoli); stored != computed {
		return Footer{}, fmt.Errorf("%w: footer checksum is %#010x but %#010x is stored", ErrDamaged, computed, stored)
	}
	return Footer{
		Segment: frame.SegmentAddress{
			StreamID:  binary.LittleEndian.Uint64(encoded[12:]),
			SegmentID: binary.LittleEndian.Uint64(encoded[20:]),
		},
		Epoch:           binary.LittleEndian.Uint64(encoded[28:]),
		FirstSequence:   binary.LittleEndian.Uint64(encoded[36:]),
		SealedEnd:       binary.LittleEndian.Uint64(encoded[44:]),
		FramesBytes:     binary.LittleEndian.Uint64(encoded[52:]),
		IndexEntryCount: binary.LittleEndian.Uint32(encoded[60:]),
		indexChecksum:   binary.LittleEndian.Uint32(encoded[64:]),
	}, nil
}

// block is where one block of an object starts.
type block struct {
	// firstSequence is the sequence number of the block's first record.
	firstSequence uint64
	// offset is where the block starts in the object.
	offset uint64
}

// Index is the block index of a tiered object: where to start reading for a
// given sequence number.
type Index struct {
	// blocks are in object order, which is sequence order.
	blocks []block
	// framesBytes is where the last block ends.
	framesBytes uint64
}

// DecodeIndex verifies and decodes the block index that footer describes.
// The error wraps [ErrDamaged].
func DecodeIndex(footer Footer, indexBytes []byte) (Index, error) {
	if uint64(len(indexBytes)) != footer.IndexEnd()-footer.IndexStart() {
		return Index{}, fmt.Errorf("%w: the block index is not the size the footer says", ErrDamaged)
	}
	if crc32.Checksum(indexBytes, castagnoli) != footer.indexChecksum {
		return Index{}, fmt.Errorf("%w: the block index fails its checksum", ErrDamaged)
	}
	blocks := make([]block, 0, footer.IndexEntryCount)
	for unread := indexBytes; len(unread) > 0; unread = unread[indexEntryBytes:] {
		blocks = append(blocks, block{
			firstSequence: binary.LittleEndian.Uint64(unread),
			offset:        binary.LittleEndian.Uint64(unread[8:]),
		})
	}
	for position := 1; position < len(blocks); position++ {
		previous, current := blocks[position-1], blocks[position]
		if previous.firstSequence >= current.firstSequence || previous.offset >= current.offset {
			return Index{}, fmt.Errorf("%w: the block index is not in order", ErrDamaged)
		}
	}
	hasBlocks := len(blocks) > 0
	if hasBlocks != (footer.FramesBytes > 0) {
		return Index{}, fmt.Errorf("%w: the block index does not cover the frames", ErrDamaged)
	}
	if hasBlocks {
		first, last := blocks[0], blocks[len(blocks)-1]
		if first.offset != 0 || first.firstSequence != footer.FirstSequence || last.offset >= footer.FramesBytes {
			return Index{}, fmt.Errorf("%w: the block index does not describe the frames", ErrDamaged)
		}
	}
	return Index{blocks: blocks, framesBytes: footer.FramesBytes}, nil
}

// BlocksFrom returns the byte range [start, end) of the object to fetch in
// order to read from sequence: the block that holds it and as many of the
// following blocks as fit in maxBytes. It is always at least one whole
// block, so the range starts and ends between two frames. The range is
// empty only if the segment holds no records.
func (index Index) BlocksFrom(sequence uint64, maxBytes uint64) (start, end uint64) {
	if len(index.blocks) == 0 {
		return 0, 0
	}
	// The first block that starts after the sequence, minus one, is the
	// block that holds it. A sequence below the segment reads from the start.
	blocksAtOrBefore := sort.Search(len(index.blocks), func(position int) bool {
		return index.blocks[position].firstSequence > sequence
	})
	firstBlock := max(blocksAtOrBefore, 1) - 1
	start = index.blocks[firstBlock].offset
	end = start
	// A block ends where the next one starts; the last one ends where the
	// frames end. The first block is fetched whatever its size.
	for nextBlock := firstBlock + 1; nextBlock <= len(index.blocks); nextBlock++ {
		blockEnd := index.framesBytes
		if nextBlock < len(index.blocks) {
			blockEnd = index.blocks[nextBlock].offset
		}
		isFirstBlock := nextBlock == firstBlock+1
		if !isFirstBlock && blockEnd-start > maxBytes {
			break
		}
		end = blockEnd
	}
	return start, end
}
