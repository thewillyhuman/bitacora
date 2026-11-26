package bitacora

import (
	"errors"
	"fmt"

	"github.com/thewillyhuman/bitacora/src/go/internal/metadata"
)

var (
	// ErrStreamNotFound says no stream has the given name.
	ErrStreamNotFound = errors.New("bitacora: stream does not exist")
	// ErrInvalidArgument says a name, a range or an option cannot be used.
	ErrInvalidArgument = errors.New("bitacora: invalid argument")
	// ErrUnavailable says the metadata store, the replicas that hold the
	// records, or the object store could not be reached. Trying again later
	// may work.
	ErrUnavailable = errors.New("bitacora: unavailable")
	// ErrTruncated says the records asked for were deleted by retention.
	// The error is a [*TruncatedError], which carries the low-water mark.
	ErrTruncated = errors.New("bitacora: records were truncated")
)

// TruncatedError is the [ErrTruncated] of a particular read.
type TruncatedError struct {
	// LowWaterMark is the first sequence number that can still be read.
	LowWaterMark uint64
}

// Error implements error.
func (truncated *TruncatedError) Error() string {
	return fmt.Sprintf("%v: the stream starts at %d", ErrTruncated, truncated.LowWaterMark)
}

// Is makes errors.Is(err, ErrTruncated) true for a TruncatedError.
func (truncated *TruncatedError) Is(target error) bool { return target == ErrTruncated }

// fromMetadata turns an error of the metadata store into the error of this
// package that a caller can test for, keeping the original as the cause.
func fromMetadata(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, metadata.ErrStreamNotFound):
		return fmt.Errorf("%w: %w", ErrStreamNotFound, err)
	case errors.Is(err, metadata.ErrInvalidName):
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	default:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}
