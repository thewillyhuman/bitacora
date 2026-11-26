package bitacora

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ObjectStore is where readers fetch sealed segments from once the replicas
// have dropped their copies. It is the read half of the cluster's object
// store; this client never writes to it.
type ObjectStore interface {
	// GetRange returns bytes [start, end) of the object stored under key.
	// The range lies inside the object.
	GetRange(ctx context.Context, key string, start, end uint64) ([]byte, error)
}

// LocalObjectStore reads the object store that the cluster keeps in a
// directory of the local file system: the `local_directory` kind of the
// cluster configuration. Every process of the cluster must see the same
// directory, so this is for a cluster on one machine or on a shared file
// system.
type LocalObjectStore struct {
	// objectsDirectory holds one file per object, at the object's key.
	objectsDirectory string
}

// NewLocalObjectStore returns the object store rooted at rootDirectory, the
// path the cluster configuration names. Objects are the files below its
// "objects" directory.
func NewLocalObjectStore(rootDirectory string) *LocalObjectStore {
	return &LocalObjectStore{objectsDirectory: filepath.Join(rootDirectory, "objects")}
}

// GetRange implements [ObjectStore].
func (store *LocalObjectStore) GetRange(_ context.Context, key string, start, end uint64) ([]byte, error) {
	if !isPlainRelativeKey(key) || start > end {
		return nil, fmt.Errorf("%w: object key %q, range %d to %d", ErrInvalidArgument, key, start, end)
	}
	object, err := os.Open(filepath.Join(store.objectsDirectory, filepath.FromSlash(key)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: object %q does not exist", ErrUnavailable, key)
		}
		return nil, fmt.Errorf("%w: cannot open object %q: %w", ErrUnavailable, key, err)
	}
	defer object.Close()
	contents := make([]byte, end-start)
	// A read that fills the buffer succeeded, whatever it says about the
	// end of the file.
	if readBytes, err := object.ReadAt(contents, int64(start)); readBytes < len(contents) {
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("%w: cannot read bytes %d to %d of object %q: %w", ErrUnavailable, start, end, key, err)
	}
	return contents, nil
}

// isPlainRelativeKey reports whether key is a relative path whose
// components are all ordinary names, so that it cannot point outside the
// store.
func isPlainRelativeKey(key string) bool {
	if key == "" {
		return false
	}
	for component := range strings.SplitSeq(key, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}
