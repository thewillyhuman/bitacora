package bitacora

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// localStoreWith returns a local object store that holds one object.
func localStoreWith(t *testing.T, key string, contents string) *LocalObjectStore {
	t.Helper()
	root := t.TempDir()
	objectPath := filepath.Join(root, "objects", filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		t.Fatalf("create the object's directory: %v", err)
	}
	if err := os.WriteFile(objectPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("write the object: %v", err)
	}
	return NewLocalObjectStore(root)
}

func TestLocalObjectStoreReadsRanges(t *testing.T) {
	store := localStoreWith(t, "segments/7/3", "0123456789")

	cases := []struct {
		name       string
		start, end uint64
		want       string
	}{
		{name: "the whole object", start: 0, end: 10, want: "0123456789"},
		{name: "the middle", start: 3, end: 6, want: "345"},
		{name: "the last byte", start: 9, end: 10, want: "9"},
		{name: "nothing", start: 4, end: 4, want: ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			contents, err := store.GetRange(t.Context(), "segments/7/3", testCase.start, testCase.end)
			if err != nil || string(contents) != testCase.want {
				t.Errorf("GetRange(%d, %d) = %q, %v; want %q", testCase.start, testCase.end, contents, err, testCase.want)
			}
		})
	}
}

func TestLocalObjectStoreRefusesWhatItCannotServe(t *testing.T) {
	store := localStoreWith(t, "segments/7/3", "0123456789")

	cases := []struct {
		name       string
		key        string
		start, end uint64
		want       error
	}{
		{name: "an object that does not exist", key: "segments/7/4", start: 0, end: 1, want: ErrUnavailable},
		{name: "a range past the end of the object", key: "segments/7/3", start: 8, end: 12, want: ErrUnavailable},
		{name: "a range that ends before it starts", key: "segments/7/3", start: 5, end: 4, want: ErrInvalidArgument},
		{name: "an empty key", key: "", start: 0, end: 1, want: ErrInvalidArgument},
		{name: "a key that climbs out of the store", key: "../objects/segments/7/3", start: 0, end: 1, want: ErrInvalidArgument},
		{name: "a key with an empty component", key: "segments//3", start: 0, end: 1, want: ErrInvalidArgument},
		{name: "an absolute key", key: "/etc/hosts", start: 0, end: 1, want: ErrInvalidArgument},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			contents, err := store.GetRange(t.Context(), testCase.key, testCase.start, testCase.end)
			if !errors.Is(err, testCase.want) || contents != nil {
				t.Errorf("GetRange(%q, %d, %d) = %q, %v; want %v", testCase.key, testCase.start, testCase.end, contents, err, testCase.want)
			}
		})
	}
}
