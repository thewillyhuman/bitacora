// Package style_test checks that every Go file of the module is formatted
// the way gofmt formats it, so that a file that is not fails the tests and
// not a review.
package style_test

import (
	"bytes"
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryGoFileIsFormatted(t *testing.T) {
	// Under Bazel and under `go test` alike, the test runs in the module's
	// directory. Under Bazel it holds the files the target lists as data.
	checked := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(source)
		if err != nil {
			t.Errorf("%s: cannot be parsed: %v", path, err)
			return nil
		}
		if !bytes.Equal(source, formatted) {
			t.Errorf("%s is not formatted: run gofmt -w on it", path)
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}
	// A test that finds no files checks nothing: the build file lost its
	// data.
	if checked < 10 {
		t.Fatalf("only %d Go files were found; the test target must list every package's sources", checked)
	}
}
