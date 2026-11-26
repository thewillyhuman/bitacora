package main

import (
	"os"
	"strings"
	"testing"
)

// The page that shows the example, from this directory. Bazel puts the
// repository's files in the same places below the test's working directory
// as they have in the repository.
const pagePath = "../../../../docs/clients/go.md"

// TestThePageShowsThisExample keeps docs/clients/go.md from falling behind:
// the example it prints must be this program, which is compiled with the
// module and run against a real cluster by the interoperability tests.
func TestThePageShowsThisExample(t *testing.T) {
	program, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read the example: %v", err)
	}
	page, err := os.ReadFile(pagePath)
	if err != nil {
		t.Fatalf("read the page: %v", err)
	}
	shown := "```go\n" + string(program) + "```\n"
	if !strings.Contains(string(page), shown) {
		t.Errorf("%s does not show src/go/examples/quickstart/main.go as it is: copy the file into the page's example", pagePath)
	}
}
