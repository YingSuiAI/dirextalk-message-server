package p2p

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestStoreAdaptersAreCapturedBeforeUse(t *testing.T) {
	patterns := []string{
		"service_*.go",
		"projector_*.go",
		"mcp_*.go",
		"native_agent_runner.go",
		"consumer.go",
	}
	chainedStoreAdapter := regexp.MustCompile(`s\.[A-Za-z]+Store\(\)\.`)
	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if match := chainedStoreAdapter.Find(source); match != nil {
				t.Fatalf("%s chains store adapter call %q; capture the adapter and check nil before use", path, string(match))
			}
		}
	}
}
