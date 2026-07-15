package broker

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGoConnectionStackDeploymentObserveCommandFixture(t *testing.T) {
	actual, err := json.Marshal(testDeploymentObserveCommand(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "..", "..", "cloud-orchestrator", "connection-stack-v2", "testdata", "deployment-observe-command-v1.json")
	if os.Getenv("DIREXTALK_UPDATE_GOLDEN") == "1" {
		var formatted bytes.Buffer
		if err := json.Indent(&formatted, actual, "", "  "); err != nil {
			t.Fatal(err)
		}
		formatted.WriteByte('\n')
		if err := os.WriteFile(path, formatted.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, expected); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, compact.Bytes()) {
		t.Fatalf("deployment observe fixture drifted; got:\n%s", actual)
	}
}
