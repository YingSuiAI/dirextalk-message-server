package broker

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGoConnectionStackDeploymentCommandFixture(t *testing.T) {
	command := testDeploymentCommand(t)
	actual, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "..", "..", "cloud-orchestrator", "connection-stack-v2", "testdata", "deployment-command-v1.json")
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected = bytes.TrimSpace(expected)
	var compact bytes.Buffer
	if err := json.Compact(&compact, expected); err != nil {
		t.Fatal(err)
	}
	expected = compact.Bytes()
	if !bytes.Equal(actual, expected) {
		t.Fatalf("deployment fixture drifted; got:\n%s", actual)
	}
}
