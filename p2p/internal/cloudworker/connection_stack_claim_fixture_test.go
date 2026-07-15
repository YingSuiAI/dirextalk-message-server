package cloudworker

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGoConnectionStackWorkerClaimFixture(t *testing.T) {
	manifest := BootstrapManifest{
		Schema: BootstrapManifestV1Schema, ConnectionID: "connection-create-0001", DeploymentID: "deployment-create-0001",
		BootstrapSessionID: "bootstrap-0123456789abcdef0123456789abcdef",
		BootstrapEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/worker-sessions",
		WorkerImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArtifactManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ExpiresAt: "2026-07-15T08:10:00.000Z",
	}
	claim, err := NewClaimRequest(manifest, InstanceIdentityProof{
		DocumentB64: base64.StdEncoding.EncodeToString([]byte(`{"accountId":"123456789012","instanceId":"i-0123456789abcdef0"}`)),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("fixture-signature")),
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "..", "cloud-orchestrator", "connection-stack-v2", "testdata", "worker-claim-v1.json")
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
		t.Fatalf("worker claim fixture drifted; got:\n%s", actual)
	}
}
