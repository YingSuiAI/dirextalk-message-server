package contract

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOrchestratorDeploymentObserveFixtureIsAccepted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "deployment-observe-command-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	command, err := Parse(bytes.TrimSpace(raw))
	if err != nil {
		t.Fatal(err)
	}
	request, err := command.DeploymentObserveRequest()
	if err != nil || request.DeploymentID != "deployment-create-0001" {
		t.Fatalf("request=%#v err=%v", request, err)
	}
	publicKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x7a}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := command.VerifyNodeSignature(publicKey); err != nil {
		t.Fatalf("fixture signature: %v", err)
	}
	encoded, err := json.Marshal(command)
	var compact bytes.Buffer
	compactErr := json.Compact(&compact, raw)
	if err != nil || compactErr != nil || !bytes.Equal(encoded, compact.Bytes()) {
		t.Fatalf("fixture is not canonical: %v", err)
	}
}

func TestCloudWorkerClaimFixtureIsAccepted(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "worker-claim-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		t.Fatal(err)
	}
	claim, err := ParseWorkerSessionClaimRequest(compact.Bytes())
	if err != nil || claim.BootstrapSessionID != "bootstrap-0123456789abcdef0123456789abcdef" {
		t.Fatalf("claim=%#v err=%v", claim, err)
	}
}
