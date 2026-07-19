package contract

import (
	"bytes"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeploymentCommandMatchesOrchestratorApprovalAndNodeSignatures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "deployment-command-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	command, err := Parse(bytes.TrimSpace(raw))
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := command.VerifyNodeSignature(nodeKey); err != nil {
		t.Fatalf("VerifyNodeSignature(): %v", err)
	}
	proof, err := command.Approval()
	if err != nil {
		t.Fatalf("Approval(): %v", err)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := proof.Verify(deviceKey, time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Verify approval: %v", err)
	}
	request, err := command.DeploymentRequest()
	if err != nil || request.DeploymentID != "deployment-create-0001" || request.WorkerArtifact.AMIID != "ami-0123456789abcdef0" {
		t.Fatalf("deployment request = %#v, err %v", request, err)
	}
}

func TestDeploymentCommandRejectsWrongApprovalKey(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "deployment-command-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	command, err := Parse(bytes.TrimSpace(raw))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := command.Approval()
	if err != nil {
		t.Fatal(err)
	}
	wrongKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x50}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if got := Code(proof.Verify(wrongKey, time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC))); got != "invalid_approval_signature" {
		t.Fatalf("Verify() code = %q", got)
	}
}

func TestDeploymentCommandRejectsApprovalDriftAndExpandedScope(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "deployment-command-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.TrimSpace(raw)

	tampered := []byte(strings.Replace(string(raw), `"quote_digest": "sha256:3c54`, `"quote_digest": "sha256:dddd`, 1))
	command, err := Parse(tampered)
	if err != nil {
		t.Fatalf("Parse tampered proof: %v", err)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if got := Code(command.VerifyNodeSignature(nodeKey)); got != "approval_proof_mismatch" {
		t.Fatalf("tampered proof code = %q", got)
	}

	expanded := []byte(strings.Replace(string(raw), `"purchase_option": "on_demand"`, `"purchase_option": "on_demand", "bootstrap_token": "must-not-leak"`, 1))
	command, err = Parse(expanded)
	if err != nil {
		t.Fatalf("Parse expanded proof: %v", err)
	}
	if got := Code(command.VerifyNodeSignature(nodeKey)); got != "invalid_approval_proof" {
		t.Fatalf("expanded proof code = %q", got)
	}
}
