package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestDeploymentDestroyAcceptsDeploymentApprovalWithoutService(t *testing.T) {
	broker, createdStore, _, createCommand := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, commandPath, createCommand); response.Code != http.StatusOK {
		t.Fatalf("seed deployment: %d %s", response.Code, response.Body.String())
	}
	store := &memoryDestroyRepository{memoryCommandStore: createdStore, destroys: map[string]commandstore.DeploymentDestroyReservation{}}
	provider := &recordingDestroyProvider{}
	broker.DeploymentDestroyEnabled = true
	broker.DeploymentDestroyStore = store
	broker.DeploymentDestroyProvider = provider
	raw := signedRetainedDeploymentDestroyCommand(t)

	response := serve(t, broker, http.MethodPost, commandPath, raw)
	if response.Code != http.StatusOK {
		t.Fatalf("deployment destroy: %d %s", response.Code, response.Body.String())
	}
	reservation := store.destroys["connection-create-0001\x00deployment-create-0001"]
	if reservation.ServiceID != "" || provider.calls != 1 || provider.last.DeploymentID != "deployment-create-0001" || len(provider.last.VolumeIDs) != 1 || len(createdStore.approvalUses) != 2 || len(createdStore.challengeUses) != 2 {
		t.Fatalf("reservation=%#v provider=%#v approval uses=%d challenge uses=%d", reservation, provider.last, len(createdStore.approvalUses), len(createdStore.challengeUses))
	}
	var result contract.DeploymentDestroyResult
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Status != "verified_destroyed" || result.Deployment.InstanceID != "i-0123456789abcdef0" {
		t.Fatalf("result = %#v", result)
	}
}

func signedRetainedDeploymentDestroyCommand(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 1, 0, 0, time.UTC)
	proof := contract.DeploymentDestroyApprovalProof{
		SchemaVersion: "cloud-orchestrator/v1", Intent: "deployment_destroy", ApprovalID: "approval-retained-destroy-0001", ChallengeID: "challenge-retained-destroy-0001", SignerKeyID: "device-key-1",
		DeploymentID: "deployment-create-0001", DeploymentRevision: 7, PlanID: "plan-golden-0001", CloudConnectionID: "connection-create-0001", ResourceStatus: "retained_tracked",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	payloadToSign, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(deviceKey, payloadToSign))
	destroyRequest := contract.DeploymentDestroyRequest{Schema: contract.DeploymentDestroySchema, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs}
	payload, _ := json.Marshal(destroyRequest)
	payloadDigest := sha256.Sum256(payload)
	proofJSON, _ := json.Marshal(proof)
	command := contract.Command{Schema: contract.CommandSchema, ConnectionID: proof.CloudConnectionID, CommandID: "command-retained-destroy-0001", NodeKeyID: "node-key-1", IssuedAt: contract.CanonicalInstant(now), ExpiresAt: contract.CanonicalInstant(now.Add(5 * time.Minute)), ExpectedGeneration: 2, NodeCounter: 12, Action: contract.ActionDeploymentDestroy, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(payloadDigest[:]), ApprovalProof: proofJSON}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(nodeKey, []byte(base)))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
