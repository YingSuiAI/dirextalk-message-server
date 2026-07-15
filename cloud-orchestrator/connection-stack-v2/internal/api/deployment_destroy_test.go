package api

import (
	"bytes"
	"context"
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

func TestDeploymentDestroyConsumesApprovalResumesAndCommitsOnlyAfterReadBack(t *testing.T) {
	broker, createdStore, _, createCommand := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, commandPath, createCommand); response.Code != http.StatusOK {
		t.Fatalf("seed deployment: %d %s", response.Code, response.Body.String())
	}
	store := &memoryDestroyRepository{memoryCommandStore: createdStore, destroys: map[string]commandstore.DeploymentDestroyReservation{}}
	provider := &recordingDestroyProvider{inProgress: true}
	broker.DeploymentDestroyEnabled = true
	broker.DeploymentDestroyStore = store
	broker.DeploymentDestroyProvider = provider
	raw := signedDestroyCommand(t)

	first := serve(t, broker, http.MethodPost, commandPath, raw)
	assertHTTPError(t, first, http.StatusConflict, "deployment_destroy_in_progress")
	if len(store.destroys) != 1 || provider.calls != 1 || len(createdStore.records) != 1 {
		t.Fatalf("in-progress state destroys=%d calls=%d receipts=%d", len(store.destroys), provider.calls, len(createdStore.records))
	}
	provider.inProgress = false
	second := serve(t, broker, http.MethodPost, commandPath, raw)
	if second.Code != http.StatusOK {
		t.Fatalf("destroy completion: %d %s", second.Code, second.Body.String())
	}
	wantReplay := second.Body.String()
	var result contract.DeploymentDestroyResult
	if json.Unmarshal(second.Body.Bytes(), &result) != nil || result.Status != "verified_destroyed" || result.Deployment.InstanceID != "i-0123456789abcdef0" {
		t.Fatalf("destroy result = %#v", result)
	}
	replay := serve(t, broker, http.MethodPost, commandPath, raw)
	if replay.Code != http.StatusOK || replay.Body.String() != wantReplay || provider.calls != 2 {
		t.Fatalf("exact replay=%d %s calls=%d want=%s", replay.Code, replay.Body.String(), provider.calls, wantReplay)
	}
}

type recordingDestroyProvider struct {
	calls      int
	inProgress bool
}

func (provider *recordingDestroyProvider) EnsureVerifiedDestroyed(_ context.Context, _ DeploymentDestroySpec) (bool, error) {
	provider.calls++
	if provider.inProgress {
		return false, NewError("deployment_destroy_in_progress", 409)
	}
	return true, nil
}

type memoryDestroyRepository struct {
	*memoryCommandStore
	destroys map[string]commandstore.DeploymentDestroyReservation
}

func (store *memoryDestroyRepository) LookupDeploymentDestroy(_ context.Context, connectionID, deploymentID string) (commandstore.DeploymentDestroyReservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.destroys[connectionID+"\x00"+deploymentID]
	return value, ok, nil
}
func (store *memoryDestroyRepository) ReserveDeploymentDestroy(_ context.Context, reservation commandstore.DeploymentDestroyReservation) (commandstore.DeploymentDestroyReservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := reservation.ConnectionID + "\x00" + reservation.DeploymentID
	if existing, ok := store.destroys[key]; ok {
		return existing, false, nil
	}
	if _, used := store.approvalUses[reservation.ConnectionID+"\x00"+reservation.ApprovalID]; used {
		return commandstore.DeploymentDestroyReservation{}, false, commandstore.NewError("approval_already_consumed")
	}
	if _, used := store.challengeUses[reservation.ConnectionID+"\x00"+reservation.ChallengeID]; used {
		return commandstore.DeploymentDestroyReservation{}, false, commandstore.NewError("challenge_already_consumed")
	}
	store.destroys[key] = reservation
	store.approvalUses[reservation.ConnectionID+"\x00"+reservation.ApprovalID] = reservation.DeploymentID
	store.challengeUses[reservation.ConnectionID+"\x00"+reservation.ChallengeID] = reservation.DeploymentID
	store.lastCounters[reservation.ConnectionID] = reservation.NodeCounter
	return reservation, true, nil
}
func (store *memoryDestroyRepository) FinalizeDeploymentDestroy(_ context.Context, reservation commandstore.DeploymentDestroyReservation, receipt commandstore.Record) (commandstore.Record, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := receipt.ConnectionID + "\x00" + receipt.CommandID
	if existing, ok := store.records[key]; ok {
		return existing, false, nil
	}
	reservation.State = "finalized"
	reservation.ResultJSON = append([]byte(nil), receipt.ResultJSON...)
	store.destroys[reservation.ConnectionID+"\x00"+reservation.DeploymentID] = reservation
	store.records[key] = receipt
	return receipt, true, nil
}

func signedDestroyCommand(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 1, 0, 0, time.UTC)
	proof := contract.ServiceDestroyApprovalProof{SchemaVersion: "cloud-orchestrator/v1", Intent: "service_destroy", ApprovalID: "approval-destroy-0001", ChallengeID: "challenge-destroy-0001", SignerKeyID: "device-key-1", ServiceID: "service-destroy-0001", ServiceRevision: 2, DeploymentID: "deployment-create-0001", DeploymentRevision: 5, CloudConnectionID: "connection-create-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	payloadToSign, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(deviceKey, payloadToSign))
	request := contract.DeploymentDestroyRequest{Schema: contract.DeploymentDestroySchema, ServiceID: proof.ServiceID, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs}
	payload, _ := json.Marshal(request)
	payloadDigest := sha256.Sum256(payload)
	proofJSON, _ := json.Marshal(proof)
	command := contract.Command{Schema: contract.CommandSchema, ConnectionID: proof.CloudConnectionID, CommandID: "command-destroy-0001", NodeKeyID: "node-key-1", IssuedAt: contract.CanonicalInstant(now), ExpiresAt: contract.CanonicalInstant(now.Add(5 * time.Minute)), ExpectedGeneration: 2, NodeCounter: 10, Action: contract.ActionDeploymentDestroy, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(payloadDigest[:]), ApprovalProof: proofJSON}
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
