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

func TestServiceRestoreConsumesApprovalResumesAndReplaysAWSReceipt(t *testing.T) {
	broker, createdStore, _, createCommand := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, commandPath, createCommand); response.Code != http.StatusOK {
		t.Fatalf("seed deployment: %d %s", response.Code, response.Body.String())
	}
	backupStore := &memoryBackupRepository{memoryCommandStore: createdStore, backups: map[string]commandstore.ServiceBackupReservation{}}
	broker.ServiceBackupEnabled = true
	broker.ServiceBackupStore = backupStore
	broker.ServiceBackupProvider = &recordingBackupProvider{}
	if response := serve(t, broker, http.MethodPost, commandPath, signedBackupCommand(t)); response.Code != http.StatusOK {
		t.Fatalf("seed backup: %d %s", response.Code, response.Body.String())
	}

	restoreStore := &memoryRestoreRepository{memoryBackupRepository: backupStore, restores: map[string]commandstore.ServiceRestoreReservation{}}
	provider := &recordingRestoreProvider{pending: true}
	broker.ServiceRestoreEnabled = true
	broker.ServiceRestoreStore = restoreStore
	broker.ServiceRestoreProvider = provider
	raw := signedRestoreCommand(t)
	assertHTTPError(t, serve(t, broker, http.MethodPost, commandPath, raw), http.StatusConflict, "service_restore_in_progress")
	provider.pending = false
	completed := serve(t, broker, http.MethodPost, commandPath, raw)
	if completed.Code != http.StatusOK {
		t.Fatalf("restore completion: %d %s", completed.Code, completed.Body.String())
	}
	replay := serve(t, broker, http.MethodPost, commandPath, raw)
	if replay.Code != http.StatusOK || replay.Body.String() != completed.Body.String() || provider.calls != 2 {
		t.Fatalf("replay=%d calls=%d body=%s", replay.Code, provider.calls, replay.Body.String())
	}
}

type recordingRestoreProvider struct {
	calls   int
	pending bool
}

func (provider *recordingRestoreProvider) EnsureRestore(_ context.Context, spec ServiceRestoreSpec) (contract.ServiceRestoreAWSEvidence, bool, error) {
	provider.calls++
	if provider.pending {
		return contract.ServiceRestoreAWSEvidence{}, false, nil
	}
	swap := spec.VolumeSwaps[0]
	return contract.ServiceRestoreAWSEvidence{
		RestoreID: spec.RestoreID, ServiceID: spec.ServiceID, DeploymentID: spec.DeploymentID, BackupID: spec.BackupID,
		InstanceID: spec.InstanceID, Region: spec.Region, AvailabilityZone: spec.AvailabilityZone, Outcome: "restored", InstanceState: "running",
		Replacements: []contract.ServiceRestoreReplacementVolume{{OriginalVolumeID: swap.OriginalVolumeID, ReplacementVolumeID: "vol-0fedcba9876543210", SnapshotID: swap.SnapshotID, DeviceName: swap.DeviceName, State: "attached_current", Encrypted: true, DeleteOnTermination: swap.DeleteOnTermination}},
	}, true, nil
}

type memoryRestoreRepository struct {
	*memoryBackupRepository
	restores map[string]commandstore.ServiceRestoreReservation
}

func (store *memoryRestoreRepository) LookupServiceRestore(_ context.Context, connectionID, restoreID string) (commandstore.ServiceRestoreReservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, found := store.restores[connectionID+"\x00"+restoreID]
	return value, found, nil
}

func (store *memoryRestoreRepository) ReserveServiceRestore(_ context.Context, reservation commandstore.ServiceRestoreReservation) (commandstore.ServiceRestoreReservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := reservation.ConnectionID + "\x00" + reservation.RestoreID
	if existing, found := store.restores[key]; found {
		return existing, false, nil
	}
	store.restores[key] = reservation
	store.approvalUses[reservation.ConnectionID+"\x00"+reservation.ApprovalID] = reservation.RestoreID
	store.challengeUses[reservation.ConnectionID+"\x00"+reservation.ChallengeID] = reservation.RestoreID
	return reservation, true, nil
}

func (store *memoryRestoreRepository) FinalizeServiceRestore(_ context.Context, reservation commandstore.ServiceRestoreReservation, receipt commandstore.Record) (commandstore.Record, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	reservation.State = "finalized"
	reservation.ResultJSON = append([]byte(nil), receipt.ResultJSON...)
	store.restores[reservation.ConnectionID+"\x00"+reservation.RestoreID] = reservation
	store.records[receipt.ConnectionID+"\x00"+receipt.CommandID] = receipt
	return receipt, true, nil
}

func signedRestoreCommand(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 1, 0, 0, time.UTC)
	swap := contract.ServiceRestoreVolumeSwap{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}
	proof := contract.ServiceRestoreApprovalProof{
		SchemaVersion: "cloud-orchestrator/v1", Intent: "service_restore", ApprovalID: "approval-restore-0001", ChallengeID: "challenge-restore-0001", SignerKeyID: "device-key-1",
		RestoreID: "restore-api-0001", ServiceID: "service-backup-0001", ServiceRevision: 3, DeploymentID: "deployment-create-0001", DeploymentRevision: 6,
		CloudConnectionID: "connection-create-0001", BackupID: "backup-0001", BackupRevision: 2, RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", Region: "us-east-1", AvailabilityZone: "us-east-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original",
		QuoteID: "quote-restore-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuoteValidUntil: now.Add(15 * time.Minute), Unincluded: []string{"taxes"}, VolumeSwaps: []contract.ServiceRestoreVolumeSwap{swap},
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(deviceKey, payload))
	request := contract.ServiceRestoreRequest{Schema: contract.ServiceRestoreSchema, RestoreID: proof.RestoreID, ServiceID: proof.ServiceID, DeploymentID: proof.DeploymentID, BackupID: proof.BackupID, InstanceID: proof.InstanceID, Region: proof.Region, AvailabilityZone: proof.AvailabilityZone, RestoreMode: proof.RestoreMode, DowntimeRequired: true, OriginalVolumeRetention: proof.OriginalVolumeRetention, FailurePolicy: proof.FailurePolicy, QuoteID: proof.QuoteID, QuoteValidUntil: contract.CanonicalInstant(proof.QuoteValidUntil), VolumeSwaps: proof.VolumeSwaps}
	requestJSON, _ := json.Marshal(request)
	sum := sha256.Sum256(requestJSON)
	proofJSON, _ := json.Marshal(proof)
	command := contract.Command{Schema: contract.CommandSchema, ConnectionID: proof.CloudConnectionID, CommandID: "command-restore-0001", NodeKeyID: "node-key-1", IssuedAt: contract.CanonicalInstant(now), ExpiresAt: contract.CanonicalInstant(now.Add(4 * time.Minute)), ExpectedGeneration: 2, NodeCounter: 11, Action: contract.ActionServiceRestore, PayloadB64: base64.StdEncoding.EncodeToString(requestJSON), PayloadSHA256: hex.EncodeToString(sum[:]), ApprovalProof: proofJSON}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(nodeKey, []byte(base)))
	raw, _ := json.Marshal(command)
	return raw
}
