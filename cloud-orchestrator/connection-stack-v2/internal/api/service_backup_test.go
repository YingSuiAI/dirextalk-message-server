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

func TestServiceBackupConsumesApprovalResumesAndReplaysReceipt(t *testing.T) {
	broker, createdStore, _, createCommand := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, commandPath, createCommand); response.Code != http.StatusOK {
		t.Fatalf("seed deployment: %d %s", response.Code, response.Body.String())
	}
	store := &memoryBackupRepository{memoryCommandStore: createdStore, backups: map[string]commandstore.ServiceBackupReservation{}}
	provider := &recordingBackupProvider{pending: true}
	broker.ServiceBackupEnabled = true
	broker.ServiceBackupStore = store
	broker.ServiceBackupProvider = provider
	raw := signedBackupCommand(t)
	first := serve(t, broker, http.MethodPost, commandPath, raw)
	assertHTTPError(t, first, http.StatusConflict, "service_backup_in_progress")
	provider.pending = false
	second := serve(t, broker, http.MethodPost, commandPath, raw)
	if second.Code != http.StatusOK {
		t.Fatalf("backup completion: %d %s", second.Code, second.Body.String())
	}
	want := second.Body.String()
	replay := serve(t, broker, http.MethodPost, commandPath, raw)
	if replay.Code != http.StatusOK || replay.Body.String() != want || provider.calls != 2 {
		t.Fatalf("replay=%d calls=%d body=%s", replay.Code, provider.calls, replay.Body.String())
	}
}

type recordingBackupProvider struct {
	calls   int
	pending bool
}

func (p *recordingBackupProvider) EnsureBackup(_ context.Context, s ServiceBackupSpec) (contract.ServiceBackupEvidence, bool, error) {
	p.calls++
	if p.pending {
		return contract.ServiceBackupEvidence{}, false, nil
	}
	return contract.ServiceBackupEvidence{BackupID: s.BackupID, ServiceID: s.ServiceID, DeploymentID: s.DeploymentID, InstanceID: s.InstanceID, RetentionPolicy: "manual", ImageID: "ami-0123456789abcdef0", Snapshots: []contract.ServiceBackupSnapshot{{VolumeID: s.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}}, true, nil
}

type memoryBackupRepository struct {
	*memoryCommandStore
	backups map[string]commandstore.ServiceBackupReservation
}

func (s *memoryBackupRepository) LookupServiceBackup(_ context.Context, c, b string) (commandstore.ServiceBackupReservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.backups[c+"\x00"+b]
	return v, ok, nil
}
func (s *memoryBackupRepository) ReserveServiceBackup(_ context.Context, r commandstore.ServiceBackupReservation) (commandstore.ServiceBackupReservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := r.ConnectionID + "\x00" + r.BackupID
	if v, ok := s.backups[key]; ok {
		return v, false, nil
	}
	s.backups[key] = r
	s.approvalUses[r.ConnectionID+"\x00"+r.ApprovalID] = r.BackupID
	s.challengeUses[r.ConnectionID+"\x00"+r.ChallengeID] = r.BackupID
	return r, true, nil
}
func (s *memoryBackupRepository) FinalizeServiceBackup(_ context.Context, r commandstore.ServiceBackupReservation, receipt commandstore.Record) (commandstore.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.State = "finalized"
	r.ResultJSON = append([]byte(nil), receipt.ResultJSON...)
	s.backups[r.ConnectionID+"\x00"+r.BackupID] = r
	s.records[receipt.ConnectionID+"\x00"+receipt.CommandID] = receipt
	return receipt, true, nil
}
func signedBackupCommand(t *testing.T) []byte {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 1, 0, 0, time.UTC)
	proof := contract.ServiceBackupApprovalProof{SchemaVersion: "cloud-orchestrator/v1", Intent: "service_backup", ApprovalID: "approval-backup-0001", ChallengeID: "challenge-backup-0001", SignerKeyID: "device-key-1", BackupID: "backup-0001", ServiceID: "service-backup-0001", ServiceRevision: 2, DeploymentID: "deployment-create-0001", DeploymentRevision: 5, CloudConnectionID: "connection-create-0001", RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: "manual", IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	payload, e := proof.SigningPayload()
	if e != nil {
		t.Fatal(e)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(deviceKey, payload))
	request := contract.ServiceBackupRequest{Schema: contract.ServiceBackupSchema, BackupID: proof.BackupID, ServiceID: proof.ServiceID, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, RetentionPolicy: "manual"}
	requestJSON, _ := json.Marshal(request)
	sum := sha256.Sum256(requestJSON)
	proofJSON, _ := json.Marshal(proof)
	command := contract.Command{Schema: contract.CommandSchema, ConnectionID: proof.CloudConnectionID, CommandID: "command-backup-0001", NodeKeyID: "node-key-1", IssuedAt: contract.CanonicalInstant(now), ExpiresAt: contract.CanonicalInstant(now.Add(4 * time.Minute)), ExpectedGeneration: 2, NodeCounter: 10, Action: contract.ActionServiceBackup, PayloadB64: base64.StdEncoding.EncodeToString(requestJSON), PayloadSHA256: hex.EncodeToString(sum[:]), ApprovalProof: proofJSON}
	base, e := command.SignatureBase()
	if e != nil {
		t.Fatal(e)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(nodeKey, []byte(base)))
	raw, _ := json.Marshal(command)
	return raw
}
