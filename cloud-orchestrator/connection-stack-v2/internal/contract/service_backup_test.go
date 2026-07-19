package contract

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestServiceBackupApprovalPayloadMatchesProductCoreGolden(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	proof := ServiceBackupApprovalProof{SchemaVersion: approvalSchemaVersion, Intent: serviceBackupApprovalIntent, ApprovalID: "approval-backup-0001", ChallengeID: "challenge-backup-0001", SignerKeyID: "device-backup-0001", BackupID: "backup-0001", ServiceID: "service-backup-0001", ServiceRevision: 3, DeploymentID: "deployment-backup-0001", DeploymentRevision: 7, CloudConnectionID: "connection-backup-0001", RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:" + strings.Repeat("a", 64), InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, RetentionPolicy: serviceBackupRetentionManual, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != "4d464600d3db5ebfbaf3d559640175a0280d34f7a1d5a6e74cc771757842ad56" {
		t.Fatalf("backup payload digest=%s", got)
	}
}

func TestServiceBackupCommandBindsAndValidatesCompletedEncryptedSnapshots(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	proof := ServiceBackupApprovalProof{SchemaVersion: approvalSchemaVersion, Intent: serviceBackupApprovalIntent, ApprovalID: "approval-backup-0001", ChallengeID: "challenge-backup-0001", SignerKeyID: "device-backup-0001", BackupID: "backup-0001", ServiceID: "service-backup-0001", ServiceRevision: 3, DeploymentID: "deployment-backup-0001", DeploymentRevision: 7, CloudConnectionID: "connection-backup-0001", RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:" + strings.Repeat("a", 64), InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, RetentionPolicy: serviceBackupRetentionManual, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if err = proof.Verify(publicKey, now); err != nil {
		t.Fatal(err)
	}
	request := ServiceBackupRequest{Schema: ServiceBackupSchema, BackupID: proof.BackupID, ServiceID: proof.ServiceID, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, RetentionPolicy: proof.RetentionPolicy}
	requestJSON, _ := json.Marshal(request)
	proofJSON, _ := json.Marshal(proof)
	command := signedServiceBackupCommand(t, requestJSON, proofJSON, privateKey, now)
	if command.ValidateServiceBackupBinding() != nil {
		t.Fatal("valid backup binding rejected")
	}
	evidence := ServiceBackupEvidence{BackupID: request.BackupID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, InstanceID: request.InstanceID, RetentionPolicy: request.RetentionPolicy, ImageID: "ami-0123456789abcdef0", Snapshots: []ServiceBackupSnapshot{{VolumeID: request.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}}
	raw, err := MarshalCommittedServiceBackupResult(command, evidence)
	if err != nil {
		t.Fatal(err)
	}
	var result ServiceBackupResult
	if json.Unmarshal(raw, &result) != nil || ValidateServiceBackupResult(command, result) != nil {
		t.Fatal("committed backup result rejected")
	}
	result.Backup.Snapshots[0].Encrypted = false
	if ValidateServiceBackupResult(command, result) == nil {
		t.Fatal("unencrypted snapshot accepted")
	}
}

func signedServiceBackupCommand(t *testing.T, payload, proof []byte, key ed25519.PrivateKey, now time.Time) Command {
	t.Helper()
	sum := sha256.Sum256(payload)
	command := Command{Schema: CommandSchema, ConnectionID: "connection-backup-0001", CommandID: "command-backup-0001", NodeKeyID: "node-key-1", IssuedAt: now.Format(canonicalInstantLayout), ExpiresAt: now.Add(4 * time.Minute).Format(canonicalInstantLayout), ExpectedGeneration: 1, NodeCounter: 22, Action: ActionServiceBackup, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(sum[:]), ApprovalProof: proof}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(base)))
	return command
}
