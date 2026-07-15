package broker

import (
	"bytes"
	"crypto/ed25519"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"testing"
	"time"
)

func TestServiceBackupCommandBindsApprovalAndEncryptedReadBack(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	target := cloudcontracts.ServiceBackupTargetV1{BackupID: "backup-broker-0001", ServiceID: "service-broker-0001", ServiceRevision: 3, DeploymentID: "deployment-broker-0001", DeploymentRevision: 7, CloudConnectionID: "connection-broker-0001", RecipeID: "recipe-broker-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: "manual"}
	approval, e := cloudcontracts.NewServiceBackupApprovalV1(target, "approval-broker-0001", "challenge-broker-0001", "device-broker-0001", now, now.Add(5*time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	approval, e = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, 32)), now)
	if e != nil {
		t.Fatal(e)
	}
	request := ServiceBackupRequest{Schema: ServiceBackupSchema, BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, RetentionPolicy: "manual"}
	command, e := NewServiceBackupCommand(ServiceBackupCommandInput{ConnectionID: target.CloudConnectionID, CommandID: "command-broker-0001", NodeKeyID: "node-broker-0001", ExpectedGeneration: 1, NodeCounter: 5, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: request, ApprovalProof: approval, PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x51}, 32))})
	if e != nil {
		t.Fatal(e)
	}
	result := ServiceBackupResult{Schema: ServiceBackupResultSchema, Status: "backup_available", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: 1, NodeCounter: 5, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), Action: ServiceBackupAction}, Backup: ServiceBackupEvidence{BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, RetentionPolicy: "manual", ImageID: "ami-0123456789abcdef0", Snapshots: []ServiceBackupSnapshot{{VolumeID: target.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}}}
	if e = ValidateServiceBackupResult(command, result); e != nil {
		t.Fatal(e)
	}
	result.Backup.Snapshots[0].Encrypted = false
	if ValidateServiceBackupResult(command, result) == nil {
		t.Fatal("unencrypted snapshot accepted")
	}
}
