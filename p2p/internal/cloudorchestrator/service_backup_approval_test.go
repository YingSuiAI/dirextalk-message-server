package cloudorchestrator_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceBackupApprovalBindsExactTrackedVolumes(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	target := cloudorchestrator.ServiceBackupTargetV1{BackupID: "backup-0001", ServiceID: "service-backup-0001", ServiceRevision: 3, DeploymentID: "deployment-backup-0001", DeploymentRevision: 7, CloudConnectionID: "connection-backup-0001", RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"}, RetentionPolicy: cloudorchestrator.ServiceBackupRetentionManual}
	approval, err := cloudorchestrator.NewServiceBackupApprovalV1(target, "approval-backup-0001", "challenge-backup-0001", "device-backup-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil || signed.Verify(publicKey, now.Add(time.Minute)) != nil {
		t.Fatalf("sign/verify err=%v", err)
	}
	if signed.VolumeIDs[0] != "vol-0aaaaaaaaaaaaaaaa" || signed.ValidateAgainst(target, now.Add(time.Minute)) != nil {
		t.Fatalf("normalized approval=%#v", signed)
	}
	tampered := target
	tampered.VolumeIDs = []string{"vol-0cccccccccccccccc"}
	if signed.ValidateAgainst(tampered, now.Add(time.Minute)) == nil {
		t.Fatal("tampered backup volume accepted")
	}
}

func TestServiceBackupApprovalGolden(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	target := cloudorchestrator.ServiceBackupTargetV1{BackupID: "backup-0001", ServiceID: "service-backup-0001", ServiceRevision: 3, DeploymentID: "deployment-backup-0001", DeploymentRevision: 7, CloudConnectionID: "connection-backup-0001", RecipeID: "recipe-backup-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, RetentionPolicy: cloudorchestrator.ServiceBackupRetentionManual}
	approval, err := cloudorchestrator.NewServiceBackupApprovalV1(target, "approval-backup-0001", "challenge-backup-0001", "device-backup-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := approval.SigningPayload()
	sum := sha256.Sum256(payload)
	const want = "4d464600d3db5ebfbaf3d559640175a0280d34f7a1d5a6e74cc771757842ad56"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("backup payload digest=%s", got)
	}
}
