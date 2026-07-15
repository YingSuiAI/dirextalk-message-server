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

const serviceRestoreApprovalGoldenSHA256 = "80ec3dedccb317676cecbd772607046909d2ee51adf02129243fdab080fa8ad2"

func TestServiceRestoreApprovalBindsExactInPlaceVolumeSwap(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	target := restoreTarget(now)
	approval, err := cloudorchestrator.NewServiceRestoreApprovalV1(target, "approval-restore-0001", "challenge-restore-0001", "device-restore-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil || signed.Verify(publicKey, now.Add(time.Minute)) != nil || signed.ValidateAgainst(target, now.Add(time.Minute)) != nil {
		t.Fatalf("restore sign/verify err=%v approval=%#v", err, signed)
	}

	for name, mutate := range map[string]func(*cloudorchestrator.ServiceRestoreTargetV1){
		"instance": func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.InstanceID = "i-0bbbbbbbbbbbbbbbb" },
		"snapshot": func(v *cloudorchestrator.ServiceRestoreTargetV1) {
			v.VolumeSwaps[0].SnapshotID = "snap-0bbbbbbbbbbbbbbbb"
		},
		"device": func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.VolumeSwaps[0].DeviceName = "/dev/xvdf" },
		"az":     func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.AvailabilityZone = "us-east-1b" },
		"price":  func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.EstimatedThirtyDayMinor++ },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := restoreTarget(now)
			mutate(&tampered)
			if signed.ValidateAgainst(tampered, now.Add(time.Minute)) == nil {
				t.Fatal("tampered restore target accepted")
			}
		})
	}
}

func TestServiceRestoreTargetRejectsUnsafeRollbackPolicy(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(*cloudorchestrator.ServiceRestoreTargetV1){
		"clone mode":       func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.RestoreMode = "clone" },
		"no downtime":      func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.DowntimeRequired = false },
		"delete originals": func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.OriginalVolumeRetention = "delete" },
		"no fallback":      func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.FailurePolicy = "leave_partial" },
		"unencrypted":      func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.VolumeSwaps[0].Encrypted = false },
		"expired quote":    func(v *cloudorchestrator.ServiceRestoreTargetV1) { v.QuoteValidUntil = now },
		"duplicate original": func(v *cloudorchestrator.ServiceRestoreTargetV1) {
			v.VolumeSwaps = append(v.VolumeSwaps, v.VolumeSwaps[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			target := restoreTarget(now)
			mutate(&target)
			if target.ValidateAt(now) == nil {
				t.Fatal("unsafe restore target accepted")
			}
		})
	}
}

func TestServiceRestoreApprovalDeterministicCBORGolden(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	approval, err := cloudorchestrator.NewServiceRestoreApprovalV1(restoreTarget(now), "approval-restore-0001", "challenge-restore-0001", "device-restore-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if got := hex.EncodeToString(digest[:]); got != serviceRestoreApprovalGoldenSHA256 {
		t.Fatalf("service restore approval golden digest=%s", got)
	}
}

func restoreTarget(now time.Time) cloudorchestrator.ServiceRestoreTargetV1 {
	return cloudorchestrator.ServiceRestoreTargetV1{
		RestoreID: "restore-0001", ServiceID: "service-restore-0001", ServiceRevision: 4,
		DeploymentID: "deployment-restore-0001", DeploymentRevision: 8,
		CloudConnectionID: "connection-restore-0001", BackupID: "backup-restore-0001", BackupRevision: 2,
		RecipeID: "recipe-restore-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", Region: "us-east-1", AvailabilityZone: "us-east-1a",
		RestoreMode: cloudorchestrator.ServiceRestoreModeInPlace, DowntimeRequired: true,
		OriginalVolumeRetention: cloudorchestrator.ServiceRestoreRetentionManual,
		FailurePolicy:           cloudorchestrator.ServiceRestoreFailureReattachOriginal,
		QuoteID:                 "quote-restore-0001", Currency: "USD", EstimatedHourlyMinor: 12, EstimatedThirtyDayMinor: 8640,
		QuoteValidUntil: now.Add(15 * time.Minute), Unincluded: []string{"data transfer", "tax"},
		VolumeSwaps: []cloudorchestrator.ServiceRestoreVolumeSwapV1{{
			OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda",
			VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true,
		}},
	}
}
