package broker

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceRestoreCommandBindsApprovedSnapshotAndFallbackPolicy(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	target := cloudcontracts.ServiceRestoreTargetV1{
		RestoreID: "restore-broker-0001", ServiceID: "service-broker-0001", ServiceRevision: 3,
		DeploymentID: "deployment-broker-0001", DeploymentRevision: 6, CloudConnectionID: "connection-broker-0001",
		BackupID: "backup-broker-0001", BackupRevision: 2, RecipeID: "recipe-broker-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", AvailabilityZone: "ap-south-1a", RestoreMode: cloudcontracts.ServiceRestoreModeInPlace, DowntimeRequired: true,
		OriginalVolumeRetention: cloudcontracts.ServiceRestoreRetentionManual, FailurePolicy: cloudcontracts.ServiceRestoreFailureReattachOriginal,
		QuoteID: "quote-broker-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuoteValidUntil: now.Add(15 * time.Minute),
		VolumeSwaps: []cloudcontracts.ServiceRestoreVolumeSwapV1{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}},
	}
	approval, err := cloudcontracts.NewServiceRestoreApprovalV1(target, "approval-broker-0001", "challenge-broker-0001", "device-broker-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, 32)), now)
	if err != nil {
		t.Fatal(err)
	}
	request := ServiceRestoreRequest{Schema: ServiceRestoreSchema, RestoreID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, InstanceID: target.InstanceID, Region: target.Region, AvailabilityZone: target.AvailabilityZone, RestoreMode: target.RestoreMode, DowntimeRequired: true, OriginalVolumeRetention: target.OriginalVolumeRetention, FailurePolicy: target.FailurePolicy, QuoteID: target.QuoteID, QuoteValidUntil: canonicalInstant(target.QuoteValidUntil), VolumeSwaps: []ServiceRestoreVolumeSwap{{OriginalVolumeID: target.VolumeSwaps[0].OriginalVolumeID, SnapshotID: target.VolumeSwaps[0].SnapshotID, DeviceName: target.VolumeSwaps[0].DeviceName, VolumeType: target.VolumeSwaps[0].VolumeType, SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}
	command, err := NewServiceRestoreCommand(ServiceRestoreCommandInput{ConnectionID: target.CloudConnectionID, CommandID: "command-broker-0001", NodeKeyID: "node-broker-0001", ExpectedGeneration: 2, NodeCounter: 12, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: request, ApprovalProof: approval, PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x32}, 32))})
	if err != nil {
		t.Fatal(err)
	}
	request.VolumeSwaps[0].SnapshotID = "snap-0fedcba9876543210"
	if command.ValidateBinding(ServiceRestoreCommandBinding{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: request, ApprovalProof: approval}) == nil {
		t.Fatal("snapshot drift was accepted")
	}
}
