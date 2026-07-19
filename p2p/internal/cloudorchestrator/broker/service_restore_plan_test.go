package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"
)

func TestServiceRestorePlanCommandAndResultBindExactAWSPlan(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	request := ServiceRestorePlanRequest{Schema: ServiceRestorePlanSchema, RestorePlanID: "restore-plan-0001", ServiceID: "service-restore-0001", DeploymentID: "deployment-restore-0001", BackupID: "backup-restore-0001", InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", ImageID: "ami-0123456789abcdef0", SnapshotRefs: []ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0"}}}
	command, err := NewServiceRestorePlanCommand(ServiceRestorePlanCommandInput{ConnectionID: "connection-restore-0001", CommandID: "command-restore-0001", NodeKeyID: "node-restore-0001", ExpectedGeneration: 2, NodeCounter: 7, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: request, PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x61}, 32))})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(command)
	parsed, err := ParseServiceRestorePlanCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	result := ServiceRestorePlanResult{Schema: ServiceRestorePlanResultSchema, Status: "restore_plan_ready", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: 2, NodeCounter: 7, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), Action: ServiceRestorePlanAction}, Plan: ServiceRestorePlan{Schema: ServiceRestorePlanSchema, RestorePlanID: request.RestorePlanID, ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, InstanceID: request.InstanceID, Region: request.Region, AvailabilityZone: "ap-south-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "restore-quote-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuotedAt: "2026-07-15T12:00:00.000Z", ValidUntil: "2026-07-15T12:15:00.000Z", Unincluded: []string{"taxes"}, VolumeSwaps: []ServiceRestoreVolumeSwap{{OriginalVolumeID: request.SnapshotRefs[0].OriginalVolumeID, SnapshotID: request.SnapshotRefs[0].SnapshotID, DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}}
	if err = ValidateServiceRestorePlanResult(parsed, result); err != nil {
		t.Fatal(err)
	}
	result.Plan.VolumeSwaps[0].SnapshotID = "snap-0fedcba9876543210"
	if ValidateServiceRestorePlanResult(parsed, result) == nil {
		t.Fatal("snapshot drift must fail closed")
	}
}
