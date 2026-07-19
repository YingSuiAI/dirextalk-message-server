package contract

import (
	"encoding/json"
	"testing"
	"time"
)

func TestServiceRestorePlanBindsReadBackAndIdempotentReceipt(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	request := ServiceRestorePlanRequest{Schema: ServiceRestorePlanSchema, RestorePlanID: "restore-plan-0001", ServiceID: "service-restore-0001", DeploymentID: "deployment-restore-0001", BackupID: "backup-restore-0001", InstanceID: "i-0123456789abcdef0", Region: "us-east-1", ImageID: "ami-0123456789abcdef0", SnapshotRefs: []ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0"}}}
	command := fixtureCommand(t, ActionServiceRestorePlan, "command-restore-plan-0001", "node-key-1", 2, 31, CanonicalInstant(now), CanonicalInstant(now.Add(5*time.Minute)), request)
	requestSHA, _ := command.RequestSHA256()
	plan := ServiceRestorePlan{Schema: ServiceRestorePlanSchema, RestorePlanID: request.RestorePlanID, ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, InstanceID: request.InstanceID, Region: request.Region, AvailabilityZone: "us-east-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "quote-restore-plan-0001", Currency: "USD", EstimatedHourlyMinor: 12, EstimatedThirtyDayMinor: 8640, QuotedAt: CanonicalInstant(now), ValidUntil: CanonicalInstant(now.Add(15 * time.Minute)), Unincluded: []string{"data_transfer", "taxes"}, VolumeSwaps: []ServiceRestoreVolumeSwap{{OriginalVolumeID: request.SnapshotRefs[0].OriginalVolumeID, SnapshotID: request.SnapshotRefs[0].SnapshotID, DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}
	raw, err := MarshalCommittedServiceRestorePlanResult(command, plan)
	if err != nil || ValidateCommittedResult(command, raw) != nil {
		t.Fatalf("restore plan result err=%v validate=%v", err, ValidateCommittedResult(command, raw))
	}
	replay, err := IdempotentResult(command, raw)
	if err != nil {
		t.Fatal(err)
	}
	var result ServiceRestorePlanResult
	if json.Unmarshal(replay, &result) != nil || result.Status != "idempotent" || result.Receipt.Disposition != "idempotent" || ValidateServiceRestorePlanResult(command, result) != nil {
		t.Fatalf("invalid restore plan replay: %s", replay)
	}
	result.Plan.VolumeSwaps[0].SnapshotID = "snap-0bbbbbbbbbbbbbbbb"
	if ValidateServiceRestorePlanResult(command, result) == nil {
		t.Fatal("restore plan accepted snapshot drift")
	}
}
