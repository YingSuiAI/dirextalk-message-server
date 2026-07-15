package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"testing"
	"time"
)

func TestModuleRoutesRestoreThroughDedicatedDeviceApprovalActions(t *testing.T) {
	now := time.Date(2026, 7, 15, 23, 30, 0, 0, time.UTC)
	target := cloudcontracts.ServiceRestoreTargetV1{RestoreID: "restore-plan-module-0001", ServiceID: "service-restore-module-0001", ServiceRevision: 3, DeploymentID: "deployment-restore-module-0001", DeploymentRevision: 6, CloudConnectionID: "connection-restore-module-0001", BackupID: "backup-restore-module-0001", BackupRevision: 2, RecipeID: "recipe-restore-module-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", AvailabilityZone: "ap-south-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "restore-quote-module-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuoteValidUntil: now.Add(15 * time.Minute), Unincluded: []string{"taxes"}, VolumeSwaps: []cloudcontracts.ServiceRestoreVolumeSwapV1{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}
	approval, e := cloudcontracts.NewServiceRestoreApprovalV1(target, "approval-restore-module-0001", "challenge-restore-module-0001", "device-restore-module-0001", now, now.Add(5*time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	store := &restoreConfirmationModuleStore{prepared: PrepareServiceRestoreResult{Confirmation: ServiceRestoreConfirmation{Service: Service{ServiceID: target.ServiceID, Revision: 3}, Deployment: Deployment{DeploymentID: target.DeploymentID, Revision: 6}, Plan: ServiceRestorePlan{RestorePlanID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, Status: "ready_for_confirmation", Revision: 2}, Approval: approval}, Created: true}}
	published := 0
	m := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-generated-0001" }, Publish: func(context.Context, string, string, map[string]any) error { published++; return nil }})
	result, apiErr := m.Handlers()[actionServicesRestoreConfirmationPrepare](t.Context(), map[string]any{"service_id": target.ServiceID, "restore_plan_id": target.RestoreID, "expected_revision": float64(3), "idempotency_key": "55555555-5555-4555-8555-555555555555"})
	if apiErr != nil || result == nil || store.prepare.RestorePlanID != target.RestoreID {
		t.Fatalf("prepare=%#v request=%#v err=%v", result, store.prepare, apiErr)
	}
	approval, e = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x69}, 32)), now)
	if e != nil {
		t.Fatal(e)
	}
	store.approved = ApproveServiceRestoreResult{Service: store.prepared.Confirmation.Service, Restore: ServiceRestore{RestoreID: target.RestoreID, RestorePlanID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, Status: "queued", Revision: 1}, Job: Job{JobID: "job-restore-module-0001", PlanID: "plan-module-0001", DeploymentID: target.DeploymentID, Kind: "restore", Execution: "queued", Outcome: "pending", Revision: 1}, Created: true}
	result, apiErr = m.Handlers()[actionServicesRestoreApprove](t.Context(), map[string]any{"service_id": target.ServiceID, "restore_plan_id": target.RestoreID, "expected_revision": float64(3), "approval": approval, "idempotency_key": "66666666-6666-4666-8666-666666666666"})
	if apiErr != nil || result == nil || store.approve.Approval.Intent != cloudcontracts.ServiceRestoreApprovalIntent || published != 1 {
		t.Fatalf("approve=%#v request=%#v published=%d err=%v", result, store.approve, published, apiErr)
	}
}

type restoreConfirmationModuleStore struct {
	Store
	prepared PrepareServiceRestoreResult
	approved ApproveServiceRestoreResult
	prepare  PrepareServiceRestoreRequest
	approve  ApproveServiceRestoreRequest
}

func (s *restoreConfirmationModuleStore) PrepareCloudServiceRestore(_ context.Context, r PrepareServiceRestoreRequest) (PrepareServiceRestoreResult, error) {
	s.prepare = r
	return s.prepared, nil
}
func (s *restoreConfirmationModuleStore) ApproveCloudServiceRestore(_ context.Context, r ApproveServiceRestoreRequest) (ApproveServiceRestoreResult, error) {
	s.approve = r
	return s.approved, nil
}
