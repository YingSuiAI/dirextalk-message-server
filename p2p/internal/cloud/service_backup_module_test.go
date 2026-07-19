package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestModuleRoutesBackupThroughDeviceApprovedOperationActions(t *testing.T) {
	now := time.Date(2026, time.July, 15, 19, 0, 0, 0, time.UTC)
	target := cloudcontracts.ServiceBackupTargetV1{BackupID: "backup-module-0001", ServiceID: "service-module-0001", ServiceRevision: 3, DeploymentID: "deployment-module-0001", DeploymentRevision: 6, CloudConnectionID: "connection-module-0001", RecipeID: "recipe-module-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: cloudcontracts.ServiceBackupRetentionManual}
	approval, err := cloudcontracts.NewServiceBackupApprovalV1(target, "approval-module-0001", "challenge-module-0001", "device-module-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	store := &serviceBackupModuleStore{prepared: PrepareServiceBackupResult{Confirmation: ServiceBackupConfirmation{Service: Service{ServiceID: target.ServiceID, Revision: int64(target.ServiceRevision)}, Deployment: Deployment{DeploymentID: target.DeploymentID, Revision: int64(target.DeploymentRevision)}, Approval: approval}, Created: true}}
	published := 0
	module := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-module-generated-0001" }, Publish: func(context.Context, string, string, map[string]any) error { published++; return nil }})
	planResult, apiErr := module.Handlers()[actionServicesOperationPlan](t.Context(), map[string]any{"service_id": target.ServiceID, "expected_revision": float64(target.ServiceRevision), "operation": "backup", "idempotency_key": "11111111-1111-4111-8111-111111111111"})
	if apiErr != nil || planResult == nil || store.prepareRequest.ServiceID != target.ServiceID {
		t.Fatalf("plan=%#v request=%#v error=%v", planResult, store.prepareRequest, apiErr)
	}
	approval, err = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x55}, ed25519.SeedSize)), now)
	if err != nil {
		t.Fatal(err)
	}
	store.approved = ApproveServiceBackupResult{Service: store.prepared.Confirmation.Service, Backup: ServiceBackup{BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, Status: "queued", RetentionPolicy: "manual", Revision: 1}, Job: Job{JobID: "job-module-0001", PlanID: "plan-module-0001", DeploymentID: target.DeploymentID, Kind: "backup", Execution: "queued", Outcome: "pending", Revision: 1}, Created: true}
	approvedResult, apiErr := module.Handlers()[actionServicesOperationApprove](t.Context(), map[string]any{"service_id": target.ServiceID, "expected_revision": float64(target.ServiceRevision), "approval": approval, "idempotency_key": "22222222-2222-4222-8222-222222222222"})
	result, ok := approvedResult.(map[string]any)
	if apiErr != nil || !ok || result["operation"] != "backup" || store.approveRequest.Approval.Intent != cloudcontracts.ServiceBackupApprovalIntent || published != 1 {
		t.Fatalf("approve=%#v request=%#v published=%d error=%v", approvedResult, store.approveRequest, published, apiErr)
	}
}

type serviceBackupModuleStore struct {
	Store
	prepared       PrepareServiceBackupResult
	approved       ApproveServiceBackupResult
	prepareRequest PrepareServiceBackupRequest
	approveRequest ApproveServiceBackupRequest
}

func (s *serviceBackupModuleStore) PrepareCloudServiceBackup(_ context.Context, request PrepareServiceBackupRequest) (PrepareServiceBackupResult, error) {
	s.prepareRequest = request
	return s.prepared, nil
}

func (s *serviceBackupModuleStore) ApproveCloudServiceBackup(_ context.Context, request ApproveServiceBackupRequest) (ApproveServiceBackupResult, error) {
	s.approveRequest = request
	return s.approved, nil
}
