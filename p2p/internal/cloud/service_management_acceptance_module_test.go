package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestModuleRoutesManagementAcceptanceThroughHTTPDeviceApproval(t *testing.T) {
	now := time.Date(2026, 7, 16, 4, 0, 0, 0, time.UTC)
	target := cloudcontracts.ServiceManagementAcceptanceTargetV1{AcceptanceID: "acceptance-management-module", ServiceID: "service-management-module", ServiceRevision: 3, DeploymentID: "deployment-management-module", DeploymentRevision: 5, CloudConnectionID: "connection-management-module", RecipeID: "recipe-management-module", RecipeDigest: managementDigest("a"), RecipeRevision: 2, RecipeMaturity: cloudcontracts.RecipeAwaitingManagementAccept, InstalledManifestDigest: managementDigest("b"), ArtifactDigest: managementDigest("c"), ReadinessSemanticEvidenceDigest: cloudcontracts.FixedReadinessEvidenceDigestV1, ReadinessStackObservationDigest: managementDigest("e"), RestartOperationID: "operation-management-module", RestartOperationRevision: 2, BackupID: "backup-management-module", BackupRevision: 2, RestoreID: "restore-management-module", RestoreRevision: 2, SourceArtifactDigests: []string{managementDigest("d")}, Health: cloudcontracts.HealthContractV1{Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/live"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/ready"}, Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "semantic"}}, Lifecycle: cloudcontracts.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"}, DestroyInstanceID: "i-0123456789abcdef0", DestroyVolumeIDs: []string{"vol-0123456789abcdef0"}, DestroyNetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, AcceptancePolicy: cloudcontracts.ServiceManagementAcceptancePolicy}
	approval, err := cloudcontracts.NewServiceManagementAcceptanceApprovalV1(target, "approval-management-module", "challenge-management-module", "device-management-module", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	store := &managementAcceptanceModuleStore{prepared: PrepareServiceManagementAcceptanceResult{Confirmation: ServiceManagementAcceptanceConfirmation{Service: Service{ServiceID: target.ServiceID, Status: "awaiting_management_acceptance", Revision: 3}, Recipe: Recipe{RecipeID: target.RecipeID, Maturity: "awaiting_management_acceptance", Revision: 2}, Approval: approval}, Created: true, ServiceChanged: true}}
	published := 0
	m := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-generated" }, Publish: func(context.Context, string, string, map[string]any) error { published++; return nil }})
	result, apiErr := m.Handlers()[actionServicesManagementPlan](t.Context(), map[string]any{"service_id": target.ServiceID, "expected_revision": float64(2), "idempotency_key": "77777777-7777-4777-8777-777777777777"})
	if apiErr != nil || result == nil || store.prepare.ExpectedRevision != 2 || published != 1 {
		t.Fatalf("prepare=%#v request=%#v published=%d err=%v", result, store.prepare, published, apiErr)
	}
	signed, err := approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, 32)), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	store.approved = ApproveServiceManagementAcceptanceResult{Service: Service{ServiceID: target.ServiceID, Status: "active", Revision: 4}, Recipe: Recipe{RecipeID: target.RecipeID, Maturity: "managed", Revision: 3}, Acceptance: ServiceManagementAcceptance{AcceptanceID: target.AcceptanceID, ServiceID: target.ServiceID, RecipeID: target.RecipeID, Status: "approved", Revision: 2}, Created: true}
	result, apiErr = m.Handlers()[actionServicesManagementApprove](t.Context(), map[string]any{"service_id": target.ServiceID, "expected_revision": float64(3), "approval": signed, "idempotency_key": "88888888-8888-4888-8888-888888888888"})
	if apiErr != nil || result == nil || store.approve.Approval.Intent != cloudcontracts.ServiceManagementAcceptanceIntent || published != 2 {
		t.Fatalf("approve=%#v request=%#v published=%d err=%v", result, store.approve, published, apiErr)
	}
}

type managementAcceptanceModuleStore struct {
	Store
	prepared PrepareServiceManagementAcceptanceResult
	approved ApproveServiceManagementAcceptanceResult
	prepare  PrepareServiceManagementAcceptanceRequest
	approve  ApproveServiceManagementAcceptanceRequest
}

func (s *managementAcceptanceModuleStore) PrepareCloudServiceManagementAcceptance(_ context.Context, r PrepareServiceManagementAcceptanceRequest) (PrepareServiceManagementAcceptanceResult, error) {
	s.prepare = r
	return s.prepared, nil
}
func (s *managementAcceptanceModuleStore) ApproveCloudServiceManagementAcceptance(_ context.Context, r ApproveServiceManagementAcceptanceRequest) (ApproveServiceManagementAcceptanceResult, error) {
	s.approve = r
	return s.approved, nil
}

func managementDigest(fill string) string {
	return "sha256:" + strings.Repeat(fill, 64)
}
