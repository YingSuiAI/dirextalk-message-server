package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestManagedAcceptanceFacadePreservesLegacyJSONAndRecoversExactAgentOperation(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	clock := now
	operationID := uuid.NewString()
	target := cloudcontracts.ServiceManagementAcceptanceTargetV1{
		AgentInstanceID: uuid.NewString(), OwnerID: "@owner:example.com",
		AcceptanceID: operationID, ServiceID: "service-management-module", ServiceRevision: 3,
		DeploymentID: "deployment-management-module", DeploymentRevision: 5, CloudConnectionID: "connection-management-module", ConnectionRevision: 4,
		PlanID: uuid.NewString(), PlanRevision: 3, PlanHash: managementDigest("9"),
		RecipeID: "recipe-management-module", RecipeDigest: managementDigest("a"), RecipeRevision: 2,
		RecipeMaturity: cloudcontracts.RecipeAwaitingManagementAccept, InstalledManifestDigest: managementDigest("b"),
		ArtifactDigest: managementDigest("c"), ReadinessSemanticEvidenceDigest: cloudcontracts.FixedReadinessEvidenceDigestV1,
		ReadinessStackObservationDigest: managementDigest("e"), RestartOperationID: "operation-management-module",
		RestartOperationRevision: 2, BackupID: "backup-management-module", BackupRevision: 2,
		RestoreID: "restore-management-module", RestoreRevision: 2, SourceArtifactDigests: []string{managementDigest("d")},
		HealthRevision: 4, HealthMonitorKind: "service", HealthStatus: "healthy", HealthEvidenceType: "independent_external",
		HealthEvidenceDigest: managementDigest("8"), HealthObservedAt: now.Add(-time.Minute), Currency: "USD", CostAlertAmountMinor: 2500,
		Health: cloudcontracts.HealthContractV1{
			Liveness:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/live"},
			Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/ready"},
			Semantic:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "semantic"},
		},
		Lifecycle: cloudcontracts.ServiceManagementAcceptanceLifecycleV2{
			Start: "start", Stop: "stop", Maintenance: "maintenance", Restart: "restart", Upgrade: "upgrade",
			Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy",
		},
		Resources: []cloudcontracts.ServiceManagementAcceptanceResourceV2{
			{ResourceID: uuid.NewString(), Type: "ec2", Revision: 2, ProviderID: "i-0123456789abcdef0", TagDigest: managementDigest("7")},
		},
		DestroyInstanceID: "i-0123456789abcdef0", DestroyVolumeIDs: []string{"vol-0123456789abcdef0"},
		DestroyNetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, AcceptancePolicy: cloudcontracts.ServiceManagementAcceptancePolicy,
	}
	approval, err := cloudcontracts.NewServiceManagementAcceptanceApprovalV1(
		target, uuid.NewString(), uuid.NewString(), "device-management-module", now, now.Add(5*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	compatibilityBackup := ServiceBackup{BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID,
		Status: "available", RetentionPolicy: "manual", ImageID: "ami-0123456789abcdef0", SnapshotIDs: []string{"snap-0123456789abcdef0"},
		Revision: int64(target.BackupRevision), CreatedAt: now.Add(-30 * time.Minute).UnixMilli(), UpdatedAt: now.Add(-20 * time.Minute).UnixMilli()}
	compatibilityRestore := ServiceRestore{RestoreID: target.RestoreID, RestorePlanID: "restore-plan-management-module",
		ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, Status: "succeeded",
		OriginalVolumeIDs: []string{"vol-11111111111111111"}, ReplacementVolumeIDs: append([]string(nil), target.DestroyVolumeIDs...),
		Revision: int64(target.RestoreRevision), CreatedAt: now.Add(-15 * time.Minute).UnixMilli(), UpdatedAt: now.Add(-10 * time.Minute).UnixMilli()}
	serviceAwaiting := Service{ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, RecipeID: target.RecipeID,
		Status: "awaiting_management_acceptance", Revision: 3, Backups: []ServiceBackup{compatibilityBackup}, Restores: []ServiceRestore{compatibilityRestore}}
	recipeAwaiting := Recipe{RecipeID: target.RecipeID, Maturity: "awaiting_management_acceptance", Revision: 2}
	scopeDigest, err := managedAcceptanceScopeDigest(approval)
	if err != nil {
		t.Fatal(err)
	}
	client := &agentControlModuleClient{
		managedChallenge: AgentCloudManagedAcceptanceChallenge{
			OperationID: operationID, OwnerID: "@owner:example.com", ScopeDigest: scopeDigest, Revision: 1,
			Confirmation: ServiceManagementAcceptanceConfirmation{Service: serviceAwaiting, Recipe: recipeAwaiting, Approval: approval},
		},
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: target.DeploymentID, DeploymentRevision: int64(target.DeploymentRevision), SignerKeyID: approval.SignerKeyID,
	}, found: true}
	published := 0
	module := New(store, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return clock },
		Publish:                 func(context.Context, string, string, map[string]any) error { published++; return nil },
		AgentCloudControlClient: client,
	})

	result, apiErr := module.Handlers()[actionServicesManagementPlan](t.Context(), map[string]any{
		"service_id": target.ServiceID, "expected_revision": float64(3), "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	confirmation := result.(map[string]any)["confirmation"].(ServiceManagementAcceptanceConfirmation)
	if !reflect.DeepEqual(confirmation, client.managedChallenge.Confirmation) ||
		client.managedChallengeRequest.ServiceID != target.ServiceID ||
		client.managedChallengeRequest.DeploymentID != target.DeploymentID ||
		client.managedChallengeRequest.SignerKeyID != approval.SignerKeyID ||
		client.managedChallengeRequest.ExpectedDeploymentRevision != int64(target.DeploymentRevision) || published != 0 {
		t.Fatalf("prepare result=%#v request=%#v published=%d", result, client.managedChallengeRequest, published)
	}

	signed, err := approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, 32)), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	activeService := Service{ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, RecipeID: target.RecipeID,
		Status: "active", Revision: 4, Backups: []ServiceBackup{compatibilityBackup}, Restores: []ServiceRestore{compatibilityRestore}}
	managedRecipe := Recipe{RecipeID: target.RecipeID, Maturity: "managed", Revision: 3}
	acceptance := ServiceManagementAcceptance{AcceptanceID: operationID, ServiceID: target.ServiceID, RecipeID: target.RecipeID, Status: "approved", Revision: 2}
	recovered := AgentCloudManagedAcceptanceOperation{
		OperationID: operationID, OwnerID: "@owner:example.com", ApprovalID: approval.ApprovalID,
		DeploymentID: target.DeploymentID, ScopeDigest: scopeDigest,
		Status: "succeeded", Revision: 3, Service: activeService, Recipe: managedRecipe, Acceptance: acceptance,
	}
	client.managedApproveErr = ErrAgentCloudControlUnavailable
	client.managedOperation, client.managedOperationFound, client.managedOperationFoundAfter = recovered, true, 1
	result, apiErr = module.Handlers()[actionServicesManagementApprove](t.Context(), map[string]any{
		"service_id": target.ServiceID, "expected_revision": float64(3), "approval": signed, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if !reflect.DeepEqual(result.(map[string]any)["service"], activeService) || !reflect.DeepEqual(result.(map[string]any)["recipe"], managedRecipe) ||
		!reflect.DeepEqual(result.(map[string]any)["acceptance"], acceptance) || client.managedApproveRequest.OperationID != operationID ||
		client.managedApproveRequest.ExpectedScopeDigest != scopeDigest ||
		client.managedApproveRequest.Approval.Signature != signed.Signature ||
		len(client.managedApproveRequest.ApprovalSignature.Signature) != ed25519.SignatureSize || published != 0 {
		t.Fatalf("approve result=%#v request=%#v published=%d", result, client.managedApproveRequest, published)
	}
	if len(result.(map[string]any)["service"].(Service).Backups) != 1 || len(result.(map[string]any)["service"].(Service).Restores) != 1 {
		t.Fatalf("legacy service evidence was dropped: %#v", result)
	}
	result, apiErr = module.Handlers()[actionServicesManagementApprove](t.Context(), map[string]any{
		"service_id": target.ServiceID, "expected_revision": float64(3), "approval": signed, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil || !reflect.DeepEqual(result.(map[string]any)["acceptance"], acceptance) ||
		client.managedApproveCalls != 1 || client.managedOperationCalls != 3 {
		t.Fatalf("unexpired exact readback result=%#v approve_calls=%d get_calls=%d err=%v", result, client.managedApproveCalls, client.managedOperationCalls, apiErr)
	}
	clock = now.Add(6 * time.Minute)
	store.compatibility.SignerKeyID = "device-management-rotated"
	result, apiErr = module.Handlers()[actionServicesManagementApprove](t.Context(), map[string]any{
		"service_id": target.ServiceID, "expected_revision": float64(3), "approval": signed, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil || !reflect.DeepEqual(result.(map[string]any)["acceptance"], acceptance) ||
		client.managedApproveCalls != 1 || client.managedOperationCalls != 4 {
		t.Fatalf("expired exact readback result=%#v approve_calls=%d get_calls=%d err=%v", result, client.managedApproveCalls, client.managedOperationCalls, apiErr)
	}
	clock = now
	store.compatibility.SignerKeyID = approval.SignerKeyID
	client.managedOperation.Status = "running"
	result, apiErr = module.Handlers()[actionServicesManagementApprove](t.Context(), map[string]any{
		"service_id": target.ServiceID, "expected_revision": float64(3), "approval": signed, "idempotency_key": uuid.NewString(),
	})
	if apiErr == nil || result != nil {
		t.Fatalf("non-succeeded operation was accepted: result=%#v err=%v", result, apiErr)
	}
}

type managedAcceptanceProjectionStore struct {
	Store
	compatibility ManagedAcceptanceCompatibility
	found         bool
	err           error
}

func (store *managedAcceptanceProjectionStore) GetCloudManagedAcceptanceCompatibility(context.Context, string, string) (ManagedAcceptanceCompatibility, bool, error) {
	return store.compatibility, store.found, store.err
}

func managementDigest(fill string) string {
	return "sha256:" + strings.Repeat(fill, 64)
}
