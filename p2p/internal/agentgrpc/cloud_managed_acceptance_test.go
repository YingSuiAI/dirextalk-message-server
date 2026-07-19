package agentgrpc

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestManagedAcceptanceTransportPreservesLegacySigningPayloadAndExactResult(t *testing.T) {
	now := time.Date(2026, 7, 17, 14, 0, 0, 0, time.UTC)
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	operationID, deploymentID := uuid.NewString(), uuid.NewString()
	connectionID := uuid.NewString()
	target := cloudcontracts.ServiceManagementAcceptanceTargetV1{
		AgentInstanceID: uuid.NewString(), OwnerID: runner.ownerID,
		AcceptanceID: operationID, ServiceID: "service-managed-acceptance", ServiceRevision: 3,
		DeploymentID: deploymentID, DeploymentRevision: 5, CloudConnectionID: connectionID, ConnectionRevision: 4,
		PlanID: uuid.NewString(), PlanRevision: 3, PlanHash: digestFor("9"),
		RecipeID: "recipe-managed-acceptance", RecipeDigest: digestFor("a"), RecipeRevision: 2,
		RecipeMaturity: cloudcontracts.RecipeAwaitingManagementAccept, InstalledManifestDigest: digestFor("b"),
		ArtifactDigest: digestFor("c"), ReadinessSemanticEvidenceDigest: cloudcontracts.FixedReadinessEvidenceDigestV1,
		ReadinessStackObservationDigest: digestFor("d"), RestartOperationID: "restart-managed-acceptance",
		RestartOperationRevision: 2, BackupID: "backup-managed-acceptance", BackupRevision: 2,
		RestoreID: "restore-managed-acceptance", RestoreRevision: 2, SourceArtifactDigests: []string{digestFor("e")},
		HealthRevision: 4, HealthMonitorKind: "service", HealthStatus: "healthy", HealthEvidenceType: "independent_external",
		HealthEvidenceDigest: digestFor("8"), HealthObservedAt: now.Add(-time.Minute), Currency: "USD", CostAlertAmountMinor: 2500,
		Health: cloudcontracts.HealthContractV1{
			Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/live"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/ready"},
			Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "semantic"},
		},
		Lifecycle: cloudcontracts.ServiceManagementAcceptanceLifecycleV2{Start: "start", Stop: "stop", Maintenance: "maintenance", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
		Resources: []cloudcontracts.ServiceManagementAcceptanceResourceV2{
			{ResourceID: uuid.NewString(), Type: "ec2", Revision: 2, ProviderID: "i-0123456789abcdef0", TagDigest: digestFor("7")},
		},
		DestroyInstanceID: "i-0123456789abcdef0", DestroyVolumeIDs: []string{"vol-0123456789abcdef0"},
		DestroyNetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, AcceptancePolicy: cloudcontracts.ServiceManagementAcceptancePolicy,
	}
	approval, err := cloudcontracts.NewServiceManagementAcceptanceApprovalV1(target, uuid.NewString(), uuid.NewString(), "device-managed-acceptance", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	scopeDigest := fmt.Sprintf("sha256:%x", sum[:])
	scope := managedScopeProto(runner.ownerID, target)
	challenge := &agentv1.CloudManagedAcceptanceChallenge{
		OperationId: operationID, ChallengeId: approval.ChallengeID, ApprovalId: approval.ApprovalID, SignerKeyId: approval.SignerKeyID,
		Scope: scope, ScopeDigest: scopeDigest, IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)),
		Revision: 1, SigningPayloadCbor: payload,
		CompatibilityService: managedServiceProto(target, "awaiting_management_acceptance", 3),
		CompatibilityRecipe:  managedRecipeProto(target, "awaiting_management_acceptance", 2),
	}
	operation := &agentv1.CloudManagedAcceptanceOperation{
		OperationId: operationID, Challenge: challenge,
		Status:   agentv1.CloudManagedAcceptanceOperationStatus_CLOUD_MANAGED_ACCEPTANCE_OPERATION_STATUS_SUCCEEDED,
		Revision: 2, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now.Add(time.Second)),
		CompatibilityService: managedServiceProto(target, "active", 4),
		CompatibilityRecipe:  managedRecipeProto(target, "managed", 3),
		CompatibilityAcceptance: &agentv1.CloudManagedCompatibilityAcceptance{
			AcceptanceId: operationID, ServiceId: target.ServiceID, RecipeId: target.RecipeID, Status: "approved",
			Revision: 2, CreatedAtUnixMs: now.UnixMilli(), UpdatedAtUnixMs: now.Add(time.Second).UnixMilli(),
		},
	}
	var createRequest *agentv1.CreateCloudManagedAcceptanceChallengeRequest
	var approveRequest *agentv1.ApproveCloudManagedAcceptanceRequest
	server.cloud.createManaged = func(request *agentv1.CreateCloudManagedAcceptanceChallengeRequest) (*agentv1.CreateCloudManagedAcceptanceChallengeResponse, error) {
		createRequest = request
		return &agentv1.CreateCloudManagedAcceptanceChallengeResponse{Challenge: challenge}, nil
	}
	server.cloud.approveManaged = func(request *agentv1.ApproveCloudManagedAcceptanceRequest) (*agentv1.ApproveCloudManagedAcceptanceResponse, error) {
		approveRequest = request
		return &agentv1.ApproveCloudManagedAcceptanceResponse{Operation: operation}, nil
	}
	server.cloud.getManaged = func(request *agentv1.GetCloudManagedAcceptanceOperationRequest) (*agentv1.GetCloudManagedAcceptanceOperationResponse, error) {
		if request.GetOwnerId() != runner.ownerID || request.GetOperationId() != operationID {
			t.Fatalf("get request=%+v", request)
		}
		return &agentv1.GetCloudManagedAcceptanceOperationResponse{Operation: operation}, nil
	}

	mapped, err := runner.CreateCloudManagedAcceptanceChallenge(t.Context(), cloudmodule.AgentCloudManagedAcceptanceChallengeRequest{
		IdempotencyKey: uuid.NewString(), ServiceID: target.ServiceID, DeploymentID: deploymentID,
		SignerKeyID: approval.SignerKeyID, ExpectedDeploymentRevision: int64(target.DeploymentRevision),
	})
	if err != nil || !reflect.DeepEqual(mapped.Confirmation.Approval, approval) || !bytes.Equal(payload, challenge.GetSigningPayloadCbor()) ||
		len(mapped.Confirmation.Service.Backups) != 1 || mapped.Confirmation.Service.Backups[0].BackupID != target.BackupID ||
		len(mapped.Confirmation.Service.Restores) != 1 || mapped.Confirmation.Service.Restores[0].RestoreID != target.RestoreID ||
		createRequest.GetOwnerId() != runner.ownerID || createRequest.GetExpectedDeploymentRevision() != int64(target.DeploymentRevision) {
		t.Fatalf("challenge=%#v request=%+v err=%v", mapped, createRequest, err)
	}
	signed, err := approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x32}, 32)), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	signature := cloudmodule.AgentCloudApprovalSignature{ApprovalID: signed.ApprovalID, ChallengeID: signed.ChallengeID, SignerKeyID: signed.SignerKeyID,
		ExpiresAt: signed.ExpiresAt, Signature: mustDecodeManagedSignature(t, signed.Signature)}
	result, err := runner.ApproveCloudManagedAcceptance(t.Context(), cloudmodule.AgentCloudManagedAcceptanceApproveRequest{
		IdempotencyKey: uuid.NewString(), OperationID: operationID, ServiceID: target.ServiceID, DeploymentID: deploymentID,
		ExpectedServiceRevision: int64(target.ServiceRevision), ExpectedOperationRevision: 1, ExpectedScopeDigest: scopeDigest,
		Approval: signed, ApprovalSignature: signature,
	})
	if err != nil || result.Status != "succeeded" || result.Acceptance.AcceptanceID != operationID ||
		approveRequest.GetOwnerId() != runner.ownerID || approveRequest.GetAcceptanceId() != operationID ||
		approveRequest.GetScopeDigest() != scopeDigest || len(approveRequest.GetApproval().GetSignature()) != ed25519.SignatureSize {
		t.Fatalf("operation=%#v request=%+v err=%v", result, approveRequest, err)
	}
	read, found, err := runner.GetCloudManagedAcceptanceOperation(t.Context(), cloudmodule.AgentCloudManagedAcceptanceOperationRequest{OperationID: operationID})
	if err != nil || !found || read.Acceptance.AcceptanceID != operationID {
		t.Fatalf("read=%#v found=%v err=%v", read, found, err)
	}
	challenge.CompatibilityService.Restores[0].ReplacementVolumeIds = []string{"vol-22222222222222222"}
	if _, err = runner.CreateCloudManagedAcceptanceChallenge(t.Context(), cloudmodule.AgentCloudManagedAcceptanceChallengeRequest{
		IdempotencyKey: uuid.NewString(), ServiceID: target.ServiceID, DeploymentID: deploymentID,
		SignerKeyID: approval.SignerKeyID, ExpectedDeploymentRevision: int64(target.DeploymentRevision),
	}); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("mismatched restore scope error=%v", err)
	}
	challenge.CompatibilityService.Restores[0].ReplacementVolumeIds = append([]string(nil), target.DestroyVolumeIDs...)
	challenge.SigningPayloadCbor = append(append([]byte(nil), payload...), 0)
	if _, err = runner.CreateCloudManagedAcceptanceChallenge(t.Context(), cloudmodule.AgentCloudManagedAcceptanceChallengeRequest{
		IdempotencyKey: uuid.NewString(), ServiceID: target.ServiceID, DeploymentID: deploymentID,
		SignerKeyID: approval.SignerKeyID, ExpectedDeploymentRevision: int64(target.DeploymentRevision),
	}); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("tampered signing payload error=%v", err)
	}
}

func managedScopeProto(owner string, target cloudcontracts.ServiceManagementAcceptanceTargetV1) *agentv1.CloudManagedAcceptanceScope {
	scope := &agentv1.CloudManagedAcceptanceScope{
		AgentInstanceId: target.AgentInstanceID, AcceptanceId: target.AcceptanceID, ServiceId: target.ServiceID, ServiceRevision: target.ServiceRevision, OwnerId: owner,
		DeploymentId: target.DeploymentID, DeploymentRevision: int64(target.DeploymentRevision), ConnectionId: target.CloudConnectionID,
		ConnectionRevision: target.ConnectionRevision, PlanId: target.PlanID, PlanRevision: target.PlanRevision, PlanHash: target.PlanHash,
		RecipeId: target.RecipeID, RecipeDigest: target.RecipeDigest, RecipeRevision: target.RecipeRevision, RecipeMaturity: string(target.RecipeMaturity),
		InstalledManifestDigest: target.InstalledManifestDigest, ArtifactDigest: target.ArtifactDigest,
		ReadinessSemanticEvidenceDigest: target.ReadinessSemanticEvidenceDigest, ReadinessStackObservationDigest: target.ReadinessStackObservationDigest,
		RestartOperationId: target.RestartOperationID, RestartOperationRevision: target.RestartOperationRevision,
		BackupId: target.BackupID, BackupRevision: target.BackupRevision, RestoreId: target.RestoreID, RestoreRevision: target.RestoreRevision,
		SourceArtifactDigests: append([]string(nil), target.SourceArtifactDigests...),
		HealthRevision:        target.HealthRevision, HealthMonitorKind: target.HealthMonitorKind, HealthEvidenceDigest: target.HealthEvidenceDigest,
		HealthObservedAt: timestamppb.New(target.HealthObservedAt),
		HealthStatus:     target.HealthStatus, HealthEvidenceType: target.HealthEvidenceType,
		Currency: target.Currency, CostAlertAmountMinor: target.CostAlertAmountMinor,
		Lifecycle: &agentv1.CloudManagedLifecycleContract{Start: target.Lifecycle.Start, Stop: target.Lifecycle.Stop, Restart: target.Lifecycle.Restart,
			Upgrade: target.Lifecycle.Upgrade, Rollback: target.Lifecycle.Rollback, Backup: target.Lifecycle.Backup, Restore: target.Lifecycle.Restore,
			Destroy: target.Lifecycle.Destroy, Maintenance: target.Lifecycle.Maintenance},
		Health: &agentv1.CloudManagedHealthContract{
			Liveness:  &agentv1.CloudManagedHealthProbe{Kind: string(target.Health.Liveness.Kind), Target: target.Health.Liveness.Target},
			Readiness: &agentv1.CloudManagedHealthProbe{Kind: string(target.Health.Readiness.Kind), Target: target.Health.Readiness.Target},
			Semantic:  &agentv1.CloudManagedHealthProbe{Kind: string(target.Health.Semantic.Kind), Target: target.Health.Semantic.Target},
		},
		DestroyInstanceId: target.DestroyInstanceID, DestroyVolumeIds: append([]string(nil), target.DestroyVolumeIDs...),
		DestroyNetworkInterfaceIds: append([]string(nil), target.DestroyNetworkInterfaceIDs...), AcceptancePolicy: target.AcceptancePolicy,
	}
	for _, item := range target.Resources {
		scope.Resources = append(scope.Resources, &agentv1.CloudManagedAcceptanceResource{
			ResourceId: item.ResourceID, Type: agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EC2, Revision: item.Revision, ProviderId: item.ProviderID, TagDigest: item.TagDigest,
		})
	}
	return scope
}

func managedServiceProto(target cloudcontracts.ServiceManagementAcceptanceTargetV1, status string, revision int64) *agentv1.CloudManagedCompatibilityService {
	return &agentv1.CloudManagedCompatibilityService{ServiceId: target.ServiceID, DeploymentId: target.DeploymentID, RecipeId: target.RecipeID,
		Name: "Managed Service", ServiceStatus: status, IntegrationStatus: "integrated", Revision: revision,
		CreatedAtUnixMs: time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC).UnixMilli(), UpdatedAtUnixMs: time.Date(2026, 7, 17, 14, 0, 0, 0, time.UTC).UnixMilli(),
		Backups: []*agentv1.CloudManagedCompatibilityBackup{{
			BackupId: target.BackupID, ServiceId: target.ServiceID, DeploymentId: target.DeploymentID, Status: "available",
			RetentionPolicy: "manual", ImageId: "ami-0123456789abcdef0", SnapshotIds: []string{"snap-0123456789abcdef0"},
			Revision: int64(target.BackupRevision), CreatedAtUnixMs: time.Date(2026, 7, 17, 13, 10, 0, 0, time.UTC).UnixMilli(),
			UpdatedAtUnixMs: time.Date(2026, 7, 17, 13, 20, 0, 0, time.UTC).UnixMilli(),
		}},
		Restores: []*agentv1.CloudManagedCompatibilityRestore{{
			RestoreId: target.RestoreID, RestorePlanId: "restore-plan-managed-acceptance", ServiceId: target.ServiceID,
			DeploymentId: target.DeploymentID, BackupId: target.BackupID, Status: "succeeded",
			OriginalVolumeIds: []string{"vol-11111111111111111"}, ReplacementVolumeIds: append([]string(nil), target.DestroyVolumeIDs...),
			Revision: int64(target.RestoreRevision), CreatedAtUnixMs: time.Date(2026, 7, 17, 13, 30, 0, 0, time.UTC).UnixMilli(),
			UpdatedAtUnixMs: time.Date(2026, 7, 17, 13, 40, 0, 0, time.UTC).UnixMilli(),
		}}}
}

func managedRecipeProto(target cloudcontracts.ServiceManagementAcceptanceTargetV1, maturity string, revision int64) *agentv1.CloudManagedCompatibilityRecipe {
	return &agentv1.CloudManagedCompatibilityRecipe{RecipeId: target.RecipeID, Name: "Managed Recipe", Version: "1.0.0", Digest: target.RecipeDigest,
		Maturity: maturity, Revision: revision, CreatedAtUnixMs: time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC).UnixMilli(),
		UpdatedAtUnixMs: time.Date(2026, 7, 17, 14, 0, 0, 0, time.UTC).UnixMilli()}
}

func mustDecodeManagedSignature(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
