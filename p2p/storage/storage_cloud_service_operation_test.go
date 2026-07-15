package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDatabaseStoreServiceOperationApprovalDerivesManagedActionAtomically(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, publicSPKI, now.UnixMilli())
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_deployments SET resource_status='retained_tracked' WHERE deployment_id='deployment-destroy-0001'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='retained_tracked' WHERE deployment_id='deployment-destroy-0001'`); err != nil {
		t.Fatal(err)
	}
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "execution-operation-install-0001",
		DeploymentID: "deployment-destroy-0001", PlanID: "plan-destroy-0001",
		PlanHash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PlanRevision: 4,
		RecipeDigest:                 "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		WorkerResourceManifestDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		ArtifactDigest:               cloudcontracts.FixedProbeManagedArtifactDigest, ActionID: cloudcontracts.FixedProbeInstallActionID,
		RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_binary_installed", "probe_service_installed", "probe_health_verified"},
		SemanticReadiness: cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: cloudcontracts.FixedReadinessEvidenceDigestV1},
		VolumeSlots:       []cloudcontracts.VolumeSlotV1{{SlotID: "data", VolumeRef: "volume_ref:data"}},
		DataSlots:         []cloudcontracts.DataSlotV1{{SlotID: "knowledge", DataRef: "data_ref:knowledge", ReadOnly: true}},
		SecretSlots:       []cloudcontracts.SecretSlotV1{{SlotID: "model", SecretRef: "secret_ref:model"}},
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, _ := json.Marshal(manifest)
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_execution_manifests(execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at) VALUES($1,$2,$3,4,$4,$5,$6,$7,$8,'approved',2,$9,$9)`, manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, manifest.PlanHash, "connection-destroy-0001", manifestDigest, []byte{1}, string(manifestJSON), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	seedManagedOperationArtifact(t, store, manifest, now.UnixMilli())

	prepare := cloudmodule.PrepareServiceOperationRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, Operation: cloudcontracts.ServiceOperationRestart, IdempotencyHash: "prepare-operation-idem", RequestDigest: "prepare-operation-request", ApprovalID: "approval-operation-0001", ChallengeID: "challenge-operation-0001", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, err := store.PrepareCloudServiceOperation(ctx, prepare)
	if err != nil || !prepared.Created {
		t.Fatalf("prepare operation=%#v err=%v", prepared, err)
	}
	approval := prepared.Confirmation.Approval
	if approval.ActionID != cloudcontracts.FixedProbeRestartActionID || approval.ArtifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || approval.ExpectedServiceStatus != "experimental" || approval.Signature != "" ||
		!reflect.DeepEqual(approval.VolumeSlots, manifest.VolumeSlots) || !reflect.DeepEqual(approval.DataSlots, manifest.DataSlots) || !reflect.DeepEqual(approval.SecretSlots, manifest.SecretSlots) {
		t.Fatalf("derived approval=%#v", approval)
	}
	replay, err := store.PrepareCloudServiceOperation(ctx, prepare)
	if err != nil || replay.Created {
		t.Fatalf("prepare replay=%#v err=%v", replay, err)
	}
	conflict := prepare
	conflict.RequestDigest = "different"
	if _, err = store.PrepareCloudServiceOperation(ctx, conflict); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("prepare conflict=%v", err)
	}

	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tampered := signed
	tampered.ActionID = cloudcontracts.FixedProbeStopActionID
	bad := serviceOperationApprovalRequest(tampered, now.Add(time.Minute).UnixMilli())
	if _, err = store.ApproveCloudServiceOperation(ctx, bad); !errors.Is(err, cloudmodule.ErrServiceOperationConfirmationInvalid) && !errors.Is(err, cloudmodule.ErrServiceOperationApprovalSignature) {
		t.Fatalf("tampered operation=%v", err)
	}
	tamperedSlots := approval
	tamperedSlots.SecretSlots = append(tamperedSlots.SecretSlots, cloudcontracts.SecretSlotV1{SlotID: "extra", SecretRef: "secret_ref:extra"})
	tamperedSlots, err = tamperedSlots.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tamperedSlotRequest := serviceOperationApprovalRequest(tamperedSlots, now.Add(time.Minute).UnixMilli())
	tamperedSlotRequest.IdempotencyHash = "approve-operation-tampered-slot-idem"
	if _, err = store.ApproveCloudServiceOperation(ctx, tamperedSlotRequest); !errors.Is(err, cloudmodule.ErrServiceOperationConfirmationInvalid) {
		t.Fatalf("tampered operation slots=%v", err)
	}
	var queuedAfterTamper int
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_service_operation_tasks WHERE approval_id=$1`, approval.ApprovalID).Scan(&queuedAfterTamper); err != nil || queuedAfterTamper != 0 {
		t.Fatalf("tampered slots entered issue path count=%d err=%v", queuedAfterTamper, err)
	}
	request := serviceOperationApprovalRequest(signed, now.Add(time.Minute).UnixMilli())
	approved, err := store.ApproveCloudServiceOperation(ctx, request)
	if err != nil || !approved.Created || approved.Job.Kind != "restart" || approved.Job.Checkpoint != "service_operation_queued" || approved.Service.Revision != 2 {
		t.Fatalf("approved operation=%#v err=%v", approved, err)
	}
	var actionID, taskStatus, artifactDigest string
	if err = store.DB().QueryRowContext(ctx, `SELECT manifest_json,task_status FROM p2p_cloud_service_operation_tasks WHERE operation_id=$1`, request.OperationID).Scan(&manifestJSON, &taskStatus); err != nil {
		t.Fatal(err)
	}
	var operationManifest cloudcontracts.RecipeExecutionManifestV1
	if json.Unmarshal([]byte(manifestJSON), &operationManifest) != nil {
		t.Fatal("decode operation manifest")
	}
	actionID, artifactDigest = operationManifest.ActionID, operationManifest.ArtifactDigest
	if actionID != cloudcontracts.FixedProbeRestartActionID || artifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || taskStatus != "queued" ||
		operationManifest.SemanticReadiness != manifest.SemanticReadiness ||
		!reflect.DeepEqual(operationManifest.VolumeSlots, manifest.VolumeSlots) || !reflect.DeepEqual(operationManifest.DataSlots, manifest.DataSlots) || !reflect.DeepEqual(operationManifest.SecretSlots, manifest.SecretSlots) {
		t.Fatalf("sealed task action=%s artifact=%s status=%s", actionID, artifactDigest, taskStatus)
	}
	if _, err = store.PrepareCloudServiceDestroy(ctx, cloudmodule.PrepareServiceDestroyRequest{OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision), IdempotencyHash: "destroy-during-operation-idem", RequestDigest: "destroy-during-operation-request", ApprovalID: "destroy-during-operation-approval", ChallengeID: "destroy-during-operation-challenge", CreatedAt: now.Add(2 * time.Minute).UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}); !errors.Is(err, cloudmodule.ErrServiceDestroyConfirmationInvalid) {
		t.Fatalf("destroy while managed operation is active = %v", err)
	}
	approvedReplay, err := store.ApproveCloudServiceOperation(ctx, request)
	if err != nil || approvedReplay.Created || approvedReplay.Job.JobID != approved.Job.JobID {
		t.Fatalf("approve replay=%#v err=%v", approvedReplay, err)
	}
}

func seedManagedOperationArtifact(t *testing.T, store *DatabaseStore, manifest cloudcontracts.RecipeExecutionManifestV1, now int64) {
	t.Helper()
	artifact := cloudcontracts.CompiledRecipeArtifactV1{
		SchemaVersion: cloudcontracts.CompiledRecipeArtifactV1Schema, RecipeID: "recipe-destroy-0001", RecipeDigest: manifest.RecipeDigest, RecipeRevision: 1,
		OfficialSourceArtifactDigests: []string{"sha256:" + strings.Repeat("1", 64)}, Architecture: cloudcontracts.ArchitectureAMD64,
		Requirements:                 cloudcontracts.ResourceRequirementsV1{MinVCPU: 2, MinMemoryMiB: 2048, MinDiskGiB: 20, Architecture: cloudcontracts.ArchitectureAMD64},
		WorkerResourceManifestDigest: manifest.WorkerResourceManifestDigest, ArtifactDigest: manifest.ArtifactDigest,
		ImageSource: cloudcontracts.OCIImageSourceReferenceV1("ghcr.io/dirextalk/fixed-probe@" + manifest.ArtifactDigest), MediaType: "application/vnd.dirextalk.recipe", SizeBytes: 1 << 20,
		Actions: []cloudcontracts.CompiledRecipeActionV1{
			{Kind: cloudcontracts.CompiledRecipeActionInstall, ActionID: cloudcontracts.FixedProbeInstallActionID, RootRequired: true, TimeoutSeconds: manifest.TimeoutSeconds, CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...)},
			{Kind: cloudcontracts.CompiledRecipeActionStart, ActionID: cloudcontracts.FixedProbeStartActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_started", "probe_health_verified"}},
			{Kind: cloudcontracts.CompiledRecipeActionStop, ActionID: cloudcontracts.FixedProbeStopActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_stopped"}},
			{Kind: cloudcontracts.CompiledRecipeActionRestart, ActionID: cloudcontracts.FixedProbeRestartActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_restarted", "probe_health_verified"}},
		},
		SemanticReadiness: manifest.SemanticReadiness, HealthContractDigest: "sha256:" + strings.Repeat("2", 64), LifecycleContractDigest: "sha256:" + strings.Repeat("3", 64),
		VolumeSlots: []cloudcontracts.RecipeVolumeSlotRequirementV1{{SlotID: "data", Purpose: "service data", ReadOnly: false}},
		DataSlots:   []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge data", ReadOnly: true}},
		SecretSlots: []cloudcontracts.RecipeSecretSlotRequirementV1{{SlotID: "model", Purpose: "model credential", Delivery: cloudcontracts.SecretDeliveryFile}},
	}
	canonical, err := artifact.CanonicalCompiledRecipeArtifactCBOR()
	if err != nil {
		t.Fatal(err)
	}
	descriptorDigest, err := artifact.Digest()
	if err != nil {
		t.Fatal(err)
	}
	descriptorJSON, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(context.Background(), `INSERT INTO p2p_cloud_recipe_artifacts(artifact_digest,descriptor_digest,recipe_id,recipe_revision,recipe_digest,worker_resource_manifest_digest,canonical_cbor,descriptor_json,status,revision,created_at,updated_at) VALUES($1,$2,$3,1,$4,$5,$6,$7,'verified',1,$8,$8)`, artifact.ArtifactDigest, descriptorDigest, artifact.RecipeID, artifact.RecipeDigest, artifact.WorkerResourceManifestDigest, canonical, string(descriptorJSON), now); err != nil {
		t.Fatal(err)
	}
}

func TestServiceOperationManifestKeepsFixedProbeEmptySlots(t *testing.T) {
	semanticProbe := cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: cloudcontracts.FixedReadinessEvidenceDigestV1}
	installed := cloudcontracts.RecipeExecutionManifestV1{DeploymentID: "deployment-fixed-probe", PlanID: "plan-fixed-probe", PlanHash: "sha256:" + strings.Repeat("a", 64), PlanRevision: 1, WorkerResourceManifestDigest: "sha256:" + strings.Repeat("b", 64), SemanticReadiness: semanticProbe}
	target := cloudcontracts.ServiceOperationTargetV1{RecipeDigest: "sha256:" + strings.Repeat("c", 64), ArtifactDigest: cloudcontracts.FixedProbeManagedArtifactDigest, ActionID: cloudcontracts.FixedProbeRestartActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_restarted", "probe_health_verified"}}
	manifest := serviceOperationManifest(installed, "operation-fixed-probe", target)
	if manifest.SemanticReadiness != semanticProbe || len(manifest.VolumeSlots) != 0 || len(manifest.DataSlots) != 0 || len(manifest.SecretSlots) != 0 {
		t.Fatalf("fixed probe empty slots changed: %#v", manifest)
	}
}

func serviceOperationApprovalRequest(approval cloudcontracts.ServiceOperationApprovalV1, createdAt int64) cloudmodule.ApproveServiceOperationRequest {
	return cloudmodule.ApproveServiceOperationRequest{OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision), IdempotencyHash: "approve-operation-idem", Approval: approval, OperationID: "operation-0001", JobID: "job-operation-0001", OutboxID: "outbox-operation-0001", JobEventID: "event-operation-job-0001", CreatedAt: createdAt}
}
