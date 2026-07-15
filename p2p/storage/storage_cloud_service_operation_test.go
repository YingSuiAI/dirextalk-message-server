package storage

import (
	"context"
	"encoding/json"
	"errors"
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
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "execution-operation-install-0001",
		DeploymentID: "deployment-destroy-0001", PlanID: "plan-destroy-0001",
		PlanHash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PlanRevision: 4,
		RecipeDigest:                 "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		WorkerResourceManifestDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		ArtifactDigest:               cloudcontracts.FixedProbeManagedArtifactDigest, ActionID: cloudcontracts.FixedProbeInstallActionID,
		RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_binary_installed", "probe_service_installed", "probe_health_verified"},
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, _ := json.Marshal(manifest)
	if _, err = store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_execution_manifests(execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at) VALUES($1,$2,$3,4,$4,$5,$6,$7,$8,'approved',2,$9,$9)`, manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, manifest.PlanHash, "connection-destroy-0001", manifestDigest, []byte{1}, string(manifestJSON), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}

	prepare := cloudmodule.PrepareServiceOperationRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, Operation: cloudcontracts.ServiceOperationStop, IdempotencyHash: "prepare-operation-idem", RequestDigest: "prepare-operation-request", ApprovalID: "approval-operation-0001", ChallengeID: "challenge-operation-0001", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, err := store.PrepareCloudServiceOperation(ctx, prepare)
	if err != nil || !prepared.Created {
		t.Fatalf("prepare operation=%#v err=%v", prepared, err)
	}
	approval := prepared.Confirmation.Approval
	if approval.ActionID != cloudcontracts.FixedProbeStopActionID || approval.ArtifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || approval.ExpectedServiceStatus != "experimental" || approval.Signature != "" {
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
	tampered.ActionID = cloudcontracts.FixedProbeRestartActionID
	bad := serviceOperationApprovalRequest(tampered, now.Add(time.Minute).UnixMilli())
	if _, err = store.ApproveCloudServiceOperation(ctx, bad); !errors.Is(err, cloudmodule.ErrServiceOperationConfirmationInvalid) && !errors.Is(err, cloudmodule.ErrServiceOperationApprovalSignature) {
		t.Fatalf("tampered operation=%v", err)
	}
	request := serviceOperationApprovalRequest(signed, now.Add(time.Minute).UnixMilli())
	approved, err := store.ApproveCloudServiceOperation(ctx, request)
	if err != nil || !approved.Created || approved.Job.Kind != "stop" || approved.Job.Checkpoint != "service_operation_queued" || approved.Service.Revision != 2 {
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
	if actionID != cloudcontracts.FixedProbeStopActionID || artifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || taskStatus != "queued" || len(operationManifest.SecretSlots) != 0 {
		t.Fatalf("sealed task action=%s artifact=%s status=%s", actionID, artifactDigest, taskStatus)
	}
	if _, err = store.PrepareCloudServiceDestroy(ctx, cloudmodule.PrepareServiceDestroyRequest{OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision), IdempotencyHash: "destroy-during-operation-idem", RequestDigest: "destroy-during-operation-request", ApprovalID: "destroy-during-operation-approval", ChallengeID: "destroy-during-operation-challenge", CreatedAt: now.Add(2*time.Minute).UnixMilli(), ExpiresAt: now.Add(5*time.Minute).UnixMilli()}); !errors.Is(err, cloudmodule.ErrServiceDestroyConfirmationInvalid) {
		t.Fatalf("destroy while managed operation is active = %v", err)
	}
	approvedReplay, err := store.ApproveCloudServiceOperation(ctx, request)
	if err != nil || approvedReplay.Created || approvedReplay.Job.JobID != approved.Job.JobID {
		t.Fatalf("approve replay=%#v err=%v", approvedReplay, err)
	}
}

func serviceOperationApprovalRequest(approval cloudcontracts.ServiceOperationApprovalV1, createdAt int64) cloudmodule.ApproveServiceOperationRequest {
	return cloudmodule.ApproveServiceOperationRequest{OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision), IdempotencyHash: "approve-operation-idem", Approval: approval, OperationID: "operation-0001", JobID: "job-operation-0001", OutboxID: "outbox-operation-0001", JobEventID: "event-operation-job-0001", CreatedAt: createdAt}
}
