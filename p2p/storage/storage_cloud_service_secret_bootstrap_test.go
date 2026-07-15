package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

func TestDatabaseStoreServiceSecretBootstrapBindsAcceptedCurrentTaskAndReplaysExactly(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	owner, deployment, privateKey, manifest := seedCloudRecipeExecutionReadyDeployment(t, store, now)
	registerTrustedArtifactForExecutionManifest(t, store, manifest, now.Add(90*time.Second).UnixMilli())
	registered, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, cloudmodule.RegisterTrustedRecipeExecutionManifestRequest{Manifest: manifest, RegisteredAt: now.Add(2 * time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	preparedExecution, err := store.PrepareCloudRecipeExecutionConfirmation(ctx, cloudmodule.PrepareRecipeExecutionConfirmationRequest{
		OwnerMXID: owner, DeploymentID: deployment.DeploymentID, ExpectedRevision: deployment.Revision,
		IdempotencyHash: "secret-bootstrap-execution-prepare-idempotency", RequestDigest: "secret-bootstrap-execution-prepare-request",
		ApprovalID: "secret-bootstrap-execution-approval-1", ChallengeID: "secret-bootstrap-execution-challenge-1",
		CreatedAt: now.Add(3 * time.Minute).UnixMilli(), ExpiresAt: now.Add(8 * time.Minute).UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	signedExecution, err := preparedExecution.Confirmation.Approval.Sign(privateKey, now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approvedExecution, err := store.ApproveCloudRecipeExecution(ctx, cloudmodule.ApproveRecipeExecutionRequest{
		OwnerMXID: owner, DeploymentID: deployment.DeploymentID, ExpectedRevision: deployment.Revision,
		IdempotencyHash: "secret-bootstrap-execution-approve-idempotency", Approval: signedExecution,
		Job: cloudmodule.Job{JobID: "job-secret-bootstrap-execution-1", PlanID: deployment.PlanID, DeploymentID: deployment.DeploymentID,
			Kind: "install", Execution: "queued", Outcome: "pending", Checkpoint: "install_queued", Revision: 1,
			CreatedAt: now.Add(4 * time.Minute).UnixMilli(), UpdatedAt: now.Add(4 * time.Minute).UnixMilli()},
		OutboxID: "outbox-secret-bootstrap-execution-1", JobEventID: "event-secret-bootstrap-execution-1", CreatedAt: now.Add(4 * time.Minute).UnixMilli(),
	})
	if err != nil || approvedExecution.Execution.Status != "approved" {
		t.Fatalf("approve execution = %#v, err=%v", approvedExecution, err)
	}

	brokerURL := "https://abcdefghij.execute-api.ap-south-1.amazonaws.com/prod/v2/commands"
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_connection_brokers SET broker_command_url=$1 WHERE cloud_connection_id=$2`, brokerURL, deployment.ConnectionID); err != nil {
		t.Fatal(err)
	}
	checkpointJSON, _ := json.Marshal(manifest.CheckpointSequence)
	taskID := "task-service-secret-bootstrap-1"
	createdAt := now.Add(5 * time.Minute).UnixMilli()
	if _, err = store.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_install_tasks(execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,'i-0123456789abcdef0',$6,'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',$7,'queued',$8,$8)
	`, manifest.ExecutionID, taskID, deployment.DeploymentID, deployment.PlanID, deployment.ConnectionID, registered.Execution.RecipeExecutionManifestDigest, string(checkpointJSON), createdAt); err != nil {
		t.Fatal(err)
	}
	command, err := broker.NewRecipeTaskCommand(broker.RecipeTaskCommandInput{
		ConnectionID: deployment.ConnectionID, CommandID: "command-service-secret-bootstrap-1", NodeKeyID: "node-key-recipe-execution-1",
		ExpectedGeneration: 1, NodeCounter: 1, IssuedAt: time.UnixMilli(createdAt), ExpiresAt: now.Add(10 * time.Minute),
		Action: broker.RecipeTaskIssueAction, PrivateKey: privateKey,
		Issue: broker.RecipeTaskIssueRequest{Schema: broker.RecipeTaskIssueSchema, ExecutionID: manifest.ExecutionID, DeploymentID: deployment.DeploymentID,
			TaskID: taskID, TaskKind: "recipe_execution", ManifestDigest: registered.Execution.RecipeExecutionManifestDigest,
			InputDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CheckpointSequence: manifest.CheckpointSequence, Manifest: manifest},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelopeJSON, _ := json.Marshal(command)
	payloadJSON, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	if _, err = store.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_install_commands(command_id,execution_id,deployment_id,task_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',1,'worker.recipe_task.issue','node-key-recipe-execution-1',1,1,$6,$7,$8,$9,$10,$11,'accepted',$10,$10)
	`, command.CommandID, manifest.ExecutionID, deployment.DeploymentID, taskID, deployment.ConnectionID, string(payloadJSON), command.PayloadSHA256,
		command.RequestSHA256(), string(envelopeJSON), createdAt, now.Add(10*time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}

	request := cloudmodule.PrepareServiceSecretBootstrapRequest{
		OwnerMXID: owner, DeploymentID: deployment.DeploymentID, SlotID: "model_token", ExpectedRevision: deployment.Revision,
		IdempotencyHash: "service-secret-bootstrap-idempotency-hash", RequestDigest: "service-secret-bootstrap-request-digest",
		SessionID: "service-secret-session-1", ApprovalID: "service-secret-approval-1", ChallengeID: "service-secret-challenge-1",
		CreatedAt: now.Add(6 * time.Minute).UnixMilli(), ExpiresAt: now.Add(16 * time.Minute).UnixMilli(),
	}

	stale := request
	stale.ExpectedRevision++
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, stale); !errors.Is(err, cloudmodule.ErrServiceSecretBootstrapConflict) {
		t.Fatalf("stale revision error = %v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET canonical_payload_json='{}' WHERE execution_id=$1`, manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, request); !errors.Is(err, cloudmodule.ErrServiceSecretBootstrapInvalid) {
		t.Fatalf("tampered signed payload error = %v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET canonical_payload_json=$1 WHERE execution_id=$2`, string(payloadJSON), manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET task_id='task-tampered' WHERE execution_id=$1`, manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, request); !errors.Is(err, cloudmodule.ErrServiceSecretBootstrapInvalid) {
		t.Fatalf("tampered accepted task error = %v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET task_id=$1 WHERE execution_id=$2`, taskID, manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status='succeeded' WHERE execution_id=$1`, manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, request); !errors.Is(err, cloudmodule.ErrServiceSecretBootstrapInvalid) {
		t.Fatalf("terminal task error = %v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status='running' WHERE execution_id=$1`, manifest.ExecutionID); err != nil {
		t.Fatal(err)
	}

	prepared, err := store.PrepareCloudServiceSecretBootstrap(ctx, request)
	if err != nil || !prepared.Created {
		t.Fatalf("prepare secret bootstrap = %#v, err=%v", prepared, err)
	}
	lateReplacement := request
	lateReplacement.IdempotencyHash = "service-secret-bootstrap-late-replacement"
	lateReplacement.RequestDigest = "service-secret-bootstrap-late-replacement-request"
	lateReplacement.SessionID = "service-secret-session-late-replacement"
	lateReplacement.ApprovalID = "service-secret-approval-late-replacement"
	lateReplacement.ChallengeID = "service-secret-challenge-late-replacement"
	lateReplacement.CreatedAt = request.ExpiresAt + 1
	lateReplacement.ExpiresAt = lateReplacement.CreatedAt + int64((10 * time.Minute).Milliseconds())
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, lateReplacement); !errors.Is(err, cloudmodule.ErrServiceSecretBootstrapConflict) {
		t.Fatalf("unreconciled expired approval was bypassed: %v", err)
	}
	approval := prepared.Confirmation.Approval
	if approval.TaskID != taskID || approval.ExecutionID != manifest.ExecutionID || approval.DeploymentID != deployment.DeploymentID ||
		approval.ManifestDigest != registered.Execution.RecipeExecutionManifestDigest || approval.SlotID != "model_token" ||
		approval.Purpose != "model provider access" || approval.Delivery != "environment" || approval.Signature != "" ||
		prepared.StackBaseURL != "https://abcdefghij.execute-api.ap-south-1.amazonaws.com/prod" {
		t.Fatalf("prepared secret approval binding = %#v base_url=%q", approval, prepared.StackBaseURL)
	}
	replay, err := store.PrepareCloudServiceSecretBootstrap(ctx, request)
	if err != nil || replay.Created || replay.Confirmation.Approval != approval || replay.StackBaseURL != prepared.StackBaseURL {
		t.Fatalf("replay = %#v, err=%v", replay, err)
	}
	conflict := request
	conflict.RequestDigest = "different-service-secret-bootstrap-request"
	if _, err = store.PrepareCloudServiceSecretBootstrap(ctx, conflict); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("idempotency conflict error = %v", err)
	}

	var persisted string
	if err = store.DB().QueryRowContext(ctx, `SELECT row_to_json(a)::text FROM p2p_cloud_service_secret_bootstrap_approvals a WHERE approval_id=$1`, approval.ApprovalID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{brokerURL, "session_token", "ciphertext", "secret_value", "sk-canary-never-persist"} {
		if strings.Contains(persisted, forbidden) {
			t.Fatalf("secret bootstrap ledger contains forbidden material %q: %s", forbidden, persisted)
		}
	}
}
