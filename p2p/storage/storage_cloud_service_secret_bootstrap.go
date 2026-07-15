package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

var _ cloudmodule.ServiceSecretBootstrapStore = (*DatabaseStore)(nil)

type storedRecipeInstallTaskProof struct {
	ExecutionID, TaskID, DeploymentID, PlanID, ConnectionID, InstanceID string
	ManifestDigest, InputDigest, CheckpointSequenceJSON, Status         string
}

func (s *DatabaseStore) PrepareCloudServiceSecretBootstrap(ctx context.Context, request cloudmodule.PrepareServiceSecretBootstrapRequest) (cloudmodule.PrepareServiceSecretBootstrapResult, error) {
	if validatePrepareServiceSecretBootstrapRequest(request) != nil {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	var result cloudmodule.PrepareServiceSecretBootstrapResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceSecretBootstrapReplay(ctx, tx, request); err != nil || found {
			result = replay
			return err
		}
		owner, err := lockCloudRecipeExecutionOwner(ctx, tx, request.DeploymentID)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		if owner != request.OwnerMXID {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		deployment, err := lockCloudDeploymentForRecipeExecution(ctx, tx, request.DeploymentID)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		if deployment.Revision != request.ExpectedRevision {
			return cloudmodule.ErrServiceSecretBootstrapConflict
		}
		if !recipeExecutionDeploymentReady(deployment) {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		plan, err := lockCloudPlanForRecipeExecution(ctx, tx, deployment.PlanID)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		approvedPlan, approvedPlanHash, err := loadApprovedPlanV1ForRecipeExecution(ctx, tx, plan)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		manifest, err := lockRecipeExecutionManifestByDeploymentID(ctx, tx, request.DeploymentID)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		if manifest.Execution.Status != recipeExecutionStatusApproved {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		if manifest.PlanHash != approvedPlanHash || validateRecipeExecutionStoredBinding(deployment, approvedPlan, manifest) != nil {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		if err = validateRecipeExecutionDeploymentBinding(ctx, tx, deployment, approvedPlan, manifest.Manifest, request.CreatedAt); err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}

		task, err := lockCurrentRecipeInstallTaskProof(ctx, tx, manifest)
		if err != nil {
			return err
		}
		if err := lockAcceptedRecipeInstallIssueProof(ctx, tx, task); err != nil {
			return err
		}
		artifact, err := lockVerifiedRecipeArtifactForExecution(ctx, tx, manifest.Manifest, approvedPlan)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		recipe, err := lockExactCurrentCloudRecipe(ctx, tx, artifact.RecipeID, artifact.RecipeRevision, artifact.RecipeDigest)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		reference, err := exactServiceSecretSlotBinding(request.SlotID, approvedPlan, recipe, artifact, manifest.Manifest)
		if err != nil {
			return err
		}
		signerKeyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, approvedPlan.CloudConnectionID)
		if err != nil {
			return mapServiceSecretBootstrapStateError(err)
		}
		if signerKeyID == "" {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		stackBaseURL, err := lockServiceSecretStackBaseURL(ctx, tx, approvedPlan.CloudConnectionID)
		if err != nil {
			return err
		}

		approval, err := cloudcontracts.NewServiceSecretApprovalV1(cloudcontracts.ServiceSecretApprovalV1{
			ApprovalID: request.ApprovalID, ChallengeID: request.ChallengeID, SignerKeyID: signerKeyID,
			SessionID: request.SessionID, ConnectionID: approvedPlan.CloudConnectionID, DeploymentID: deployment.DeploymentID,
			TaskID: task.TaskID, ExecutionID: task.ExecutionID, ManifestDigest: manifest.Execution.RecipeExecutionManifestDigest,
			RecipeDigest: manifest.Manifest.RecipeDigest, ArtifactDigest: manifest.Manifest.ArtifactDigest,
			SlotID: request.SlotID, SecretRef: reference.SecretRef, Purpose: reference.Purpose, Delivery: string(reference.Delivery),
			IssuedAt: time.UnixMilli(request.CreatedAt).UTC(), ExpiresAt: time.UnixMilli(request.ExpiresAt).UTC(),
		})
		if err != nil {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		signingPayload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		approvalJSON, err := json.Marshal(approval)
		if err != nil {
			return err
		}
		insertResult, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_service_secret_bootstrap_approvals (
				approval_id,challenge_id,session_id,owner_mxid,deployment_id,deployment_revision,plan_id,plan_revision,cloud_connection_id,
				task_id,execution_id,manifest_digest,recipe_digest,artifact_digest,slot_id,secret_ref,purpose,delivery,context_digest,
				signer_key_id,approval_json,signing_payload_cbor,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,'pending',$23,$24,$25,$26,$26)
			ON CONFLICT DO NOTHING
		`, approval.ApprovalID, approval.ChallengeID, approval.SessionID, request.OwnerMXID, deployment.DeploymentID, deployment.Revision,
			approvedPlan.PlanID, approvedPlan.Revision, approval.ConnectionID, approval.TaskID, approval.ExecutionID, approval.ManifestDigest,
			approval.RecipeDigest, approval.ArtifactDigest, approval.SlotID, approval.SecretRef, approval.Purpose, approval.Delivery,
			approval.ContextDigest, approval.SignerKeyID, string(approvalJSON), signingPayload, request.IdempotencyHash, request.RequestDigest,
			request.ExpiresAt, request.CreatedAt)
		if err != nil {
			return err
		}
		inserted, err := insertResult.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			if replay, found, replayErr := loadServiceSecretBootstrapReplay(ctx, tx, request); replayErr != nil || found {
				result = replay
				return replayErr
			}
			return cloudmodule.ErrServiceSecretBootstrapConflict
		}
		result = cloudmodule.PrepareServiceSecretBootstrapResult{
			Confirmation: cloudmodule.ServiceSecretBootstrapConfirmation{Approval: approval},
			StackBaseURL: stackBaseURL,
			Created:      true,
		}
		return nil
	})
	return result, err
}

func validatePrepareServiceSecretBootstrapRequest(request cloudmodule.PrepareServiceSecretBootstrapRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || request.DeploymentID == "" || request.SlotID == "" || request.ExpectedRevision <= 0 ||
		request.IdempotencyHash == "" || request.RequestDigest == "" || request.SessionID == "" || request.ApprovalID == "" || request.ChallengeID == "" ||
		request.CreatedAt <= 0 || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((10*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return nil
}

func lockCurrentRecipeInstallTaskProof(ctx context.Context, tx *sql.Tx, manifest storedRecipeExecutionManifest) (storedRecipeInstallTaskProof, error) {
	var task storedRecipeInstallTaskProof
	err := tx.QueryRowContext(ctx, `
		SELECT execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status
		FROM p2p_cloud_recipe_install_tasks WHERE execution_id=$1 FOR UPDATE
	`, manifest.Execution.ExecutionID).Scan(&task.ExecutionID, &task.TaskID, &task.DeploymentID, &task.PlanID, &task.ConnectionID, &task.InstanceID,
		&task.ManifestDigest, &task.InputDigest, &task.CheckpointSequenceJSON, &task.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecipeInstallTaskProof{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	if err != nil {
		return storedRecipeInstallTaskProof{}, err
	}
	var checkpoints []string
	var resourceInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT instance_id FROM p2p_cloud_deployment_resources WHERE deployment_id=$1 FOR UPDATE`, task.DeploymentID).Scan(&resourceInstanceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storedRecipeInstallTaskProof{}, cloudmodule.ErrServiceSecretBootstrapInvalid
		}
		return storedRecipeInstallTaskProof{}, err
	}
	if json.Unmarshal([]byte(task.CheckpointSequenceJSON), &checkpoints) != nil || !reflect.DeepEqual(checkpoints, manifest.Manifest.CheckpointSequence) ||
		task.ExecutionID != manifest.Execution.ExecutionID || task.DeploymentID != manifest.Execution.DeploymentID || task.PlanID != manifest.Execution.PlanID ||
		task.ConnectionID != manifest.ConnectionID || task.ManifestDigest != manifest.Execution.RecipeExecutionManifestDigest || task.InstanceID == "" || task.InstanceID != resourceInstanceID ||
		!validServiceSecretSHA256Digest(task.InputDigest) || (task.Status != "queued" && task.Status != "running") {
		return storedRecipeInstallTaskProof{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return task, nil
}

func lockAcceptedRecipeInstallIssueProof(ctx context.Context, tx *sql.Tx, task storedRecipeInstallTaskProof) error {
	var commandID, executionID, deploymentID, taskID, connectionID, requestDigest, nodeKeyID, payloadJSON, payloadSHA, requestSHA, envelopeJSON, state string
	var expectedGeneration, nodeCounter, issuedAt, expiresAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT command_id,execution_id,deployment_id,task_id,cloud_connection_id,request_digest,node_key_id,expected_generation,node_counter,
			canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state
		FROM p2p_cloud_recipe_install_commands
		WHERE execution_id=$1 AND action='worker.recipe_task.issue'
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, task.ExecutionID).Scan(&commandID, &executionID, &deploymentID, &taskID, &connectionID, &requestDigest, &nodeKeyID, &expectedGeneration, &nodeCounter,
		&payloadJSON, &payloadSHA, &requestSHA, &envelopeJSON, &issuedAt, &expiresAt, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	if err != nil {
		return err
	}
	command, parseErr := broker.ParseRecipeTaskCommand([]byte(envelopeJSON))
	payload, decodeErr := base64.StdEncoding.DecodeString(command.PayloadB64)
	var issue broker.RecipeTaskIssueRequest
	issueErr := json.Unmarshal(payload, &issue)
	if executionID != task.ExecutionID || deploymentID != task.DeploymentID || taskID != task.TaskID || connectionID != task.ConnectionID || state != "accepted" ||
		!validServiceSecretSHA256Digest(requestDigest) || parseErr != nil || decodeErr != nil || issueErr != nil || string(payload) != payloadJSON ||
		command.CommandID != commandID || command.ConnectionID != connectionID || command.NodeKeyID != nodeKeyID || command.ExpectedGeneration != expectedGeneration ||
		command.NodeCounter != nodeCounter || command.Action != broker.RecipeTaskIssueAction || command.PayloadSHA256 != payloadSHA || command.RequestSHA256() != requestSHA ||
		issue.ExecutionID != task.ExecutionID || issue.DeploymentID != task.DeploymentID || issue.TaskID != task.TaskID || issue.ManifestDigest != task.ManifestDigest ||
		issue.InputDigest != task.InputDigest || !reflect.DeepEqual(issue.CheckpointSequence, mustDecodeCheckpointSequence(task.CheckpointSequenceJSON)) ||
		issue.Manifest.ExecutionID != task.ExecutionID || issue.Manifest.DeploymentID != task.DeploymentID || issuedAt <= 0 || expiresAt <= issuedAt {
		return cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return nil
}

func mustDecodeCheckpointSequence(raw string) []string {
	var values []string
	if json.Unmarshal([]byte(raw), &values) != nil {
		return nil
	}
	return values
}

func exactServiceSecretSlotBinding(slotID string, plan cloudcontracts.PlanV1, recipe cloudcontracts.RecipeV1, artifact cloudcontracts.CompiledRecipeArtifactV1, manifest cloudcontracts.RecipeExecutionManifestV1) (cloudcontracts.SecretReferenceV1, error) {
	var requirement *cloudcontracts.RecipeSecretSlotRequirementV1
	for i := range recipe.SecretSlots {
		if recipe.SecretSlots[i].SlotID == slotID {
			if requirement != nil {
				return cloudcontracts.SecretReferenceV1{}, cloudmodule.ErrServiceSecretBootstrapInvalid
			}
			requirement = &recipe.SecretSlots[i]
		}
	}
	if requirement == nil {
		return cloudcontracts.SecretReferenceV1{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	expected, err := cloudcontracts.SecretReferenceForRecipeSlot(plan.PlanID, *requirement)
	if err != nil {
		return cloudcontracts.SecretReferenceV1{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	planMatches, artifactMatches, manifestMatches := 0, 0, 0
	for _, reference := range plan.SecretScope {
		if reference.SecretRef == expected.SecretRef && reference == expected {
			planMatches++
		}
	}
	for _, slot := range artifact.SecretSlots {
		if slot.SlotID == slotID && slot == *requirement {
			artifactMatches++
		}
	}
	for _, slot := range manifest.SecretSlots {
		if slot.SlotID == slotID && slot.SecretRef == expected.SecretRef {
			manifestMatches++
		}
	}
	if planMatches != 1 || artifactMatches != 1 || manifestMatches != 1 {
		return cloudcontracts.SecretReferenceV1{}, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return expected, nil
}

func lockServiceSecretStackBaseURL(ctx context.Context, tx *sql.Tx, connectionID string) (string, error) {
	var brokerURL, brokerRegion, connectionRegion, connectionStatus string
	err := tx.QueryRowContext(ctx, `
		SELECT broker.broker_command_url,broker.broker_region,connection.region,connection.status
		FROM p2p_cloud_connection_brokers broker
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=broker.cloud_connection_id
		WHERE broker.cloud_connection_id=$1 FOR UPDATE OF broker,connection
	`, connectionID).Scan(&brokerURL, &brokerRegion, &connectionRegion, &connectionStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return "", cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	if err != nil {
		return "", err
	}
	if connectionStatus != "active" || brokerRegion != connectionRegion || cloudmodule.ValidateConnectionRegistrationEndpoint(brokerURL, connectionRegion) != nil {
		return "", cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	origin, err := cloudmodule.StackBaseURLFromBrokerCommandURL(brokerURL)
	if err != nil {
		return "", cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return origin, nil
}

func loadServiceSecretBootstrapReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PrepareServiceSecretBootstrapRequest) (cloudmodule.PrepareServiceSecretBootstrapResult, bool, error) {
	var requestDigest, approvalJSON, connectionID string
	var signingPayload []byte
	err := tx.QueryRowContext(ctx, `
		SELECT prepare_request_digest,approval_json,signing_payload_cbor,cloud_connection_id
		FROM p2p_cloud_service_secret_bootstrap_approvals
		WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2 FOR UPDATE
	`, request.OwnerMXID, request.IdempotencyHash).Scan(&requestDigest, &approvalJSON, &signingPayload, &connectionID)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, false, err
	}
	if requestDigest != request.RequestDigest {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	var approval cloudcontracts.ServiceSecretApprovalV1
	if decodeCloudContractJSON(approvalJSON, &approval) != nil || approval.Validate() != nil || approval.ConnectionID != connectionID {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, true, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	payload, err := approval.SigningPayload()
	if err != nil || !bytes.Equal(payload, signingPayload) {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, true, cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	origin, err := lockServiceSecretStackBaseURL(ctx, tx, connectionID)
	if err != nil {
		return cloudmodule.PrepareServiceSecretBootstrapResult{}, true, err
	}
	return cloudmodule.PrepareServiceSecretBootstrapResult{Confirmation: cloudmodule.ServiceSecretBootstrapConfirmation{Approval: approval}, StackBaseURL: origin}, true, nil
}

func validServiceSecretSHA256Digest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validServiceSecretHexSHA256(strings.TrimPrefix(value, "sha256:"))
}

func validServiceSecretHexSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func mapServiceSecretBootstrapStateError(err error) error {
	if errors.Is(err, cloudmodule.ErrRecipeExecutionConfirmationConflict) {
		return cloudmodule.ErrServiceSecretBootstrapConflict
	}
	if errors.Is(err, cloudmodule.ErrRecipeExecutionConfirmationInvalid) || errors.Is(err, cloudmodule.ErrRecipeExecutionManifestInvalid) || errors.Is(err, cloudmodule.ErrRecipeArtifactInvalid) || errors.Is(err, cloudmodule.ErrPlanConfirmationInvalid) {
		return cloudmodule.ErrServiceSecretBootstrapInvalid
	}
	return err
}
