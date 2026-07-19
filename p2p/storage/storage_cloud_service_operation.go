package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.ServiceOperationConfirmationStore = (*DatabaseStore)(nil)

type serviceOperationState struct {
	OwnerMXID             string
	Service               cloudmodule.Service
	Deployment            cloudmodule.Deployment
	RecipeDigest          string
	InstanceID            string
	PrivateResourceStatus string
	ManifestDigest        string
	InstallManifest       cloudcontracts.RecipeExecutionManifestV1
	CompiledArtifact      cloudcontracts.CompiledRecipeArtifactV1
}

type storedServiceOperationApproval struct {
	ApprovalID, OwnerMXID, ServiceID, Operation, SignerKeyID, ApprovalJSON, ServiceJSON, DeploymentJSON string
	Status, PrepareRequestDigest, ApproveRequestDigest, ResultServiceJSON, ResultJobJSON                string
	ServiceRevision, ExpiresAt                                                                          int64
	SigningPayload                                                                                      []byte
}

func (s *DatabaseStore) PrepareCloudServiceOperation(ctx context.Context, request cloudmodule.PrepareServiceOperationRequest) (cloudmodule.PrepareServiceOperationResult, error) {
	if validatePrepareServiceOperationRequest(request) != nil {
		return cloudmodule.PrepareServiceOperationResult{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	result := cloudmodule.PrepareServiceOperationResult{}
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceOperationPrepareReplay(ctx, tx, request); err != nil || found {
			result = replay
			return err
		}
		state, err := lockServiceOperationState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision {
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		target, err := serviceOperationTarget(state, request.Operation)
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		var active int
		if err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM p2p_cloud_service_operation_tasks WHERE service_id=$1 AND task_status IN('queued','running'))+(SELECT COUNT(*) FROM p2p_cloud_service_backups WHERE service_id=$1 AND backup_status IN('queued','running'))+(SELECT COUNT(*) FROM p2p_cloud_service_restores WHERE service_id=$1 AND restore_status IN('queued','running','verifying','restore_blocked'))`, request.ServiceID).Scan(&active); err != nil || active != 0 {
			if err != nil {
				return err
			}
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		approval, err := cloudcontracts.NewServiceOperationApprovalV1(target, request.ApprovalID, request.ChallengeID, keyID, time.UnixMilli(request.CreatedAt), time.UnixMilli(request.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		approvalJSON, _ := json.Marshal(approval)
		payload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		serviceJSON, _ := json.Marshal(state.Service)
		deploymentJSON, _ := json.Marshal(state.Deployment)
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_operation_approvals(
			approval_id,challenge_id,owner_mxid,service_id,service_revision,operation,deployment_id,deployment_revision,cloud_connection_id,
			recipe_id,recipe_digest,installed_manifest_digest,artifact_digest,action_id,approval_json,signing_payload,signer_key_id,service_json,deployment_json,
			status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'pending',$20,$21,$22,$23,$23)`,
			approval.ApprovalID, approval.ChallengeID, request.OwnerMXID, target.ServiceID, target.ServiceRevision, target.Operation,
			target.DeploymentID, target.DeploymentRevision, target.CloudConnectionID, target.RecipeID, target.RecipeDigest,
			target.InstalledManifestDigest, target.ArtifactDigest, target.ActionID, string(approvalJSON), payload, keyID,
			string(serviceJSON), string(deploymentJSON), request.IdempotencyHash, request.RequestDigest, request.ExpiresAt, request.CreatedAt)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		result = cloudmodule.PrepareServiceOperationResult{Confirmation: cloudmodule.ServiceOperationConfirmation{Service: state.Service, Deployment: state.Deployment, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudServiceOperation(ctx context.Context, request cloudmodule.ApproveServiceOperationRequest) (cloudmodule.ApproveServiceOperationResult, error) {
	requestDigest, err := serviceOperationApprovalRequestDigest(request)
	if err != nil || validateApproveServiceOperationRequest(request) != nil {
		return cloudmodule.ApproveServiceOperationResult{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	result := cloudmodule.ApproveServiceOperationResult{}
	var terminalErr error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceOperationApproveReplay(ctx, tx, request, requestDigest); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockServiceOperationApproval(ctx, tx, request.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != request.OwnerMXID || stored.ServiceID != request.ServiceID || stored.ServiceRevision != request.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		if stored.ExpiresAt <= request.CreatedAt {
			if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, request.IdempotencyHash, requestDigest, request.CreatedAt, stored.ApprovalID); err != nil {
				return err
			}
			terminalErr = cloudmodule.ErrServiceOperationApprovalExpired
			return nil
		}
		state, err := lockServiceOperationState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision {
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		target, err := serviceOperationTarget(state, request.Approval.Operation)
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		storedApproval, err := decodeStoredServiceOperationApproval(stored.ApprovalJSON)
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		incomingPayload, err := request.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || request.Approval.Verify(publicKey, time.UnixMilli(request.CreatedAt)) != nil {
			return cloudmodule.ErrServiceOperationApprovalSignature
		}
		if request.Approval.ValidateAgainst(target, time.UnixMilli(request.CreatedAt)) != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		var active int
		if err = tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM p2p_cloud_service_operation_tasks WHERE service_id=$1 AND task_status IN('queued','running'))+(SELECT COUNT(*) FROM p2p_cloud_service_backups WHERE service_id=$1 AND backup_status IN('queued','running'))+(SELECT COUNT(*) FROM p2p_cloud_service_restores WHERE service_id=$1 AND restore_status IN('queued','running','verifying','restore_blocked'))`, request.ServiceID).Scan(&active); err != nil || active != 0 {
			if err != nil {
				return err
			}
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		manifest := serviceOperationManifest(state.InstallManifest, request.OperationID, target)
		manifestDigest, err := manifest.Digest()
		if err != nil {
			return cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		manifestJSON, _ := json.Marshal(manifest)
		checkpointsJSON, _ := json.Marshal(manifest.CheckpointSequence)
		inputDigest := serviceOperationInputDigest(manifestDigest)
		taskID := stableCloudOperationID("cloud_service_operation_task_", request.OperationID, manifestDigest)
		job := cloudmodule.Job{JobID: request.JobID, PlanID: state.Deployment.PlanID, DeploymentID: state.Deployment.DeploymentID, Kind: string(request.Approval.Operation), Execution: "queued", Outcome: "pending", Checkpoint: "service_operation_queued", Revision: 1, CreatedAt: request.CreatedAt, UpdatedAt: request.CreatedAt}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_operation_tasks(operation_id,approval_id,service_id,service_revision,expected_service_status,operation,execution_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,manifest_json,checkpoint_sequence_json,task_id,job_id,task_status,available_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$1,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'queued',$17,$17,$17)`, request.OperationID, request.Approval.ApprovalID, state.Service.ServiceID, state.Service.Revision, target.ExpectedServiceStatus, request.Approval.Operation, state.Deployment.DeploymentID, state.Deployment.PlanID, state.Deployment.ConnectionID, state.InstanceID, manifestDigest, inputDigest, string(manifestJSON), string(checkpointsJSON), taskID, request.JobID, request.CreatedAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrServiceOperationConfirmationConflict
			}
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,$2,$3,$4,'queued','pending','service_operation_queued','',1,$5,$5)`, job.JobID, job.PlanID, job.DeploymentID, job.Kind, request.CreatedAt); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,'service_operation','queued','Device-approved managed service operation is queued; the cloud resource remains active and billable.','service_operation_queued','',1,$2,$2)`, job.JobID, request.CreatedAt); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]string{"operation_id": request.OperationID})
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at) VALUES($1,$2,'service_operation',$3,$4,$5,$5)`, request.OutboxID, cloudmodule.OutboxKindServiceOperationRequested, request.OperationID, string(payload), request.CreatedAt); err != nil {
			return err
		}
		serviceJSON, _ := json.Marshal(state.Service)
		jobJSON, _ := json.Marshal(job)
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,operation_id=$4,job_id=$5,result_service_json=$6,result_job_json=$7,updated_at=$8 WHERE approval_id=$9 AND status='pending'`, request.IdempotencyHash, requestDigest, request.Approval.Signature, request.OperationID, request.JobID, string(serviceJSON), string(jobJSON), request.CreatedAt, request.Approval.ApprovalID)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if !exactlyOneRow(r) {
			return cloudmodule.ErrServiceOperationConfirmationConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, request.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveServiceOperationResult{Service: state.Service, Operation: request.Approval.Operation, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminalErr
}

func lockServiceOperationState(ctx context.Context, tx *sql.Tx, serviceID string) (serviceOperationState, error) {
	var state serviceOperationState
	var manifestJSON string
	err := tx.QueryRowContext(ctx, `SELECT goal.owner_mxid,
		service.service_id,service.deployment_id,service.recipe_id,service.name,service.service_status,service.integration_status,service.revision,service.created_at,service.updated_at,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,deployment.revision,deployment.created_at,deployment.updated_at,
		recipe.digest,resource.instance_id,resource.resource_status,manifest.manifest_digest,manifest.manifest_json
		FROM p2p_cloud_services service JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_plans plan ON plan.plan_id=deployment.plan_id JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
		JOIN p2p_cloud_recipes recipe ON recipe.recipe_id=service.recipe_id JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.deployment_id=deployment.deployment_id AND manifest.status='approved'
		WHERE service.service_id=$1 FOR UPDATE OF service,deployment,resource,manifest`, serviceID).Scan(&state.OwnerMXID,
		&state.Service.ServiceID, &state.Service.DeploymentID, &state.Service.RecipeID, &state.Service.Name, &state.Service.Status, &state.Service.Integration, &state.Service.Revision, &state.Service.CreatedAt, &state.Service.UpdatedAt,
		&state.Deployment.DeploymentID, &state.Deployment.PlanID, &state.Deployment.ConnectionID, &state.Deployment.Execution, &state.Deployment.Outcome, &state.Deployment.Resource, &state.Deployment.Revision, &state.Deployment.CreatedAt, &state.Deployment.UpdatedAt,
		&state.RecipeDigest, &state.InstanceID, &state.PrivateResourceStatus, &state.ManifestDigest, &manifestJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return state, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	if err != nil {
		return state, err
	}
	if decodeServiceOperationManifest(manifestJSON, &state.InstallManifest) != nil {
		return state, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	artifact, artifactErr := lockServiceOperationArtifact(ctx, tx, state.InstallManifest.ArtifactDigest)
	if artifactErr != nil {
		return state, artifactErr
	}
	state.CompiledArtifact = artifact
	return state, nil
}

func lockServiceOperationArtifact(ctx context.Context, tx *sql.Tx, artifactDigest string) (cloudcontracts.CompiledRecipeArtifactV1, error) {
	var descriptorJSON string
	if err := tx.QueryRowContext(ctx, `SELECT descriptor_json FROM p2p_cloud_recipe_artifacts WHERE artifact_digest=$1 AND status='verified' FOR UPDATE`, artifactDigest).Scan(&descriptorJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudcontracts.CompiledRecipeArtifactV1{}, cloudmodule.ErrServiceOperationConfirmationInvalid
		}
		return cloudcontracts.CompiledRecipeArtifactV1{}, err
	}
	artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1([]byte(descriptorJSON))
	if err != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return artifact, nil
}

func serviceOperationTarget(state serviceOperationState, operation cloudcontracts.ServiceOperation) (cloudcontracts.ServiceOperationTargetV1, error) {
	install, installFound := compiledRecipeAction(state.CompiledArtifact, cloudcontracts.CompiledRecipeActionInstall)
	if state.Service.DeploymentID != state.Deployment.DeploymentID || state.Deployment.Execution != "finished" || state.Deployment.Outcome != "succeeded" || !serviceResourcesTracked(state.Deployment.Resource, state.PrivateResourceStatus) || state.InstallManifest.RecipeDigest != state.RecipeDigest || state.InstallManifest.VerifyDigest(state.ManifestDigest) != nil || !installFound || !compiledArtifactBindsInstalledManifest(state.CompiledArtifact, state.InstallManifest, install, state.Service.RecipeID) {
		return cloudcontracts.ServiceOperationTargetV1{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	capability, err := cloudcontracts.ManagedCapabilityFromCompiledArtifact(state.CompiledArtifact, state.ManifestDigest)
	if err != nil {
		return cloudcontracts.ServiceOperationTargetV1{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	action, ok := capability.Action(operation)
	if !ok || capability.Validate() != nil {
		return cloudcontracts.ServiceOperationTargetV1{}, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	target := cloudcontracts.ServiceOperationTargetV1{Operation: operation, ServiceID: state.Service.ServiceID, ServiceRevision: uint64(state.Service.Revision), ExpectedServiceStatus: state.Service.Status, DeploymentID: state.Deployment.DeploymentID, DeploymentRevision: uint64(state.Deployment.Revision), CloudConnectionID: state.Deployment.ConnectionID, RecipeID: state.Service.RecipeID, RecipeDigest: state.RecipeDigest, InstalledManifestDigest: state.ManifestDigest, ArtifactDigest: capability.ArtifactDigest, ActionID: action.ActionID, RootRequired: capability.RootRequired, TimeoutSeconds: action.TimeoutSeconds, CheckpointSequence: append([]string(nil), action.CheckpointSequence...), VolumeSlots: append([]cloudcontracts.VolumeSlotV1(nil), state.InstallManifest.VolumeSlots...), DataSlots: append([]cloudcontracts.DataSlotV1(nil), state.InstallManifest.DataSlots...), SecretSlots: append([]cloudcontracts.SecretSlotV1(nil), state.InstallManifest.SecretSlots...)}
	return target, target.Validate()
}

func serviceResourcesTracked(publicStatus, privateStatus string) bool {
	return privateStatus == publicStatus && (publicStatus == "active" || publicStatus == "retained_tracked")
}

func compiledRecipeAction(artifact cloudcontracts.CompiledRecipeArtifactV1, kind cloudcontracts.CompiledRecipeActionKind) (cloudcontracts.CompiledRecipeActionV1, bool) {
	for _, action := range artifact.Actions {
		if action.Kind == kind {
			return action, true
		}
	}
	return cloudcontracts.CompiledRecipeActionV1{}, false
}

func compiledArtifactBindsInstalledManifest(artifact cloudcontracts.CompiledRecipeArtifactV1, manifest cloudcontracts.RecipeExecutionManifestV1, install cloudcontracts.CompiledRecipeActionV1, recipeID string) bool {
	return artifact.Validate() == nil && artifact.RecipeID == recipeID && artifact.RecipeDigest == manifest.RecipeDigest && artifact.ArtifactDigest == manifest.ArtifactDigest &&
		artifact.WorkerResourceManifestDigest == manifest.WorkerResourceManifestDigest && install.ActionID == manifest.ActionID && install.RootRequired == manifest.RootRequired &&
		install.TimeoutSeconds == manifest.TimeoutSeconds && equalServiceOperationStrings(install.CheckpointSequence, manifest.CheckpointSequence) &&
		compiledStorageSlotsBindManifest(artifact, manifest)
}

func compiledStorageSlotsBindManifest(artifact cloudcontracts.CompiledRecipeArtifactV1, manifest cloudcontracts.RecipeExecutionManifestV1) bool {
	if len(artifact.VolumeSlots) != len(manifest.VolumeSlots) || len(artifact.DataSlots) != len(manifest.DataSlots) || len(artifact.SecretSlots) != len(manifest.SecretSlots) {
		return false
	}
	volumes := make(map[string]bool, len(artifact.VolumeSlots))
	for _, slot := range artifact.VolumeSlots {
		volumes[slot.SlotID] = slot.ReadOnly
	}
	for _, slot := range manifest.VolumeSlots {
		if readOnly, ok := volumes[slot.SlotID]; !ok || readOnly != slot.ReadOnly {
			return false
		}
		delete(volumes, slot.SlotID)
	}
	data := make(map[string]bool, len(artifact.DataSlots))
	for _, slot := range artifact.DataSlots {
		data[slot.SlotID] = slot.ReadOnly
	}
	for _, slot := range manifest.DataSlots {
		if readOnly, ok := data[slot.SlotID]; !ok || readOnly != slot.ReadOnly {
			return false
		}
		delete(data, slot.SlotID)
	}
	secrets := make(map[string]struct{}, len(artifact.SecretSlots))
	for _, slot := range artifact.SecretSlots {
		secrets[slot.SlotID] = struct{}{}
	}
	for _, slot := range manifest.SecretSlots {
		if _, ok := secrets[slot.SlotID]; !ok {
			return false
		}
		delete(secrets, slot.SlotID)
	}
	return len(volumes) == 0 && len(data) == 0 && len(secrets) == 0
}

func equalServiceOperationStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func serviceOperationManifest(installed cloudcontracts.RecipeExecutionManifestV1, operationID string, target cloudcontracts.ServiceOperationTargetV1) cloudcontracts.RecipeExecutionManifestV1 {
	return cloudcontracts.RecipeExecutionManifestV1{SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: operationID, DeploymentID: installed.DeploymentID, PlanID: installed.PlanID, PlanHash: installed.PlanHash, PlanRevision: installed.PlanRevision, RecipeDigest: target.RecipeDigest, WorkerResourceManifestDigest: installed.WorkerResourceManifestDigest, ArtifactDigest: target.ArtifactDigest, ActionID: target.ActionID, RootRequired: target.RootRequired, TimeoutSeconds: target.TimeoutSeconds, CheckpointSequence: append([]string(nil), target.CheckpointSequence...), SemanticReadiness: installed.SemanticReadiness, VolumeSlots: append([]cloudcontracts.VolumeSlotV1(nil), target.VolumeSlots...), DataSlots: append([]cloudcontracts.DataSlotV1(nil), target.DataSlots...), SecretSlots: append([]cloudcontracts.SecretSlotV1(nil), target.SecretSlots...)}
}

func validatePrepareServiceOperationRequest(request cloudmodule.PrepareServiceOperationRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.ServiceID) == "" || request.ExpectedRevision <= 0 || request.IdempotencyHash == "" || request.RequestDigest == "" || request.ApprovalID == "" || request.ChallengeID == "" || request.CreatedAt <= 0 || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return nil
}

func validateApproveServiceOperationRequest(request cloudmodule.ApproveServiceOperationRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || request.ServiceID == "" || request.ExpectedRevision <= 0 || request.IdempotencyHash == "" || request.OperationID == "" || request.JobID == "" || request.OutboxID == "" || request.JobEventID == "" || request.CreatedAt <= 0 || request.Approval.Validate() != nil || request.Approval.Signature == "" {
		return cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return nil
}

func decodeServiceOperationManifest(raw string, target *cloudcontracts.RecipeExecutionManifestV1) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("service operation manifest has trailing JSON")
	}
	return target.Validate()
}

func decodeStoredServiceOperationApproval(raw string) (cloudcontracts.ServiceOperationApprovalV1, error) {
	var approval cloudcontracts.ServiceOperationApprovalV1
	if json.Unmarshal([]byte(raw), &approval) != nil || approval.Signature != "" || approval.Validate() != nil {
		return approval, errors.New("stored service operation approval is invalid")
	}
	return approval, nil
}

func loadServiceOperationPrepareReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PrepareServiceOperationRequest) (cloudmodule.PrepareServiceOperationResult, bool, error) {
	var approvalJSON, serviceJSON, deploymentJSON, requestDigest string
	err := tx.QueryRowContext(ctx, `SELECT approval_json,service_json,deployment_json,prepare_request_digest FROM p2p_cloud_service_operation_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2`, request.OwnerMXID, request.IdempotencyHash).Scan(&approvalJSON, &serviceJSON, &deploymentJSON, &requestDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceOperationResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareServiceOperationResult{}, false, err
	}
	if requestDigest != request.RequestDigest {
		return cloudmodule.PrepareServiceOperationResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	approval, err := decodeStoredServiceOperationApproval(approvalJSON)
	var service cloudmodule.Service
	var deployment cloudmodule.Deployment
	if err != nil || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil {
		return cloudmodule.PrepareServiceOperationResult{}, true, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return cloudmodule.PrepareServiceOperationResult{Confirmation: cloudmodule.ServiceOperationConfirmation{Service: service, Deployment: deployment, Approval: approval}}, true, nil
}

func lockServiceOperationApproval(ctx context.Context, tx *sql.Tx, approvalID string) (storedServiceOperationApproval, error) {
	var v storedServiceOperationApproval
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,service_id,service_revision,operation,signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),result_service_json,result_job_json,expires_at FROM p2p_cloud_service_operation_approvals WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(&v.ApprovalID, &v.OwnerMXID, &v.ServiceID, &v.ServiceRevision, &v.Operation, &v.SignerKeyID, &v.ApprovalJSON, &v.SigningPayload, &v.ServiceJSON, &v.DeploymentJSON, &v.Status, &v.PrepareRequestDigest, &v.ApproveRequestDigest, &v.ResultServiceJSON, &v.ResultJobJSON, &v.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return v, err
}

func loadServiceOperationApproveReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.ApproveServiceOperationRequest, digest string) (cloudmodule.ApproveServiceOperationResult, bool, error) {
	var storedDigest, status, serviceJSON, jobJSON, operation string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(approve_request_digest,''),status,result_service_json,result_job_json,operation FROM p2p_cloud_service_operation_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2`, request.OwnerMXID, request.IdempotencyHash).Scan(&storedDigest, &status, &serviceJSON, &jobJSON, &operation)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveServiceOperationResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveServiceOperationResult{}, false, err
	}
	if storedDigest != digest {
		return cloudmodule.ApproveServiceOperationResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveServiceOperationResult{}, true, cloudmodule.ErrServiceOperationApprovalExpired
	}
	var service cloudmodule.Service
	var job cloudmodule.Job
	if status != "approved" || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(jobJSON), &job) != nil {
		return cloudmodule.ApproveServiceOperationResult{}, true, cloudmodule.ErrServiceOperationConfirmationInvalid
	}
	return cloudmodule.ApproveServiceOperationResult{Service: service, Operation: cloudcontracts.ServiceOperation(operation), Job: job}, true, nil
}

func serviceOperationApprovalRequestDigest(request cloudmodule.ApproveServiceOperationRequest) (string, error) {
	payload, err := request.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		ServiceID        string `json:"service_id"`
		ExpectedRevision int64  `json:"expected_revision"`
		ApprovalID       string `json:"approval_id"`
		SigningPayload   []byte `json:"signing_payload"`
		Signature        string `json:"signature"`
	}{request.ServiceID, request.ExpectedRevision, request.Approval.ApprovalID, payload, request.Approval.Signature})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func serviceOperationInputDigest(manifestDigest string) string {
	sum := sha256.Sum256([]byte("dirextalk.service-operation-input/v1\x00" + manifestDigest))
	return "sha256:" + hex.EncodeToString(sum[:])
}
func stableCloudOperationID(prefix string, values ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return fmt.Sprintf("%s%x", prefix, sum[:16])
}
