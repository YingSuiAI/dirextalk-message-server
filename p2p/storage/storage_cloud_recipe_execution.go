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
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.RecipeExecutionConfirmationStore = (*DatabaseStore)(nil)
var _ cloudmodule.TrustedRecipeExecutionManifestStore = (*DatabaseStore)(nil)

const (
	recipeExecutionStatusRegistered       = "registered"
	recipeExecutionStatusApprovalPrepared = "approval_prepared"
	recipeExecutionStatusApproved         = "approved"
	recipeExecutionApprovalPending        = "pending"
	recipeExecutionApprovalApproved       = "approved"
	recipeExecutionApprovalExpired        = "expired"
)

type storedRecipeExecutionManifest struct {
	Execution    cloudmodule.RecipeExecution
	PlanRevision int64
	PlanHash     string
	ConnectionID string
	Manifest     cloudcontracts.RecipeExecutionManifestV1
}

type storedRecipeExecutionApproval struct {
	ApprovalID           string
	OwnerMXID            string
	ExecutionID          string
	DeploymentID         string
	DeploymentRevision   int64
	PlanID               string
	PlanRevision         int64
	SignerKeyID          string
	ManifestDigest       string
	ApprovalJSON         string
	SigningPayload       []byte
	ExpiresAt            int64
	Status               string
	PrepareRequestDigest string
	ApproveRequestDigest sql.NullString
	Signature            string
	JobID                string
}

// RegisterTrustedCloudRecipeExecutionManifest is intentionally not wired into
// ProductCore. The caller must be the internal Orchestrator compiler after it
// has verified the compiled artifact. This method proves the manifest against
// the approved Plan, active exclusive Worker resource, Broker resource-manifest
// binding, and active Worker session before preserving a de-secreted record.
func (s *DatabaseStore) RegisterTrustedCloudRecipeExecutionManifest(ctx context.Context, request cloudmodule.RegisterTrustedRecipeExecutionManifestRequest) (cloudmodule.RegisterTrustedRecipeExecutionManifestResult, error) {
	if err := validateTrustedRecipeExecutionManifestRequest(request); err != nil {
		return cloudmodule.RegisterTrustedRecipeExecutionManifestResult{}, err
	}
	manifestDigest, err := request.Manifest.Digest()
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeExecutionManifestResult{}, cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	manifestCBOR, err := request.Manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeExecutionManifestResult{}, cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	manifestJSON, err := json.Marshal(request.Manifest)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeExecutionManifestResult{}, err
	}

	result := cloudmodule.RegisterTrustedRecipeExecutionManifestResult{}
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if stored, found, err := lockRecipeExecutionManifestByExecutionID(ctx, tx, request.Manifest.ExecutionID); err != nil || found {
			if err != nil {
				return err
			}
			if stored.Execution.RecipeExecutionManifestDigest != manifestDigest {
				return cloudmodule.ErrRecipeExecutionManifestConflict
			}
			result = cloudmodule.RegisterTrustedRecipeExecutionManifestResult{Execution: stored.Execution}
			return nil
		}

		deployment, err := lockCloudDeploymentForRecipeExecution(ctx, tx, request.Manifest.DeploymentID)
		if err != nil {
			return err
		}
		plan, err := lockCloudPlanForRecipeExecution(ctx, tx, deployment.PlanID)
		if err != nil {
			return err
		}
		approvedPlan, approvedPlanHash, err := loadApprovedPlanV1ForRecipeExecution(ctx, tx, plan)
		if err != nil {
			return err
		}
		if err := validateRecipeExecutionDeploymentBinding(ctx, tx, deployment, approvedPlan, request.Manifest, request.RegisteredAt); err != nil {
			return err
		}
		if request.Manifest.PlanHash != approvedPlanHash || request.Manifest.ValidateForPlan(approvedPlan) != nil {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}

		execution := cloudmodule.RecipeExecution{
			ExecutionID: request.Manifest.ExecutionID, DeploymentID: deployment.DeploymentID, PlanID: plan.PlanID,
			RecipeExecutionManifestDigest: manifestDigest, Status: recipeExecutionStatusRegistered, Revision: 1,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_recipe_execution_manifests (
				execution_id, deployment_id, plan_id, plan_revision, plan_hash, cloud_connection_id,
				manifest_digest, manifest_cbor, manifest_json, status, revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11, $11)
		`, execution.ExecutionID, execution.DeploymentID, execution.PlanID, approvedPlan.Revision, approvedPlanHash,
			approvedPlan.CloudConnectionID, manifestDigest, manifestCBOR, string(manifestJSON), execution.Status, request.RegisteredAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrRecipeExecutionManifestConflict
			}
			return err
		}
		result = cloudmodule.RegisterTrustedRecipeExecutionManifestResult{Execution: execution, Created: true}
		return nil
	})
	return result, err
}

// PrepareCloudRecipeExecutionConfirmation derives a new one-time execution
// challenge from the already trusted manifest. It never accepts a client
// manifest/artifact and does not create a Job, Worker task, Broker command, or
// provider mutation.
func (s *DatabaseStore) PrepareCloudRecipeExecutionConfirmation(ctx context.Context, request cloudmodule.PrepareRecipeExecutionConfirmationRequest) (cloudmodule.PrepareRecipeExecutionConfirmationResult, error) {
	if err := validatePrepareRecipeExecutionConfirmationRequest(request); err != nil {
		return cloudmodule.PrepareRecipeExecutionConfirmationResult{}, err
	}
	result := cloudmodule.PrepareRecipeExecutionConfirmationResult{}
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadRecipeExecutionPrepareReplay(ctx, tx, request); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}

		ownerMXID, err := lockCloudRecipeExecutionOwner(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if ownerMXID != request.OwnerMXID {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		deployment, err := lockCloudDeploymentForRecipeExecution(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if deployment.Revision != request.ExpectedRevision || !recipeExecutionDeploymentReady(deployment) {
			return cloudmodule.ErrRecipeExecutionConfirmationConflict
		}
		manifest, err := lockRecipeExecutionManifestByDeploymentID(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if manifest.Execution.Status == recipeExecutionStatusApproved {
			return cloudmodule.ErrRecipeExecutionConfirmationConflict
		}
		plan, err := lockCloudPlanForRecipeExecution(ctx, tx, deployment.PlanID)
		if err != nil {
			return err
		}
		approvedPlan, _, err := loadApprovedPlanV1ForRecipeExecution(ctx, tx, plan)
		if err != nil {
			return err
		}
		if err := validateRecipeExecutionStoredBinding(deployment, approvedPlan, manifest); err != nil {
			return err
		}
		if err := validateRecipeExecutionDeploymentBinding(ctx, tx, deployment, approvedPlan, manifest.Manifest, request.CreatedAt); err != nil {
			return err
		}
		if err := expireStalePendingRecipeExecutionApproval(ctx, tx, manifest.Execution.ExecutionID, request.CreatedAt); err != nil {
			return err
		}
		if pending, found, err := lockPendingRecipeExecutionApproval(ctx, tx, manifest.Execution.ExecutionID); err != nil || found {
			if err != nil {
				return err
			}
			if pending.ExpiresAt > request.CreatedAt {
				return cloudmodule.ErrRecipeExecutionConfirmationConflict
			}
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		deviceKeyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, approvedPlan.CloudConnectionID)
		if err != nil || deviceKeyID == "" {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		approval, err := cloudcontracts.NewRecipeExecutionApprovalV1(
			approvedPlan,
			manifest.Manifest,
			cloudcontracts.RecipeExecutionTargetV1{DeploymentID: deployment.DeploymentID, DeploymentRevision: uint64(deployment.Revision)},
			request.ApprovalID,
			request.ChallengeID,
			deviceKeyID,
			time.UnixMilli(request.CreatedAt).UTC(),
			time.UnixMilli(request.ExpiresAt).UTC(),
		)
		if err != nil {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		signingPayload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		approvalJSON, err := json.Marshal(approval)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_recipe_execution_approvals (
				approval_id, owner_mxid, execution_id, deployment_id, deployment_revision,
				plan_id, plan_revision, signer_key_id, manifest_digest, approval_json,
				signing_payload_cbor, expires_at, status, prepare_idempotency_hash,
				prepare_request_digest, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'pending', $13, $14, $15, $15)
		`, approval.ApprovalID, request.OwnerMXID, manifest.Execution.ExecutionID, deployment.DeploymentID, deployment.Revision,
			plan.PlanID, plan.Revision, approval.SignerKeyID, manifest.Execution.RecipeExecutionManifestDigest,
			string(approvalJSON), signingPayload, request.ExpiresAt, request.IdempotencyHash, request.RequestDigest, request.CreatedAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		updatedExecution, err := advanceRecipeExecutionManifestStatus(ctx, tx, manifest.Execution, recipeExecutionStatusApprovalPrepared, request.CreatedAt)
		if err != nil {
			return err
		}
		result = cloudmodule.PrepareRecipeExecutionConfirmationResult{
			Confirmation: cloudmodule.RecipeExecutionConfirmation{Execution: updatedExecution, Approval: approval},
			Created:      true,
		}
		return nil
	})
	return result, err
}

// ApproveCloudRecipeExecution consumes a device signature for the exact
// persisted manifest challenge and atomically queues one install Job. The
// generated outbox carries only execution_id and is intentionally not consumed
// by any current runner: this stage has no root executor, Worker issue path, or
// AWS/Broker mutation.
func (s *DatabaseStore) ApproveCloudRecipeExecution(ctx context.Context, request cloudmodule.ApproveRecipeExecutionRequest) (cloudmodule.ApproveRecipeExecutionResult, error) {
	requestDigest, err := recipeExecutionApprovalRequestDigest(request)
	if err != nil {
		return cloudmodule.ApproveRecipeExecutionResult{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	if err := validateApproveRecipeExecutionRequest(request); err != nil {
		return cloudmodule.ApproveRecipeExecutionResult{}, err
	}
	result := cloudmodule.ApproveRecipeExecutionResult{}
	var terminalErr error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, replayErr := loadRecipeExecutionApprovalReplay(ctx, tx, request, requestDigest); replayErr != nil || found {
			if replayErr != nil {
				return replayErr
			}
			result = replay
			return nil
		}

		ownerMXID, err := lockCloudRecipeExecutionOwner(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if ownerMXID != request.OwnerMXID {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		deployment, err := lockCloudDeploymentForRecipeExecution(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if deployment.Revision != request.ExpectedRevision || !recipeExecutionDeploymentReady(deployment) {
			return cloudmodule.ErrRecipeExecutionConfirmationConflict
		}
		manifest, err := lockRecipeExecutionManifestByDeploymentID(ctx, tx, request.DeploymentID)
		if err != nil {
			return err
		}
		if manifest.Execution.Status != recipeExecutionStatusApprovalPrepared || manifest.Execution.RecipeExecutionManifestDigest != request.Approval.RecipeExecutionManifestDigest {
			return cloudmodule.ErrRecipeExecutionConfirmationConflict
		}
		plan, err := lockCloudPlanForRecipeExecution(ctx, tx, deployment.PlanID)
		if err != nil {
			return err
		}
		approvedPlan, _, err := loadApprovedPlanV1ForRecipeExecution(ctx, tx, plan)
		if err != nil {
			return err
		}
		if err := validateRecipeExecutionStoredBinding(deployment, approvedPlan, manifest); err != nil {
			return err
		}
		if err := validateRecipeExecutionDeploymentBinding(ctx, tx, deployment, approvedPlan, manifest.Manifest, request.CreatedAt); err != nil {
			return err
		}
		stored, err := lockRecipeExecutionApproval(ctx, tx, request.Approval.ApprovalID, manifest.Execution.ExecutionID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != request.OwnerMXID || stored.DeploymentID != deployment.DeploymentID || stored.DeploymentRevision != deployment.Revision ||
			stored.PlanID != plan.PlanID || stored.PlanRevision != plan.Revision || stored.ManifestDigest != manifest.Execution.RecipeExecutionManifestDigest || stored.Status != recipeExecutionApprovalPending {
			return cloudmodule.ErrRecipeExecutionConfirmationConflict
		}
		if stored.ExpiresAt <= request.CreatedAt {
			if err := expireRecipeExecutionApproval(ctx, tx, stored, request, requestDigest); err != nil {
				return err
			}
			terminalErr = cloudmodule.ErrRecipeExecutionApprovalExpired
			return nil
		}
		storedApproval, err := decodeStoredRecipeExecutionApproval(stored.ApprovalJSON)
		if err != nil {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		incomingPayload, err := request.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) || storedApproval.SignerKeyID != stored.SignerKeyID {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		deviceKeyID, devicePublicKey, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, approvedPlan.CloudConnectionID)
		if err != nil || deviceKeyID != stored.SignerKeyID {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(devicePublicKey)
		if err != nil || request.Approval.Verify(publicKey, time.UnixMilli(request.CreatedAt).UTC()) != nil {
			return cloudmodule.ErrRecipeExecutionApprovalSignature
		}
		target := cloudcontracts.RecipeExecutionTargetV1{DeploymentID: deployment.DeploymentID, DeploymentRevision: uint64(deployment.Revision)}
		if request.Approval.ValidateAgainst(approvedPlan, manifest.Manifest, target, time.UnixMilli(request.CreatedAt).UTC()) != nil {
			return cloudmodule.ErrRecipeExecutionConfirmationInvalid
		}
		if err := validateRecipeExecutionInstallRequestShape(request, plan, deployment); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_jobs (
				job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
				checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, $2, $3, 'install', 'queued', 'pending', 'install_queued', '', 1, $4, $4)
		`, request.Job.JobID, plan.PlanID, deployment.DeploymentID, request.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_job_steps (
				job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, 'install', 'queued', 'A device-approved sealed Recipe install is queued; no Worker task or cloud mutation has started.', 'install_queued', '', 1, $2, $2)
		`, request.Job.JobID, request.CreatedAt); err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]string{"execution_id": manifest.Execution.ExecutionID})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_outbox (outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at)
			VALUES ($1, $2, 'recipe_execution', $3, $4, $5)
		`, request.OutboxID, cloudmodule.OutboxKindRecipeExecutionInstallRequested, manifest.Execution.ExecutionID, string(payload), request.CreatedAt); err != nil {
			return err
		}
		approvalUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_recipe_execution_approvals
			SET status = 'approved', approve_idempotency_hash = $1, approve_request_digest = $2,
				signature = $3, job_id = $4, updated_at = $5
			WHERE approval_id = $6 AND status = 'pending'
		`, request.IdempotencyHash, requestDigest, request.Approval.Signature, request.Job.JobID, request.CreatedAt, stored.ApprovalID)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if err := requireRecipeExecutionApprovalMutation(approvalUpdate); err != nil {
			return err
		}
		updatedExecution, err := advanceRecipeExecutionManifestStatus(ctx, tx, manifest.Execution, recipeExecutionStatusApproved, request.CreatedAt)
		if err != nil {
			return err
		}
		job := request.Job
		if err := writeCloudConfirmationEvent(ctx, tx, request.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveRecipeExecutionResult{Execution: updatedExecution, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminalErr
}

func validateTrustedRecipeExecutionManifestRequest(request cloudmodule.RegisterTrustedRecipeExecutionManifestRequest) error {
	if request.RegisteredAt <= 0 || request.Manifest.Validate() != nil {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	return nil
}

func validatePrepareRecipeExecutionConfirmationRequest(request cloudmodule.PrepareRecipeExecutionConfirmationRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.DeploymentID) == "" || request.ExpectedRevision <= 0 ||
		request.IdempotencyHash == "" || request.RequestDigest == "" || request.ApprovalID == "" || request.ChallengeID == "" ||
		request.CreatedAt <= 0 || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return nil
}

func validateApproveRecipeExecutionRequest(request cloudmodule.ApproveRecipeExecutionRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.DeploymentID) == "" || request.ExpectedRevision <= 0 ||
		request.IdempotencyHash == "" || request.CreatedAt <= 0 || request.Approval.ApprovalID == "" || request.Approval.Signature == "" {
		return cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return nil
}

func recipeExecutionDeploymentReady(deployment cloudmodule.Deployment) bool {
	return deployment.Execution == "verifying" && deployment.Outcome == "pending" && deployment.Resource == "active" && deployment.Revision > 0
}

func validateRecipeExecutionDeploymentBinding(ctx context.Context, tx *sql.Tx, deployment cloudmodule.Deployment, plan cloudcontracts.PlanV1, manifest cloudcontracts.RecipeExecutionManifestV1, now int64) error {
	if !recipeExecutionDeploymentReady(deployment) || manifest.DeploymentID != deployment.DeploymentID || deployment.PlanID != plan.PlanID ||
		deployment.ConnectionID != plan.CloudConnectionID || manifest.PlanID != plan.PlanID || manifest.PlanRevision != plan.Revision || now <= 0 {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	var resourceConnectionID, resourceStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT cloud_connection_id, resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id = $1 FOR UPDATE
	`, deployment.DeploymentID).Scan(&resourceConnectionID, &resourceStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
		return err
	}
	if resourceConnectionID != deployment.ConnectionID || resourceStatus != "active" {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	var brokerManifestDigest string
	if err := tx.QueryRowContext(ctx, `
		SELECT worker_resource_manifest_digest FROM p2p_cloud_connection_brokers WHERE cloud_connection_id = $1 FOR UPDATE
	`, deployment.ConnectionID).Scan(&brokerManifestDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
		return err
	}
	if brokerManifestDigest != manifest.WorkerResourceManifestDigest {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	var observationConnectionID, sessionState string
	var workerLeaseExpiresAt int64
	if err := tx.QueryRowContext(ctx, `
		SELECT cloud_connection_id, worker_session_state, worker_lease_expires_at
		FROM p2p_cloud_worker_bootstrap_observations WHERE deployment_id = $1 FOR UPDATE
	`, deployment.DeploymentID).Scan(&observationConnectionID, &sessionState, &workerLeaseExpiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
		return err
	}
	if observationConnectionID != deployment.ConnectionID || sessionState != "active" || workerLeaseExpiresAt <= now {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	return nil
}

func validateRecipeExecutionStoredBinding(deployment cloudmodule.Deployment, plan cloudcontracts.PlanV1, manifest storedRecipeExecutionManifest) error {
	manifestDigest, err := manifest.Manifest.Digest()
	if err != nil || manifestDigest != manifest.Execution.RecipeExecutionManifestDigest || manifest.PlanRevision != int64(plan.Revision) ||
		manifest.PlanHash == "" || manifest.Manifest.ValidateForPlan(plan) != nil || manifest.Manifest.DeploymentID != deployment.DeploymentID ||
		manifest.Execution.PlanID != plan.PlanID || manifest.Execution.DeploymentID != deployment.DeploymentID ||
		deployment.ConnectionID != plan.CloudConnectionID || !recipeExecutionDeploymentReady(deployment) {
		return cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return nil
}

func lockCloudRecipeExecutionOwner(ctx context.Context, tx *sql.Tx, deploymentID string) (string, error) {
	var ownerMXID string
	err := tx.QueryRowContext(ctx, `
		SELECT goal.owner_mxid
		FROM p2p_cloud_deployments AS deployment
		JOIN p2p_cloud_goals AS goal ON goal.plan_id = deployment.plan_id
		WHERE deployment.deployment_id = $1
		FOR UPDATE OF deployment, goal
	`, deploymentID).Scan(&ownerMXID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return ownerMXID, err
}

func lockCloudDeploymentForRecipeExecution(ctx context.Context, tx *sql.Tx, deploymentID string) (cloudmodule.Deployment, error) {
	var deployment cloudmodule.Deployment
	err := scanCloudDeployment(tx.QueryRowContext(ctx, `
		SELECT deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status, revision, created_at, updated_at
		FROM p2p_cloud_deployments WHERE deployment_id = $1 FOR UPDATE
	`, deploymentID), &deployment)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.Deployment{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return deployment, err
}

func lockCloudPlanForRecipeExecution(ctx context.Context, tx *sql.Tx, planID string) (cloudmodule.Plan, error) {
	var plan cloudmodule.Plan
	err := scanCloudPlan(tx.QueryRowContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans WHERE plan_id = $1 FOR UPDATE`, planID), &plan)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.Plan{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return plan, err
}

// loadApprovedPlanV1ForRecipeExecution rebuilds the current approval surface
// from the immutable confirmation version. Plan approval increments the public
// Plan revision after the ready-for-confirmation version was saved, so the
// stored version must be promoted to approved/current before hashing. Status
// itself is intentionally omitted from the Plan hash; revision is not.
func loadApprovedPlanV1ForRecipeExecution(ctx context.Context, tx *sql.Tx, plan cloudmodule.Plan) (cloudcontracts.PlanV1, string, error) {
	if plan.Status != cloudmodule.PlanStatusApproved || plan.Revision <= 1 || plan.PlanHash == "" || plan.ConnectionID == "" || plan.RecipeDigest == "" {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	var displayJSON, storedHash string
	var storedRevision int64
	err := tx.QueryRowContext(ctx, `
		SELECT display_json, plan_hash, revision
		FROM p2p_cloud_plan_versions
		WHERE plan_id = $1
		ORDER BY revision DESC
		LIMIT 1
		FOR UPDATE
	`, plan.PlanID).Scan(&displayJSON, &storedHash, &storedRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	if err != nil {
		return cloudcontracts.PlanV1{}, "", err
	}
	var storedPlan cloudcontracts.PlanV1
	if err := decodeCloudContractJSON(displayJSON, &storedPlan); err != nil || storedPlan.Validate() != nil || storedPlan.Status != cloudcontracts.PlanReadyForConfirmation ||
		storedPlan.PlanID != plan.PlanID || storedPlan.CloudConnectionID != plan.ConnectionID || storedPlan.Recipe.Digest != plan.RecipeDigest ||
		int64(storedPlan.Revision) != storedRevision || storedRevision+1 != plan.Revision {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	storedPlanHash, err := storedPlan.Hash()
	if err != nil || storedPlanHash != storedHash || storedHash != plan.PlanHash {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	storedPlan.Status = cloudcontracts.PlanApproved
	storedPlan.Revision = uint64(plan.Revision)
	if err := storedPlan.Validate(); err != nil {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	approvedHash, err := storedPlan.Hash()
	if err != nil {
		return cloudcontracts.PlanV1{}, "", cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return storedPlan, approvedHash, nil
}

func lockRecipeExecutionManifestByExecutionID(ctx context.Context, tx *sql.Tx, executionID string) (storedRecipeExecutionManifest, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT execution_id, deployment_id, plan_id, plan_revision, plan_hash, cloud_connection_id,
			manifest_digest, manifest_cbor, manifest_json, status, revision
		FROM p2p_cloud_recipe_execution_manifests WHERE execution_id = $1 FOR UPDATE
	`, executionID)
	manifest, err := scanStoredRecipeExecutionManifest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecipeExecutionManifest{}, false, nil
	}
	if err != nil {
		return storedRecipeExecutionManifest{}, false, err
	}
	return manifest, true, nil
}

func lockRecipeExecutionManifestByDeploymentID(ctx context.Context, tx *sql.Tx, deploymentID string) (storedRecipeExecutionManifest, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT execution_id, deployment_id, plan_id, plan_revision, plan_hash, cloud_connection_id,
			manifest_digest, manifest_cbor, manifest_json, status, revision
		FROM p2p_cloud_recipe_execution_manifests WHERE deployment_id = $1 FOR UPDATE
	`, deploymentID)
	manifest, err := scanStoredRecipeExecutionManifest(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecipeExecutionManifest{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return manifest, err
}

func scanStoredRecipeExecutionManifest(row *sql.Row) (storedRecipeExecutionManifest, error) {
	var result storedRecipeExecutionManifest
	var executionID, deploymentID, planID, manifestDigest, manifestJSON, status string
	var manifestCBOR []byte
	var revision int64
	if err := row.Scan(&executionID, &deploymentID, &planID, &result.PlanRevision, &result.PlanHash, &result.ConnectionID,
		&manifestDigest, &manifestCBOR, &manifestJSON, &status, &revision); err != nil {
		return storedRecipeExecutionManifest{}, err
	}
	var manifest cloudcontracts.RecipeExecutionManifestV1
	if err := decodeCloudContractJSON(manifestJSON, &manifest); err != nil || manifest.Validate() != nil {
		return storedRecipeExecutionManifest{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	computedDigest, err := manifest.Digest()
	if err != nil || computedDigest != manifestDigest {
		return storedRecipeExecutionManifest{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	computedCBOR, err := manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil || !bytes.Equal(computedCBOR, manifestCBOR) || manifest.ExecutionID != executionID || manifest.DeploymentID != deploymentID ||
		manifest.PlanID != planID || int64(manifest.PlanRevision) != result.PlanRevision || manifest.PlanHash != result.PlanHash {
		return storedRecipeExecutionManifest{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	if revision <= 0 || (status != recipeExecutionStatusRegistered && status != recipeExecutionStatusApprovalPrepared && status != recipeExecutionStatusApproved) {
		return storedRecipeExecutionManifest{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	result.Manifest = manifest
	result.Execution = cloudmodule.RecipeExecution{
		ExecutionID: executionID, DeploymentID: deploymentID, PlanID: planID,
		RecipeExecutionManifestDigest: manifestDigest, Status: status, Revision: revision,
	}
	return result, nil
}

func advanceRecipeExecutionManifestStatus(ctx context.Context, tx *sql.Tx, execution cloudmodule.RecipeExecution, nextStatus string, now int64) (cloudmodule.RecipeExecution, error) {
	if execution.ExecutionID == "" || execution.Revision <= 0 || (nextStatus != recipeExecutionStatusApprovalPrepared && nextStatus != recipeExecutionStatusApproved) {
		return cloudmodule.RecipeExecution{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	updated, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_recipe_execution_manifests
		SET status = $1, revision = $2, updated_at = $3
		WHERE execution_id = $4 AND revision = $5 AND status = $6
	`, nextStatus, execution.Revision+1, now, execution.ExecutionID, execution.Revision, execution.Status)
	if err != nil {
		return cloudmodule.RecipeExecution{}, err
	}
	if err := requireRecipeExecutionApprovalMutation(updated); err != nil {
		return cloudmodule.RecipeExecution{}, err
	}
	execution.Status = nextStatus
	execution.Revision++
	return execution, nil
}

func expireStalePendingRecipeExecutionApproval(ctx context.Context, tx *sql.Tx, executionID string, now int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_recipe_execution_approvals
		SET status = 'expired', updated_at = $1
		WHERE execution_id = $2 AND status = 'pending' AND expires_at <= $1
	`, now, executionID)
	return err
}

func lockPendingRecipeExecutionApproval(ctx context.Context, tx *sql.Tx, executionID string) (storedRecipeExecutionApproval, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT approval_id, owner_mxid, execution_id, deployment_id, deployment_revision,
			plan_id, plan_revision, signer_key_id, manifest_digest, approval_json,
			signing_payload_cbor, expires_at, status, prepare_request_digest,
			approve_request_digest, signature, job_id
		FROM p2p_cloud_recipe_execution_approvals
		WHERE execution_id = $1 AND status = 'pending'
		FOR UPDATE
	`, executionID)
	approval, err := scanStoredRecipeExecutionApproval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecipeExecutionApproval{}, false, nil
	}
	if err != nil {
		return storedRecipeExecutionApproval{}, false, err
	}
	return approval, true, nil
}

func lockRecipeExecutionApproval(ctx context.Context, tx *sql.Tx, approvalID, executionID string) (storedRecipeExecutionApproval, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT approval_id, owner_mxid, execution_id, deployment_id, deployment_revision,
			plan_id, plan_revision, signer_key_id, manifest_digest, approval_json,
			signing_payload_cbor, expires_at, status, prepare_request_digest,
			approve_request_digest, signature, job_id
		FROM p2p_cloud_recipe_execution_approvals
		WHERE approval_id = $1 AND execution_id = $2
		FOR UPDATE
	`, approvalID, executionID)
	approval, err := scanStoredRecipeExecutionApproval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRecipeExecutionApproval{}, cloudmodule.ErrRecipeExecutionConfirmationConflict
	}
	return approval, err
}

func scanStoredRecipeExecutionApproval(row *sql.Row) (storedRecipeExecutionApproval, error) {
	var approval storedRecipeExecutionApproval
	err := row.Scan(&approval.ApprovalID, &approval.OwnerMXID, &approval.ExecutionID, &approval.DeploymentID, &approval.DeploymentRevision,
		&approval.PlanID, &approval.PlanRevision, &approval.SignerKeyID, &approval.ManifestDigest, &approval.ApprovalJSON,
		&approval.SigningPayload, &approval.ExpiresAt, &approval.Status, &approval.PrepareRequestDigest,
		&approval.ApproveRequestDigest, &approval.Signature, &approval.JobID)
	return approval, err
}

func loadRecipeExecutionPrepareReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PrepareRecipeExecutionConfirmationRequest) (cloudmodule.PrepareRecipeExecutionConfirmationResult, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT approval.approval_json, approval.prepare_request_digest,
			manifest.execution_id, manifest.deployment_id, manifest.plan_id, manifest.manifest_digest, manifest.status, manifest.revision
		FROM p2p_cloud_recipe_execution_approvals AS approval
		JOIN p2p_cloud_recipe_execution_manifests AS manifest ON manifest.execution_id = approval.execution_id
		WHERE approval.owner_mxid = $1 AND approval.prepare_idempotency_hash = $2
	`, request.OwnerMXID, request.IdempotencyHash)
	var approvalJSON, requestDigest string
	var execution cloudmodule.RecipeExecution
	if err := row.Scan(&approvalJSON, &requestDigest, &execution.ExecutionID, &execution.DeploymentID, &execution.PlanID,
		&execution.RecipeExecutionManifestDigest, &execution.Status, &execution.Revision); errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareRecipeExecutionConfirmationResult{}, false, nil
	} else if err != nil {
		return cloudmodule.PrepareRecipeExecutionConfirmationResult{}, false, err
	}
	if requestDigest != request.RequestDigest {
		return cloudmodule.PrepareRecipeExecutionConfirmationResult{}, false, cloudmodule.ErrIdempotencyConflict
	}
	approval, err := decodeStoredRecipeExecutionApproval(approvalJSON)
	if err != nil {
		return cloudmodule.PrepareRecipeExecutionConfirmationResult{}, false, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return cloudmodule.PrepareRecipeExecutionConfirmationResult{
		Confirmation: cloudmodule.RecipeExecutionConfirmation{Execution: execution, Approval: approval},
	}, true, nil
}

func loadRecipeExecutionApprovalReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.ApproveRecipeExecutionRequest, requestDigest string) (cloudmodule.ApproveRecipeExecutionResult, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT approval.status, approval.approve_request_digest, approval.job_id,
			manifest.execution_id, manifest.deployment_id, manifest.plan_id, manifest.manifest_digest, manifest.status, manifest.revision
		FROM p2p_cloud_recipe_execution_approvals AS approval
		JOIN p2p_cloud_recipe_execution_manifests AS manifest ON manifest.execution_id = approval.execution_id
		WHERE approval.owner_mxid = $1 AND approval.approve_idempotency_hash = $2
	`, request.OwnerMXID, request.IdempotencyHash)
	var status, jobID string
	var storedRequestDigest sql.NullString
	var execution cloudmodule.RecipeExecution
	if err := row.Scan(&status, &storedRequestDigest, &jobID, &execution.ExecutionID, &execution.DeploymentID, &execution.PlanID,
		&execution.RecipeExecutionManifestDigest, &execution.Status, &execution.Revision); errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveRecipeExecutionResult{}, false, nil
	} else if err != nil {
		return cloudmodule.ApproveRecipeExecutionResult{}, false, err
	}
	if !storedRequestDigest.Valid || storedRequestDigest.String != requestDigest {
		return cloudmodule.ApproveRecipeExecutionResult{}, false, cloudmodule.ErrIdempotencyConflict
	}
	switch status {
	case recipeExecutionApprovalApproved:
		job, err := loadCloudRecipeExecutionJob(ctx, tx, jobID)
		if err != nil {
			return cloudmodule.ApproveRecipeExecutionResult{}, false, err
		}
		return cloudmodule.ApproveRecipeExecutionResult{Execution: execution, Job: job}, true, nil
	case recipeExecutionApprovalExpired:
		return cloudmodule.ApproveRecipeExecutionResult{}, false, cloudmodule.ErrRecipeExecutionApprovalExpired
	default:
		return cloudmodule.ApproveRecipeExecutionResult{}, false, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
}

func loadCloudRecipeExecutionJob(ctx context.Context, tx *sql.Tx, jobID string) (cloudmodule.Job, error) {
	var job cloudmodule.Job
	err := scanCloudJob(tx.QueryRowContext(ctx, `SELECT `+cloudJobColumns+` FROM p2p_cloud_jobs WHERE job_id = $1`, jobID), &job)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.Job{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	if err != nil {
		return cloudmodule.Job{}, err
	}
	if job.Kind != "install" {
		return cloudmodule.Job{}, cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return job, nil
}

func decodeStoredRecipeExecutionApproval(value string) (cloudcontracts.RecipeExecutionApprovalV1, error) {
	var approval cloudcontracts.RecipeExecutionApprovalV1
	if err := decodeCloudContractJSON(value, &approval); err != nil || approval.Validate() != nil || approval.Signature != "" {
		return cloudcontracts.RecipeExecutionApprovalV1{}, errors.New("stored cloud recipe execution approval is invalid")
	}
	return approval, nil
}

func expireRecipeExecutionApproval(ctx context.Context, tx *sql.Tx, approval storedRecipeExecutionApproval, request cloudmodule.ApproveRecipeExecutionRequest, requestDigest string) error {
	updated, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_recipe_execution_approvals
		SET status = 'expired', approve_idempotency_hash = $1, approve_request_digest = $2, updated_at = $3
		WHERE approval_id = $4 AND status = 'pending'
	`, request.IdempotencyHash, requestDigest, request.CreatedAt, approval.ApprovalID)
	if err != nil {
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		return err
	}
	return requireRecipeExecutionApprovalMutation(updated)
}

func validateRecipeExecutionInstallRequestShape(request cloudmodule.ApproveRecipeExecutionRequest, plan cloudmodule.Plan, deployment cloudmodule.Deployment) error {
	job := request.Job
	if job.JobID == "" || job.PlanID != plan.PlanID || job.DeploymentID != deployment.DeploymentID || job.Kind != "install" ||
		job.Execution != "queued" || job.Outcome != "pending" || job.Checkpoint != "install_queued" || job.ErrorCode != "" || job.Revision != 1 ||
		job.CreatedAt != request.CreatedAt || job.UpdatedAt != request.CreatedAt || request.OutboxID == "" || request.JobEventID == "" {
		return cloudmodule.ErrRecipeExecutionConfirmationInvalid
	}
	return nil
}

func recipeExecutionApprovalRequestDigest(request cloudmodule.ApproveRecipeExecutionRequest) (string, error) {
	payload, err := request.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	return digestCloudRecipeExecutionValues(
		[]byte(request.DeploymentID),
		[]byte(fmt.Sprint(request.ExpectedRevision)),
		payload,
		[]byte(request.Approval.Signature),
	)
}

func digestCloudRecipeExecutionValues(values ...[]byte) (string, error) {
	// Reuse the length-delimited SHA-256 scheme used for Plan approval request
	// idempotency, while keeping Recipe execution material on an independent
	// helper so its scope cannot accidentally drift into the purchase path.
	hash := sha256.New()
	for _, value := range values {
		if _, err := hash.Write([]byte(fmt.Sprintf("%08x", len(value)))); err != nil {
			return "", err
		}
		if _, err := hash.Write(value); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func requireRecipeExecutionApprovalMutation(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return cloudmodule.ErrRecipeExecutionConfirmationConflict
	}
	return nil
}
