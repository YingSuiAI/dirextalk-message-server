package storage

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
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

var _ cloudmodule.PlanConfirmationStore = (*DatabaseStore)(nil)

// PrepareCloudPlanConfirmation makes the first spend-bound transition only
// after the quoted AWS capacity, persisted recipe requirements and registered
// Flutter device key all agree. It deliberately creates a no-ingress,
// no-secret, no-integration PlanV1; broader scopes must be separately planned
// and approved in a later stage.
func (s *DatabaseStore) PrepareCloudPlanConfirmation(ctx context.Context, request cloudmodule.PreparePlanConfirmationRequest) (cloudmodule.PreparePlanConfirmationResult, error) {
	if err := validatePrepareCloudPlanConfirmationRequest(request); err != nil {
		return cloudmodule.PreparePlanConfirmationResult{}, err
	}
	result := cloudmodule.PreparePlanConfirmationResult{}
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadPlanConfirmationPrepareReplay(ctx, tx, request); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}

		plan, err := lockOwnedCloudPlan(ctx, tx, request.OwnerMXID, request.PlanID)
		if err != nil {
			return err
		}
		// A concurrent identical request can have committed while this
		// transaction waited on the Plan row. Re-check its idempotency slot
		// before interpreting the now-newer Plan revision as a conflict.
		if replay, found, err := loadPlanConfirmationPrepareReplay(ctx, tx, request); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		if plan.Revision != request.ExpectedRevision || plan.Status != cloudmodule.PlanStatusQuoting || plan.QuoteID != request.QuoteID || plan.PlanHash != "" || plan.RecipeDigest == "" {
			return cloudmodule.ErrPlanConfirmationConflict
		}
		quote, quoteDigest, err := lockCloudQuoteForConfirmation(ctx, tx, request.QuoteID)
		if err != nil {
			return err
		}
		if quote.CloudConnectionID != plan.ConnectionID || !quote.ValidUntil.After(time.UnixMilli(request.CreatedAt).UTC()) || request.ExpiresAt > quote.ValidUntil.UnixMilli() {
			return cloudmodule.ErrPlanQuoteExpired
		}
		candidate, found := quoteCandidateByTier(quote, request.CandidateTier)
		if !found {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		recipe, recipeDigest, recipeMaturity, err := lockCloudRecipeForConfirmation(ctx, tx, plan.RecipeDigest)
		if err != nil {
			return err
		}
		if err := requireCandidateMeetsRecipe(candidate, recipe.Requirements); err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		deviceKeyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, plan.ConnectionID)
		if err != nil {
			return err
		}
		if deviceKeyID == "" {
			return cloudmodule.ErrPlanConfirmationInvalid
		}

		confirmedRevision := plan.Revision + 1
		secretScope, err := cloudcontracts.SecretScopeForRecipe(plan.PlanID, recipe.SecretSlots)
		if err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		planV1 := cloudcontracts.PlanV1{
			SchemaVersion:     cloudcontracts.SchemaVersionV1,
			PlanID:            plan.PlanID,
			Revision:          uint64(confirmedRevision),
			Status:            cloudcontracts.PlanReadyForConfirmation,
			CloudConnectionID: plan.ConnectionID,
			Recipe: cloudcontracts.RecipeBindingV1{
				RecipeID: recipe.RecipeID,
				Digest:   recipeDigest,
				Maturity: recipeMaturity,
			},
			Quote: cloudcontracts.QuoteBindingV1{
				QuoteID:     quote.QuoteID,
				Digest:      quoteDigest,
				ValidUntil:  quote.ValidUntil,
				CandidateID: candidate.CandidateID,
			},
			ResourceScope: cloudcontracts.ResourceScopeV1{
				Region:            quote.Region,
				AvailabilityZones: append([]string(nil), candidate.AvailabilityZones...),
				InstanceType:      candidate.InstanceType,
				Architecture:      candidate.Architecture,
				VCPU:              candidate.VCPU,
				MemoryMiB:         candidate.MemoryMiB,
				GPUCount:          candidate.GPUCount,
				GPUMemoryMiB:      candidate.GPUMemoryMiB,
				DiskGiB:           candidate.EstimatedDiskGiB,
				PurchaseOption:    candidate.PurchaseOption,
			},
			NetworkScope: cloudcontracts.NetworkScopeV1{
				PublicIngress: false, EntryPoint: cloudcontracts.EntryPointNone,
				TLSRequired: false, AuthenticationRequired: false,
			},
			SecretScope:      secretScope,
			IntegrationScope: []cloudcontracts.IntegrationScopeV1{},
		}
		planHash, err := planV1.Hash()
		if err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		planCBOR, err := planV1.CanonicalPlanCBOR()
		if err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		approval, err := cloudcontracts.NewApprovalV1(planV1, request.ApprovalID, request.ChallengeID, deviceKeyID, time.UnixMilli(request.ExpiresAt).UTC())
		if err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		signingPayload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrPlanConfirmationInvalid
		}
		approvalJSON, err := json.Marshal(approval)
		if err != nil {
			return err
		}
		planDisplay, err := json.Marshal(planV1)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_plan_versions (
				plan_id, revision, canonical_cbor, display_json, plan_hash, recipe_digest,
				quote_id, quote_digest, quote_valid_until, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`, plan.PlanID, confirmedRevision, planCBOR, string(planDisplay), planHash, recipeDigest,
			quote.QuoteID, quoteDigest, quote.ValidUntil.UnixMilli(), request.CreatedAt); err != nil {
			return err
		}
		updated, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_plans
			SET status = 'ready_for_confirmation', plan_hash = $1, revision = $2, updated_at = $3
			WHERE plan_id = $4 AND revision = $5 AND status = 'quoting' AND quote_id = $6 AND plan_hash = ''
		`, planHash, confirmedRevision, request.CreatedAt, plan.PlanID, plan.Revision, quote.QuoteID)
		if err != nil {
			return err
		}
		if err := requireCloudPlanConfirmationMutation(updated); err != nil {
			return err
		}
		approvalInsert, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_plan_approvals (
				approval_id, owner_mxid, plan_id, plan_revision, challenge_id, signer_key_id,
				plan_hash, approval_json, signing_payload_cbor, expires_at, status,
				prepare_idempotency_hash, prepare_request_digest, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', $11, $12, $13, $13)
			ON CONFLICT (owner_mxid, prepare_idempotency_hash) DO NOTHING
		`, approval.ApprovalID, request.OwnerMXID, plan.PlanID, confirmedRevision, approval.ChallengeID, approval.SignerKeyID,
			planHash, string(approvalJSON), signingPayload, request.ExpiresAt, request.IdempotencyHash, request.RequestDigest, request.CreatedAt)
		if err != nil {
			return err
		}
		if err := requireCloudPlanConfirmationMutation(approvalInsert); err != nil {
			return cloudmodule.ErrIdempotencyConflict
		}
		plan.Status = cloudmodule.PlanStatusReadyForConfirmation
		plan.PlanHash = planHash
		plan.Revision = confirmedRevision
		plan.UpdatedAt = request.CreatedAt
		eventID := stableCloudConfirmationID("cloud_event_", plan.PlanID, fmt.Sprint(plan.Revision), "confirmation_ready")
		if err := writeCloudConfirmationEvent(ctx, tx,
			eventID,
			"cloud.plan.changed", "plan", plan.PlanID, plan.Revision, plan, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.PreparePlanConfirmationResult{
			Confirmation: cloudmodule.PlanConfirmation{Plan: plan, Approval: approval},
			EventID:      eventID,
			Created:      true,
		}
		return nil
	})
	return result, err
}

// ApproveCloudPlan verifies a persisted one-time device challenge, marks the
// plan approved and atomically queues a private provision request. It creates
// no EC2 resource itself; the independent Orchestrator owns the later typed
// Broker command and must replay its exact durable envelope on uncertainty.
func (s *DatabaseStore) ApproveCloudPlan(ctx context.Context, request cloudmodule.ApproveCloudPlanRequest) (cloudmodule.ApproveCloudPlanResult, error) {
	requestDigest, err := cloudApprovalRequestDigest(request)
	if err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, cloudmodule.ErrPlanApprovalInvalid
	}
	if err := validateApproveCloudPlanRequest(request); err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, err
	}
	result := cloudmodule.ApproveCloudPlanResult{}
	var terminalErr error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadPlanApprovalReplay(ctx, tx, request, requestDigest); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}

		plan, err := lockOwnedCloudPlan(ctx, tx, request.OwnerMXID, request.PlanID)
		if err != nil {
			if errors.Is(err, cloudmodule.ErrPlanConfirmationInvalid) {
				return cloudmodule.ErrPlanApprovalInvalid
			}
			return err
		}
		// See the matching prepare path: repeat the lookup after waiting for
		// the Plan row so a simultaneous HTTP retry returns its original
		// Deployment instead of becoming a false revision conflict.
		if replay, found, err := loadPlanApprovalReplay(ctx, tx, request, requestDigest); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		if plan.Status == cloudmodule.PlanStatusExpired {
			return cloudmodule.ErrPlanApprovalExpired
		}
		if plan.Revision != request.ExpectedRevision || plan.Status != cloudmodule.PlanStatusReadyForConfirmation || plan.PlanHash == "" {
			return cloudmodule.ErrPlanApprovalConflict
		}
		stored, err := lockPendingPlanApproval(ctx, tx, request.Approval.ApprovalID, plan.PlanID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != request.OwnerMXID || stored.PlanRevision != plan.Revision || stored.PlanHash != plan.PlanHash || stored.Status != "pending" {
			return cloudmodule.ErrPlanApprovalConflict
		}
		now := time.UnixMilli(request.CreatedAt).UTC()
		if !time.UnixMilli(stored.ExpiresAt).UTC().After(now) {
			if err := expirePendingCloudPlanApproval(ctx, tx, plan, stored, request, requestDigest); err != nil {
				return err
			}
			terminalErr = cloudmodule.ErrPlanApprovalExpired
			return nil
		}
		storedApproval, err := decodeStoredApproval(stored.ApprovalJSON)
		if err != nil {
			return cloudmodule.ErrPlanApprovalInvalid
		}
		incomingPayload, err := request.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrPlanApprovalInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) || storedApproval.SignerKeyID != stored.SignerKeyID ||
			storedApproval.PlanID != plan.PlanID || storedApproval.PlanHash != plan.PlanHash || int64(storedApproval.PlanRevision) != plan.Revision {
			return cloudmodule.ErrPlanApprovalInvalid
		}
		if !storedApproval.QuoteValidUntil.After(now) {
			if err := expirePendingCloudPlanApproval(ctx, tx, plan, stored, request, requestDigest); err != nil {
				return err
			}
			terminalErr = cloudmodule.ErrPlanApprovalExpired
			return nil
		}
		deviceKeyID, devicePublicKey, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, plan.ConnectionID)
		if err != nil || deviceKeyID != stored.SignerKeyID {
			return cloudmodule.ErrPlanApprovalInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(devicePublicKey)
		if err != nil || request.Approval.Verify(publicKey, now) != nil {
			return cloudmodule.ErrPlanApprovalSignature
		}
		if err := validateProvisionRequestShape(request, plan); err != nil {
			return err
		}

		approvedRevision := plan.Revision + 1
		updated, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_plans
			SET status = 'approved', revision = $1, updated_at = $2
			WHERE plan_id = $3 AND revision = $4 AND status = 'ready_for_confirmation' AND plan_hash = $5
		`, approvedRevision, request.CreatedAt, plan.PlanID, plan.Revision, plan.PlanHash)
		if err != nil {
			return err
		}
		if err := requireCloudPlanApprovalMutation(updated); err != nil {
			return err
		}
		deployment := request.Deployment
		deployment.ConnectionID = plan.ConnectionID
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_deployments (
				deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status,
				revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
		`, deployment.DeploymentID, plan.PlanID, deployment.ConnectionID, deployment.Execution, deployment.Outcome,
			deployment.Resource, deployment.Revision, request.CreatedAt); err != nil {
			return err
		}
		job := request.Job
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_jobs (
				job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
				checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, '', $8, $9, $9)
		`, job.JobID, plan.PlanID, deployment.DeploymentID, job.Kind, job.Execution, job.Outcome,
			job.Checkpoint, job.Revision, request.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_job_steps (
				job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, 'provision', 'queued', 'A device-approved dedicated Worker request is queued; no cloud resource exists yet.', 'provision_queued', '', 1, $2, $2)
		`, job.JobID, request.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_outbox (outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at)
			VALUES ($1, $2, 'deployment', $3, $4, $5)
		`, request.Outbox.OutboxID, request.Outbox.Kind, deployment.DeploymentID, request.Outbox.PayloadJSON, request.CreatedAt); err != nil {
			return err
		}
		approvalUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_plan_approvals
			SET status = 'approved', approve_idempotency_hash = $1, approve_request_digest = $2,
				signature = $3, deployment_id = $4, updated_at = $5
			WHERE approval_id = $6 AND status = 'pending'
		`, request.IdempotencyHash, requestDigest, request.Approval.Signature, deployment.DeploymentID, request.CreatedAt, stored.ApprovalID)
		if err != nil {
			// Separate plans can be approved concurrently. The partial unique index
			// is the final cross-plan idempotency guard, so surface its collision as
			// the stable ProductCore conflict instead of an opaque database error.
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if err := requireCloudPlanApprovalMutation(approvalUpdate); err != nil {
			return err
		}
		plan.Status = cloudmodule.PlanStatusApproved
		plan.Revision = approvedRevision
		plan.UpdatedAt = request.CreatedAt
		if err := writeCloudConfirmationEvent(ctx, tx, request.PlanEventID, "cloud.plan.changed", "plan", plan.PlanID, plan.Revision, plan, request.CreatedAt); err != nil {
			return err
		}
		if err := writeCloudConfirmationEvent(ctx, tx, request.DeploymentEventID, "cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deployment, request.CreatedAt); err != nil {
			return err
		}
		if err := writeCloudConfirmationEvent(ctx, tx, request.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveCloudPlanResult{Plan: plan, Deployment: deployment, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminalErr
}

type storedPlanApproval struct {
	ApprovalID           string
	OwnerMXID            string
	PlanID               string
	PlanRevision         int64
	SignerKeyID          string
	PlanHash             string
	ApprovalJSON         string
	SigningPayload       []byte
	ExpiresAt            int64
	Status               string
	PrepareRequestDigest string
	ApproveRequestDigest sql.NullString
	DeploymentID         string
}

func validatePrepareCloudPlanConfirmationRequest(request cloudmodule.PreparePlanConfirmationRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.PlanID) == "" || strings.TrimSpace(request.QuoteID) == "" ||
		request.ExpectedRevision <= 0 || request.IdempotencyHash == "" || request.RequestDigest == "" || request.ApprovalID == "" || request.ChallengeID == "" ||
		request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((15*time.Minute).Milliseconds()) {
		return cloudmodule.ErrPlanConfirmationInvalid
	}
	if request.CandidateTier != "economy" && request.CandidateTier != "recommended" && request.CandidateTier != "performance" {
		return cloudmodule.ErrPlanConfirmationInvalid
	}
	return nil
}

func validateApproveCloudPlanRequest(request cloudmodule.ApproveCloudPlanRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.PlanID) == "" || request.ExpectedRevision <= 0 || request.IdempotencyHash == "" ||
		request.CreatedAt <= 0 || request.Approval.ApprovalID == "" || request.Approval.Signature == "" {
		return cloudmodule.ErrPlanApprovalInvalid
	}
	return nil
}

func lockOwnedCloudPlan(ctx context.Context, tx *sql.Tx, ownerMXID, planID string) (cloudmodule.Plan, error) {
	var owner string
	if err := tx.QueryRowContext(ctx, `SELECT owner_mxid FROM p2p_cloud_goals WHERE plan_id = $1 FOR UPDATE`, planID).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.Plan{}, cloudmodule.ErrPlanConfirmationInvalid
		}
		return cloudmodule.Plan{}, err
	}
	if owner != ownerMXID {
		return cloudmodule.Plan{}, cloudmodule.ErrPlanConfirmationInvalid
	}
	var plan cloudmodule.Plan
	if err := scanCloudPlan(tx.QueryRowContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans WHERE plan_id = $1 FOR UPDATE`, planID), &plan); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.Plan{}, cloudmodule.ErrPlanConfirmationInvalid
		}
		return cloudmodule.Plan{}, err
	}
	return plan, nil
}

func lockCloudQuoteForConfirmation(ctx context.Context, tx *sql.Tx, quoteID string) (cloudcontracts.QuoteV1, string, error) {
	var displayJSON, digest string
	var validUntil int64
	if err := tx.QueryRowContext(ctx, `
		SELECT display_json, digest, valid_until FROM p2p_cloud_quotes WHERE quote_id = $1 FOR UPDATE
	`, quoteID).Scan(&displayJSON, &digest, &validUntil); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudcontracts.QuoteV1{}, "", cloudmodule.ErrPlanConfirmationInvalid
		}
		return cloudcontracts.QuoteV1{}, "", err
	}
	var quote cloudcontracts.QuoteV1
	if err := decodeCloudContractJSON(displayJSON, &quote); err != nil || quote.Validate() != nil || quote.QuoteID != quoteID || quote.ValidUntil.UnixMilli() != validUntil {
		return cloudcontracts.QuoteV1{}, "", cloudmodule.ErrPlanConfirmationInvalid
	}
	computed, err := quote.Digest()
	if err != nil || computed != digest {
		return cloudcontracts.QuoteV1{}, "", cloudmodule.ErrPlanConfirmationInvalid
	}
	return quote, digest, nil
}

func lockCloudRecipeForConfirmation(ctx context.Context, tx *sql.Tx, expectedDigest string) (cloudcontracts.RecipeV1, string, cloudcontracts.RecipeMaturity, error) {
	var recipeID, digest, maturity, displayJSON string
	if err := tx.QueryRowContext(ctx, `
		SELECT recipe.recipe_id, recipe.digest, recipe.maturity, recipe_version.display_json
		FROM p2p_cloud_recipes AS recipe
		JOIN p2p_cloud_recipe_versions AS recipe_version ON recipe_version.recipe_id = recipe.recipe_id AND recipe_version.revision = recipe.revision
		WHERE recipe.digest = $1
		FOR UPDATE OF recipe, recipe_version
	`, expectedDigest).Scan(&recipeID, &digest, &maturity, &displayJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudcontracts.RecipeV1{}, "", "", cloudmodule.ErrPlanConfirmationInvalid
		}
		return cloudcontracts.RecipeV1{}, "", "", err
	}
	var recipe cloudcontracts.RecipeV1
	if err := decodeCloudContractJSON(displayJSON, &recipe); err != nil || recipe.Validate() != nil || recipe.RecipeID != recipeID {
		return cloudcontracts.RecipeV1{}, "", "", cloudmodule.ErrPlanConfirmationInvalid
	}
	computed, err := recipe.Digest()
	if err != nil || computed != digest || digest != expectedDigest || (maturity != string(cloudcontracts.RecipeExperimental) && maturity != string(cloudcontracts.RecipeAwaitingManagementAccept) && maturity != string(cloudcontracts.RecipeManaged)) {
		return cloudcontracts.RecipeV1{}, "", "", cloudmodule.ErrPlanConfirmationInvalid
	}
	return recipe, digest, cloudcontracts.RecipeMaturity(maturity), nil
}

func lockCloudDeviceApprovalKey(ctx context.Context, tx *sql.Tx, ownerMXID, connectionID string) (string, string, error) {
	var keyID, publicKey string
	err := tx.QueryRowContext(ctx, `
		SELECT device_approval_key_id, device_approval_public_key_spki_base64
		FROM p2p_cloud_connection_bootstraps
		WHERE owner_mxid = $1 AND cloud_connection_id = $2 AND status = 'active'
		FOR UPDATE
	`, ownerMXID, connectionID).Scan(&keyID, &publicKey)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", cloudmodule.ErrPlanConfirmationInvalid
	}
	if err != nil {
		return "", "", err
	}
	return keyID, publicKey, nil
}

func quoteCandidateByTier(quote cloudcontracts.QuoteV1, tier string) (cloudcontracts.QuoteCandidateV1, bool) {
	for _, candidate := range quote.Candidates {
		if string(candidate.Tier) == tier {
			return candidate, true
		}
	}
	return cloudcontracts.QuoteCandidateV1{}, false
}

func requireCandidateMeetsRecipe(candidate cloudcontracts.QuoteCandidateV1, requirements cloudcontracts.ResourceRequirementsV1) error {
	// Spot is deliberately unavailable until a Recipe has passed checkpoint,
	// resume, and interruption tests. Do not let a future quote producer widen
	// that first-release policy merely by returning a different purchase option.
	if candidate.PurchaseOption != cloudcontracts.PurchaseOnDemand || candidate.Architecture != requirements.Architecture || candidate.VCPU < requirements.MinVCPU || candidate.MemoryMiB < requirements.MinMemoryMiB ||
		candidate.EstimatedDiskGiB < requirements.MinDiskGiB || candidate.GPUCount < requirements.MinGPUCount || candidate.GPUMemoryMiB < requirements.MinGPUMemoryMiB {
		return errors.New("quoted capacity does not meet recipe requirements")
	}
	return nil
}

func loadPlanConfirmationPrepareReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PreparePlanConfirmationRequest) (cloudmodule.PreparePlanConfirmationResult, bool, error) {
	var approval storedPlanApproval
	err := tx.QueryRowContext(ctx, `
		SELECT approval_id, owner_mxid, plan_id, plan_revision, signer_key_id, plan_hash, approval_json,
			signing_payload_cbor, expires_at, status, prepare_request_digest, approve_request_digest, deployment_id
		FROM p2p_cloud_plan_approvals
		WHERE owner_mxid = $1 AND prepare_idempotency_hash = $2
		FOR UPDATE
	`, request.OwnerMXID, request.IdempotencyHash).Scan(
		&approval.ApprovalID, &approval.OwnerMXID, &approval.PlanID, &approval.PlanRevision, &approval.SignerKeyID, &approval.PlanHash, &approval.ApprovalJSON,
		&approval.SigningPayload, &approval.ExpiresAt, &approval.Status, &approval.PrepareRequestDigest, &approval.ApproveRequestDigest, &approval.DeploymentID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PreparePlanConfirmationResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PreparePlanConfirmationResult{}, false, err
	}
	if approval.PlanID != request.PlanID || approval.PrepareRequestDigest != request.RequestDigest {
		return cloudmodule.PreparePlanConfirmationResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	plan, err := loadCloudPlanForConfirmation(ctx, tx, approval.PlanID)
	if err != nil {
		return cloudmodule.PreparePlanConfirmationResult{}, true, err
	}
	storedApproval, err := decodeStoredApproval(approval.ApprovalJSON)
	if err != nil {
		return cloudmodule.PreparePlanConfirmationResult{}, true, cloudmodule.ErrPlanConfirmationInvalid
	}
	return cloudmodule.PreparePlanConfirmationResult{
		Confirmation: cloudmodule.PlanConfirmation{Plan: plan, Approval: storedApproval},
		Created:      false,
	}, true, nil
}

func loadPlanApprovalReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.ApproveCloudPlanRequest, requestDigest string) (cloudmodule.ApproveCloudPlanResult, bool, error) {
	var approval storedPlanApproval
	err := tx.QueryRowContext(ctx, `
		SELECT approval_id, owner_mxid, plan_id, plan_revision, signer_key_id, plan_hash, approval_json,
			signing_payload_cbor, expires_at, status, prepare_request_digest, approve_request_digest, deployment_id
		FROM p2p_cloud_plan_approvals
		WHERE owner_mxid = $1 AND approve_idempotency_hash = $2
		FOR UPDATE
	`, request.OwnerMXID, request.IdempotencyHash).Scan(
		&approval.ApprovalID, &approval.OwnerMXID, &approval.PlanID, &approval.PlanRevision, &approval.SignerKeyID, &approval.PlanHash, &approval.ApprovalJSON,
		&approval.SigningPayload, &approval.ExpiresAt, &approval.Status, &approval.PrepareRequestDigest, &approval.ApproveRequestDigest, &approval.DeploymentID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveCloudPlanResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, false, err
	}
	if approval.PlanID != request.PlanID || !approval.ApproveRequestDigest.Valid || approval.ApproveRequestDigest.String != requestDigest {
		return cloudmodule.ApproveCloudPlanResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if approval.Status == "expired" {
		return cloudmodule.ApproveCloudPlanResult{}, true, cloudmodule.ErrPlanApprovalExpired
	}
	if approval.Status != "approved" || approval.DeploymentID == "" {
		return cloudmodule.ApproveCloudPlanResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	plan, err := loadCloudPlanForConfirmation(ctx, tx, approval.PlanID)
	if err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, true, err
	}
	deployment, err := loadCloudDeploymentForConfirmation(ctx, tx, approval.DeploymentID)
	if err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, true, err
	}
	job, err := loadCloudProvisionJobForConfirmation(ctx, tx, deployment.DeploymentID)
	if err != nil {
		return cloudmodule.ApproveCloudPlanResult{}, true, err
	}
	return cloudmodule.ApproveCloudPlanResult{Plan: plan, Deployment: deployment, Job: job}, true, nil
}

func lockPendingPlanApproval(ctx context.Context, tx *sql.Tx, approvalID, planID string) (storedPlanApproval, error) {
	var approval storedPlanApproval
	err := tx.QueryRowContext(ctx, `
		SELECT approval_id, owner_mxid, plan_id, plan_revision, signer_key_id, plan_hash, approval_json,
			signing_payload_cbor, expires_at, status, prepare_request_digest, approve_request_digest, deployment_id
		FROM p2p_cloud_plan_approvals
		WHERE approval_id = $1 AND plan_id = $2
		FOR UPDATE
	`, approvalID, planID).Scan(
		&approval.ApprovalID, &approval.OwnerMXID, &approval.PlanID, &approval.PlanRevision, &approval.SignerKeyID, &approval.PlanHash, &approval.ApprovalJSON,
		&approval.SigningPayload, &approval.ExpiresAt, &approval.Status, &approval.PrepareRequestDigest, &approval.ApproveRequestDigest, &approval.DeploymentID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedPlanApproval{}, cloudmodule.ErrPlanApprovalInvalid
	}
	if err != nil {
		return storedPlanApproval{}, err
	}
	return approval, nil
}

// expirePendingCloudPlanApproval closes both state machines together. An
// expired challenge cannot leave a ready Plan that no client can approve or
// re-confirm, and its idempotency record must remain replayable after a lost
// HTTP response.
func expirePendingCloudPlanApproval(ctx context.Context, tx *sql.Tx, plan cloudmodule.Plan, approval storedPlanApproval, request cloudmodule.ApproveCloudPlanRequest, requestDigest string) error {
	approvalUpdate, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_plan_approvals
		SET status = 'expired', approve_idempotency_hash = $1, approve_request_digest = $2, updated_at = $3
		WHERE approval_id = $4 AND status = 'pending'
	`, request.IdempotencyHash, requestDigest, request.CreatedAt, approval.ApprovalID)
	if err != nil {
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		return err
	}
	if err := requireCloudPlanApprovalMutation(approvalUpdate); err != nil {
		return err
	}

	expiredRevision := plan.Revision + 1
	planUpdate, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_plans
		SET status = 'expired', revision = $1, updated_at = $2
		WHERE plan_id = $3 AND revision = $4 AND status = 'ready_for_confirmation' AND plan_hash = $5
	`, expiredRevision, request.CreatedAt, plan.PlanID, plan.Revision, plan.PlanHash)
	if err != nil {
		return err
	}
	if err := requireCloudPlanApprovalMutation(planUpdate); err != nil {
		return err
	}
	plan.Status = cloudmodule.PlanStatusExpired
	plan.Revision = expiredRevision
	plan.UpdatedAt = request.CreatedAt
	eventID := stableCloudConfirmationID("cloud_event_", plan.PlanID, fmt.Sprint(plan.Revision), "approval_expired")
	return writeCloudConfirmationEvent(ctx, tx, eventID, "cloud.plan.changed", "plan", plan.PlanID, plan.Revision, plan, request.CreatedAt)
}

func validateProvisionRequestShape(request cloudmodule.ApproveCloudPlanRequest, plan cloudmodule.Plan) error {
	deployment, job, outbox := request.Deployment, request.Job, request.Outbox
	if deployment.DeploymentID == "" || deployment.PlanID != plan.PlanID || deployment.Execution != "queued" || deployment.Outcome != "pending" || deployment.Resource != "none" ||
		deployment.Revision != 1 || deployment.CreatedAt != request.CreatedAt || deployment.UpdatedAt != request.CreatedAt ||
		job.JobID == "" || job.PlanID != plan.PlanID || job.DeploymentID != deployment.DeploymentID || job.Kind != "provision" ||
		job.Execution != "queued" || job.Outcome != "pending" || job.Checkpoint != "provision_queued" || job.ErrorCode != "" || job.Revision != 1 ||
		job.CreatedAt != request.CreatedAt || job.UpdatedAt != request.CreatedAt || outbox.OutboxID == "" || outbox.Kind != cloudmodule.OutboxKindDeploymentProvisionRequested ||
		outbox.AggregateType != "deployment" || outbox.AggregateID != deployment.DeploymentID || outbox.CreatedAt != request.CreatedAt ||
		outbox.PayloadJSON != `{"deployment_id":"`+deployment.DeploymentID+`"}` || request.PlanEventID == "" || request.DeploymentEventID == "" || request.JobEventID == "" {
		return cloudmodule.ErrPlanApprovalInvalid
	}
	return nil
}

func writeCloudConfirmationEvent(ctx context.Context, tx *sql.Tx, eventID, eventType, aggregateType, aggregateID string, revision int64, summary any, now int64) error {
	if eventID == "" || revision <= 0 {
		return cloudmodule.ErrPlanApprovalInvalid
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_events (event_id, type, aggregate_type, aggregate_id, revision, summary_json, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, eventID, eventType, aggregateType, aggregateID, revision, string(payload), now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_projection_outbox (projection_id, cloud_event_id, type, payload_json, available_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $5)
	`, cloudProjectionID(eventID), eventID, eventType, string(payload), now)
	return err
}

func loadCloudPlanForConfirmation(ctx context.Context, tx *sql.Tx, planID string) (cloudmodule.Plan, error) {
	var plan cloudmodule.Plan
	if err := scanCloudPlan(tx.QueryRowContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans WHERE plan_id = $1`, planID), &plan); err != nil {
		return cloudmodule.Plan{}, err
	}
	return plan, nil
}

func loadCloudDeploymentForConfirmation(ctx context.Context, tx *sql.Tx, deploymentID string) (cloudmodule.Deployment, error) {
	var deployment cloudmodule.Deployment
	err := scanCloudDeployment(tx.QueryRowContext(ctx, `
		SELECT deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status, revision, created_at, updated_at
		FROM p2p_cloud_deployments WHERE deployment_id = $1
	`, deploymentID), &deployment)
	return deployment, err
}

func loadCloudProvisionJobForConfirmation(ctx context.Context, tx *sql.Tx, deploymentID string) (cloudmodule.Job, error) {
	var job cloudmodule.Job
	err := scanCloudJob(tx.QueryRowContext(ctx, `
		SELECT `+cloudJobColumns+` FROM p2p_cloud_jobs
		WHERE deployment_id = $1 AND kind = 'provision'
	`, deploymentID), &job)
	return job, err
}

func decodeCloudContractJSON(value string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("cloud contract contains trailing JSON")
	}
	return nil
}

func decodeStoredApproval(value string) (cloudcontracts.ApprovalV1, error) {
	var approval cloudcontracts.ApprovalV1
	if err := decodeCloudContractJSON(value, &approval); err != nil || approval.Validate() != nil || approval.Signature != "" {
		return cloudcontracts.ApprovalV1{}, errors.New("stored cloud approval is invalid")
	}
	return approval, nil
}

func parseCloudApprovalPublicKey(value string) (ed25519.PublicKey, error) {
	der, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("cloud approval key is not Ed25519")
	}
	return key, nil
}

func cloudApprovalRequestDigest(request cloudmodule.ApproveCloudPlanRequest) (string, error) {
	payload, err := request.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, value := range [][]byte{[]byte(request.PlanID), []byte(fmt.Sprint(request.ExpectedRevision)), payload, []byte(request.Approval.Signature)} {
		if _, err := hash.Write([]byte(fmt.Sprintf("%08x", len(value)))); err != nil {
			return "", err
		}
		if _, err := hash.Write(value); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func stableCloudConfirmationID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(value))
	}
	return prefix + hex.EncodeToString(hash.Sum(nil))[:32]
}

func requireCloudPlanConfirmationMutation(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return cloudmodule.ErrPlanConfirmationConflict
	}
	return nil
}

func requireCloudPlanApprovalMutation(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return cloudmodule.ErrPlanApprovalConflict
	}
	return nil
}
