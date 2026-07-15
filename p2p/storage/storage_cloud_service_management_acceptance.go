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
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type managementAcceptanceState struct {
	Operation                       serviceOperationState
	Recipe                          cloudmodule.Recipe
	RecipeContract                  cloudcontracts.RecipeV1
	RecipeCanonical                 []byte
	RecipeDisplay                   string
	ReadinessSemanticEvidenceDigest string
	ReadinessStackObservationDigest string
	RestartID                       string
	RestartRevision                 int64
	BackupID                        string
	BackupRevision                  int64
	RestoreID                       string
	RestoreRevision                 int64
	VolumeIDs                       []string
	NetworkIDs                      []string
}

type storedManagementAcceptance struct {
	AcceptanceID, ApprovalID, OwnerMXID, ServiceID, RecipeID, SignerKeyID                   string
	TargetJSON, ApprovalJSON, ServiceJSON, RecipeJSON, Status, PrepareDigest, ApproveDigest string
	ResultServiceJSON, ResultRecipeJSON, ResultAcceptanceJSON                               string
	ServiceRevision, RecipeRevision, ExpiresAt                                              int64
	SigningPayload                                                                          []byte
}

func (s *DatabaseStore) PrepareCloudServiceManagementAcceptance(ctx context.Context, r cloudmodule.PrepareServiceManagementAcceptanceRequest) (cloudmodule.PrepareServiceManagementAcceptanceResult, error) {
	if validatePrepareManagementAcceptance(r) != nil {
		return cloudmodule.PrepareServiceManagementAcceptanceResult{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	var result cloudmodule.PrepareServiceManagementAcceptanceResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadManagementAcceptancePrepareReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_management_acceptances SET status='expired',revision=revision+1,updated_at=$1 WHERE service_id=$2 AND status='pending' AND expires_at<=$1`, r.CreatedAt, r.ServiceID); err != nil {
			return err
		}
		state, err := lockManagementAcceptanceState(ctx, tx, r.ServiceID)
		if err != nil {
			return err
		}
		if state.Operation.OwnerMXID != r.OwnerMXID || state.Operation.Service.Revision != r.ExpectedRevision {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		if !managementAcceptanceEvidenceReady(ctx, tx, state) || serviceHasActiveOperation(ctx, tx, r.ServiceID) || serviceHasActiveBackup(ctx, tx, r.ServiceID) || serviceHasActiveRestore(ctx, tx, r.ServiceID) {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Operation.Deployment.ConnectionID)
		if err != nil {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		serviceTransitioning := state.Operation.Service.Status == "experimental" && (state.Recipe.Maturity == "experimental" || state.Recipe.Maturity == "managed")
		recipeTransitioning := serviceTransitioning && state.Recipe.Maturity == "experimental"
		if !serviceTransitioning && (state.Operation.Service.Status != "awaiting_management_acceptance" || (state.Recipe.Maturity != "awaiting_management_acceptance" && state.Recipe.Maturity != "managed")) {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		if serviceTransitioning {
			state.Operation.Service.Status = "awaiting_management_acceptance"
			state.Operation.Service.Revision++
			state.Operation.Service.UpdatedAt = r.CreatedAt
		}
		if recipeTransitioning {
			state.Recipe.Maturity = "awaiting_management_acceptance"
			state.Recipe.Revision++
			state.Recipe.UpdatedAt = r.CreatedAt
		}
		target := managementAcceptanceTarget(r.AcceptanceID, state)
		approval, err := cloudcontracts.NewServiceManagementAcceptanceApprovalV1(target, r.ApprovalID, r.ChallengeID, keyID, time.UnixMilli(r.CreatedAt), time.UnixMilli(r.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		payload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		approvalJSON, _ := json.Marshal(approval)
		targetJSON, _ := json.Marshal(target)
		serviceJSON, _ := json.Marshal(state.Operation.Service)
		recipeJSON, _ := json.Marshal(state.Recipe)
		if serviceTransitioning {
			serviceUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='awaiting_management_acceptance',revision=$1,updated_at=$2 WHERE service_id=$3 AND revision=$4 AND service_status='experimental'`, state.Operation.Service.Revision, r.CreatedAt, r.ServiceID, r.ExpectedRevision)
			if err != nil || !exactlyOneRow(serviceUpdate) {
				return cloudmodule.ErrServiceManagementAcceptanceConflict
			}
		}
		if recipeTransitioning {
			recipeUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipes SET maturity='awaiting_management_acceptance',revision=$1,updated_at=$2 WHERE recipe_id=$3 AND revision=$4 AND maturity='experimental'`, state.Recipe.Revision, r.CreatedAt, state.Recipe.RecipeID, state.Recipe.Revision-1)
			if err != nil || !exactlyOneRow(recipeUpdate) {
				return cloudmodule.ErrServiceManagementAcceptanceConflict
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) VALUES($1,$2,$3,$4,$5,'awaiting_management_acceptance',$6)`, state.Recipe.RecipeID, state.Recipe.Revision, state.RecipeCanonical, state.RecipeDisplay, state.Recipe.Digest, r.CreatedAt); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_management_acceptances(acceptance_id,approval_id,challenge_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,recipe_id,recipe_revision,signer_key_id,target_json,approval_json,signing_payload,service_json,recipe_json,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'pending',$17,$18,$19,$20,$20)`, r.AcceptanceID, r.ApprovalID, r.ChallengeID, r.OwnerMXID, r.ServiceID, state.Operation.Service.Revision, state.Operation.Deployment.DeploymentID, state.Operation.Deployment.Revision, state.Recipe.RecipeID, state.Recipe.Revision, keyID, string(targetJSON), string(approvalJSON), payload, string(serviceJSON), string(recipeJSON), r.IdempotencyHash, r.RequestDigest, r.ExpiresAt, r.CreatedAt)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if serviceTransitioning {
			if err = writeCloudConfirmationEvent(ctx, tx, r.ServiceEventID, "cloud.service.changed", "service", r.ServiceID, state.Operation.Service.Revision, state.Operation.Service, r.CreatedAt); err != nil {
				return err
			}
		}
		result = cloudmodule.PrepareServiceManagementAcceptanceResult{Confirmation: cloudmodule.ServiceManagementAcceptanceConfirmation{Service: state.Operation.Service, Recipe: state.Recipe, Approval: approval}, Created: true, ServiceChanged: serviceTransitioning}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudServiceManagementAcceptance(ctx context.Context, r cloudmodule.ApproveServiceManagementAcceptanceRequest) (cloudmodule.ApproveServiceManagementAcceptanceResult, error) {
	requestDigest, err := managementAcceptanceApproveDigest(r)
	if err != nil || validateApproveManagementAcceptance(r) != nil {
		return cloudmodule.ApproveServiceManagementAcceptanceResult{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	var result cloudmodule.ApproveServiceManagementAcceptanceResult
	var terminal error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadManagementAcceptanceApproveReplay(ctx, tx, r, requestDigest); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockManagementAcceptance(ctx, tx, r.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != r.OwnerMXID || stored.ServiceID != r.ServiceID || stored.ServiceRevision != r.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		if stored.ExpiresAt <= r.CreatedAt {
			if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_management_acceptances SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,revision=revision+1,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, r.IdempotencyHash, requestDigest, r.CreatedAt, stored.ApprovalID); err != nil {
				return err
			}
			terminal = cloudmodule.ErrServiceManagementAcceptanceExpired
			return nil
		}
		state, err := lockManagementAcceptanceState(ctx, tx, r.ServiceID)
		if err != nil {
			return err
		}
		if state.Operation.OwnerMXID != r.OwnerMXID || state.Operation.Service.Status != "awaiting_management_acceptance" || state.Operation.Service.Revision != r.ExpectedRevision || (state.Recipe.Maturity != "awaiting_management_acceptance" && state.Recipe.Maturity != "managed") || state.Recipe.Revision != stored.RecipeRevision || !managementAcceptanceEvidenceReady(ctx, tx, state) {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		var storedTarget cloudcontracts.ServiceManagementAcceptanceTargetV1
		var storedApproval cloudcontracts.ServiceManagementAcceptanceApprovalV1
		if decodeCloudContractJSON(stored.TargetJSON, &storedTarget) != nil || decodeCloudContractJSON(stored.ApprovalJSON, &storedApproval) != nil {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		incomingPayload, err := r.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Operation.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || r.Approval.Verify(publicKey, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrServiceManagementAcceptanceSignature
		}
		if r.Approval.ValidateAgainst(managementAcceptanceTarget(stored.AcceptanceID, state), time.UnixMilli(r.CreatedAt)) != nil || r.Approval.ValidateAgainst(storedTarget, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrServiceManagementAcceptanceInvalid
		}
		state.Operation.Service.Status = "active"
		state.Operation.Service.Revision++
		state.Operation.Service.UpdatedAt = r.CreatedAt
		recipeTransitioning := state.Recipe.Maturity == "awaiting_management_acceptance"
		if recipeTransitioning {
			state.Recipe.Maturity = "managed"
			state.Recipe.Revision++
			state.Recipe.UpdatedAt = r.CreatedAt
		}
		serviceUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='active',revision=$1,updated_at=$2 WHERE service_id=$3 AND revision=$4 AND service_status='awaiting_management_acceptance'`, state.Operation.Service.Revision, r.CreatedAt, r.ServiceID, r.ExpectedRevision)
		if err != nil || !exactlyOneRow(serviceUpdate) {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		if recipeTransitioning {
			recipeUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipes SET maturity='managed',revision=$1,updated_at=$2 WHERE recipe_id=$3 AND revision=$4 AND maturity='awaiting_management_acceptance'`, state.Recipe.Revision, r.CreatedAt, state.Recipe.RecipeID, state.Recipe.Revision-1)
			if err != nil || !exactlyOneRow(recipeUpdate) {
				return cloudmodule.ErrServiceManagementAcceptanceConflict
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) VALUES($1,$2,$3,$4,$5,'managed',$6)`, state.Recipe.RecipeID, state.Recipe.Revision, state.RecipeCanonical, state.RecipeDisplay, state.Recipe.Digest, r.CreatedAt); err != nil {
				return err
			}
		}
		acceptance := cloudmodule.ServiceManagementAcceptance{AcceptanceID: stored.AcceptanceID, ServiceID: r.ServiceID, RecipeID: state.Recipe.RecipeID, Status: "approved", Revision: 2, CreatedAt: r.Approval.IssuedAt.UnixMilli(), UpdatedAt: r.CreatedAt}
		serviceJSON, _ := json.Marshal(state.Operation.Service)
		recipeJSON, _ := json.Marshal(state.Recipe)
		acceptanceJSON, _ := json.Marshal(acceptance)
		update, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_management_acceptances SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,result_service_json=$3,result_recipe_json=$4,result_acceptance_json=$5,revision=2,updated_at=$6 WHERE approval_id=$7 AND status='pending'`, r.IdempotencyHash, requestDigest, string(serviceJSON), string(recipeJSON), string(acceptanceJSON), r.CreatedAt, stored.ApprovalID)
		if err != nil || !exactlyOneRow(update) {
			return cloudmodule.ErrServiceManagementAcceptanceConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.ServiceEventID, "cloud.service.changed", "service", r.ServiceID, state.Operation.Service.Revision, state.Operation.Service, r.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveServiceManagementAcceptanceResult{Service: state.Operation.Service, Recipe: state.Recipe, Acceptance: acceptance, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminal
}

func lockManagementAcceptanceState(ctx context.Context, tx *sql.Tx, serviceID string) (managementAcceptanceState, error) {
	operation, err := lockServiceOperationState(ctx, tx, serviceID)
	if err != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	state := managementAcceptanceState{Operation: operation}
	var volumeJSON, networkJSON string
	if err = tx.QueryRowContext(ctx, `SELECT recipe.name,recipe.version,recipe.digest,recipe.maturity,recipe.revision,recipe.created_at,recipe.updated_at,version.canonical_cbor,version.display_json,resource.volume_ids_json,resource.network_interface_ids_json FROM p2p_cloud_recipes recipe JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=$2 WHERE recipe.recipe_id=$1 FOR UPDATE OF recipe,version,resource`, operation.Service.RecipeID, operation.Deployment.DeploymentID).Scan(&state.Recipe.Name, &state.Recipe.Version, &state.Recipe.Digest, &state.Recipe.Maturity, &state.Recipe.Revision, &state.Recipe.CreatedAt, &state.Recipe.UpdatedAt, &state.RecipeCanonical, &state.RecipeDisplay, &volumeJSON, &networkJSON); err != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	state.Recipe.RecipeID = operation.Service.RecipeID
	if decodeCloudContractJSON(state.RecipeDisplay, &state.RecipeContract) != nil || state.RecipeContract.Validate() != nil || state.RecipeContract.RecipeID != state.Recipe.RecipeID {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	computed, err := state.RecipeContract.Digest()
	if err != nil || computed != state.Recipe.Digest || state.Recipe.Digest != operation.RecipeDigest {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	if json.Unmarshal([]byte(volumeJSON), &state.VolumeIDs) != nil || json.Unmarshal([]byte(networkJSON), &state.NetworkIDs) != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	var restoreUpdated int64
	var replacementJSON string
	if err = tx.QueryRowContext(ctx, `SELECT restore_id,backup_id,revision,updated_at,replacement_volume_ids_json FROM p2p_cloud_service_restores WHERE service_id=$1 AND deployment_id=$2 AND restore_status='succeeded' ORDER BY updated_at DESC,restore_id LIMIT 1`, serviceID, operation.Deployment.DeploymentID).Scan(&state.RestoreID, &state.BackupID, &state.RestoreRevision, &restoreUpdated, &replacementJSON); err != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	var replacements []string
	if json.Unmarshal([]byte(replacementJSON), &replacements) != nil || !sameStringSet(replacements, state.VolumeIDs) {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	if err = tx.QueryRowContext(ctx, `SELECT revision FROM p2p_cloud_service_backups WHERE backup_id=$1 AND service_id=$2 AND deployment_id=$3 AND backup_status='available'`, state.BackupID, serviceID, operation.Deployment.DeploymentID).Scan(&state.BackupRevision); err != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	if err = tx.QueryRowContext(ctx, `SELECT task.operation_id,job.revision FROM p2p_cloud_service_operation_tasks task JOIN p2p_cloud_jobs job ON job.job_id=task.job_id WHERE task.service_id=$1 AND task.deployment_id=$2 AND task.operation='restart' AND task.task_status='succeeded' AND task.updated_at>=$3 ORDER BY task.updated_at DESC,task.operation_id LIMIT 1`, serviceID, operation.Deployment.DeploymentID, restoreUpdated).Scan(&state.RestartID, &state.RestartRevision); err != nil {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	var expectation string
	if err = tx.QueryRowContext(ctx, `SELECT semantic_expectation_digest,semantic_evidence_digest,stack_observation_digest FROM p2p_cloud_service_readiness_tasks WHERE service_id=$1 AND deployment_id=$2 AND purpose='install' AND task_status='succeeded'`, serviceID, operation.Deployment.DeploymentID).Scan(&expectation, &state.ReadinessSemanticEvidenceDigest, &state.ReadinessStackObservationDigest); err != nil || expectation != cloudcontracts.FixedReadinessEvidenceDigestV1 || state.ReadinessSemanticEvidenceDigest != expectation || !validSHA256Digest(state.ReadinessStackObservationDigest) {
		return managementAcceptanceState{}, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return state, nil
}

func managementAcceptanceEvidenceReady(ctx context.Context, tx *sql.Tx, s managementAcceptanceState) bool {
	if s.Operation.Service.Status != "experimental" && s.Operation.Service.Status != "awaiting_management_acceptance" {
		return false
	}
	if s.Recipe.Maturity != "experimental" && s.Recipe.Maturity != "awaiting_management_acceptance" && s.Recipe.Maturity != "managed" {
		return false
	}
	if s.Operation.Deployment.Execution != "finished" || s.Operation.Deployment.Outcome != "succeeded" || s.Operation.Deployment.Resource != "active" || s.Operation.PrivateResourceStatus != "active" || s.Operation.InstallManifest.ArtifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || s.Operation.InstallManifest.ActionID != cloudcontracts.FixedProbeInstallActionID || s.Operation.InstallManifest.RecipeDigest != s.Recipe.Digest || s.Operation.InstallManifest.VerifyDigest(s.Operation.ManifestDigest) != nil {
		return false
	}
	for _, source := range s.RecipeContract.Sources {
		if !source.Official || strings.TrimSpace(source.Version) == "" || strings.TrimSpace(source.Commit) == "" || !validSHA256Digest(source.ArtifactDigest) {
			return false
		}
	}
	if s.ReadinessSemanticEvidenceDigest != cloudcontracts.FixedReadinessEvidenceDigestV1 || !validSHA256Digest(s.ReadinessStackObservationDigest) {
		return false
	}
	destroy := serviceDestroyTarget(serviceDestroyState{Service: s.Operation.Service, Deployment: s.Operation.Deployment, RecipeDigest: s.Recipe.Digest, InstanceID: s.Operation.InstanceID, VolumeIDs: s.VolumeIDs, NetworkInterfaceIDs: s.NetworkIDs, PrivateResourceStatus: s.Operation.PrivateResourceStatus})
	return destroy.Validate() == nil
}

func managementAcceptanceTarget(id string, s managementAcceptanceState) cloudcontracts.ServiceManagementAcceptanceTargetV1 {
	sources := make([]string, 0, len(s.RecipeContract.Sources))
	for _, source := range s.RecipeContract.Sources {
		sources = append(sources, source.ArtifactDigest)
	}
	return cloudcontracts.ServiceManagementAcceptanceTargetV1{
		AcceptanceID: id, ServiceID: s.Operation.Service.ServiceID, ServiceRevision: uint64(s.Operation.Service.Revision),
		DeploymentID: s.Operation.Deployment.DeploymentID, DeploymentRevision: uint64(s.Operation.Deployment.Revision), CloudConnectionID: s.Operation.Deployment.ConnectionID,
		RecipeID: s.Recipe.RecipeID, RecipeDigest: s.Recipe.Digest, RecipeRevision: uint64(s.Recipe.Revision), RecipeMaturity: cloudcontracts.RecipeMaturity(s.Recipe.Maturity), InstalledManifestDigest: s.Operation.ManifestDigest, ArtifactDigest: s.Operation.InstallManifest.ArtifactDigest,
		ReadinessSemanticEvidenceDigest: s.ReadinessSemanticEvidenceDigest, ReadinessStackObservationDigest: s.ReadinessStackObservationDigest,
		RestartOperationID: s.RestartID, RestartOperationRevision: uint64(s.RestartRevision), BackupID: s.BackupID, BackupRevision: uint64(s.BackupRevision), RestoreID: s.RestoreID, RestoreRevision: uint64(s.RestoreRevision),
		SourceArtifactDigests: sources, Health: s.RecipeContract.Health, Lifecycle: s.RecipeContract.Lifecycle,
		VolumeSlots: append([]cloudcontracts.VolumeSlotV1(nil), s.Operation.InstallManifest.VolumeSlots...), DataSlots: append([]cloudcontracts.DataSlotV1(nil), s.Operation.InstallManifest.DataSlots...), SecretSlots: append([]cloudcontracts.SecretSlotV1(nil), s.Operation.InstallManifest.SecretSlots...),
		DestroyInstanceID: s.Operation.InstanceID, DestroyVolumeIDs: append([]string(nil), s.VolumeIDs...), DestroyNetworkInterfaceIDs: append([]string(nil), s.NetworkIDs...), AcceptancePolicy: cloudcontracts.ServiceManagementAcceptancePolicy,
	}
}

func validatePrepareManagementAcceptance(r cloudmodule.PrepareServiceManagementAcceptanceRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.RequestDigest == "" || r.AcceptanceID == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.ServiceEventID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return nil
}

func validateApproveManagementAcceptance(r cloudmodule.ApproveServiceManagementAcceptanceRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.ServiceEventID == "" || r.CreatedAt <= 0 || r.Approval.Validate() != nil || r.Approval.Signature == "" {
		return cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return nil
}

func loadManagementAcceptancePrepareReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.PrepareServiceManagementAcceptanceRequest) (cloudmodule.PrepareServiceManagementAcceptanceResult, bool, error) {
	var digest, serviceJSON, recipeJSON, approvalJSON string
	err := tx.QueryRowContext(ctx, `SELECT prepare_request_digest,service_json,recipe_json,approval_json FROM p2p_cloud_service_management_acceptances WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&digest, &serviceJSON, &recipeJSON, &approvalJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceManagementAcceptanceResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareServiceManagementAcceptanceResult{}, false, err
	}
	if digest != r.RequestDigest {
		return cloudmodule.PrepareServiceManagementAcceptanceResult{}, false, cloudmodule.ErrIdempotencyConflict
	}
	var result cloudmodule.PrepareServiceManagementAcceptanceResult
	if decodeCloudContractJSON(serviceJSON, &result.Confirmation.Service) != nil || decodeCloudContractJSON(recipeJSON, &result.Confirmation.Recipe) != nil || decodeCloudContractJSON(approvalJSON, &result.Confirmation.Approval) != nil {
		return result, false, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return result, true, nil
}

func lockManagementAcceptance(ctx context.Context, tx *sql.Tx, approvalID string) (storedManagementAcceptance, error) {
	var s storedManagementAcceptance
	err := tx.QueryRowContext(ctx, `SELECT acceptance_id,approval_id,owner_mxid,service_id,service_revision,recipe_id,recipe_revision,signer_key_id,target_json,approval_json,signing_payload,service_json,recipe_json,status,prepare_request_digest,approve_request_digest,result_service_json,result_recipe_json,result_acceptance_json,expires_at FROM p2p_cloud_service_management_acceptances WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(&s.AcceptanceID, &s.ApprovalID, &s.OwnerMXID, &s.ServiceID, &s.ServiceRevision, &s.RecipeID, &s.RecipeRevision, &s.SignerKeyID, &s.TargetJSON, &s.ApprovalJSON, &s.SigningPayload, &s.ServiceJSON, &s.RecipeJSON, &s.Status, &s.PrepareDigest, &s.ApproveDigest, &s.ResultServiceJSON, &s.ResultRecipeJSON, &s.ResultAcceptanceJSON, &s.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return s, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return s, err
}

func loadManagementAcceptanceApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApproveServiceManagementAcceptanceRequest, digest string) (cloudmodule.ApproveServiceManagementAcceptanceResult, bool, error) {
	var storedDigest, serviceJSON, recipeJSON, acceptanceJSON, status string
	err := tx.QueryRowContext(ctx, `SELECT approve_request_digest,result_service_json,result_recipe_json,result_acceptance_json,status FROM p2p_cloud_service_management_acceptances WHERE owner_mxid=$1 AND approve_idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&storedDigest, &serviceJSON, &recipeJSON, &acceptanceJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveServiceManagementAcceptanceResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveServiceManagementAcceptanceResult{}, false, err
	}
	if storedDigest != digest {
		return cloudmodule.ApproveServiceManagementAcceptanceResult{}, false, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveServiceManagementAcceptanceResult{}, true, cloudmodule.ErrServiceManagementAcceptanceExpired
	}
	var result cloudmodule.ApproveServiceManagementAcceptanceResult
	if decodeCloudContractJSON(serviceJSON, &result.Service) != nil || decodeCloudContractJSON(recipeJSON, &result.Recipe) != nil || decodeCloudContractJSON(acceptanceJSON, &result.Acceptance) != nil {
		return result, false, cloudmodule.ErrServiceManagementAcceptanceInvalid
	}
	return result, true, nil
}

func managementAcceptanceApproveDigest(r cloudmodule.ApproveServiceManagementAcceptanceRequest) (string, error) {
	payload, err := json.Marshal(struct {
		OwnerMXID, ServiceID string
		ExpectedRevision     int64
		Approval             cloudcontracts.ServiceManagementAcceptanceApprovalV1
	}{r.OwnerMXID, r.ServiceID, r.ExpectedRevision, r.Approval})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func sameStringSet(left, right []string) bool {
	left, right = append([]string(nil), left...), append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return fmt.Sprint(left) == fmt.Sprint(right)
}

func validSHA256Digest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
