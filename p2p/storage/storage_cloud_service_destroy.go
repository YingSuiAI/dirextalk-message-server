package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.ServiceDestroyConfirmationStore = (*DatabaseStore)(nil)

type storedServiceDestroyApproval struct {
	ApprovalID, OwnerMXID, ServiceID, DeploymentID, ConnectionID, RecipeID, RecipeDigest string
	SignerKeyID, ApprovalJSON, ServiceJSON, DeploymentJSON                               string
	ResultServiceJSON, ResultDeploymentJSON, ResultJobJSON                               string
	Status, PrepareRequestDigest                                                         string
	ApproveRequestDigest                                                                 sql.NullString
	SigningPayload                                                                       []byte
	ServiceRevision, DeploymentRevision, ExpiresAt                                       int64
	JobID                                                                                string
}

type serviceDestroyState struct {
	OwnerMXID             string
	Service               cloudmodule.Service
	Deployment            cloudmodule.Deployment
	RecipeDigest          string
	InstanceID            string
	VolumeIDs             []string
	NetworkInterfaceIDs   []string
	PrivateResourceStatus string
}

func (s *DatabaseStore) PrepareCloudServiceDestroy(ctx context.Context, request cloudmodule.PrepareServiceDestroyRequest) (cloudmodule.PrepareServiceDestroyResult, error) {
	if err := validatePrepareServiceDestroyRequest(request); err != nil {
		return cloudmodule.PrepareServiceDestroyResult{}, err
	}
	result := cloudmodule.PrepareServiceDestroyResult{}
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceDestroyPrepareReplay(ctx, tx, request); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		state, err := lockServiceDestroyState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if replay, found, err := loadServiceDestroyPrepareReplay(ctx, tx, request); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		if !serviceDestroyStateReady(state) {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		target := serviceDestroyTarget(state)
		approval, err := cloudcontracts.NewServiceDestroyApprovalV1(target, request.ApprovalID, request.ChallengeID, keyID, time.UnixMilli(request.CreatedAt).UTC(), time.UnixMilli(request.ExpiresAt).UTC())
		if err != nil {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		payload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		approvalJSON, err := json.Marshal(approval)
		if err != nil {
			return err
		}
		serviceJSON, err := json.Marshal(state.Service)
		if err != nil {
			return err
		}
		deploymentJSON, err := json.Marshal(state.Deployment)
		if err != nil {
			return err
		}
		volumesJSON, _ := json.Marshal(target.VolumeIDs)
		interfacesJSON, _ := json.Marshal(target.NetworkInterfaceIDs)
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_destroy_approvals (
			approval_id,challenge_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,
			cloud_connection_id,recipe_id,recipe_digest,instance_id,volume_ids_json,network_interface_ids_json,
			signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,
			prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'pending',$19,$20,$21,$22,$22)`,
			request.ApprovalID, request.ChallengeID, request.OwnerMXID, target.ServiceID, target.ServiceRevision,
			target.DeploymentID, target.DeploymentRevision, target.CloudConnectionID, target.RecipeID, target.RecipeDigest,
			target.InstanceID, string(volumesJSON), string(interfacesJSON), keyID, string(approvalJSON), payload,
			string(serviceJSON), string(deploymentJSON), request.IdempotencyHash, request.RequestDigest, request.ExpiresAt, request.CreatedAt)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		result = cloudmodule.PrepareServiceDestroyResult{Confirmation: cloudmodule.ServiceDestroyConfirmation{Service: state.Service, Deployment: state.Deployment, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudServiceDestroy(ctx context.Context, request cloudmodule.ApproveServiceDestroyRequest) (cloudmodule.ApproveServiceDestroyResult, error) {
	requestDigest, err := serviceDestroyApprovalRequestDigest(request)
	if err != nil || validateApproveServiceDestroyRequest(request) != nil {
		return cloudmodule.ApproveServiceDestroyResult{}, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	result := cloudmodule.ApproveServiceDestroyResult{}
	var terminalErr error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceDestroyApprovalReplay(ctx, tx, request, requestDigest); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		stored, err := lockServiceDestroyApproval(ctx, tx, request.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if replay, found, err := loadServiceDestroyApprovalReplay(ctx, tx, request, requestDigest); err != nil || found {
			if err != nil {
				return err
			}
			result = replay
			return nil
		}
		if stored.OwnerMXID != request.OwnerMXID || stored.ServiceID != request.ServiceID || stored.ServiceRevision != request.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		if stored.ExpiresAt <= request.CreatedAt {
			if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, request.IdempotencyHash, requestDigest, request.CreatedAt, stored.ApprovalID); err != nil {
				return err
			}
			terminalErr = cloudmodule.ErrServiceDestroyApprovalExpired
			return nil
		}
		state, err := lockServiceDestroyState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision || !serviceDestroyStateReady(state) {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		storedApproval, err := decodeStoredServiceDestroyApproval(stored.ApprovalJSON)
		if err != nil {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		incomingPayload, err := request.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) || storedApproval.SignerKeyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || request.Approval.Verify(publicKey, time.UnixMilli(request.CreatedAt).UTC()) != nil {
			return cloudmodule.ErrServiceDestroyApprovalSignature
		}
		if request.Approval.ValidateAgainst(serviceDestroyTarget(state), time.UnixMilli(request.CreatedAt).UTC()) != nil {
			return cloudmodule.ErrServiceDestroyConfirmationInvalid
		}
		service := state.Service
		service.Status, service.Revision, service.UpdatedAt = "destroying", service.Revision+1, request.CreatedAt
		deployment := state.Deployment
		deployment.Resource, deployment.Revision, deployment.UpdatedAt = "destroying", deployment.Revision+1, request.CreatedAt
		job := cloudmodule.Job{JobID: request.JobID, PlanID: deployment.PlanID, DeploymentID: deployment.DeploymentID, Kind: "destroy", Execution: "queued", Outcome: "pending", Checkpoint: "destroy_queued", Revision: 1, CreatedAt: request.CreatedAt, UpdatedAt: request.CreatedAt}
		serviceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='destroying',revision=$1,updated_at=$2 WHERE service_id=$3 AND revision=$4 AND service_status IN ('experimental','active','degraded')`, service.Revision, request.CreatedAt, service.ServiceID, state.Service.Revision)
		if err != nil {
			return err
		}
		if !exactlyOneRow(serviceResult) {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		deploymentResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployments SET resource_status='destroying',revision=$1,updated_at=$2 WHERE deployment_id=$3 AND revision=$4 AND resource_status IN ('active','retained_tracked')`, deployment.Revision, request.CreatedAt, deployment.DeploymentID, state.Deployment.Revision)
		if err != nil {
			return err
		}
		if !exactlyOneRow(deploymentResult) {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		resourceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='destroying',updated_at=$1 WHERE deployment_id=$2 AND cloud_connection_id=$3 AND instance_id=$4 AND resource_status IN ('active','retained_tracked')`, request.CreatedAt, deployment.DeploymentID, deployment.ConnectionID, state.InstanceID)
		if err != nil {
			return err
		}
		if !exactlyOneRow(resourceResult) {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,$2,$3,'destroy','queued','pending','destroy_queued','',1,$4,$4)`, job.JobID, job.PlanID, job.DeploymentID, request.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,'destroy','queued','Device-approved typed resource destruction is queued; resources remain billable until AWS read-back verifies deletion.','destroy_queued','',1,$2,$2)`, job.JobID, request.CreatedAt); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]string{"service_id": service.ServiceID})
		if _, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at) VALUES($1,$2,'service',$3,$4,$5)`, request.OutboxID, cloudmodule.OutboxKindServiceDestroyRequested, service.ServiceID, string(payload), request.CreatedAt); err != nil {
			return err
		}
		serviceJSON, _ := json.Marshal(service)
		deploymentJSON, _ := json.Marshal(deployment)
		jobJSON, _ := json.Marshal(job)
		approvalUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,job_id=$4,result_service_json=$5,result_deployment_json=$6,result_job_json=$7,updated_at=$8 WHERE approval_id=$9 AND status='pending'`, request.IdempotencyHash, requestDigest, request.Approval.Signature, job.JobID, string(serviceJSON), string(deploymentJSON), string(jobJSON), request.CreatedAt, stored.ApprovalID)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if !exactlyOneRow(approvalUpdate) {
			return cloudmodule.ErrServiceDestroyConfirmationConflict
		}
		if err := writeCloudConfirmationEvent(ctx, tx, request.ServiceEventID, "cloud.service.changed", "service", service.ServiceID, service.Revision, service, request.CreatedAt); err != nil {
			return err
		}
		if err := writeCloudConfirmationEvent(ctx, tx, request.DeploymentEventID, "cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deployment, request.CreatedAt); err != nil {
			return err
		}
		if err := writeCloudConfirmationEvent(ctx, tx, request.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveServiceDestroyResult{Service: service, Deployment: deployment, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminalErr
}

func validatePrepareServiceDestroyRequest(request cloudmodule.PrepareServiceDestroyRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.ServiceID) == "" || request.ExpectedRevision <= 0 || request.IdempotencyHash == "" || request.RequestDigest == "" || request.ApprovalID == "" || request.ChallengeID == "" || request.CreatedAt <= 0 || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return nil
}

func validateApproveServiceDestroyRequest(request cloudmodule.ApproveServiceDestroyRequest) error {
	if strings.TrimSpace(request.OwnerMXID) == "" || strings.TrimSpace(request.ServiceID) == "" || request.ExpectedRevision <= 0 || request.IdempotencyHash == "" || request.JobID == "" || request.OutboxID == "" || request.ServiceEventID == "" || request.DeploymentEventID == "" || request.JobEventID == "" || request.CreatedAt <= 0 || request.Approval.Validate() != nil || request.Approval.Signature == "" {
		return cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return nil
}

func lockServiceDestroyState(ctx context.Context, tx *sql.Tx, serviceID string) (serviceDestroyState, error) {
	var state serviceDestroyState
	var volumeJSON, interfaceJSON string
	err := tx.QueryRowContext(ctx, `SELECT goal.owner_mxid,
		service.service_id,service.deployment_id,service.recipe_id,service.name,service.service_status,service.integration_status,service.revision,service.created_at,service.updated_at,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,deployment.revision,deployment.created_at,deployment.updated_at,
		recipe.digest,resource.instance_id,resource.volume_ids_json,resource.network_interface_ids_json,resource.resource_status
		FROM p2p_cloud_services service JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_plans plan ON plan.plan_id=deployment.plan_id JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
		JOIN p2p_cloud_recipes recipe ON recipe.recipe_id=service.recipe_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		WHERE service.service_id=$1 FOR UPDATE OF service,deployment,resource,recipe`, serviceID).Scan(&state.OwnerMXID,
		&state.Service.ServiceID, &state.Service.DeploymentID, &state.Service.RecipeID, &state.Service.Name, &state.Service.Status, &state.Service.Integration, &state.Service.Revision, &state.Service.CreatedAt, &state.Service.UpdatedAt,
		&state.Deployment.DeploymentID, &state.Deployment.PlanID, &state.Deployment.ConnectionID, &state.Deployment.Execution, &state.Deployment.Outcome, &state.Deployment.Resource, &state.Deployment.Revision, &state.Deployment.CreatedAt, &state.Deployment.UpdatedAt,
		&state.RecipeDigest, &state.InstanceID, &volumeJSON, &interfaceJSON, &state.PrivateResourceStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return state, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	if err != nil {
		return state, err
	}
	if json.Unmarshal([]byte(volumeJSON), &state.VolumeIDs) != nil || json.Unmarshal([]byte(interfaceJSON), &state.NetworkInterfaceIDs) != nil {
		return state, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return state, nil
}

func serviceDestroyStateReady(state serviceDestroyState) bool {
	if state.Service.DeploymentID != state.Deployment.DeploymentID || state.Service.RecipeID == "" || state.RecipeDigest == "" || state.Deployment.ConnectionID == "" || state.Deployment.Execution != "finished" || state.Deployment.Outcome != "succeeded" {
		return false
	}
	if state.Service.Status != "experimental" && state.Service.Status != "active" && state.Service.Status != "degraded" {
		return false
	}
	if state.Deployment.Resource != "active" && state.Deployment.Resource != "retained_tracked" {
		return false
	}
	if state.PrivateResourceStatus != state.Deployment.Resource {
		return false
	}
	return serviceDestroyTarget(state).Validate() == nil
}

func serviceDestroyTarget(state serviceDestroyState) cloudcontracts.ServiceDestroyTargetV1 {
	return cloudcontracts.ServiceDestroyTargetV1{ServiceID: state.Service.ServiceID, ServiceRevision: uint64(state.Service.Revision), DeploymentID: state.Deployment.DeploymentID, DeploymentRevision: uint64(state.Deployment.Revision), CloudConnectionID: state.Deployment.ConnectionID, RecipeID: state.Service.RecipeID, RecipeDigest: state.RecipeDigest, InstanceID: state.InstanceID, VolumeIDs: state.VolumeIDs, NetworkInterfaceIDs: state.NetworkInterfaceIDs}
}

func loadServiceDestroyPrepareReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PrepareServiceDestroyRequest) (cloudmodule.PrepareServiceDestroyResult, bool, error) {
	var approvalJSON, serviceJSON, deploymentJSON, digest string
	err := tx.QueryRowContext(ctx, `SELECT approval_json,service_json,deployment_json,prepare_request_digest FROM p2p_cloud_service_destroy_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2`, request.OwnerMXID, request.IdempotencyHash).Scan(&approvalJSON, &serviceJSON, &deploymentJSON, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceDestroyResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareServiceDestroyResult{}, false, err
	}
	if digest != request.RequestDigest {
		return cloudmodule.PrepareServiceDestroyResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	approval, err := decodeStoredServiceDestroyApproval(approvalJSON)
	var service cloudmodule.Service
	var deployment cloudmodule.Deployment
	if err != nil || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil || service.ServiceID != request.ServiceID || service.Revision != request.ExpectedRevision {
		return cloudmodule.PrepareServiceDestroyResult{}, true, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return cloudmodule.PrepareServiceDestroyResult{Confirmation: cloudmodule.ServiceDestroyConfirmation{Service: service, Deployment: deployment, Approval: approval}}, true, nil
}

func lockServiceDestroyApproval(ctx context.Context, tx *sql.Tx, approvalID string) (storedServiceDestroyApproval, error) {
	var value storedServiceDestroyApproval
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,cloud_connection_id,recipe_id,recipe_digest,signer_key_id,approval_json,signing_payload,service_json,deployment_json,result_service_json,result_deployment_json,result_job_json,status,prepare_request_digest,approve_request_digest,expires_at,job_id FROM p2p_cloud_service_destroy_approvals WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(
		&value.ApprovalID, &value.OwnerMXID, &value.ServiceID, &value.ServiceRevision, &value.DeploymentID, &value.DeploymentRevision, &value.ConnectionID, &value.RecipeID, &value.RecipeDigest, &value.SignerKeyID, &value.ApprovalJSON, &value.SigningPayload, &value.ServiceJSON, &value.DeploymentJSON, &value.ResultServiceJSON, &value.ResultDeploymentJSON, &value.ResultJobJSON, &value.Status, &value.PrepareRequestDigest, &value.ApproveRequestDigest, &value.ExpiresAt, &value.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		return value, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return value, err
}

func loadServiceDestroyApprovalReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.ApproveServiceDestroyRequest, requestDigest string) (cloudmodule.ApproveServiceDestroyResult, bool, error) {
	var storedDigest, status, serviceJSON, deploymentJSON, jobJSON string
	err := tx.QueryRowContext(ctx, `SELECT approve_request_digest,status,result_service_json,result_deployment_json,result_job_json FROM p2p_cloud_service_destroy_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2`, request.OwnerMXID, request.IdempotencyHash).Scan(&storedDigest, &status, &serviceJSON, &deploymentJSON, &jobJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveServiceDestroyResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveServiceDestroyResult{}, false, err
	}
	if storedDigest != requestDigest {
		return cloudmodule.ApproveServiceDestroyResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveServiceDestroyResult{}, true, cloudmodule.ErrServiceDestroyApprovalExpired
	}
	var service cloudmodule.Service
	var deployment cloudmodule.Deployment
	var job cloudmodule.Job
	if status != "approved" || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil || json.Unmarshal([]byte(jobJSON), &job) != nil {
		return cloudmodule.ApproveServiceDestroyResult{}, true, cloudmodule.ErrServiceDestroyConfirmationInvalid
	}
	return cloudmodule.ApproveServiceDestroyResult{Service: service, Deployment: deployment, Job: job}, true, nil
}

func decodeStoredServiceDestroyApproval(raw string) (cloudcontracts.ServiceDestroyApprovalV1, error) {
	var approval cloudcontracts.ServiceDestroyApprovalV1
	if json.Unmarshal([]byte(raw), &approval) != nil || approval.Signature != "" || approval.Validate() != nil {
		return cloudcontracts.ServiceDestroyApprovalV1{}, errors.New("stored service destroy approval is invalid")
	}
	return approval, nil
}

func serviceDestroyApprovalRequestDigest(request cloudmodule.ApproveServiceDestroyRequest) (string, error) {
	payload, err := request.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(struct {
		ServiceID        string `json:"service_id"`
		ExpectedRevision int64  `json:"expected_revision"`
		ApprovalID       string `json:"approval_id"`
		SigningPayload   []byte `json:"signing_payload"`
		Signature        string `json:"signature"`
	}{request.ServiceID, request.ExpectedRevision, request.Approval.ApprovalID, payload, request.Approval.Signature})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func exactlyOneRow(result sql.Result) bool {
	if result == nil {
		return false
	}
	count, err := result.RowsAffected()
	return err == nil && count == 1
}
