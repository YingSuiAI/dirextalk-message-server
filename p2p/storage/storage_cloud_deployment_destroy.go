package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type cloudDeploymentDestroyState struct {
	OwnerMXID           string
	Deployment          cloudmodule.Deployment
	PrivateStatus       string
	InstanceID          string
	VolumeIDs           []string
	NetworkInterfaceIDs []string
	SecretRefs          []string
	PendingJobs         int
}

type storedCloudDeploymentDestroy struct {
	ApprovalID, OwnerMXID, DeploymentID, PlanID, ConnectionID, ResourceStatus, InstanceID, SignerKeyID string
	VolumeIDsJSON, NetworkInterfaceIDsJSON, SecretRefsJSON, ApprovalJSON, PrepareDeploymentJSON        string
	Status, PrepareDigest, ApproveDigest, Signature, JobID, ResultDeploymentJSON, ResultJobJSON        string
	DeploymentRevision, ExpiresAt                                                                      int64
	SigningPayload                                                                                     []byte
}

func (s *DatabaseStore) PrepareCloudDeploymentDestroy(ctx context.Context, r cloudmodule.PrepareDeploymentDestroyRequest) (cloudmodule.PrepareDeploymentDestroyResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.DeploymentID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.PrepareDeploymentDestroyResult{}, cloudmodule.ErrDeploymentDestroyInvalid
	}
	r.RequestDigest = deploymentDestroyPrepareDigest(r)
	var result cloudmodule.PrepareDeploymentDestroyResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadDeploymentDestroyPrepareReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_destroy_approvals SET status='expired',updated_at=$1 WHERE deployment_id=$2 AND status='pending' AND expires_at<=$1`, r.CreatedAt, r.DeploymentID); err != nil {
			return err
		}
		state, err := lockCloudDeploymentDestroyState(ctx, tx, r.DeploymentID)
		if errors.Is(err, sql.ErrNoRows) || state.OwnerMXID != r.OwnerMXID {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		if err != nil {
			return err
		}
		if state.Deployment.Revision != r.ExpectedRevision {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		target, err := cloudDeploymentDestroyTarget(state)
		if err != nil {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		var pending int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_deployment_destroy_approvals WHERE deployment_id=$1 AND status='pending'`, r.DeploymentID).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID == "" {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		approval, err := cloudcontracts.NewDeploymentDestroyApprovalV1(target, r.ApprovalID, r.ChallengeID, keyID, time.UnixMilli(r.CreatedAt), time.UnixMilli(r.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		payload, _ := approval.SigningPayload()
		approvalJSON, _ := json.Marshal(approval)
		deploymentJSON, _ := json.Marshal(state.Deployment)
		volumesJSON, _ := json.Marshal(target.VolumeIDs)
		interfacesJSON, _ := json.Marshal(target.NetworkInterfaceIDs)
		secretRefsJSON, _ := json.Marshal(target.SecretRefs)
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_deployment_destroy_approvals(
			approval_id,challenge_id,owner_mxid,deployment_id,deployment_revision,plan_id,cloud_connection_id,resource_status,instance_id,
			volume_ids_json,network_interface_ids_json,secret_refs_json,signer_key_id,approval_json,signing_payload_cbor,prepare_deployment_json,
			status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at
		)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'pending',$17,$18,$19,$20,$20)`,
			approval.ApprovalID, approval.ChallengeID, r.OwnerMXID, target.DeploymentID, target.DeploymentRevision, target.PlanID, target.CloudConnectionID,
			target.ResourceStatus, target.InstanceID, string(volumesJSON), string(interfacesJSON), string(secretRefsJSON), keyID, string(approvalJSON), payload,
			string(deploymentJSON), r.IdempotencyHash, r.RequestDigest, r.ExpiresAt, r.CreatedAt)
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		if err != nil {
			return err
		}
		result = cloudmodule.PrepareDeploymentDestroyResult{Confirmation: cloudmodule.DeploymentDestroyConfirmation{Deployment: state.Deployment, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudDeploymentDestroy(ctx context.Context, r cloudmodule.ApproveDeploymentDestroyRequest) (cloudmodule.ApproveDeploymentDestroyResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.DeploymentID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.Approval.Signature == "" || r.JobID == "" || r.OutboxID == "" || r.DeploymentEventID == "" || r.JobEventID == "" || r.CreatedAt <= 0 {
		return cloudmodule.ApproveDeploymentDestroyResult{}, cloudmodule.ErrDeploymentDestroyInvalid
	}
	digest, err := deploymentDestroyApproveDigest(r)
	if err != nil {
		return cloudmodule.ApproveDeploymentDestroyResult{}, cloudmodule.ErrDeploymentDestroyInvalid
	}
	r.RequestDigest = digest
	var result cloudmodule.ApproveDeploymentDestroyResult
	var terminal error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadDeploymentDestroyApproveReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockStoredCloudDeploymentDestroy(ctx, tx, r.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != r.OwnerMXID || stored.DeploymentID != r.DeploymentID || stored.DeploymentRevision != r.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		if stored.ExpiresAt <= r.CreatedAt {
			_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_destroy_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.CreatedAt, stored.ApprovalID)
			terminal = cloudmodule.ErrDeploymentDestroyExpired
			return err
		}
		state, err := lockCloudDeploymentDestroyState(ctx, tx, r.DeploymentID)
		if errors.Is(err, sql.ErrNoRows) || state.OwnerMXID != r.OwnerMXID {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		if err != nil {
			return err
		}
		if state.Deployment.Revision != stored.DeploymentRevision {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		target, err := cloudDeploymentDestroyTarget(state)
		if err != nil {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		var prepared cloudcontracts.DeploymentDestroyApprovalV1
		if decodeCloudContractJSON(stored.ApprovalJSON, &prepared) != nil {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		incomingPayload, incomingErr := r.Approval.SigningPayload()
		preparedPayload, preparedErr := prepared.SigningPayload()
		if incomingErr != nil || preparedErr != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) || !bytes.Equal(preparedPayload, stored.SigningPayload) || !reflect.DeepEqual(prepared.Target(), target) {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID || keyID != r.Approval.SignerKeyID {
			return cloudmodule.ErrDeploymentDestroyInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || r.Approval.Verify(publicKey, time.UnixMilli(r.CreatedAt)) != nil || r.Approval.ValidateAgainst(target, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrDeploymentDestroySignature
		}

		deployment := state.Deployment
		deployment.Resource, deployment.Revision, deployment.UpdatedAt = "destroying", deployment.Revision+1, r.CreatedAt
		deploymentUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployments SET resource_status='destroying',revision=$1,updated_at=$2 WHERE deployment_id=$3 AND revision=$4 AND resource_status=$5`, deployment.Revision, r.CreatedAt, deployment.DeploymentID, state.Deployment.Revision, state.Deployment.Resource)
		if err != nil || !exactlyOneRow(deploymentUpdate) {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		resourceUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='destroying',updated_at=$1 WHERE deployment_id=$2 AND cloud_connection_id=$3 AND instance_id=$4 AND resource_status=$5`, r.CreatedAt, deployment.DeploymentID, deployment.ConnectionID, target.InstanceID, target.ResourceStatus)
		if err != nil || !exactlyOneRow(resourceUpdate) {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		job := cloudmodule.Job{JobID: r.JobID, PlanID: deployment.PlanID, DeploymentID: deployment.DeploymentID, Kind: "destroy", Execution: "queued", Outcome: "pending", Checkpoint: "destroy_queued", Revision: 1, CreatedAt: r.CreatedAt, UpdatedAt: r.CreatedAt}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,$2,$3,'destroy','queued','pending','destroy_queued','',1,$4,$4)`, job.JobID, job.PlanID, job.DeploymentID, r.CreatedAt); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,'destroy','queued','Device-approved typed residual resource destruction is queued; resources remain billable until AWS read-back verifies deletion.','destroy_queued','',1,$2,$2)`, job.JobID, r.CreatedAt); err != nil {
			return err
		}
		outboxPayload, _ := json.Marshal(struct {
			DeploymentID string   `json:"deployment_id"`
			SecretRefs   []string `json:"secret_refs,omitempty"`
		}{DeploymentID: deployment.DeploymentID, SecretRefs: target.SecretRefs})
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at) VALUES($1,$2,'deployment',$3,$4,$5)`, r.OutboxID, cloudmodule.OutboxKindDeploymentDestroyRequested, deployment.DeploymentID, string(outboxPayload), r.CreatedAt); err != nil {
			return err
		}
		deploymentJSON, _ := json.Marshal(deployment)
		jobJSON, _ := json.Marshal(job)
		approvalUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_destroy_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,job_id=$4,result_deployment_json=$5,result_job_json=$6,updated_at=$7 WHERE approval_id=$8 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.Approval.Signature, job.JobID, string(deploymentJSON), string(jobJSON), r.CreatedAt, stored.ApprovalID)
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		if err != nil || !exactlyOneRow(approvalUpdate) {
			return cloudmodule.ErrDeploymentDestroyConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.DeploymentEventID, "cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deployment, r.CreatedAt); err != nil {
			return err
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, r.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveDeploymentDestroyResult{Deployment: deployment, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminal
}

func lockCloudDeploymentDestroyState(ctx context.Context, tx *sql.Tx, deploymentID string) (cloudDeploymentDestroyState, error) {
	var state cloudDeploymentDestroyState
	var volumeJSON, interfaceJSON string
	err := tx.QueryRowContext(ctx, `SELECT goal.owner_mxid,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,deployment.revision,deployment.created_at,deployment.updated_at,
		resource.resource_status,resource.instance_id,resource.volume_ids_json,resource.network_interface_ids_json
		FROM p2p_cloud_deployments deployment JOIN p2p_cloud_plans plan ON plan.plan_id=deployment.plan_id
		JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		WHERE deployment.deployment_id=$1 AND NOT EXISTS (SELECT 1 FROM p2p_cloud_services service WHERE service.deployment_id=deployment.deployment_id)
		FOR UPDATE OF deployment,resource`, deploymentID).Scan(
		&state.OwnerMXID, &state.Deployment.DeploymentID, &state.Deployment.PlanID, &state.Deployment.ConnectionID, &state.Deployment.Execution,
		&state.Deployment.Outcome, &state.Deployment.Resource, &state.Deployment.Revision, &state.Deployment.CreatedAt, &state.Deployment.UpdatedAt,
		&state.PrivateStatus, &state.InstanceID, &volumeJSON, &interfaceJSON)
	if err != nil {
		return state, err
	}
	if json.Unmarshal([]byte(volumeJSON), &state.VolumeIDs) != nil || json.Unmarshal([]byte(interfaceJSON), &state.NetworkInterfaceIDs) != nil {
		return state, cloudmodule.ErrDeploymentDestroyInvalid
	}
	rows, err := tx.QueryContext(ctx, `SELECT secret_ref FROM p2p_cloud_service_secret_bootstrap_approvals WHERE deployment_id=$1 AND status='ready' ORDER BY secret_ref FOR UPDATE`, deploymentID)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref string
		if err = rows.Scan(&ref); err != nil {
			return state, err
		}
		state.SecretRefs = append(state.SecretRefs, ref)
	}
	if err = rows.Err(); err != nil {
		return state, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_jobs WHERE deployment_id=$1 AND outcome_status='pending'`, deploymentID).Scan(&state.PendingJobs); err != nil {
		return state, err
	}
	return state, nil
}

func cloudDeploymentDestroyTarget(state cloudDeploymentDestroyState) (cloudcontracts.DeploymentDestroyTargetV1, error) {
	if state.Deployment.Revision <= 0 || state.PrivateStatus != state.Deployment.Resource || state.PendingJobs != 0 || state.Deployment.Execution != "finished" || !terminalDeploymentDestroyOutcome(state.Deployment.Outcome) {
		return cloudcontracts.DeploymentDestroyTargetV1{}, cloudmodule.ErrDeploymentDestroyConflict
	}
	target := cloudcontracts.DeploymentDestroyTargetV1{
		DeploymentID: state.Deployment.DeploymentID, DeploymentRevision: uint64(state.Deployment.Revision), PlanID: state.Deployment.PlanID,
		CloudConnectionID: state.Deployment.ConnectionID, ResourceStatus: state.Deployment.Resource, InstanceID: state.InstanceID,
		VolumeIDs: append([]string(nil), state.VolumeIDs...), NetworkInterfaceIDs: append([]string(nil), state.NetworkInterfaceIDs...), SecretRefs: append([]string(nil), state.SecretRefs...),
	}
	sort.Strings(target.VolumeIDs)
	sort.Strings(target.NetworkInterfaceIDs)
	sort.Strings(target.SecretRefs)
	return target, target.Validate()
}

func terminalDeploymentDestroyOutcome(outcome string) bool {
	switch outcome {
	case "succeeded", "failed", "canceled", "interrupted":
		return true
	default:
		return false
	}
}

func deploymentDestroyPrepareDigest(r cloudmodule.PrepareDeploymentDestroyRequest) string {
	sum := sha256.Sum256([]byte(r.OwnerMXID + "\x00" + r.DeploymentID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10)))
	return hex.EncodeToString(sum[:])
}

func deploymentDestroyApproveDigest(r cloudmodule.ApproveDeploymentDestroyRequest) (string, error) {
	payload, err := r.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	_, _ = sum.Write([]byte(r.OwnerMXID + "\x00" + r.DeploymentID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10) + "\x00"))
	_, _ = sum.Write(payload)
	_, _ = sum.Write([]byte("\x00" + r.Approval.Signature))
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func lockStoredCloudDeploymentDestroy(ctx context.Context, tx *sql.Tx, approvalID string) (storedCloudDeploymentDestroy, error) {
	var v storedCloudDeploymentDestroy
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,deployment_id,deployment_revision,plan_id,cloud_connection_id,resource_status,instance_id,volume_ids_json,network_interface_ids_json,secret_refs_json,signer_key_id,approval_json,signing_payload_cbor,prepare_deployment_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),signature,job_id,result_deployment_json,result_job_json,expires_at FROM p2p_cloud_deployment_destroy_approvals WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(
		&v.ApprovalID, &v.OwnerMXID, &v.DeploymentID, &v.DeploymentRevision, &v.PlanID, &v.ConnectionID, &v.ResourceStatus, &v.InstanceID,
		&v.VolumeIDsJSON, &v.NetworkInterfaceIDsJSON, &v.SecretRefsJSON, &v.SignerKeyID, &v.ApprovalJSON, &v.SigningPayload, &v.PrepareDeploymentJSON,
		&v.Status, &v.PrepareDigest, &v.ApproveDigest, &v.Signature, &v.JobID, &v.ResultDeploymentJSON, &v.ResultJobJSON, &v.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, cloudmodule.ErrDeploymentDestroyInvalid
	}
	return v, err
}

func loadDeploymentDestroyPrepareReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.PrepareDeploymentDestroyRequest) (cloudmodule.PrepareDeploymentDestroyResult, bool, error) {
	var approvalID string
	err := tx.QueryRowContext(ctx, `SELECT approval_id FROM p2p_cloud_deployment_destroy_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&approvalID)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareDeploymentDestroyResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareDeploymentDestroyResult{}, false, err
	}
	stored, err := lockStoredCloudDeploymentDestroy(ctx, tx, approvalID)
	if err != nil || stored.PrepareDigest != r.RequestDigest {
		return cloudmodule.PrepareDeploymentDestroyResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	var approval cloudcontracts.DeploymentDestroyApprovalV1
	var deployment cloudmodule.Deployment
	if decodeCloudContractJSON(stored.ApprovalJSON, &approval) != nil || json.Unmarshal([]byte(stored.PrepareDeploymentJSON), &deployment) != nil {
		return cloudmodule.PrepareDeploymentDestroyResult{}, true, cloudmodule.ErrDeploymentDestroyInvalid
	}
	return cloudmodule.PrepareDeploymentDestroyResult{Confirmation: cloudmodule.DeploymentDestroyConfirmation{Deployment: deployment, Approval: approval}}, true, nil
}

func loadDeploymentDestroyApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApproveDeploymentDestroyRequest) (cloudmodule.ApproveDeploymentDestroyResult, bool, error) {
	var digest, deploymentJSON, jobJSON, status string
	err := tx.QueryRowContext(ctx, `SELECT approve_request_digest,result_deployment_json,result_job_json,status FROM p2p_cloud_deployment_destroy_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&digest, &deploymentJSON, &jobJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveDeploymentDestroyResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveDeploymentDestroyResult{}, false, err
	}
	if digest != r.RequestDigest {
		return cloudmodule.ApproveDeploymentDestroyResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveDeploymentDestroyResult{}, true, cloudmodule.ErrDeploymentDestroyExpired
	}
	if status != "approved" {
		return cloudmodule.ApproveDeploymentDestroyResult{}, true, cloudmodule.ErrDeploymentDestroyConflict
	}
	var deployment cloudmodule.Deployment
	var job cloudmodule.Job
	if json.Unmarshal([]byte(deploymentJSON), &deployment) != nil || json.Unmarshal([]byte(jobJSON), &job) != nil {
		return cloudmodule.ApproveDeploymentDestroyResult{}, true, cloudmodule.ErrDeploymentDestroyInvalid
	}
	return cloudmodule.ApproveDeploymentDestroyResult{Deployment: deployment, Job: job}, true, nil
}
