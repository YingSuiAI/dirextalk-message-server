package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceDestroyStore = (*Store)(nil)

func (s *Store) ClaimServiceDestroy(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceDestroyClaim, found bool, err error) {
	if s == nil || s.db == nil {
		return claim, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("service destroy lease configuration is invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return claim, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" {
		return claim, false, errors.New("service destroy lease token is invalid")
	}
	var approvalJSON, signature, volumesJSON, interfacesJSON string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,outbox.attempts,
		service.service_id,service.deployment_id,service.revision,deployment.plan_id,deployment.cloud_connection_id,deployment.revision,
		approval.job_id,approval.approval_json,approval.signature,approval.instance_id,approval.volume_ids_json,approval.network_interface_ids_json,
		connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_services service ON service.service_id=outbox.aggregate_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_service_destroy_approvals approval ON approval.service_id=service.service_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_jobs job ON job.job_id=approval.job_id AND job.kind='destroy'
		WHERE outbox.kind=$1 AND outbox.aggregate_type='service' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2
		AND service.service_status='destroying' AND service.revision=approval.service_revision+1
		AND deployment.resource_status='destroying' AND deployment.revision=approval.deployment_revision+1
		AND approval.status='approved' AND approval.signature<>'' AND resource.resource_status='destroying'
		AND resource.instance_id=approval.instance_id AND resource.volume_ids_json=approval.volume_ids_json AND resource.network_interface_ids_json=approval.network_interface_ids_json
		AND connection.status='active' AND connection.region=broker.broker_region AND job.execution_status IN('queued','provisioning') AND job.outcome_status='pending'
		ORDER BY outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox SKIP LOCKED LIMIT 1`, runtime.ServiceDestroyRequested, now).Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.Attempt, &claim.ServiceID, &claim.DeploymentID, &claim.ServiceRevision,
		&claim.PlanID, &claim.ConnectionID, &claim.DeploymentRevision, &claim.JobID, &approvalJSON, &signature, &claim.Request.InstanceID, &volumesJSON, &interfacesJSON,
		&claim.Region, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceDestroyClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	claim.LeaseToken = token
	claim.Attempt++
	if err = json.Unmarshal([]byte(approvalJSON), &claim.Approval); err != nil {
		return claim, false, errors.New("service destroy approval is invalid")
	}
	claim.Approval.Signature = signature
	if err = json.Unmarshal([]byte(volumesJSON), &claim.Request.VolumeIDs); err != nil {
		return claim, false, errors.New("service destroy volumes are invalid")
	}
	if err = json.Unmarshal([]byte(interfacesJSON), &claim.Request.NetworkInterfaceIDs); err != nil {
		return claim, false, errors.New("service destroy interfaces are invalid")
	}
	claim.Request.Schema, claim.Request.ServiceID, claim.Request.DeploymentID = broker.DeploymentDestroySchema, claim.ServiceID, claim.DeploymentID
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
		return claim, false, ErrLeaseLost
	}
	claim.Command, err = prepareServiceDestroyCommand(ctx, tx, claim, now)
	if err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func prepareServiceDestroyCommand(ctx context.Context, tx *sql.Tx, claim runtime.ServiceDestroyClaim, now int64) (runtime.ServiceDestroyCommand, error) {
	digest, err := runtime.ServiceDestroyRequestDigest(claim.Request)
	if err != nil {
		return runtime.ServiceDestroyCommand{}, err
	}
	var c runtime.ServiceDestroyCommand
	var issued, expires int64
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_destroy_commands WHERE approval_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.Approval.ApprovalID, digest).Scan(&c.CommandID, &c.Attempt, &c.NodeKeyID, &c.ExpectedGeneration, &c.NodeCounter, &c.PayloadJSON, &c.PayloadSHA256, &c.RequestSHA256, &c.SignedEnvelope, &issued, &expires, &c.State)
	if err == nil {
		c.ServiceID, c.DeploymentID, c.ConnectionID, c.RequestDigest = claim.ServiceID, claim.DeploymentID, claim.ConnectionID, digest
		c.IssuedAt = time.UnixMilli(issued).UTC()
		c.ExpiresAt = time.UnixMilli(expires).UTC()
		return c, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_destroy_commands WHERE approval_id=$1 AND request_digest=$2`, claim.Approval.ApprovalID, digest).Scan(&attempt); err != nil {
		return c, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); err != nil {
		return c, err
	}
	c = runtime.ServiceDestroyCommand{CommandID: stableID("cloud_service_destroy_command_", claim.Approval.ApprovalID, fmt.Sprint(attempt)), ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, RequestDigest: digest, State: "allocated"}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_destroy_commands(command_id,approval_id,service_id,deployment_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,'deployment.destroy',$8,$9,$10,'allocated',$11,$11)`, c.CommandID, claim.Approval.ApprovalID, c.ServiceID, c.DeploymentID, c.ConnectionID, digest, attempt, c.NodeKeyID, c.ExpectedGeneration, c.NodeCounter, now)
	return c, err
}

func (s *Store) PersistServiceDestroyCommand(ctx context.Context, claim runtime.ServiceDestroyClaim, signed runtime.SignedServiceDestroyCommand) error {
	if validatePersistedServiceDestroy(claim, signed) != nil {
		return errors.New("service destroy signed command is invalid")
	}
	return s.withServiceDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		var state, payload, requestHash, envelope string
		var issued, expires int64
		if err := tx.QueryRowContext(ctx, `SELECT state,canonical_payload_json,request_sha256,signed_envelope_json,issued_at,expires_at FROM p2p_cloud_service_destroy_commands WHERE command_id=$1 FOR UPDATE`, claim.Command.CommandID).Scan(&state, &payload, &requestHash, &envelope, &issued, &expires); err != nil {
			return err
		}
		if state == "signed" || state == "indeterminate" {
			if payload == signed.PayloadJSON && requestHash == signed.RequestSHA256 && envelope == signed.EnvelopeJSON && issued == signed.IssuedAt.UnixMilli() && expires == signed.ExpiresAt.UnixMilli() {
				return nil
			}
			return errors.New("service destroy command already signed differently")
		}
		if state != "allocated" {
			return errors.New("service destroy command state is invalid")
		}
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(r)
	})
}

func (s *Store) MarkServiceDestroyStarted(ctx context.Context, claim runtime.ServiceDestroyClaim) error {
	return s.withServiceDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		_, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "provisioning", outcome: "pending", checkpoint: "destroy_provider_pending", stepStatus: "running", stepSummary: "The approved exact resource set is being destroyed; it remains billable until AWS read-back verifies absence."})
		return err
	})
}

func (s *Store) DeferServiceDestroy(ctx context.Context, claim runtime.ServiceDestroyClaim, code string, available time.Time) error {
	code = durableErrorCode(code, "service_destroy_retryable")
	return s.withServiceDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "destroy_readback_retry", errorCode: code, stepStatus: "queued", stepSummary: "Destruction is incomplete or its response was lost; the exact signed command will be retried and no resource is reported destroyed."}); err != nil {
			return err
		}
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state=CASE WHEN state='allocated' THEN state ELSE 'indeterminate' END,attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandResult); err != nil {
			return err
		}
		when := available.UTC().UnixMilli()
		if when < now {
			when = now
		}
		return releaseServiceDestroyOutbox(ctx, tx, claim, when, code)
	})
}

func (s *Store) CompleteServiceDestroy(ctx context.Context, claim runtime.ServiceDestroyClaim, result runtime.ServiceDestroyResult) error {
	return s.withServiceDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		signed, err := loadServiceDestroySigned(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		validated := claim
		validated.Command.SignedEnvelope = signed.EnvelopeJSON
		validated.Command.PayloadJSON = signed.PayloadJSON
		validated.Command.PayloadSHA256 = signed.PayloadSHA256
		validated.Command.RequestSHA256 = signed.RequestSHA256
		validated.Command.IssuedAt = signed.IssuedAt
		validated.Command.ExpiresAt = signed.ExpiresAt
		if validatePersistedServiceDestroy(validated, signed) != nil || runtime.ValidateServiceDestroyResult(validated, signed, result) != nil || validateDestroyReceipt(validated, signed, result) != nil {
			return errors.New("service destroy result is invalid")
		}
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state='accepted',receipt_json=$1,attempts=attempts+1,last_error_code='',updated_at=$2 WHERE command_id=$3 AND state IN('signed','indeterminate')`, result.ReceiptJSON, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(r); err != nil {
			return err
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='verified_destroyed',broker_receipt_json=$1,updated_at=$2 WHERE deployment_id=$3 AND cloud_connection_id=$4 AND instance_id=$5 AND resource_status='destroying'`, result.ReceiptJSON, now, claim.DeploymentID, claim.ConnectionID, claim.Request.InstanceID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(r); err != nil {
			return err
		}
		if _, err = transitionServiceDestroyStatus(ctx, tx, claim.ServiceID, now, "destroyed"); err != nil {
			return err
		}
		if _, err = transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, "finished", "succeeded", "verified_destroyed"); err != nil {
			return err
		}
		if _, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: "verified_destroyed", stepStatus: "finished", stepSummary: "AWS read-back verified that the approved instance, interfaces, and volumes no longer exist."}); err != nil {
			return err
		}
		return completeServiceDestroyOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) FailServiceDestroy(ctx context.Context, claim runtime.ServiceDestroyClaim, code string) error {
	code = durableErrorCode(code, "service_destroy_failed")
	return s.withServiceDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state='failed',attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandResult); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='blocked',updated_at=$1 WHERE deployment_id=$2 AND resource_status='destroying'`, now, claim.DeploymentID); err != nil {
			return err
		}
		if _, err := transitionServiceDestroyStatus(ctx, tx, claim.ServiceID, now, "degraded"); err != nil {
			return err
		}
		if _, err := transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, "finished", "succeeded", "blocked"); err != nil {
			return err
		}
		if _, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "destroy_blocked", errorCode: code, stepStatus: "failed", stepSummary: "Destruction could not be verified. Resources remain tracked as blocked and may still incur charges."}); err != nil {
			return err
		}
		return completeServiceDestroyOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) withServiceDestroyClaim(ctx context.Context, claim runtime.ServiceDestroyClaim, run func(*sql.Tx, int64) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	var token, kind, aggregateType, aggregateID, serviceID, deploymentID, planID, connectionID string
	var serviceStatus, deploymentResource, privateResource, instanceID, approvalID, jobID, nodeKeyID, requestDigest string
	var until, done, serviceRevision, deploymentRevision, generation, nodeCounter int64
	err = tx.QueryRowContext(ctx, `SELECT outbox.lease_token,outbox.lease_until,outbox.completed_at,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,
		service.service_id,service.deployment_id,service.revision,service.service_status,deployment.plan_id,deployment.cloud_connection_id,deployment.revision,deployment.resource_status,
		resource.resource_status,resource.instance_id,approval.approval_id,approval.job_id,broker.node_key_id,broker.connection_generation,command.node_counter,command.request_digest
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_services service ON service.service_id=outbox.aggregate_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_service_destroy_approvals approval ON approval.service_id=service.service_id AND approval.status='approved'
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_service_destroy_commands command ON command.command_id=$2 AND command.approval_id=approval.approval_id
		WHERE outbox.outbox_id=$1 FOR UPDATE OF outbox`, claim.OutboxID, claim.Command.CommandID).Scan(
		&token, &until, &done, &kind, &aggregateType, &aggregateID, &serviceID, &deploymentID, &serviceRevision, &serviceStatus, &planID, &connectionID, &deploymentRevision, &deploymentResource,
		&privateResource, &instanceID, &approvalID, &jobID, &nodeKeyID, &generation, &nodeCounter, &requestDigest)
	if err != nil {
		return err
	}
	if token != claim.LeaseToken || until <= now || done != 0 || kind != claim.Kind || aggregateType != claim.AggregateType || aggregateID != claim.AggregateID ||
		serviceID != claim.ServiceID || deploymentID != claim.DeploymentID || planID != claim.PlanID || connectionID != claim.ConnectionID ||
		serviceRevision != claim.ServiceRevision || deploymentRevision != claim.DeploymentRevision || serviceStatus != "destroying" || deploymentResource != "destroying" || privateResource != "destroying" ||
		instanceID != claim.Request.InstanceID || approvalID != claim.Approval.ApprovalID || jobID != claim.JobID || nodeKeyID != claim.NodeKeyID || generation != claim.ExpectedGeneration ||
		nodeCounter != claim.Command.NodeCounter || requestDigest != claim.Command.RequestDigest {
		return ErrLeaseLost
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func validatePersistedServiceDestroy(claim runtime.ServiceDestroyClaim, signed runtime.SignedServiceDestroyCommand) error {
	if runtime.ValidateServiceDestroyClaim(claim) != nil || runtime.ValidateSignedServiceDestroyCommand(signed) != nil {
		return errors.New("invalid service destroy claim")
	}
	c, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return err
	}
	if err = c.ValidateBinding(broker.DeploymentDestroyCommandBinding{ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: claim.Request, ApprovalProof: claim.Approval}); err != nil || c.PayloadSHA256 != signed.PayloadSHA256 || c.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("service destroy command binding is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(c.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("service destroy payload is invalid")
	}
	return nil
}

func validateDestroyReceipt(claim runtime.ServiceDestroyClaim, signed runtime.SignedServiceDestroyCommand, result runtime.ServiceDestroyResult) error {
	command, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(result.ReceiptJSON))
	decoder.DisallowUnknownFields()
	var receipt broker.DeploymentCommandReceipt
	if err = decoder.Decode(&receipt); err != nil {
		return err
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("service destroy receipt contains trailing JSON")
	}
	return broker.ValidateDeploymentDestroyResult(command, broker.DeploymentDestroyResult{
		Schema: broker.DeploymentDestroyResultSchema, Status: result.Status, Receipt: receipt,
		Deployment: broker.DeploymentDestroyEvidence{DeploymentID: result.DeploymentID, InstanceID: result.InstanceID, VolumeIDs: result.VolumeIDs, NetworkInterfaceIDs: result.NetworkInterfaceIDs},
	})
}

func loadServiceDestroySigned(ctx context.Context, tx *sql.Tx, id string) (runtime.SignedServiceDestroyCommand, error) {
	var s runtime.SignedServiceDestroyCommand
	var issued, expires int64
	err := tx.QueryRowContext(ctx, `SELECT signed_envelope_json,canonical_payload_json,payload_sha256,request_sha256,issued_at,expires_at FROM p2p_cloud_service_destroy_commands WHERE command_id=$1 AND state IN('signed','indeterminate') FOR UPDATE`, id).Scan(&s.EnvelopeJSON, &s.PayloadJSON, &s.PayloadSHA256, &s.RequestSHA256, &issued, &expires)
	s.IssuedAt = time.UnixMilli(issued).UTC()
	s.ExpiresAt = time.UnixMilli(expires).UTC()
	return s, err
}

func transitionServiceDestroyStatus(ctx context.Context, tx *sql.Tx, id string, now int64, status string) (cloudmodule.Service, error) {
	var v cloudmodule.Service
	err := tx.QueryRowContext(ctx, `SELECT service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at FROM p2p_cloud_services WHERE service_id=$1 FOR UPDATE`, id).Scan(&v.ServiceID, &v.DeploymentID, &v.RecipeID, &v.Name, &v.Status, &v.Integration, &v.Revision, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return v, err
	}
	if v.Status == status {
		return v, nil
	}
	old := v.Revision
	v.Status = status
	v.Revision++
	v.UpdatedAt = now
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status=$1,revision=$2,updated_at=$3 WHERE service_id=$4 AND revision=$5`, status, v.Revision, now, id, old)
	if err != nil {
		return v, err
	}
	if err = requireOneAffected(r); err != nil {
		return v, err
	}
	err = writeEventAndProjection(ctx, tx, stableID("cloud_event_", id, fmt.Sprint(v.Revision), status), "cloud.service.changed", "service", id, v.Revision, map[string]any{"service_id": v.ServiceID, "deployment_id": v.DeploymentID, "recipe_id": v.RecipeID, "name": v.Name, "service_status": v.Status, "integration_status": v.Integration, "revision": v.Revision, "created_at": v.CreatedAt, "updated_at": v.UpdatedAt}, now)
	return v, err
}

func releaseServiceDestroyOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ServiceDestroyClaim, at int64, code string) error {
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,available_at=$1,last_error_code=$2 WHERE outbox_id=$3 AND lease_token=$4 AND completed_at=0`, at, code, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(r)
}
func completeServiceDestroyOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ServiceDestroyClaim, now int64) error {
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,delivered_at=$1,available_at=$1,last_error_code='' WHERE outbox_id=$2 AND lease_token=$3 AND completed_at=0`, now, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(r)
}
