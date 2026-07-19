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

	"github.com/lib/pq"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.DeploymentDestroyStore = (*Store)(nil)

func (s *Store) ClaimDeploymentDestroy(ctx context.Context, workerID string, lease time.Duration) (claim runtime.DeploymentDestroyClaim, found bool, err error) {
	if s == nil || s.db == nil {
		return claim, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("deployment destroy lease configuration is invalid")
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
		return claim, false, errors.New("deployment destroy lease token is invalid")
	}
	var approvalJSON, signature, volumesJSON, interfacesJSON, secretRefsJSON, outboxPayload string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,outbox.attempts,outbox.payload_json,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.revision,
		approval.job_id,approval.approval_json,approval.signature,approval.instance_id,approval.volume_ids_json,approval.network_interface_ids_json,approval.secret_refs_json,
		connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=outbox.aggregate_id
		JOIN p2p_cloud_deployment_destroy_approvals approval ON approval.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_jobs job ON job.job_id=approval.job_id AND job.kind='destroy'
		WHERE outbox.kind=$1 AND outbox.aggregate_type='deployment' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2
		AND deployment.resource_status='destroying' AND deployment.revision=approval.deployment_revision+1
		AND approval.status='approved' AND approval.signature<>'' AND resource.resource_status='destroying'
		AND resource.instance_id=approval.instance_id AND resource.volume_ids_json=approval.volume_ids_json AND resource.network_interface_ids_json=approval.network_interface_ids_json
		AND NOT EXISTS (SELECT 1 FROM p2p_cloud_services service WHERE service.deployment_id=deployment.deployment_id)
		AND connection.status='active' AND connection.region=broker.broker_region AND job.execution_status IN('queued','provisioning') AND job.outcome_status='pending'
		ORDER BY outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox SKIP LOCKED LIMIT 1`, runtime.DeploymentDestroyRequested, now).Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.Attempt, &outboxPayload,
		&claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.DeploymentExecution, &claim.DeploymentOutcome, &claim.DeploymentRevision,
		&claim.JobID, &approvalJSON, &signature, &claim.Request.InstanceID, &volumesJSON, &interfacesJSON, &secretRefsJSON,
		&claim.Region, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.DeploymentDestroyClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	claim.LeaseToken = token
	claim.Attempt++
	if json.Unmarshal([]byte(approvalJSON), &claim.Approval) != nil {
		return claim, false, errors.New("deployment destroy approval is invalid")
	}
	claim.Approval.Signature = signature
	if json.Unmarshal([]byte(volumesJSON), &claim.Request.VolumeIDs) != nil || json.Unmarshal([]byte(interfacesJSON), &claim.Request.NetworkInterfaceIDs) != nil || json.Unmarshal([]byte(secretRefsJSON), &claim.Request.SecretRefs) != nil {
		return claim, false, errors.New("deployment destroy resource set is invalid")
	}
	var privateOutbox struct {
		DeploymentID string   `json:"deployment_id"`
		SecretRefs   []string `json:"secret_refs,omitempty"`
	}
	decoder := json.NewDecoder(strings.NewReader(outboxPayload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&privateOutbox) != nil || privateOutbox.DeploymentID != claim.DeploymentID || !sameDestroyStrings(privateOutbox.SecretRefs, claim.Request.SecretRefs) || !sameDestroyStrings(claim.Approval.SecretRefs, claim.Request.SecretRefs) {
		return claim, false, errors.New("deployment destroy private outbox binding is invalid")
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return claim, false, errors.New("deployment destroy private outbox contains trailing JSON")
	}
	claim.Request.Schema, claim.Request.DeploymentID = broker.DeploymentDestroySchema, claim.DeploymentID
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
		return claim, false, ErrLeaseLost
	}
	claim.Command, err = prepareDeploymentDestroyCommand(ctx, tx, claim, now)
	if err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func prepareDeploymentDestroyCommand(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentDestroyClaim, now int64) (runtime.ServiceDestroyCommand, error) {
	digest, err := runtime.ServiceDestroyRequestDigest(claim.Request)
	if err != nil {
		return runtime.ServiceDestroyCommand{}, err
	}
	var command runtime.ServiceDestroyCommand
	var issued, expires int64
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_destroy_commands WHERE approval_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.Approval.ApprovalID, digest).Scan(&command.CommandID, &command.Attempt, &command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON, &command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issued, &expires, &command.State)
	if err == nil {
		command.DeploymentID, command.ConnectionID, command.RequestDigest = claim.DeploymentID, claim.ConnectionID, digest
		command.IssuedAt, command.ExpiresAt = time.UnixMilli(issued).UTC(), time.UnixMilli(expires).UTC()
		return command, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return command, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_destroy_commands WHERE approval_id=$1 AND request_digest=$2`, claim.Approval.ApprovalID, digest).Scan(&attempt); err != nil {
		return command, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); err != nil {
		return command, err
	}
	command = runtime.ServiceDestroyCommand{CommandID: stableID("cloud_deployment_destroy_command_", claim.Approval.ApprovalID, fmt.Sprint(attempt)), DeploymentID: claim.DeploymentID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, RequestDigest: digest, State: "allocated"}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_destroy_commands(command_id,approval_id,service_id,deployment_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,'',$3,$4,$5,$6,'deployment.destroy',$7,$8,$9,'allocated',$10,$10)`, command.CommandID, claim.Approval.ApprovalID, command.DeploymentID, command.ConnectionID, digest, attempt, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
	return command, err
}

func (s *Store) PersistDeploymentDestroyCommand(ctx context.Context, claim runtime.DeploymentDestroyClaim, signed runtime.SignedServiceDestroyCommand) error {
	if validatePersistedDeploymentDestroy(claim, signed) != nil {
		return errors.New("deployment destroy signed command is invalid")
	}
	return s.withDeploymentDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		var state, payload, requestHash, envelope string
		var issued, expires int64
		if err := tx.QueryRowContext(ctx, `SELECT state,canonical_payload_json,request_sha256,signed_envelope_json,issued_at,expires_at FROM p2p_cloud_service_destroy_commands WHERE command_id=$1 FOR UPDATE`, claim.Command.CommandID).Scan(&state, &payload, &requestHash, &envelope, &issued, &expires); err != nil {
			return err
		}
		if state == "signed" || state == "indeterminate" {
			if payload == signed.PayloadJSON && requestHash == signed.RequestSHA256 && envelope == signed.EnvelopeJSON && issued == signed.IssuedAt.UnixMilli() && expires == signed.ExpiresAt.UnixMilli() {
				return nil
			}
			return errors.New("deployment destroy command already signed differently")
		}
		if state != "allocated" {
			return errors.New("deployment destroy command state is invalid")
		}
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (s *Store) MarkDeploymentDestroyStarted(ctx context.Context, claim runtime.DeploymentDestroyClaim) error {
	return s.withDeploymentDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		_, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "provisioning", outcome: "pending", checkpoint: "destroy_provider_pending", stepStatus: "running", stepSummary: "The approved exact resource set is being destroyed; it remains billable until AWS read-back verifies absence."})
		return err
	})
}

func (s *Store) DeferDeploymentDestroy(ctx context.Context, claim runtime.DeploymentDestroyClaim, code string, available time.Time) error {
	code = durableErrorCode(code, "deployment_destroy_retryable")
	return s.withDeploymentDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "destroy_readback_retry", errorCode: code, stepStatus: "queued", stepSummary: "Destruction is incomplete or its response was lost; the exact signed command will be retried and no resource is reported destroyed."}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state=CASE WHEN state='allocated' THEN state ELSE 'indeterminate' END,attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		when := available.UTC().UnixMilli()
		if when < now {
			when = now
		}
		return releaseDeploymentDestroyOutbox(ctx, tx, claim, when, code)
	})
}

func (s *Store) CompleteDeploymentDestroy(ctx context.Context, claim runtime.DeploymentDestroyClaim, result runtime.ServiceDestroyResult) error {
	return s.withDeploymentDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		signed, err := loadServiceDestroySigned(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		validated := claim
		validated.Command.SignedEnvelope, validated.Command.PayloadJSON, validated.Command.PayloadSHA256, validated.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		validated.Command.IssuedAt, validated.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
		if validatePersistedDeploymentDestroy(validated, signed) != nil || runtime.ValidateDeploymentDestroyResult(validated, signed, result) != nil || validateDeploymentDestroyReceipt(validated, signed, result) != nil {
			return errors.New("deployment destroy result is invalid")
		}
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state='accepted',receipt_json=$1,attempts=attempts+1,last_error_code='',updated_at=$2 WHERE command_id=$3 AND state IN('signed','indeterminate')`, result.ReceiptJSON, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(commandResult); err != nil {
			return err
		}
		resourceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='verified_destroyed',broker_receipt_json=$1,updated_at=$2 WHERE deployment_id=$3 AND cloud_connection_id=$4 AND instance_id=$5 AND resource_status='destroying'`, result.ReceiptJSON, now, claim.DeploymentID, claim.ConnectionID, claim.Request.InstanceID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(resourceResult); err != nil {
			return err
		}
		if len(claim.Request.SecretRefs) > 0 {
			if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_bootstrap_approvals SET status='destroyed',lease_owner='',lease_token='',lease_until=0,last_error_code='',revision=revision+1,updated_at=$1 WHERE deployment_id=$2 AND status='ready' AND secret_ref=ANY($3)`, now, claim.DeploymentID, pq.Array(claim.Request.SecretRefs)); err != nil {
				return err
			}
		}
		if _, err = transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, claim.DeploymentExecution, claim.DeploymentOutcome, "verified_destroyed"); err != nil {
			return err
		}
		if _, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: "verified_destroyed", stepStatus: "finished", stepSummary: serviceDestroyCompletionSummary(claim.Request.SecretRefs)}); err != nil {
			return err
		}
		return completeDeploymentDestroyOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) FailDeploymentDestroy(ctx context.Context, claim runtime.DeploymentDestroyClaim, code string) error {
	code = durableErrorCode(code, "deployment_destroy_failed")
	return s.withDeploymentDestroyClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_destroy_commands SET state='failed',attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(commandResult); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='blocked',updated_at=$1 WHERE deployment_id=$2 AND resource_status='destroying'`, now, claim.DeploymentID); err != nil {
			return err
		}
		if _, err = transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, claim.DeploymentExecution, claim.DeploymentOutcome, "blocked"); err != nil {
			return err
		}
		if _, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "destroy", "destroy", now, researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "destroy_blocked", errorCode: code, stepStatus: "failed", stepSummary: "Destruction could not be verified. Resources remain tracked as blocked and may still incur charges."}); err != nil {
			return err
		}
		return completeDeploymentDestroyOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) withDeploymentDestroyClaim(ctx context.Context, claim runtime.DeploymentDestroyClaim, run func(*sql.Tx, int64) error) (err error) {
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
	var token, kind, aggregateType, aggregateID, deploymentID, planID, connectionID string
	var deploymentExecution, deploymentOutcome, deploymentResource, privateResource, instanceID, approvalID, jobID, nodeKeyID, requestDigest string
	var until, done, deploymentRevision, generation, nodeCounter int64
	err = tx.QueryRowContext(ctx, `SELECT outbox.lease_token,outbox.lease_until,outbox.completed_at,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.revision,deployment.resource_status,
		resource.resource_status,resource.instance_id,approval.approval_id,approval.job_id,broker.node_key_id,broker.connection_generation,command.node_counter,command.request_digest
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=outbox.aggregate_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_deployment_destroy_approvals approval ON approval.deployment_id=deployment.deployment_id AND approval.status='approved'
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_service_destroy_commands command ON command.command_id=$2 AND command.approval_id=approval.approval_id
		WHERE outbox.outbox_id=$1 AND NOT EXISTS (SELECT 1 FROM p2p_cloud_services service WHERE service.deployment_id=deployment.deployment_id)
		FOR UPDATE OF outbox`, claim.OutboxID, claim.Command.CommandID).Scan(
		&token, &until, &done, &kind, &aggregateType, &aggregateID,
		&deploymentID, &planID, &connectionID, &deploymentExecution, &deploymentOutcome, &deploymentRevision, &deploymentResource,
		&privateResource, &instanceID, &approvalID, &jobID, &nodeKeyID, &generation, &nodeCounter, &requestDigest)
	if err != nil {
		return err
	}
	if token != claim.LeaseToken || until <= now || done != 0 || kind != claim.Kind || aggregateType != claim.AggregateType || aggregateID != claim.AggregateID ||
		deploymentID != claim.DeploymentID || planID != claim.PlanID || connectionID != claim.ConnectionID || deploymentExecution != claim.DeploymentExecution || deploymentOutcome != claim.DeploymentOutcome ||
		deploymentRevision != claim.DeploymentRevision || deploymentResource != "destroying" || privateResource != "destroying" || instanceID != claim.Request.InstanceID ||
		approvalID != claim.Approval.ApprovalID || jobID != claim.JobID || nodeKeyID != claim.NodeKeyID || generation != claim.ExpectedGeneration || nodeCounter != claim.Command.NodeCounter || requestDigest != claim.Command.RequestDigest {
		return ErrLeaseLost
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func validatePersistedDeploymentDestroy(claim runtime.DeploymentDestroyClaim, signed runtime.SignedServiceDestroyCommand) error {
	if runtime.ValidateDeploymentDestroyClaim(claim) != nil || runtime.ValidateSignedServiceDestroyCommand(signed) != nil {
		return errors.New("invalid deployment destroy claim")
	}
	command, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return err
	}
	if err = command.ValidateBinding(broker.DeploymentDestroyCommandBinding{ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: claim.Request, DeploymentApprovalProof: claim.Approval}); err != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("deployment destroy command binding is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("deployment destroy payload is invalid")
	}
	return nil
}

func validateDeploymentDestroyReceipt(claim runtime.DeploymentDestroyClaim, signed runtime.SignedServiceDestroyCommand, result runtime.ServiceDestroyResult) error {
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
		return errors.New("deployment destroy receipt contains trailing JSON")
	}
	return broker.ValidateDeploymentDestroyResult(command, broker.DeploymentDestroyResult{Schema: broker.DeploymentDestroyResultSchema, Status: result.Status, Receipt: receipt, Deployment: broker.DeploymentDestroyEvidence{DeploymentID: result.DeploymentID, InstanceID: result.InstanceID, VolumeIDs: result.VolumeIDs, NetworkInterfaceIDs: result.NetworkInterfaceIDs, SecretRefs: result.SecretRefs}})
}

func releaseDeploymentDestroyOutbox(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentDestroyClaim, at int64, code string) error {
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,available_at=$1,last_error_code=$2 WHERE outbox_id=$3 AND lease_token=$4 AND completed_at=0`, at, code, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func completeDeploymentDestroyOutbox(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentDestroyClaim, now int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,delivered_at=$1,available_at=$1,last_error_code='' WHERE outbox_id=$2 AND lease_token=$3 AND completed_at=0`, now, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}
