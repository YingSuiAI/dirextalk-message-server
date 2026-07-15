package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	"io"
	"strings"
	"time"
)

var _ runtime.ServiceBackupStore = (*Store)(nil)

func (s *Store) ClaimServiceBackup(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceBackupClaim, found bool, err error) {
	if s == nil || s.db == nil {
		return claim, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("service backup lease configuration is invalid")
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
		return claim, false, errors.New("service backup lease token is invalid")
	}
	var approvalJSON, signature, volumesJSON string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,outbox.attempts,backup.backup_id,backup.service_id,backup.deployment_id,backup.service_revision,backup.deployment_revision,backup.plan_id,backup.cloud_connection_id,backup.job_id,approval.approval_json,approval.signature,backup.instance_id,backup.volume_ids_json,connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_backups backup ON backup.backup_id=outbox.aggregate_id JOIN p2p_cloud_service_backup_approvals approval ON approval.approval_id=backup.approval_id JOIN p2p_cloud_services service ON service.service_id=backup.service_id JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=backup.deployment_id JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=backup.deployment_id JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=backup.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=backup.cloud_connection_id JOIN p2p_cloud_jobs job ON job.job_id=backup.job_id AND job.kind='backup' WHERE outbox.kind=$1 AND outbox.aggregate_type='service_backup' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2 AND backup.backup_status IN('queued','running') AND approval.status='approved' AND approval.signature<>'' AND service.revision=backup.service_revision AND deployment.revision=backup.deployment_revision AND deployment.resource_status IN('active','retained_tracked') AND resource.resource_status=deployment.resource_status AND resource.instance_id=backup.instance_id AND resource.volume_ids_json=backup.volume_ids_json AND connection.status='active' AND connection.region=broker.broker_region AND job.execution_status IN('queued','provisioning') AND job.outcome_status='pending' ORDER BY outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox,backup SKIP LOCKED LIMIT 1`, runtime.ServiceBackupRequested, now).Scan(&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.Attempt, &claim.BackupID, &claim.ServiceID, &claim.DeploymentID, &claim.ServiceRevision, &claim.DeploymentRevision, &claim.PlanID, &claim.ConnectionID, &claim.JobID, &approvalJSON, &signature, &claim.Request.InstanceID, &volumesJSON, &claim.Region, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceBackupClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	claim.LeaseToken = token
	claim.Attempt++
	if json.Unmarshal([]byte(approvalJSON), &claim.Approval) != nil {
		return claim, false, errors.New("service backup approval is invalid")
	}
	claim.Approval.Signature = signature
	if json.Unmarshal([]byte(volumesJSON), &claim.Request.VolumeIDs) != nil {
		return claim, false, errors.New("service backup volumes are invalid")
	}
	claim.Request.Schema = broker.ServiceBackupSchema
	claim.Request.BackupID = claim.BackupID
	claim.Request.ServiceID = claim.ServiceID
	claim.Request.DeploymentID = claim.DeploymentID
	claim.Request.RetentionPolicy = claim.Approval.RetentionPolicy
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if e != nil {
		return claim, false, e
	}
	if e = requireOneAffected(r); e != nil {
		return claim, false, ErrLeaseLost
	}
	claim.Command, e = prepareServiceBackupCommand(ctx, tx, claim, now)
	if e != nil {
		return claim, false, e
	}
	if e = tx.Commit(); e != nil {
		return claim, false, e
	}
	return claim, true, nil
}
func prepareServiceBackupCommand(ctx context.Context, tx *sql.Tx, claim runtime.ServiceBackupClaim, now int64) (runtime.ServiceBackupCommand, error) {
	digest, e := runtime.ServiceBackupRequestDigest(claim.Request)
	if e != nil {
		return runtime.ServiceBackupCommand{}, e
	}
	var c runtime.ServiceBackupCommand
	var issued, expires int64
	e = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_backup_commands WHERE backup_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.BackupID, digest).Scan(&c.CommandID, &c.Attempt, &c.NodeKeyID, &c.ExpectedGeneration, &c.NodeCounter, &c.PayloadJSON, &c.PayloadSHA256, &c.RequestSHA256, &c.SignedEnvelope, &issued, &expires, &c.State)
	if e == nil {
		c.BackupID, c.ServiceID, c.DeploymentID, c.ConnectionID, c.RequestDigest = claim.BackupID, claim.ServiceID, claim.DeploymentID, claim.ConnectionID, digest
		c.IssuedAt = time.UnixMilli(issued).UTC()
		c.ExpiresAt = time.UnixMilli(expires).UTC()
		return c, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return c, e
	}
	var attempt int
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_backup_commands WHERE backup_id=$1 AND request_digest=$2`, claim.BackupID, digest).Scan(&attempt); e != nil {
		return c, e
	}
	var counter int64
	if e = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); e != nil {
		return c, e
	}
	c = runtime.ServiceBackupCommand{CommandID: stableID("cloud_service_backup_command_", claim.BackupID, fmt.Sprint(attempt)), BackupID: claim.BackupID, ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, RequestDigest: digest, State: "allocated"}
	_, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_backup_commands(command_id,backup_id,approval_id,service_id,deployment_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,'service.backup',$9,$10,$11,'allocated',$12,$12)`, c.CommandID, c.BackupID, claim.Approval.ApprovalID, c.ServiceID, c.DeploymentID, c.ConnectionID, digest, attempt, c.NodeKeyID, c.ExpectedGeneration, c.NodeCounter, now)
	return c, e
}
func (s *Store) PersistServiceBackupCommand(ctx context.Context, claim runtime.ServiceBackupClaim, signed runtime.SignedServiceBackupCommand) error {
	if validatePersistedServiceBackup(claim, signed) != nil {
		return errors.New("service backup signed command is invalid")
	}
	return s.withServiceBackupClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		var state, payload, requestHash, envelope string
		var issued, expires int64
		if e := tx.QueryRowContext(ctx, `SELECT state,canonical_payload_json,request_sha256,signed_envelope_json,issued_at,expires_at FROM p2p_cloud_service_backup_commands WHERE command_id=$1 FOR UPDATE`, claim.Command.CommandID).Scan(&state, &payload, &requestHash, &envelope, &issued, &expires); e != nil {
			return e
		}
		if state == "signed" || state == "indeterminate" {
			if payload == signed.PayloadJSON && requestHash == signed.RequestSHA256 && envelope == signed.EnvelopeJSON && issued == signed.IssuedAt.UnixMilli() && expires == signed.ExpiresAt.UnixMilli() {
				return nil
			}
			return errors.New("service backup command already signed differently")
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}
func (s *Store) MarkServiceBackupStarted(ctx context.Context, c runtime.ServiceBackupClaim) error {
	return s.withServiceBackupClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		result, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backups SET backup_status='running',revision=revision+1,updated_at=$1 WHERE backup_id=$2 AND backup_status IN('queued','running')`, now, c.BackupID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(result); e != nil {
			return e
		}
		_, e = transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "backup", "backup", now, researchJobTransition{execution: "provisioning", outcome: "pending", checkpoint: "backup_provider_pending", stepStatus: "running", stepSummary: "The approved exact volume set is being snapshotted; the service and EC2 resources remain active and billable."})
		return e
	})
}
func (s *Store) DeferServiceBackup(ctx context.Context, c runtime.ServiceBackupClaim, code string, available time.Time) error {
	code = durableErrorCode(code, "service_backup_retryable")
	return s.withServiceBackupClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		if _, e := transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "backup", "backup", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "backup_readback_retry", errorCode: code, stepStatus: "queued", stepSummary: "Snapshot creation is incomplete or its response was lost; the exact signed command will be retried."}); e != nil {
			return e
		}
		commandResult, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_commands SET state=CASE WHEN state='allocated' THEN state ELSE 'indeterminate' END,attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, c.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(commandResult); e != nil {
			return e
		}
		when := available.UTC().UnixMilli()
		if when < now {
			when = now
		}
		return releaseServiceBackupOutbox(ctx, tx, c, when, code)
	})
}
func (s *Store) CompleteServiceBackup(ctx context.Context, c runtime.ServiceBackupClaim, result runtime.ServiceBackupResult) error {
	return s.withServiceBackupClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		signed, e := loadServiceBackupSigned(ctx, tx, c.Command.CommandID)
		if e != nil {
			return e
		}
		validated := c
		validated.Command.SignedEnvelope = signed.EnvelopeJSON
		validated.Command.PayloadJSON = signed.PayloadJSON
		validated.Command.PayloadSHA256 = signed.PayloadSHA256
		validated.Command.RequestSHA256 = signed.RequestSHA256
		validated.Command.IssuedAt = signed.IssuedAt
		validated.Command.ExpiresAt = signed.ExpiresAt
		if validatePersistedServiceBackup(validated, signed) != nil || runtime.ValidateServiceBackupResult(validated, signed, result) != nil || validateBackupReceipt(validated, signed, result) != nil {
			return errors.New("service backup result is invalid")
		}
		snapshotsJSON, _ := json.Marshal(result.Snapshots)
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_commands SET state='accepted',receipt_json=$1,attempts=attempts+1,last_error_code='',updated_at=$2 WHERE command_id=$3 AND state IN('signed','indeterminate')`, result.ReceiptJSON, now, c.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backups SET backup_status='available',image_id=$1,snapshots_json=$2,receipt_json=$3,revision=revision+1,last_error_code='',updated_at=$4 WHERE backup_id=$5 AND backup_status IN('queued','running')`, result.ImageID, string(snapshotsJSON), result.ReceiptJSON, now, c.BackupID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		if _, e = transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "backup", "backup", now, researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: "backup_available", stepStatus: "finished", stepSummary: "AWS read-back verified the retained AMI and every encrypted EBS snapshot."}); e != nil {
			return e
		}
		return completeServiceBackupOutbox(ctx, tx, c, now)
	})
}
func (s *Store) FailServiceBackup(ctx context.Context, c runtime.ServiceBackupClaim, code string) error {
	code = durableErrorCode(code, "service_backup_failed")
	return s.withServiceBackupClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		commandResult, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_commands SET state='failed',attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, c.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(commandResult); e != nil {
			return e
		}
		backupResult, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backups SET backup_status='failed',revision=revision+1,last_error_code=$1,updated_at=$2 WHERE backup_id=$3 AND backup_status IN('queued','running')`, code, now, c.BackupID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(backupResult); e != nil {
			return e
		}
		if _, e = transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "backup", "backup", now, researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "backup_failed", errorCode: code, stepStatus: "failed", stepSummary: "The backup could not be independently verified. The service and EC2 resources remain unchanged and billable."}); e != nil {
			return e
		}
		return completeServiceBackupOutbox(ctx, tx, c, now)
	})
}

func (s *Store) withServiceBackupClaim(ctx context.Context, c runtime.ServiceBackupClaim, run func(*sql.Tx, int64) error) (err error) {
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
	var token, kind, aggregateType, aggregateID, backupID, serviceID, deploymentID, planID, connectionID, status, approvalID, jobID, nodeKeyID, requestDigest string
	var publicResource, privateResource, instanceID, volumeIDsJSON string
	var until, done, serviceRev, deploymentRev, currentServiceRev, currentDeploymentRev, generation, counter int64
	err = tx.QueryRowContext(ctx, `SELECT outbox.lease_token,outbox.lease_until,outbox.completed_at,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,backup.backup_id,backup.service_id,backup.deployment_id,backup.service_revision,backup.deployment_revision,backup.plan_id,backup.cloud_connection_id,backup.backup_status,backup.approval_id,backup.job_id,broker.node_key_id,broker.connection_generation,command.node_counter,command.request_digest,service.revision,deployment.revision,deployment.resource_status,resource.resource_status,resource.instance_id,resource.volume_ids_json FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_backups backup ON backup.backup_id=outbox.aggregate_id JOIN p2p_cloud_services service ON service.service_id=backup.service_id JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=backup.deployment_id JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=backup.deployment_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=backup.cloud_connection_id JOIN p2p_cloud_service_backup_commands command ON command.command_id=$2 AND command.backup_id=backup.backup_id WHERE outbox.outbox_id=$1 FOR UPDATE OF outbox,backup`, c.OutboxID, c.Command.CommandID).Scan(&token, &until, &done, &kind, &aggregateType, &aggregateID, &backupID, &serviceID, &deploymentID, &serviceRev, &deploymentRev, &planID, &connectionID, &status, &approvalID, &jobID, &nodeKeyID, &generation, &counter, &requestDigest, &currentServiceRev, &currentDeploymentRev, &publicResource, &privateResource, &instanceID, &volumeIDsJSON)
	if err != nil {
		return err
	}
	expectedVolumes, marshalErr := json.Marshal(c.Request.VolumeIDs)
	if marshalErr != nil {
		return ErrLeaseLost
	}
	if token != c.LeaseToken || until <= now || done != 0 || kind != c.Kind || aggregateType != c.AggregateType || aggregateID != c.AggregateID || backupID != c.BackupID || serviceID != c.ServiceID || deploymentID != c.DeploymentID || serviceRev != c.ServiceRevision || deploymentRev != c.DeploymentRevision || currentServiceRev != c.ServiceRevision || currentDeploymentRev != c.DeploymentRevision || planID != c.PlanID || connectionID != c.ConnectionID || (status != "queued" && status != "running") || (publicResource != "active" && publicResource != "retained_tracked") || privateResource != publicResource || instanceID != c.Request.InstanceID || volumeIDsJSON != string(expectedVolumes) || approvalID != c.Approval.ApprovalID || jobID != c.JobID || nodeKeyID != c.NodeKeyID || generation != c.ExpectedGeneration || counter != c.Command.NodeCounter || requestDigest != c.Command.RequestDigest {
		return ErrLeaseLost
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}
func validatePersistedServiceBackup(c runtime.ServiceBackupClaim, s runtime.SignedServiceBackupCommand) error {
	if runtime.ValidateServiceBackupClaim(c) != nil || runtime.ValidateSignedServiceBackupCommand(s) != nil {
		return errors.New("invalid service backup claim")
	}
	command, e := broker.ParseServiceBackupCommand([]byte(s.EnvelopeJSON))
	if e != nil {
		return e
	}
	if command.ValidateBinding(broker.ServiceBackupCommandBinding{ConnectionID: c.ConnectionID, CommandID: c.Command.CommandID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.Command.NodeCounter, IssuedAt: s.IssuedAt, ExpiresAt: s.ExpiresAt, Request: c.Request, ApprovalProof: c.Approval}) != nil || command.PayloadSHA256 != s.PayloadSHA256 || command.RequestSHA256() != s.RequestSHA256 {
		return errors.New("service backup command binding is invalid")
	}
	payload, e := base64.StdEncoding.DecodeString(command.PayloadB64)
	if e != nil || string(payload) != s.PayloadJSON {
		return errors.New("service backup payload is invalid")
	}
	return nil
}
func validateBackupReceipt(c runtime.ServiceBackupClaim, s runtime.SignedServiceBackupCommand, r runtime.ServiceBackupResult) error {
	command, e := broker.ParseServiceBackupCommand([]byte(s.EnvelopeJSON))
	if e != nil {
		return e
	}
	decoder := json.NewDecoder(strings.NewReader(r.ReceiptJSON))
	decoder.DisallowUnknownFields()
	var receipt broker.DeploymentCommandReceipt
	if e = decoder.Decode(&receipt); e != nil {
		return e
	}
	var trailing any
	if e = decoder.Decode(&trailing); !errors.Is(e, io.EOF) {
		return errors.New("service backup receipt contains trailing JSON")
	}
	return broker.ValidateServiceBackupResult(command, broker.ServiceBackupResult{Schema: broker.ServiceBackupResultSchema, Status: r.Status, Receipt: receipt, Backup: broker.ServiceBackupEvidence{BackupID: r.BackupID, ServiceID: r.ServiceID, DeploymentID: r.DeploymentID, InstanceID: r.InstanceID, RetentionPolicy: c.Request.RetentionPolicy, ImageID: r.ImageID, Snapshots: r.Snapshots}})
}
func loadServiceBackupSigned(ctx context.Context, tx *sql.Tx, id string) (runtime.SignedServiceBackupCommand, error) {
	var s runtime.SignedServiceBackupCommand
	var issued, expires int64
	e := tx.QueryRowContext(ctx, `SELECT signed_envelope_json,canonical_payload_json,payload_sha256,request_sha256,issued_at,expires_at FROM p2p_cloud_service_backup_commands WHERE command_id=$1 AND state IN('signed','indeterminate') FOR UPDATE`, id).Scan(&s.EnvelopeJSON, &s.PayloadJSON, &s.PayloadSHA256, &s.RequestSHA256, &issued, &expires)
	s.IssuedAt = time.UnixMilli(issued).UTC()
	s.ExpiresAt = time.UnixMilli(expires).UTC()
	return s, e
}
func releaseServiceBackupOutbox(ctx context.Context, tx *sql.Tx, c runtime.ServiceBackupClaim, at int64, code string) error {
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,available_at=$1,last_error_code=$2 WHERE outbox_id=$3 AND lease_token=$4 AND completed_at=0`, at, code, c.OutboxID, c.LeaseToken)
	if e != nil {
		return e
	}
	return requireOneAffected(r)
}
func completeServiceBackupOutbox(ctx context.Context, tx *sql.Tx, c runtime.ServiceBackupClaim, now int64) error {
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,delivered_at=$1,available_at=$1,last_error_code='' WHERE outbox_id=$2 AND lease_token=$3 AND completed_at=0`, now, c.OutboxID, c.LeaseToken)
	if e != nil {
		return e
	}
	return requireOneAffected(r)
}
