package storepg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	"io"
	"strings"
	"time"
)

var _ runtime.ServiceRestorePlanStore = (*Store)(nil)

func (s *Store) ClaimServiceRestorePlan(ctx context.Context, worker string, lease time.Duration) (c runtime.ServiceRestorePlanClaim, found bool, err error) {
	if s == nil || s.db == nil {
		return c, false, errors.New("cloud orchestrator database is unavailable")
	}
	worker = strings.TrimSpace(worker)
	if worker == "" || len(worker) > 128 || lease <= 0 || lease > 5*time.Minute {
		return c, false, errors.New("service restore plan lease configuration is invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return c, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" {
		return c, false, errors.New("service restore plan lease token is invalid")
	}
	var refsJSON string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,outbox.attempts,restore.restore_plan_id,restore.service_id,restore.service_revision,restore.deployment_id,restore.deployment_revision,restore.backup_id,restore.backup_revision,restore.plan_id,restore.cloud_connection_id,restore.region,restore.instance_id,restore.image_id,restore.snapshot_refs_json,restore.job_id,broker.broker_command_url,broker.node_key_id,broker.connection_generation FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_restore_plans restore ON restore.restore_plan_id=outbox.aggregate_id JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=restore.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=restore.cloud_connection_id JOIN p2p_cloud_services service ON service.service_id=restore.service_id JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=restore.deployment_id JOIN p2p_cloud_service_backups backup ON backup.backup_id=restore.backup_id JOIN p2p_cloud_jobs job ON job.job_id=restore.job_id AND job.kind='restore_plan' WHERE outbox.kind=$1 AND outbox.aggregate_type='service_restore_plan' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2 AND restore.plan_status='planning' AND service.revision=restore.service_revision AND deployment.revision=restore.deployment_revision AND backup.revision=restore.backup_revision AND backup.backup_status='available' AND connection.status='active' AND connection.region=restore.region AND connection.region=broker.broker_region AND job.execution_status='queued' AND job.outcome_status='pending' ORDER BY outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox,restore SKIP LOCKED LIMIT 1`, runtime.ServiceRestorePlanRequested, now).Scan(&c.OutboxID, &c.Kind, &c.AggregateType, &c.AggregateID, &c.Attempt, &c.RestorePlanID, &c.ServiceID, &c.ServiceRevision, &c.DeploymentID, &c.DeploymentRevision, &c.BackupID, &c.BackupRevision, &c.PlanID, &c.ConnectionID, &c.Region, &c.Request.InstanceID, &c.Request.ImageID, &refsJSON, &c.JobID, &c.BrokerEndpoint, &c.NodeKeyID, &c.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceRestorePlanClaim{}, false, nil
	}
	if err != nil {
		return c, false, err
	}
	c.LeaseToken = token
	c.Attempt++
	c.Request.Schema = broker.ServiceRestorePlanSchema
	c.Request.RestorePlanID = c.RestorePlanID
	c.Request.ServiceID = c.ServiceID
	c.Request.DeploymentID = c.DeploymentID
	c.Request.BackupID = c.BackupID
	c.Request.Region = c.Region
	if json.Unmarshal([]byte(refsJSON), &c.Request.SnapshotRefs) != nil {
		return c, false, errors.New("service restore snapshot refs are invalid")
	}
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, worker, token, now+lease.Milliseconds(), c.OutboxID, now)
	if e != nil {
		return c, false, e
	}
	if e = requireOneAffected(r); e != nil {
		return c, false, ErrLeaseLost
	}
	c.Command, e = prepareServiceRestorePlanCommand(ctx, tx, c, now)
	if e != nil {
		return c, false, e
	}
	if e = tx.Commit(); e != nil {
		return c, false, e
	}
	return c, true, nil
}

func prepareServiceRestorePlanCommand(ctx context.Context, tx *sql.Tx, c runtime.ServiceRestorePlanClaim, now int64) (runtime.ServiceRestorePlanCommand, error) {
	digest, e := runtime.ServiceRestorePlanRequestDigest(c.Request)
	if e != nil {
		return runtime.ServiceRestorePlanCommand{}, e
	}
	var command runtime.ServiceRestorePlanCommand
	var issued, expires int64
	e = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_restore_plan_commands WHERE restore_plan_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, c.RestorePlanID, digest).Scan(&command.CommandID, &command.Attempt, &command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON, &command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issued, &expires, &command.State)
	if e == nil {
		command.RestorePlanID, command.ServiceID, command.DeploymentID, command.BackupID, command.ConnectionID, command.RequestDigest = c.RestorePlanID, c.ServiceID, c.DeploymentID, c.BackupID, c.ConnectionID, digest
		command.IssuedAt = time.UnixMilli(issued).UTC()
		command.ExpiresAt = time.UnixMilli(expires).UTC()
		return command, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return command, e
	}
	var attempt int
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_restore_plan_commands WHERE restore_plan_id=$1 AND request_digest=$2`, c.RestorePlanID, digest).Scan(&attempt); e != nil {
		return command, e
	}
	var counter int64
	if e = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, c.ConnectionID).Scan(&counter); e != nil {
		return command, e
	}
	command = runtime.ServiceRestorePlanCommand{CommandID: stableID("cloud_service_restore_plan_command_", c.RestorePlanID, fmt.Sprint(attempt)), RestorePlanID: c.RestorePlanID, ServiceID: c.ServiceID, DeploymentID: c.DeploymentID, BackupID: c.BackupID, ConnectionID: c.ConnectionID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, RequestDigest: digest, State: "allocated"}
	_, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_restore_plan_commands(command_id,restore_plan_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,'service.restore.plan',$6,$7,$8,'allocated',$9,$9)`, command.CommandID, command.RestorePlanID, command.ConnectionID, digest, attempt, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
	return command, e
}

func (s *Store) PersistServiceRestorePlanCommand(ctx context.Context, c runtime.ServiceRestorePlanClaim, signed runtime.SignedServiceRestorePlanCommand) error {
	if runtime.ValidateServiceRestorePlanClaim(c) != nil || runtime.ValidateSignedServiceRestorePlanCommand(signed) != nil {
		return errors.New("service restore plan signed command is invalid")
	}
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		var state, payload, requestHash, envelope string
		var issued, expires int64
		if e := tx.QueryRowContext(ctx, `SELECT state,canonical_payload_json,request_sha256,signed_envelope_json,issued_at,expires_at FROM p2p_cloud_service_restore_plan_commands WHERE command_id=$1 FOR UPDATE`, c.Command.CommandID).Scan(&state, &payload, &requestHash, &envelope, &issued, &expires); e != nil {
			return e
		}
		if state == "signed" || state == "indeterminate" {
			if payload == signed.PayloadJSON && requestHash == signed.RequestSHA256 && envelope == signed.EnvelopeJSON && issued == signed.IssuedAt.UnixMilli() && expires == signed.ExpiresAt.UnixMilli() {
				return nil
			}
			return errors.New("service restore plan command already signed differently")
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plan_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, c.Command.CommandID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}
func (s *Store) MarkServiceRestorePlanStarted(ctx context.Context, c runtime.ServiceRestorePlanClaim) error {
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		_, e := transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "restore_plan", "restore_plan", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "restore_plan_provider_pending", stepStatus: "running", stepSummary: "AWS is independently verifying the retained backup, original instance mappings, same-AZ replacement volumes, and current restore estimate; no mutation is authorized."})
		return e
	})
}
func (s *Store) DeferServiceRestorePlan(ctx context.Context, c runtime.ServiceRestorePlanClaim, code string, available time.Time) error {
	code = durableErrorCode(code, "service_restore_plan_retryable")
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		if _, e := transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "restore_plan", "restore_plan", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "restore_plan_readback_retry", errorCode: code, stepStatus: "queued", stepSummary: "The exact signed read-only restore plan command will be retried."}); e != nil {
			return e
		}
		if _, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plan_commands SET state=CASE WHEN state='allocated' THEN state ELSE 'indeterminate' END,attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, c.Command.CommandID); e != nil {
			return e
		}
		when := available.UTC().UnixMilli()
		if when < now {
			when = now
		}
		return releaseServiceRestorePlanOutbox(ctx, tx, c, when, code)
	})
}
func (s *Store) ExpireServiceRestorePlanCommand(ctx context.Context, c runtime.ServiceRestorePlanClaim) error {
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plan_commands SET state='expired',attempts=attempts+1,last_error_code='expired_command',updated_at=$1 WHERE command_id=$2 AND state IN('signed','indeterminate')`, now, c.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		return releaseServiceRestorePlanOutbox(ctx, tx, c, now, "expired_command")
	})
}
func (s *Store) CompleteServiceRestorePlan(ctx context.Context, c runtime.ServiceRestorePlanClaim, result runtime.ServiceRestorePlanResult) error {
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		signed, e := loadServiceRestorePlanSigned(ctx, tx, c.Command.CommandID)
		if e != nil {
			return e
		}
		validated := c
		validated.Command.SignedEnvelope, validated.Command.PayloadJSON, validated.Command.PayloadSHA256, validated.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		validated.Command.IssuedAt, validated.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
		if runtime.ValidateServiceRestorePlanResult(validated, signed, result) != nil || validateServiceRestorePlanReceipt(validated, result) != nil {
			return errors.New("service restore plan result is invalid")
		}
		p := result.Plan
		unincluded, _ := json.Marshal(p.Unincluded)
		swaps, _ := json.Marshal(p.VolumeSwaps)
		quoted, _ := time.Parse(time.RFC3339Nano, p.QuotedAt)
		valid, _ := time.Parse(time.RFC3339Nano, p.ValidUntil)
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plan_commands SET state='accepted',receipt_json=$1,attempts=attempts+1,last_error_code='',updated_at=$2 WHERE command_id=$3 AND state IN('signed','indeterminate')`, result.ReceiptJSON, now, c.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plans SET plan_status='ready_for_confirmation',availability_zone=$1,quote_id=$2,currency=$3,estimated_hourly_minor=$4,estimated_thirty_day_minor=$5,quoted_at=$6,valid_until=$7,unincluded_json=$8,volume_swaps_json=$9,broker_receipt_json=$10,revision=revision+1,last_error_code='',updated_at=$11 WHERE restore_plan_id=$12 AND plan_status='planning'`, p.AvailabilityZone, p.QuoteID, p.Currency, p.EstimatedHourlyMinor, p.EstimatedThirtyDayMinor, quoted.UnixMilli(), valid.UnixMilli(), string(unincluded), string(swaps), result.ReceiptJSON, now, c.RestorePlanID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		if _, e = transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "restore_plan", "restore_plan", now, researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: "restore_plan_ready", stepStatus: "finished", stepSummary: "AWS read-back produced an exact same-AZ in-place restore plan and fresh cost estimate; no resource has been changed."}); e != nil {
			return e
		}
		return completeServiceRestorePlanOutbox(ctx, tx, c, now)
	})
}
func (s *Store) FailServiceRestorePlan(ctx context.Context, c runtime.ServiceRestorePlanClaim, code string) error {
	code = durableErrorCode(code, "service_restore_plan_failed")
	return s.withServiceRestorePlanClaim(ctx, c, func(tx *sql.Tx, now int64) error {
		_, _ = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plan_commands SET state='failed',attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, c.Command.CommandID)
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plans SET plan_status='failed',revision=revision+1,last_error_code=$1,updated_at=$2 WHERE restore_plan_id=$3 AND plan_status='planning'`, code, now, c.RestorePlanID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		if _, e = transitionCloudJob(ctx, tx, c.JobID, c.PlanID, c.DeploymentID, "restore_plan", "restore_plan", now, researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "restore_plan_failed", errorCode: code, stepStatus: "failed", stepSummary: "The read-only restore plan could not be independently verified; the service and AWS resources are unchanged."}); e != nil {
			return e
		}
		return completeServiceRestorePlanOutbox(ctx, tx, c, now)
	})
}

func (s *Store) withServiceRestorePlanClaim(ctx context.Context, c runtime.ServiceRestorePlanClaim, fn func(*sql.Tx, int64) error) (err error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	var token, kind, aggregateType, aggregateID, status, jobID, nodeKey, requestDigest string
	var until, done, generation, counter, serviceRev, deploymentRev, backupRev int64
	e = tx.QueryRowContext(ctx, `SELECT outbox.lease_token,outbox.lease_until,outbox.completed_at,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,restore.plan_status,restore.job_id,restore.service_revision,restore.deployment_revision,restore.backup_revision,broker.node_key_id,broker.connection_generation,command.node_counter,command.request_digest FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_restore_plans restore ON restore.restore_plan_id=outbox.aggregate_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=restore.cloud_connection_id JOIN p2p_cloud_service_restore_plan_commands command ON command.command_id=$2 AND command.restore_plan_id=restore.restore_plan_id WHERE outbox.outbox_id=$1 FOR UPDATE OF outbox,restore`, c.OutboxID, c.Command.CommandID).Scan(&token, &until, &done, &kind, &aggregateType, &aggregateID, &status, &jobID, &serviceRev, &deploymentRev, &backupRev, &nodeKey, &generation, &counter, &requestDigest)
	if e != nil {
		return e
	}
	if token != c.LeaseToken || until <= now || done != 0 || kind != c.Kind || aggregateType != c.AggregateType || aggregateID != c.AggregateID || status != "planning" || jobID != c.JobID || serviceRev != c.ServiceRevision || deploymentRev != c.DeploymentRevision || backupRev != c.BackupRevision || nodeKey != c.NodeKeyID || generation != c.ExpectedGeneration || counter != c.Command.NodeCounter || requestDigest != c.Command.RequestDigest {
		return ErrLeaseLost
	}
	if e = fn(tx, now); e != nil {
		return e
	}
	return tx.Commit()
}
func releaseServiceRestorePlanOutbox(ctx context.Context, tx *sql.Tx, c runtime.ServiceRestorePlanClaim, available int64, code string) error {
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,available_at=$1,last_error_code=$2 WHERE outbox_id=$3 AND lease_token=$4 AND completed_at=0`, available, code, c.OutboxID, c.LeaseToken)
	if e != nil {
		return e
	}
	return requireOneAffected(r)
}
func completeServiceRestorePlanOutbox(ctx context.Context, tx *sql.Tx, c runtime.ServiceRestorePlanClaim, now int64) error {
	r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,last_error_code='' WHERE outbox_id=$2 AND lease_token=$3 AND completed_at=0`, now, c.OutboxID, c.LeaseToken)
	if e != nil {
		return e
	}
	return requireOneAffected(r)
}
func loadServiceRestorePlanSigned(ctx context.Context, tx *sql.Tx, id string) (s runtime.SignedServiceRestorePlanCommand, e error) {
	var issued, expires int64
	e = tx.QueryRowContext(ctx, `SELECT signed_envelope_json,canonical_payload_json,payload_sha256,request_sha256,issued_at,expires_at FROM p2p_cloud_service_restore_plan_commands WHERE command_id=$1 AND state IN('signed','indeterminate') FOR UPDATE`, id).Scan(&s.EnvelopeJSON, &s.PayloadJSON, &s.PayloadSHA256, &s.RequestSHA256, &issued, &expires)
	s.IssuedAt = time.UnixMilli(issued).UTC()
	s.ExpiresAt = time.UnixMilli(expires).UTC()
	return
}
func validateServiceRestorePlanReceipt(c runtime.ServiceRestorePlanClaim, r runtime.ServiceRestorePlanResult) error {
	command, e := broker.ParseServiceRestorePlanCommand([]byte(c.Command.SignedEnvelope))
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
		return errors.New("service restore plan receipt contains trailing JSON")
	}
	return broker.ValidateServiceRestorePlanResult(command, broker.ServiceRestorePlanResult{Schema: broker.ServiceRestorePlanResultSchema, Status: r.Status, Receipt: receipt, Plan: r.Plan})
}
