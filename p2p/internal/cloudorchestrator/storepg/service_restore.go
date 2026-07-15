package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceRestoreStore = (*Store)(nil)

func (s *Store) ClaimServiceRestore(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceRestoreClaim, found bool, err error) {
	if s == nil || s.db == nil {
		return claim, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("service restore lease configuration is invalid")
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
		return claim, false, errors.New("service restore lease token is invalid")
	}
	var approvalJSON, signature, swapsJSON, resourceVolumesJSON string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,outbox.attempts,
		restore.restore_id,restore.service_id,restore.deployment_id,restore.backup_id,restore.service_revision,restore.deployment_revision,restore.backup_revision,
		restore.plan_id,restore.cloud_connection_id,restore.job_id,approval.approval_json,approval.signature,restore.instance_id,restore.volume_swaps_json,
		connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation,resource.volume_ids_json
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_restores restore ON restore.restore_id=outbox.aggregate_id
		JOIN p2p_cloud_service_restore_approvals approval ON approval.approval_id=restore.approval_id
		JOIN p2p_cloud_service_backups backup ON backup.backup_id=restore.backup_id
		JOIN p2p_cloud_services service ON service.service_id=restore.service_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=restore.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=restore.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=restore.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=restore.cloud_connection_id
		JOIN p2p_cloud_jobs job ON job.job_id=restore.job_id AND job.kind='restore'
		WHERE outbox.kind=$1 AND outbox.aggregate_type='service_restore' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2
		AND restore.restore_status IN('queued','running') AND approval.status='approved' AND approval.signature<>''
		AND backup.revision=restore.backup_revision AND backup.backup_status='available'
		AND service.revision=restore.service_revision AND deployment.revision=restore.deployment_revision
		AND deployment.resource_status IN('active','retained_tracked') AND resource.resource_status=deployment.resource_status AND resource.instance_id=restore.instance_id
		AND connection.status='active' AND connection.region=restore.region AND connection.region=broker.broker_region
		AND job.execution_status IN('queued','provisioning') AND job.outcome_status='pending'
		ORDER BY outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox,restore SKIP LOCKED LIMIT 1`, runtime.ServiceRestoreRequested, now).Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.Attempt,
		&claim.RestoreID, &claim.ServiceID, &claim.DeploymentID, &claim.BackupID, &claim.ServiceRevision, &claim.DeploymentRevision, &claim.BackupRevision,
		&claim.PlanID, &claim.ConnectionID, &claim.JobID, &approvalJSON, &signature, &claim.Request.InstanceID, &swapsJSON,
		&claim.Region, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &resourceVolumesJSON)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceRestoreClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	claim.LeaseToken = token
	claim.Attempt++
	if json.Unmarshal([]byte(approvalJSON), &claim.Approval) != nil {
		return claim, false, errors.New("service restore approval is invalid")
	}
	claim.Approval.Signature = signature
	if err = bindServiceRestoreRequest(&claim, swapsJSON, resourceVolumesJSON); err != nil {
		return claim, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
		return claim, false, ErrLeaseLost
	}
	claim.Command, err = prepareServiceRestoreCommand(ctx, tx, claim, now)
	if err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func bindServiceRestoreRequest(claim *runtime.ServiceRestoreClaim, swapsJSON, resourceVolumesJSON string) error {
	if claim == nil || json.Unmarshal([]byte(swapsJSON), &claim.Request.VolumeSwaps) != nil {
		return errors.New("service restore volume swaps are invalid")
	}
	var resourceVolumes []string
	if json.Unmarshal([]byte(resourceVolumesJSON), &resourceVolumes) != nil {
		return errors.New("service restore resource volumes are invalid")
	}
	originals := make([]string, 0, len(claim.Request.VolumeSwaps))
	for _, swap := range claim.Request.VolumeSwaps {
		originals = append(originals, swap.OriginalVolumeID)
	}
	sort.Strings(originals)
	sort.Strings(resourceVolumes)
	if len(originals) != len(resourceVolumes) {
		return errors.New("service restore does not bind current volumes")
	}
	for i := range originals {
		if originals[i] != resourceVolumes[i] {
			return errors.New("service restore does not bind current volumes")
		}
	}
	target := claim.Approval.ServiceRestoreTargetV1
	claim.Request.Schema = broker.ServiceRestoreSchema
	claim.Request.RestoreID = claim.RestoreID
	claim.Request.ServiceID = claim.ServiceID
	claim.Request.DeploymentID = claim.DeploymentID
	claim.Request.BackupID = claim.BackupID
	claim.Request.Region = target.Region
	claim.Request.AvailabilityZone = target.AvailabilityZone
	claim.Request.RestoreMode = target.RestoreMode
	claim.Request.DowntimeRequired = target.DowntimeRequired
	claim.Request.OriginalVolumeRetention = target.OriginalVolumeRetention
	claim.Request.FailurePolicy = target.FailurePolicy
	claim.Request.QuoteID = target.QuoteID
	claim.Request.QuoteValidUntil = target.QuoteValidUntil.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
	return nil
}

func prepareServiceRestoreCommand(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, now int64) (runtime.ServiceRestoreCommand, error) {
	digest, err := runtime.ServiceRestoreRequestDigest(claim.Request)
	if err != nil {
		return runtime.ServiceRestoreCommand{}, err
	}
	var command runtime.ServiceRestoreCommand
	var issued, expires int64
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_restore_commands WHERE restore_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.RestoreID, digest).Scan(&command.CommandID, &command.Attempt, &command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON, &command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issued, &expires, &command.State)
	if err == nil {
		command.RestoreID, command.ServiceID, command.DeploymentID, command.ConnectionID, command.RequestDigest = claim.RestoreID, claim.ServiceID, claim.DeploymentID, claim.ConnectionID, digest
		command.IssuedAt, command.ExpiresAt = time.UnixMilli(issued).UTC(), time.UnixMilli(expires).UTC()
		return command, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return command, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_restore_commands WHERE restore_id=$1 AND request_digest=$2`, claim.RestoreID, digest).Scan(&attempt); err != nil {
		return command, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); err != nil {
		return command, err
	}
	command = runtime.ServiceRestoreCommand{CommandID: stableID("cloud_service_restore_command_", claim.RestoreID, fmt.Sprint(attempt)), RestoreID: claim.RestoreID, ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, RequestDigest: digest, State: "allocated"}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_restore_commands(command_id,restore_id,approval_id,service_id,deployment_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,'service.restore',$9,$10,$11,'allocated',$12,$12)`, command.CommandID, command.RestoreID, claim.Approval.ApprovalID, command.ServiceID, command.DeploymentID, command.ConnectionID, digest, attempt, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
	return command, err
}

func (s *Store) PersistServiceRestoreCommand(ctx context.Context, claim runtime.ServiceRestoreClaim, signed runtime.SignedServiceRestoreCommand) error {
	if validatePersistedServiceRestore(claim, signed) != nil {
		return errors.New("service restore signed command is invalid")
	}
	return s.withServiceRestoreClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (s *Store) MarkServiceRestoreStarted(ctx context.Context, claim runtime.ServiceRestoreClaim) error {
	return s.withServiceRestoreClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET restore_status='running',revision=revision+1,updated_at=$1 WHERE restore_id=$2 AND restore_status IN('queued','running')`, now, claim.RestoreID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		_, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "restore", "restore", now, researchJobTransition{execution: "provisioning", outcome: "pending", checkpoint: "restore_provider_pending", stepStatus: "running", stepSummary: "The approved original-instance volume swap is running. Downtime and replacement-volume charges may now accrue."})
		return err
	})
}

func (s *Store) DeferServiceRestore(ctx context.Context, claim runtime.ServiceRestoreClaim, code string, available time.Time) error {
	code = durableErrorCode(code, "service_restore_retryable")
	return s.withServiceRestoreClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "restore", "restore", now, researchJobTransition{execution: "queued", outcome: "pending", checkpoint: "restore_readback_retry", errorCode: code, stepStatus: "queued", stepSummary: "The exact signed restore command is indeterminate and will be retried without allocating another counter."}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_commands SET state=CASE WHEN state='allocated' THEN state ELSE 'indeterminate' END,attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
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
		return releaseServiceRestoreOutbox(ctx, tx, claim, when, code)
	})
}

func (s *Store) CompleteServiceRestore(ctx context.Context, claim runtime.ServiceRestoreClaim, result runtime.ServiceRestoreResult) error {
	return s.withServiceRestoreClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		signed, err := loadServiceRestoreSigned(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		validated := claim
		validated.Command.SignedEnvelope, validated.Command.PayloadJSON = signed.EnvelopeJSON, signed.PayloadJSON
		validated.Command.PayloadSHA256, validated.Command.RequestSHA256 = signed.PayloadSHA256, signed.RequestSHA256
		validated.Command.IssuedAt, validated.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
		if validatePersistedServiceRestore(validated, signed) != nil || runtime.ValidateServiceRestoreResult(validated, signed, result) != nil || validateServiceRestoreReceipt(validated, signed, result) != nil {
			return errors.New("service restore result is invalid")
		}
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_commands SET state='accepted',receipt_json=$1,attempts=attempts+1,last_error_code='',updated_at=$2 WHERE command_id=$3 AND state IN('signed','indeterminate')`, result.ReceiptJSON, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(commandResult); err != nil {
			return err
		}
		if err = applyServiceRestoreResult(ctx, tx, claim, result, now); err != nil {
			return err
		}
		return completeServiceRestoreOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) FailServiceRestore(ctx context.Context, claim runtime.ServiceRestoreClaim, code string) error {
	code = durableErrorCode(code, "service_restore_failed")
	return s.withServiceRestoreClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		commandResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_commands SET state='failed',attempts=attempts+1,last_error_code=$1,updated_at=$2 WHERE command_id=$3 AND state IN('allocated','signed','indeterminate')`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(commandResult); err != nil {
			return err
		}
		if err = finishServiceRestoreFailure(ctx, tx, claim, "failed", code, "restore_failed", "The restore command failed before an independently verified AWS terminal result. Resources remain tracked and require operator review.", now, true); err != nil {
			return err
		}
		return completeServiceRestoreOutbox(ctx, tx, claim, now)
	})
}

func applyServiceRestoreResult(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, result runtime.ServiceRestoreResult, now int64) error {
	originals := make([]string, 0, len(claim.Request.VolumeSwaps))
	replacements := make([]string, 0, len(result.Evidence.Replacements))
	for _, swap := range claim.Request.VolumeSwaps {
		originals = append(originals, swap.OriginalVolumeID)
	}
	for _, replacement := range result.Evidence.Replacements {
		replacements = append(replacements, replacement.ReplacementVolumeID)
	}
	sort.Strings(originals)
	sort.Strings(replacements)
	originalJSON, _ := json.Marshal(originals)
	replacementJSON, _ := json.Marshal(replacements)
	evidenceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET receipt_json=$1,original_volume_ids_json=$2,replacement_volume_ids_json=$3,updated_at=$4 WHERE restore_id=$5 AND restore_status IN('queued','running')`, result.ReceiptJSON, string(originalJSON), string(replacementJSON), now, claim.RestoreID)
	if err != nil {
		return err
	}
	if err = requireOneAffected(evidenceResult); err != nil {
		return err
	}
	switch result.Status {
	case "aws_restore_applied":
		resourceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET volume_ids_json=$1,updated_at=$2 WHERE deployment_id=$3 AND cloud_connection_id=$4 AND instance_id=$5 AND resource_status IN('active','retained_tracked')`, string(replacementJSON), now, claim.DeploymentID, claim.ConnectionID, claim.Request.InstanceID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(resourceResult); err != nil {
			return err
		}
		restoreResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET restore_status='verifying',revision=revision+1,last_error_code='',updated_at=$1 WHERE restore_id=$2 AND restore_status IN('queued','running')`, now, claim.RestoreID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(restoreResult); err != nil {
			return err
		}
		if _, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "restore", "restore", now, researchJobTransition{execution: "verifying", outcome: "pending", checkpoint: "restore_readiness_queued", stepStatus: "running", stepSummary: "AWS read-back verified the replacement-volume mapping. Worker semantic readiness must pass before the restore can succeed."}); err != nil {
			return err
		}
		if err = ensureRestoreReadinessTask(ctx, tx, claim, now); err != nil {
			return err
		}
		serviceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET revision=revision+1,updated_at=$1 WHERE service_id=$2 AND revision=$3`, now, claim.ServiceID, claim.ServiceRevision)
		if err != nil {
			return err
		}
		if err = requireOneAffected(serviceResult); err != nil {
			return err
		}
		return publishServiceRestoreProjection(ctx, tx, claim, "verifying", now)
	case "aws_original_restored":
		return finishServiceRestoreFailure(ctx, tx, claim, "failed", "aws_original_restored", "restore_original_restored", "The replacement swap failed and AWS read-back verified that the retained original volumes were reattached. The restore did not succeed.", now, false)
	case "restore_blocked":
		if _, err := transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, "finished", "failed", "blocked"); err != nil {
			return err
		}
		return finishServiceRestoreFailure(ctx, tx, claim, "restore_blocked", "restore_blocked", "restore_blocked", "AWS could not verify either the replacement mapping or a complete original-volume fallback. The deployment is blocked and requires manual recovery.", now, true)
	default:
		return errors.New("unsupported service restore result")
	}
}

func ensureRestoreReadinessTask(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, now int64) error {
	var executionID, manifestDigest, installEvidenceDigest, semanticDigest string
	if err := tx.QueryRowContext(ctx, `SELECT execution_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest FROM p2p_cloud_service_readiness_tasks WHERE service_id=$1 AND purpose='install' AND task_status='succeeded'`, claim.ServiceID).Scan(&executionID, &manifestDigest, &installEvidenceDigest, &semanticDigest); err != nil {
		return err
	}
	taskID := stableID("cloud_service_restore_readiness_task_", claim.RestoreID)
	result, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_readiness_tasks(task_id,execution_id,deployment_id,service_id,cloud_connection_id,instance_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest,task_status,purpose,restore_id,job_id,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'unissued','restore',$10,$11,$12,$12) ON CONFLICT(task_id) DO NOTHING`, taskID, executionID, claim.DeploymentID, claim.ServiceID, claim.ConnectionID, claim.Request.InstanceID, manifestDigest, installEvidenceDigest, semanticDigest, claim.RestoreID, claim.JobID, now)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		var purpose, restoreID, jobID string
		if err = tx.QueryRowContext(ctx, `SELECT purpose,restore_id,job_id FROM p2p_cloud_service_readiness_tasks WHERE task_id=$1`, taskID).Scan(&purpose, &restoreID, &jobID); err != nil || purpose != "restore" || restoreID != claim.RestoreID || jobID != claim.JobID {
			return errors.New("service restore readiness task binding conflict")
		}
	}
	payload, _ := json.Marshal(map[string]string{"task_id": taskID, "restore_id": claim.RestoreID})
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at) VALUES($1,$2,'service_readiness_task',$3,$4,$5,$5) ON CONFLICT(outbox_id) DO NOTHING`, stableID("cloud_service_restore_readiness_outbox_", claim.RestoreID), cloudmodule.OutboxKindServiceReadinessRequested, taskID, string(payload), now)
	return err
}

func finishServiceRestoreFailure(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, status, code, checkpoint, summary string, now int64, alert bool) error {
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET restore_status=$1,revision=revision+1,last_error_code=$2,updated_at=$3 WHERE restore_id=$4 AND restore_status IN('queued','running','verifying')`, status, code, now, claim.RestoreID)
	if err != nil {
		return err
	}
	if err = requireOneAffected(result); err != nil {
		return err
	}
	serviceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='degraded',revision=revision+1,updated_at=$1 WHERE service_id=$2 AND revision=$3`, now, claim.ServiceID, claim.ServiceRevision)
	if err != nil {
		return err
	}
	if err = requireOneAffected(serviceResult); err != nil {
		return err
	}
	if _, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "restore", "restore", now, researchJobTransition{execution: "finished", outcome: "failed", checkpoint: checkpoint, errorCode: code, stepStatus: "failed", stepSummary: summary}); err != nil {
		return err
	}
	if alert {
		alertID := stableID("cloud_alert_", claim.RestoreID, code)
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_alerts(alert_id,deployment_id,service_id,severity,code,message,acknowledged,revision,created_at,updated_at) VALUES($1,$2,$3,'critical',$4,$5,FALSE,1,$6,$6) ON CONFLICT(alert_id) DO NOTHING`, alertID, claim.DeploymentID, claim.ServiceID, code, summary, now); err != nil {
			return err
		}
		if err = writeEventAndProjection(ctx, tx, stableID("cloud_event_", alertID, "1"), "cloud.alert.raised", "alert", alertID, 1, map[string]any{"alert_id": alertID, "deployment_id": claim.DeploymentID, "service_id": claim.ServiceID, "severity": "critical", "code": code, "message": summary, "acknowledged": false, "revision": int64(1), "created_at": now, "updated_at": now}, now); err != nil {
			return err
		}
	}
	return publishServiceRestoreProjection(ctx, tx, claim, status, now)
}

func publishServiceRestoreProjection(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, restoreStatus string, now int64) error {
	var service cloudmodule.Service
	if err := tx.QueryRowContext(ctx, `SELECT service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at FROM p2p_cloud_services WHERE service_id=$1`, claim.ServiceID).Scan(&service.ServiceID, &service.DeploymentID, &service.RecipeID, &service.Name, &service.Status, &service.Integration, &service.Revision, &service.CreatedAt, &service.UpdatedAt); err != nil {
		return err
	}
	backups, restores, err := loadServiceRestoreProjectionCollections(ctx, tx, claim.ServiceID)
	if err != nil {
		return err
	}
	payload := map[string]any{"service_id": service.ServiceID, "deployment_id": service.DeploymentID, "recipe_id": service.RecipeID, "name": service.Name, "service_status": service.Status, "integration_status": service.Integration, "revision": service.Revision, "created_at": service.CreatedAt, "updated_at": service.UpdatedAt, "backups": backups, "restores": restores}
	return writeEventAndProjection(ctx, tx, stableID("cloud_event_", service.ServiceID, fmt.Sprint(service.Revision), "restore", restoreStatus), "cloud.service.changed", "service", service.ServiceID, service.Revision, payload, now)
}

func loadServiceRestoreProjectionCollections(ctx context.Context, tx *sql.Tx, serviceID string) ([]cloudmodule.ServiceBackup, []cloudmodule.ServiceRestore, error) {
	backups := []cloudmodule.ServiceBackup{}
	rows, err := tx.QueryContext(ctx, `SELECT backup_id,service_id,deployment_id,backup_status,retention_policy,image_id,snapshots_json,revision,created_at,updated_at FROM p2p_cloud_service_backups WHERE service_id=$1 AND backup_status IN('available','failed') ORDER BY created_at DESC,backup_id`, serviceID)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var backup cloudmodule.ServiceBackup
		var snapshotsJSON string
		if err = rows.Scan(&backup.BackupID, &backup.ServiceID, &backup.DeploymentID, &backup.Status, &backup.RetentionPolicy, &backup.ImageID, &snapshotsJSON, &backup.Revision, &backup.CreatedAt, &backup.UpdatedAt); err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		var snapshots []struct {
			SnapshotID string `json:"snapshot_id"`
		}
		if err = json.Unmarshal([]byte(snapshotsJSON), &snapshots); err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		for _, snapshot := range snapshots {
			backup.SnapshotIDs = append(backup.SnapshotIDs, snapshot.SnapshotID)
		}
		backups = append(backups, backup)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, nil, err
	}
	restores := []cloudmodule.ServiceRestore{}
	rows, err = tx.QueryContext(ctx, `SELECT restore_id,restore_plan_id,service_id,deployment_id,backup_id,restore_status,original_volume_ids_json,replacement_volume_ids_json,revision,created_at,updated_at FROM p2p_cloud_service_restores WHERE service_id=$1 ORDER BY created_at DESC,restore_id`, serviceID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var restore cloudmodule.ServiceRestore
		var originalJSON, replacementJSON string
		if err = rows.Scan(&restore.RestoreID, &restore.RestorePlanID, &restore.ServiceID, &restore.DeploymentID, &restore.BackupID, &restore.Status, &originalJSON, &replacementJSON, &restore.Revision, &restore.CreatedAt, &restore.UpdatedAt); err != nil {
			return nil, nil, err
		}
		if err = json.Unmarshal([]byte(originalJSON), &restore.OriginalVolumeIDs); err != nil {
			return nil, nil, err
		}
		if err = json.Unmarshal([]byte(replacementJSON), &restore.ReplacementVolumeIDs); err != nil {
			return nil, nil, err
		}
		restores = append(restores, restore)
	}
	return backups, restores, rows.Err()
}

func (s *Store) withServiceRestoreClaim(ctx context.Context, claim runtime.ServiceRestoreClaim, run func(*sql.Tx, int64) error) (err error) {
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
	var token, kind, aggregateType, aggregateID, restoreID, serviceID, deploymentID, backupID, planID, connectionID, status, approvalID, jobID, nodeKeyID, requestDigest, instanceID string
	var until, completed, serviceRevision, deploymentRevision, backupRevision, currentServiceRevision, currentDeploymentRevision, currentBackupRevision, generation, counter int64
	err = tx.QueryRowContext(ctx, `SELECT outbox.lease_token,outbox.lease_until,outbox.completed_at,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,
		restore.restore_id,restore.service_id,restore.deployment_id,restore.backup_id,restore.service_revision,restore.deployment_revision,restore.backup_revision,restore.plan_id,restore.cloud_connection_id,restore.restore_status,restore.approval_id,restore.job_id,restore.instance_id,
		broker.node_key_id,broker.connection_generation,command.node_counter,command.request_digest,service.revision,deployment.revision,backup.revision
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_restores restore ON restore.restore_id=outbox.aggregate_id
		JOIN p2p_cloud_services service ON service.service_id=restore.service_id JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=restore.deployment_id
		JOIN p2p_cloud_service_backups backup ON backup.backup_id=restore.backup_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=restore.cloud_connection_id
		JOIN p2p_cloud_service_restore_commands command ON command.command_id=$2 AND command.restore_id=restore.restore_id
		WHERE outbox.outbox_id=$1 FOR UPDATE OF outbox,restore`, claim.OutboxID, claim.Command.CommandID).Scan(&token, &until, &completed, &kind, &aggregateType, &aggregateID, &restoreID, &serviceID, &deploymentID, &backupID, &serviceRevision, &deploymentRevision, &backupRevision, &planID, &connectionID, &status, &approvalID, &jobID, &instanceID, &nodeKeyID, &generation, &counter, &requestDigest, &currentServiceRevision, &currentDeploymentRevision, &currentBackupRevision)
	if err != nil {
		return err
	}
	if token != claim.LeaseToken || until <= now || completed != 0 || kind != claim.Kind || aggregateType != claim.AggregateType || aggregateID != claim.AggregateID || restoreID != claim.RestoreID || serviceID != claim.ServiceID || deploymentID != claim.DeploymentID || backupID != claim.BackupID || serviceRevision != claim.ServiceRevision || deploymentRevision != claim.DeploymentRevision || backupRevision != claim.BackupRevision || currentServiceRevision != claim.ServiceRevision || currentDeploymentRevision != claim.DeploymentRevision || currentBackupRevision != claim.BackupRevision || planID != claim.PlanID || connectionID != claim.ConnectionID || (status != "queued" && status != "running") || approvalID != claim.Approval.ApprovalID || jobID != claim.JobID || instanceID != claim.Request.InstanceID || nodeKeyID != claim.NodeKeyID || generation != claim.ExpectedGeneration || counter != claim.Command.NodeCounter || requestDigest != claim.Command.RequestDigest {
		return ErrLeaseLost
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func validatePersistedServiceRestore(claim runtime.ServiceRestoreClaim, signed runtime.SignedServiceRestoreCommand) error {
	if runtime.ValidateServiceRestoreClaim(claim) != nil || runtime.ValidateSignedServiceRestoreCommand(signed) != nil {
		return errors.New("invalid service restore claim")
	}
	command, err := broker.ParseServiceRestoreCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return err
	}
	if command.ValidateBinding(broker.ServiceRestoreCommandBinding{ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: claim.Request, ApprovalProof: claim.Approval}) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("service restore command binding is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("service restore payload is invalid")
	}
	return nil
}

func validateServiceRestoreReceipt(claim runtime.ServiceRestoreClaim, signed runtime.SignedServiceRestoreCommand, result runtime.ServiceRestoreResult) error {
	command, err := broker.ParseServiceRestoreCommand([]byte(signed.EnvelopeJSON))
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
		return errors.New("service restore receipt contains trailing JSON")
	}
	return broker.ValidateServiceRestoreResult(command, broker.ServiceRestoreResult{Schema: broker.ServiceRestoreResultSchema, Status: result.Status, Receipt: receipt, Restore: result.Evidence})
}

func loadServiceRestoreSigned(ctx context.Context, tx *sql.Tx, commandID string) (runtime.SignedServiceRestoreCommand, error) {
	var signed runtime.SignedServiceRestoreCommand
	var issued, expires int64
	err := tx.QueryRowContext(ctx, `SELECT signed_envelope_json,canonical_payload_json,payload_sha256,request_sha256,issued_at,expires_at FROM p2p_cloud_service_restore_commands WHERE command_id=$1 AND state IN('signed','indeterminate') FOR UPDATE`, commandID).Scan(&signed.EnvelopeJSON, &signed.PayloadJSON, &signed.PayloadSHA256, &signed.RequestSHA256, &issued, &expires)
	signed.IssuedAt, signed.ExpiresAt = time.UnixMilli(issued).UTC(), time.UnixMilli(expires).UTC()
	return signed, err
}

func releaseServiceRestoreOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, available int64, code string) error {
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,available_at=$1,last_error_code=$2 WHERE outbox_id=$3 AND lease_token=$4 AND completed_at=0`, available, code, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func completeServiceRestoreOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ServiceRestoreClaim, now int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,delivered_at=$1,available_at=$1,last_error_code='' WHERE outbox_id=$2 AND lease_token=$3 AND completed_at=0`, now, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}
