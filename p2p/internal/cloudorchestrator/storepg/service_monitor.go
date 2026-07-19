package storepg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceMonitorStore = (*Store)(nil)

const (
	serviceMonitorHealthyInterval = 5 * time.Minute
	serviceMonitorRetryBase       = time.Minute
	serviceMonitorRetryMax        = time.Hour
	serviceMonitorAlertCode       = "service_monitor_unhealthy"
)

func (s *Store) ClaimServiceMonitor(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceMonitorClaim, found bool, err error) {
	if s == nil || s.db == nil || strings.TrimSpace(workerID) == "" || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("service monitor claim configuration is invalid")
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
	if err = ensureServiceMonitorLedgers(ctx, tx, now); err != nil {
		return claim, false, err
	}
	if err = reconcileDriftedServiceMonitorTasks(ctx, tx, now); err != nil {
		return claim, false, err
	}
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" {
		return claim, false, errors.New("service monitor lease token is invalid")
	}
	var semanticProbeJSON string
	err = tx.QueryRowContext(ctx, `SELECT monitor.service_id,monitor.deployment_id,service.service_status,service.revision,
		deployment.revision,resource.resource_status,observation.worker_lease_epoch,
		source.execution_id,source.cloud_connection_id,source.instance_id,source.recipe_execution_manifest_digest,
		source.install_evidence_digest,source.artifact_digest,source.semantic_probe_json,source.semantic_expectation_digest,monitor.generation+1
		FROM p2p_cloud_service_monitors monitor
		JOIN p2p_cloud_services service ON service.service_id=monitor.service_id AND service.deployment_id=monitor.deployment_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=monitor.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=monitor.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=monitor.deployment_id
		JOIN p2p_cloud_service_readiness_tasks source ON source.service_id=monitor.service_id AND source.purpose='install' AND source.task_status='succeeded'
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=source.execution_id AND manifest.manifest_digest=source.recipe_execution_manifest_digest AND manifest.status='approved'
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=source.execution_id AND install.task_status='succeeded'
		WHERE monitor.monitor_status='idle' AND monitor.current_task_id='' AND monitor.next_check_at<=$1 AND monitor.lease_until<=$1
		AND service.service_status IN('active','experimental','degraded')
		AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded' AND deployment.resource_status IN('active','retained_tracked')
		AND resource.resource_status IN('active','retained_tracked') AND observation.worker_session_state='active' AND observation.worker_lease_epoch>0 AND observation.worker_lease_expires_at>$1
		ORDER BY monitor.next_check_at,monitor.updated_at,monitor.service_id FOR UPDATE OF monitor SKIP LOCKED LIMIT 1`, now).Scan(
		&claim.ServiceID, &claim.DeploymentID, &claim.ServiceStatus, &claim.ServiceRevision, &claim.DeploymentRevision,
		&claim.ResourceStatus, &claim.WorkerLeaseEpoch, &claim.ExecutionID, &claim.ConnectionID, &claim.InstanceID,
		&claim.ManifestDigest, &claim.InstallEvidenceDigest, &claim.ArtifactDigest, &semanticProbeJSON, &claim.SemanticExpectationDigest, &claim.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		if commitErr := tx.Commit(); commitErr != nil {
			return claim, false, commitErr
		}
		return runtime.ServiceMonitorClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if claim.SemanticProbe, err = decodeServiceReadinessProbe(semanticProbeJSON); err != nil {
		return claim, false, err
	}
	claim.LeaseToken = token
	claim.TaskID = stableID("cloud_service_monitor_task_", claim.ServiceID, fmt.Sprint(claim.Generation), claim.ManifestDigest, fmt.Sprint(claim.WorkerLeaseEpoch), fmt.Sprint(claim.ServiceRevision), fmt.Sprint(claim.DeploymentRevision))
	claim.OutboxID = stableID("cloud_service_monitor_outbox_", claim.ServiceID, fmt.Sprint(claim.Generation), claim.TaskID)
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',updated_at=$4
		WHERE service_id=$5 AND monitor_status='idle' AND current_task_id='' AND generation=$6 AND lease_until<=$4`, strings.TrimSpace(workerID), token, now+lease.Milliseconds(), now, claim.ServiceID, claim.Generation-1)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
		return claim, false, ErrLeaseLost
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func (s *Store) ScheduleServiceMonitor(ctx context.Context, claim runtime.ServiceMonitorClaim) (err error) {
	if runtime.ValidateServiceMonitorClaim(claim) != nil {
		return ErrLeaseLost
	}
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
	var leaseToken string
	var leaseUntil, generation int64
	var currentTask string
	if err = tx.QueryRowContext(ctx, `SELECT lease_token,lease_until,generation,current_task_id FROM p2p_cloud_service_monitors WHERE service_id=$1 FOR UPDATE`, claim.ServiceID).Scan(&leaseToken, &leaseUntil, &generation, &currentTask); err != nil || leaseToken != claim.LeaseToken || leaseUntil <= now || generation+1 != claim.Generation || currentTask != "" {
		return ErrLeaseLost
	}
	if err = verifyServiceMonitorScheduleBindings(ctx, tx, claim, now); err != nil {
		return err
	}
	probeJSON, err := json.Marshal(claim.SemanticProbe)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_readiness_tasks
		(task_id,execution_id,deployment_id,service_id,cloud_connection_id,instance_id,recipe_execution_manifest_digest,install_evidence_digest,artifact_digest,semantic_probe_json,semantic_expectation_digest,
		task_status,purpose,restore_id,job_id,monitor_generation,monitor_service_revision,monitor_deployment_revision,monitor_resource_status,worker_lease_epoch,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'unissued','monitor','','',$12,$13,$14,$15,$16,$17,$17)`, claim.TaskID, claim.ExecutionID, claim.DeploymentID, claim.ServiceID, claim.ConnectionID, claim.InstanceID,
		claim.ManifestDigest, claim.InstallEvidenceDigest, claim.ArtifactDigest, string(probeJSON), claim.SemanticExpectationDigest,
		claim.Generation, claim.ServiceRevision, claim.DeploymentRevision, claim.ResourceStatus, claim.WorkerLeaseEpoch, now)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"service_id": claim.ServiceID, "task_id": claim.TaskID, "purpose": "monitor", "generation": claim.Generation})
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at)
		VALUES($1,$2,'service_readiness_task',$3,$4,$5,$5)`, claim.OutboxID, cloudmodule.OutboxKindServiceReadinessRequested, claim.TaskID, string(payload), now)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET monitor_status='checking',generation=$1,current_task_id=$2,lease_owner='',lease_token='',lease_until=0,last_error_code='',updated_at=$3
		WHERE service_id=$4 AND lease_token=$5 AND generation=$6 AND current_task_id=''`, claim.Generation, claim.TaskID, now, claim.ServiceID, claim.LeaseToken, claim.Generation-1)
	if err != nil {
		return err
	}
	if err = requireOneAffected(result); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeferServiceMonitor(ctx context.Context, claim runtime.ServiceMonitorClaim, code string, available time.Time) error {
	if claim.ServiceID == "" || claim.LeaseToken == "" {
		return ErrLeaseLost
	}
	now := s.now().UnixMilli()
	result, err := s.db.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET next_check_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2,updated_at=$3
		WHERE service_id=$4 AND lease_token=$5 AND lease_until>$3 AND monitor_status='idle' AND current_task_id=''`, available.UTC().UnixMilli(), durableErrorCode(code, "service_monitor_retryable"), now, claim.ServiceID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func ensureServiceMonitorLedgers(ctx context.Context, tx *sql.Tx, now int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_monitors(service_id,deployment_id,monitor_status,next_check_at,created_at,updated_at)
		SELECT service.service_id,service.deployment_id,'idle',$1,$1,$1 FROM p2p_cloud_services service
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=service.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=service.deployment_id
		JOIN p2p_cloud_service_readiness_tasks source ON source.service_id=service.service_id AND source.purpose='install' AND source.task_status='succeeded'
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=source.execution_id AND manifest.manifest_digest=source.recipe_execution_manifest_digest AND manifest.status='approved'
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=source.execution_id AND install.task_status='succeeded'
		WHERE service.service_status IN('active','experimental','degraded') AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded'
		AND deployment.resource_status IN('active','retained_tracked') AND resource.resource_status IN('active','retained_tracked')
		AND observation.worker_session_state='active' AND observation.worker_lease_epoch>0 AND observation.worker_lease_expires_at>$1
		ON CONFLICT(service_id) DO NOTHING`, now)
	return err
}

func reconcileDriftedServiceMonitorTasks(ctx context.Context, tx *sql.Tx, now int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks task SET task_status='interrupted',error_code='service_monitor_state_drift',lease_owner='',lease_token='',lease_until=0,available_at=0,updated_at=$1
		FROM p2p_cloud_service_monitors monitor,p2p_cloud_services service,p2p_cloud_deployments deployment,p2p_cloud_deployment_resources resource,p2p_cloud_worker_bootstrap_observations observation
		WHERE task.task_id=monitor.current_task_id AND task.purpose='monitor' AND task.task_status IN('unissued','queued','running')
		AND service.service_id=monitor.service_id AND service.deployment_id=monitor.deployment_id
		AND deployment.deployment_id=monitor.deployment_id AND resource.deployment_id=monitor.deployment_id AND observation.deployment_id=monitor.deployment_id
		AND ((service.service_status NOT IN('active','experimental','degraded') OR deployment.execution_status<>'finished' OR deployment.outcome_status<>'succeeded' OR deployment.resource_status NOT IN('active','retained_tracked') OR resource.resource_status NOT IN('active','retained_tracked'))
		 OR (service.service_status IN('active','experimental','degraded') AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded' AND deployment.resource_status IN('active','retained_tracked') AND resource.resource_status IN('active','retained_tracked')
			AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$1
			AND (task.monitor_service_revision<>service.revision OR task.monitor_deployment_revision<>deployment.revision OR task.monitor_resource_status<>resource.resource_status OR task.worker_lease_epoch<>observation.worker_lease_epoch)))`, now)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands command SET state='failed',last_error_code='service_monitor_state_drift',updated_at=$1
		FROM p2p_cloud_service_readiness_tasks task WHERE command.task_id=task.task_id AND task.purpose='monitor' AND task.task_status='interrupted' AND task.error_code='service_monitor_state_drift' AND command.state IN('allocated','signed','indeterminate')`, now); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox outbox SET completed_at=$1,delivered_at=$1,available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code='service_monitor_state_drift'
		FROM p2p_cloud_service_readiness_tasks task WHERE outbox.aggregate_type='service_readiness_task' AND outbox.aggregate_id=task.task_id AND outbox.completed_at=0 AND task.purpose='monitor' AND task.task_status='interrupted' AND task.error_code='service_monitor_state_drift'`, now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors monitor SET monitor_status='idle',current_task_id='',next_check_at=$1,last_error_code='service_monitor_state_drift',lease_owner='',lease_token='',lease_until=0,updated_at=$1
		FROM p2p_cloud_service_readiness_tasks task WHERE task.task_id=monitor.current_task_id AND task.purpose='monitor' AND task.task_status='interrupted' AND task.error_code='service_monitor_state_drift'`, now)
	return err
}

func verifyServiceMonitorScheduleBindings(ctx context.Context, tx *sql.Tx, claim runtime.ServiceMonitorClaim, now int64) error {
	var serviceStatus, resourceStatus, workerState, executionID, connectionID, instanceID, manifestDigest, installEvidence, artifactDigest, semantic, semanticProbeJSON string
	var serviceRevision, deploymentRevision, workerEpoch, workerLease int64
	err := tx.QueryRowContext(ctx, `SELECT service.service_status,service.revision,deployment.revision,resource.resource_status,
		observation.worker_session_state,observation.worker_lease_epoch,observation.worker_lease_expires_at,
		source.execution_id,source.cloud_connection_id,source.instance_id,source.recipe_execution_manifest_digest,source.install_evidence_digest,source.artifact_digest,source.semantic_expectation_digest,source.semantic_probe_json
		FROM p2p_cloud_services service JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=service.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=service.deployment_id
		JOIN p2p_cloud_service_readiness_tasks source ON source.service_id=service.service_id AND source.purpose='install' AND source.task_status='succeeded'
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=source.execution_id AND manifest.manifest_digest=source.recipe_execution_manifest_digest AND manifest.status='approved'
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=source.execution_id AND install.task_status='succeeded'
		WHERE service.service_id=$1 AND service.deployment_id=$2 AND service.service_status IN('active','experimental','degraded')
		AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded' AND deployment.resource_status IN('active','retained_tracked')
		AND resource.resource_status IN('active','retained_tracked') FOR UPDATE OF service,deployment,resource,observation`, claim.ServiceID, claim.DeploymentID).Scan(
		&serviceStatus, &serviceRevision, &deploymentRevision, &resourceStatus, &workerState, &workerEpoch, &workerLease,
		&executionID, &connectionID, &instanceID, &manifestDigest, &installEvidence, &artifactDigest, &semantic, &semanticProbeJSON)
	semanticProbe, probeErr := decodeServiceReadinessProbe(semanticProbeJSON)
	if err != nil || serviceStatus != claim.ServiceStatus || serviceRevision != claim.ServiceRevision || deploymentRevision != claim.DeploymentRevision ||
		resourceStatus != claim.ResourceStatus || workerState != "active" || workerEpoch != claim.WorkerLeaseEpoch || workerLease <= now ||
		executionID != claim.ExecutionID || connectionID != claim.ConnectionID || instanceID != claim.InstanceID || manifestDigest != claim.ManifestDigest ||
		installEvidence != claim.InstallEvidenceDigest || artifactDigest != claim.ArtifactDigest || semantic != claim.SemanticExpectationDigest || probeErr != nil || semanticProbe != claim.SemanticProbe {
		return ErrLeaseLost
	}
	return nil
}

func verifyServiceMonitorReadinessBindings(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, now int64) error {
	var executionID, deploymentID, serviceID, connectionID, instanceID, manifestDigest, installEvidence, artifactDigest, semantic, semanticProbeJSON, taskStatus string
	var purpose, restoreID, jobID, monitorResource, serviceStatus, deploymentExecution, deploymentOutcome, deploymentResource, resourceStatus string
	var connectionStatus, connectionRegion, brokerRegion, endpoint, nodeKey, workerState, installStatus, monitorStatus, currentTask string
	var taskMonitorGeneration, taskServiceRevision, taskDeploymentRevision, taskWorkerEpoch int64
	var currentServiceRevision, currentDeploymentRevision, observationWorkerEpoch, workerLease, brokerGeneration, ledgerGeneration int64
	err := tx.QueryRowContext(ctx, `SELECT task.execution_id,task.deployment_id,task.service_id,task.cloud_connection_id,task.instance_id,
		task.recipe_execution_manifest_digest,task.install_evidence_digest,task.artifact_digest,task.semantic_expectation_digest,task.semantic_probe_json,task.task_status,task.purpose,task.restore_id,task.job_id,
		task.monitor_generation,task.monitor_service_revision,task.monitor_deployment_revision,task.monitor_resource_status,task.worker_lease_epoch,
		service.service_status,service.revision,deployment.execution_status,deployment.outcome_status,deployment.resource_status,deployment.revision,resource.resource_status,
		connection.status,connection.region,broker.broker_region,broker.broker_command_url,broker.node_key_id,broker.connection_generation,
		observation.worker_session_state,observation.worker_lease_epoch,observation.worker_lease_expires_at,install.task_status,monitor.monitor_status,monitor.current_task_id,monitor.generation
		FROM p2p_cloud_service_readiness_tasks task
		JOIN p2p_cloud_services service ON service.service_id=task.service_id AND service.deployment_id=task.deployment_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=task.deployment_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=task.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=task.execution_id AND manifest.manifest_digest=task.recipe_execution_manifest_digest AND manifest.status='approved'
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=task.execution_id
		JOIN p2p_cloud_service_monitors monitor ON monitor.service_id=task.service_id
		WHERE task.task_id=$1 FOR UPDATE OF task,service,deployment,resource,connection,broker,observation,monitor`, claim.TaskID).Scan(
		&executionID, &deploymentID, &serviceID, &connectionID, &instanceID, &manifestDigest, &installEvidence, &artifactDigest, &semantic, &semanticProbeJSON, &taskStatus, &purpose, &restoreID, &jobID,
		&taskMonitorGeneration, &taskServiceRevision, &taskDeploymentRevision, &monitorResource, &taskWorkerEpoch,
		&serviceStatus, &currentServiceRevision, &deploymentExecution, &deploymentOutcome, &deploymentResource, &currentDeploymentRevision, &resourceStatus,
		&connectionStatus, &connectionRegion, &brokerRegion, &endpoint, &nodeKey, &brokerGeneration,
		&workerState, &observationWorkerEpoch, &workerLease, &installStatus, &monitorStatus, &currentTask, &ledgerGeneration)
	semanticProbe, probeErr := decodeServiceReadinessProbe(semanticProbeJSON)
	if err != nil || executionID != claim.ExecutionID || deploymentID != claim.DeploymentID || serviceID != claim.ServiceID || connectionID != claim.ConnectionID || instanceID != claim.InstanceID ||
		manifestDigest != claim.RecipeExecutionManifestDigest || installEvidence != claim.InstallEvidenceDigest || artifactDigest != claim.ArtifactDigest || semantic != claim.SemanticExpectationDigest ||
		probeErr != nil || semanticProbe != claim.SemanticProbe ||
		purpose != "monitor" || restoreID != "" || jobID != "" || taskMonitorGeneration != claim.MonitorGeneration || ledgerGeneration != claim.MonitorGeneration || taskServiceRevision != claim.MonitorServiceRevision || currentServiceRevision != claim.MonitorServiceRevision || taskDeploymentRevision != claim.MonitorDeploymentRevision || currentDeploymentRevision != claim.MonitorDeploymentRevision || monitorResource != claim.MonitorResourceStatus || taskWorkerEpoch != claim.WorkerLeaseEpoch || observationWorkerEpoch != claim.WorkerLeaseEpoch ||
		(serviceStatus != "active" && serviceStatus != "experimental" && serviceStatus != "degraded") || deploymentExecution != "finished" || deploymentOutcome != "succeeded" || (deploymentResource != "active" && deploymentResource != "retained_tracked") || resourceStatus != claim.MonitorResourceStatus ||
		connectionStatus != "active" || connectionRegion != brokerRegion || endpoint != claim.BrokerEndpoint || nodeKey != claim.NodeKeyID || brokerGeneration != claim.ExpectedGeneration || workerState != "active" || workerLease <= now || installStatus != "succeeded" || monitorStatus != "checking" || currentTask != claim.TaskID {
		return ErrLeaseLost
	}
	if claim.Phase == runtime.ServiceReadinessPhaseIssue && taskStatus != "unissued" {
		return ErrLeaseLost
	}
	if claim.Phase == runtime.ServiceReadinessPhaseObserve && taskStatus != "queued" && taskStatus != "running" {
		return ErrLeaseLost
	}
	return nil
}

func commitServiceMonitorReadinessResult(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, result runtime.ServiceReadinessResult, now int64) error {
	var generation int64
	var currentTask, healthyStatus string
	var degradedByMonitor bool
	var failures int
	if err := tx.QueryRowContext(ctx, `SELECT generation,current_task_id,healthy_service_status,degraded_by_monitor,consecutive_failures
		FROM p2p_cloud_service_monitors WHERE service_id=$1 FOR UPDATE`, claim.ServiceID).Scan(&generation, &currentTask, &healthyStatus, &degradedByMonitor, &failures); err != nil {
		return err
	}
	if generation != claim.MonitorGeneration || currentTask != claim.TaskID {
		return ErrLeaseLost
	}
	if result.Status == "queued" || result.Status == "running" {
		update, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET monitor_status='checking',last_error_code='',updated_at=$1 WHERE service_id=$2 AND generation=$3 AND current_task_id=$4`, now, claim.ServiceID, claim.MonitorGeneration, claim.TaskID)
		if err != nil {
			return err
		}
		return requireOneAffected(update)
	}
	if result.Status == "succeeded" {
		if degradedByMonitor {
			if healthyStatus != "active" && healthyStatus != "experimental" {
				return ErrLeaseLost
			}
			var currentStatus string
			if err := tx.QueryRowContext(ctx, `SELECT service_status FROM p2p_cloud_services WHERE service_id=$1 FOR UPDATE`, claim.ServiceID).Scan(&currentStatus); err != nil {
				return err
			}
			if currentStatus != "degraded" {
				return ErrLeaseLost
			}
			if _, err := transitionServiceDestroyStatus(ctx, tx, claim.ServiceID, now, healthyStatus); err != nil {
				return err
			}
		}
		if err := acknowledgeServiceMonitorAlert(ctx, tx, claim, now); err != nil {
			return err
		}
		update, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET monitor_status='idle',current_task_id='',healthy_service_status='',degraded_by_monitor=FALSE,
			consecutive_failures=0,next_check_at=$1,last_success_at=$2,lease_owner='',lease_token='',lease_until=0,last_error_code='',updated_at=$2
			WHERE service_id=$3 AND generation=$4 AND current_task_id=$5`, time.UnixMilli(now).Add(serviceMonitorHealthyInterval).UnixMilli(), now, claim.ServiceID, claim.MonitorGeneration, claim.TaskID)
		if err != nil {
			return err
		}
		return requireOneAffected(update)
	}
	if result.Status != "failed" && result.Status != "interrupted" {
		return errors.New("service monitor terminal status is invalid")
	}
	var currentStatus string
	if err := tx.QueryRowContext(ctx, `SELECT service_status FROM p2p_cloud_services WHERE service_id=$1 FOR UPDATE`, claim.ServiceID).Scan(&currentStatus); err != nil {
		return err
	}
	if currentStatus == "active" || currentStatus == "experimental" {
		healthyStatus = currentStatus
		degradedByMonitor = true
		if _, err := transitionServiceDestroyStatus(ctx, tx, claim.ServiceID, now, "degraded"); err != nil {
			return err
		}
	} else if currentStatus != "degraded" {
		return ErrLeaseLost
	}
	if err := raiseServiceMonitorAlert(ctx, tx, claim, now); err != nil {
		return err
	}
	failures++
	delay := serviceMonitorFailureDelay(failures)
	errorCode := durableErrorCode(derefReadiness(result.ErrorCode), serviceMonitorAlertCode)
	update, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_monitors SET monitor_status='idle',current_task_id='',healthy_service_status=$1,degraded_by_monitor=$2,
		consecutive_failures=$3,next_check_at=$4,last_failure_at=$5,lease_owner='',lease_token='',lease_until=0,last_error_code=$6,updated_at=$5
		WHERE service_id=$7 AND generation=$8 AND current_task_id=$9`, healthyStatus, degradedByMonitor, failures, time.UnixMilli(now).Add(delay).UnixMilli(), now, errorCode, claim.ServiceID, claim.MonitorGeneration, claim.TaskID)
	if err != nil {
		return err
	}
	return requireOneAffected(update)
}

func serviceMonitorFailureDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := serviceMonitorRetryBase
	for i := 1; i < failures && delay < serviceMonitorRetryMax; i++ {
		delay *= 2
		if delay >= serviceMonitorRetryMax {
			return serviceMonitorRetryMax
		}
	}
	return delay
}

func raiseServiceMonitorAlert(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, now int64) error {
	alertID := stableID("cloud_alert_", claim.ServiceID, serviceMonitorAlertCode)
	message := "Continuous semantic readiness monitoring failed. The retained cloud resources remain active and billable; no resource was stopped or destroyed."
	var revision, createdAt int64
	err := tx.QueryRowContext(ctx, `SELECT revision,created_at FROM p2p_cloud_alerts WHERE alert_id=$1 FOR UPDATE`, alertID).Scan(&revision, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		revision, createdAt = 1, now
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_alerts(alert_id,deployment_id,service_id,severity,code,message,acknowledged,revision,created_at,updated_at)
			VALUES($1,$2,$3,'warning',$4,$5,FALSE,$6,$7,$8)`, alertID, claim.DeploymentID, claim.ServiceID, serviceMonitorAlertCode, message, revision, createdAt, now); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		revision++
		result, updateErr := tx.ExecContext(ctx, `UPDATE p2p_cloud_alerts SET severity='warning',code=$1,message=$2,acknowledged=FALSE,revision=$3,updated_at=$4 WHERE alert_id=$5 AND revision=$6`, serviceMonitorAlertCode, message, revision, now, alertID, revision-1)
		if updateErr != nil {
			return updateErr
		}
		if updateErr = requireOneAffected(result); updateErr != nil {
			return updateErr
		}
	}
	payload := map[string]any{"alert_id": alertID, "deployment_id": claim.DeploymentID, "service_id": claim.ServiceID, "severity": "warning", "code": serviceMonitorAlertCode, "message": message, "acknowledged": false, "revision": revision, "created_at": createdAt, "updated_at": now}
	return writeEventAndProjection(ctx, tx, stableID("cloud_event_", alertID, fmt.Sprint(revision), "raised"), "cloud.alert.raised", "alert", alertID, revision, payload, now)
}

func acknowledgeServiceMonitorAlert(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, now int64) error {
	alertID := stableID("cloud_alert_", claim.ServiceID, serviceMonitorAlertCode)
	var revision, createdAt int64
	var acknowledged bool
	var message string
	err := tx.QueryRowContext(ctx, `SELECT revision,created_at,acknowledged,message FROM p2p_cloud_alerts WHERE alert_id=$1 FOR UPDATE`, alertID).Scan(&revision, &createdAt, &acknowledged, &message)
	if errors.Is(err, sql.ErrNoRows) || acknowledged {
		return nil
	}
	if err != nil {
		return err
	}
	previous := revision
	revision++
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_alerts SET acknowledged=TRUE,revision=$1,updated_at=$2 WHERE alert_id=$3 AND revision=$4 AND acknowledged=FALSE`, revision, now, alertID, previous)
	if err != nil {
		return err
	}
	if err = requireOneAffected(result); err != nil {
		return err
	}
	payload := map[string]any{"alert_id": alertID, "deployment_id": claim.DeploymentID, "service_id": claim.ServiceID, "severity": "warning", "code": serviceMonitorAlertCode, "message": message, "acknowledged": true, "revision": revision, "created_at": createdAt, "updated_at": now}
	return writeEventAndProjection(ctx, tx, stableID("cloud_event_", alertID, fmt.Sprint(revision), "acknowledged"), "cloud.alert.raised", "alert", alertID, revision, payload, now)
}
