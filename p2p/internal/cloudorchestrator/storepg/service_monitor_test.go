package storepg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreServiceMonitorLeaseRecoveryAndHealthyRound(t *testing.T) {
	ctx := context.Background()
	clock, database, store, serviceID := seedServiceMonitorTarget(t)
	store.cfg.Now = func() time.Time { return clock }
	leaseNumber := 0
	store.cfg.NewLeaseToken = func() string {
		leaseNumber++
		return "monitor-lease-" + string(rune('0'+leaseNumber))
	}

	stale, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler-a", time.Minute)
	if err != nil || !found || stale.Generation != 1 {
		t.Fatalf("initial monitor claim=%#v found=%v err=%v", stale, found, err)
	}
	clock = clock.Add(2 * time.Minute)
	recovered, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler-b", time.Minute)
	if err != nil || !found || recovered.TaskID != stale.TaskID || recovered.OutboxID != stale.OutboxID || recovered.Generation != stale.Generation || recovered.LeaseToken == stale.LeaseToken {
		t.Fatalf("recovered monitor claim=%#v found=%v err=%v", recovered, found, err)
	}
	if err := store.ScheduleServiceMonitor(ctx, stale); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale schedule error=%v, want %v", err, ErrLeaseLost)
	}
	if err := store.ScheduleServiceMonitor(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	observe := runServiceMonitorRound(t, store, &clock, "succeeded")
	if err := store.CommitServiceReadiness(ctx, observe, monitorReadinessResult(observe, clock, "failed")); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("changed old result error=%v, want %v", err, ErrLeaseLost)
	}
	var status, monitorStatus, currentTask string
	var generation, lastSuccess, nextCheck int64
	if err := database.DB().QueryRowContext(ctx, `SELECT service.service_status,monitor.monitor_status,monitor.current_task_id,monitor.generation,monitor.last_success_at,monitor.next_check_at
		FROM p2p_cloud_services service JOIN p2p_cloud_service_monitors monitor ON monitor.service_id=service.service_id WHERE service.service_id=$1`, serviceID).Scan(&status, &monitorStatus, &currentTask, &generation, &lastSuccess, &nextCheck); err != nil {
		t.Fatal(err)
	}
	if status != "experimental" || monitorStatus != "idle" || currentTask != "" || generation != 1 || lastSuccess != clock.UnixMilli() || nextCheck != clock.Add(serviceMonitorHealthyInterval).UnixMilli() {
		t.Fatalf("service=%s monitor=%s/%s generation=%d last_success=%d next=%d", status, monitorStatus, currentTask, generation, lastSuccess, nextCheck)
	}
}

func TestStoreServiceMonitorFailureRaisesAlertAndSuccessRecoversOnlyMonitorDegradation(t *testing.T) {
	ctx := context.Background()
	clock, database, store, serviceID := seedServiceMonitorTarget(t)
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return "monitor-lease-" + strings.ReplaceAll(clock.Format("150405"), ":", "") }
	first, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler", time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim found=%v err=%v", found, err)
	}
	if err = store.ScheduleServiceMonitor(ctx, first); err != nil {
		t.Fatal(err)
	}
	failedObserve := runServiceMonitorRound(t, store, &clock, "failed")
	var serviceStatus, resourceStatus string
	var serviceRevision, alertRevision int64
	var acknowledged bool
	if err = database.DB().QueryRowContext(ctx, `SELECT service_status,revision FROM p2p_cloud_services WHERE service_id=$1`, serviceID).Scan(&serviceStatus, &serviceRevision); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, failedObserve.DeploymentID).Scan(&resourceStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT revision,acknowledged FROM p2p_cloud_alerts WHERE service_id=$1 AND code=$2`, serviceID, serviceMonitorAlertCode).Scan(&alertRevision, &acknowledged); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "degraded" || serviceRevision != 2 || resourceStatus != "active" || alertRevision != 1 || acknowledged {
		t.Fatalf("service=%s/%d resource=%s alert=%d/%v", serviceStatus, serviceRevision, resourceStatus, alertRevision, acknowledged)
	}

	clock = clock.Add(2 * time.Minute)
	second, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler", time.Minute)
	if err != nil || !found || second.Generation != 2 || second.TaskID == first.TaskID || second.OutboxID == first.OutboxID {
		t.Fatalf("second claim=%#v found=%v err=%v", second, found, err)
	}
	if err = store.ScheduleServiceMonitor(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitServiceReadiness(ctx, failedObserve, monitorReadinessResult(failedObserve, clock, "succeeded")); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old generation result error=%v, want %v", err, ErrLeaseLost)
	}
	runServiceMonitorRound(t, store, &clock, "succeeded")
	if err = database.DB().QueryRowContext(ctx, `SELECT service_status,revision FROM p2p_cloud_services WHERE service_id=$1`, serviceID).Scan(&serviceStatus, &serviceRevision); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT revision,acknowledged FROM p2p_cloud_alerts WHERE service_id=$1 AND code=$2`, serviceID, serviceMonitorAlertCode).Scan(&alertRevision, &acknowledged); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "experimental" || serviceRevision != 3 || alertRevision != 2 || !acknowledged {
		t.Fatalf("recovered service=%s/%d alert=%d/%v", serviceStatus, serviceRevision, alertRevision, acknowledged)
	}
	var serviceEvents, alertEvents int
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE aggregate_type='service' AND aggregate_id=$1`, serviceID).Scan(&serviceEvents); err != nil {
		t.Fatal(err)
	}
	alertID := stableID("cloud_alert_", serviceID, serviceMonitorAlertCode)
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE aggregate_type='alert' AND aggregate_id=$1 AND type='cloud.alert.raised'`, alertID).Scan(&alertEvents); err != nil {
		t.Fatal(err)
	}
	if serviceEvents != 2 || alertEvents != 2 {
		t.Fatalf("service events=%d alert events=%d", serviceEvents, alertEvents)
	}
}

func TestStoreServiceMonitorFailsClosedOnLifecycleStateDrift(t *testing.T) {
	ctx := context.Background()
	clock, database, store, serviceID := seedServiceMonitorTarget(t)
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return "monitor-state-drift-lease" }
	monitor, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler", time.Minute)
	if err != nil || !found {
		t.Fatalf("monitor claim found=%v err=%v", found, err)
	}
	if err = store.ScheduleServiceMonitor(ctx, monitor); err != nil {
		t.Fatal(err)
	}
	issue, found, err := store.ClaimServiceReadiness(ctx, "monitor-readiness", time.Minute)
	if err != nil || !found || issue.Purpose != "monitor" {
		t.Fatalf("readiness claim=%#v found=%v err=%v", issue, found, err)
	}
	if err = store.PersistServiceReadinessCommand(ctx, issue, signedServiceReadinessCommand(t, issue, clock)); err != nil {
		t.Fatal(err)
	}
	if _, err = database.DB().ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='stopped',revision=revision+1 WHERE service_id=$1`, serviceID); err != nil {
		t.Fatal(err)
	}
	queued := runtime.ServiceReadinessResult{ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, TaskID: issue.TaskID, Status: "queued", Attempt: issue.TaskAttempt, UpdatedAt: clock}
	if err = store.CommitServiceReadiness(ctx, issue, queued); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("state drift commit error=%v, want %v", err, ErrLeaseLost)
	}
	clock = clock.Add(2 * time.Minute)
	if _, found, err = store.ClaimServiceMonitor(ctx, "monitor-reconciler", time.Minute); err != nil || found {
		t.Fatalf("stopped service monitor found=%v err=%v", found, err)
	}
	var taskStatus string
	var outboxCompleted int64
	if err = database.DB().QueryRowContext(ctx, `SELECT task.task_status,outbox.completed_at FROM p2p_cloud_service_readiness_tasks task
		JOIN p2p_cloud_outbox outbox ON outbox.aggregate_type='service_readiness_task' AND outbox.aggregate_id=task.task_id WHERE task.task_id=$1`, issue.TaskID).Scan(&taskStatus, &outboxCompleted); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "interrupted" || outboxCompleted == 0 {
		t.Fatalf("stale task=%s outbox_completed=%d", taskStatus, outboxCompleted)
	}
	var alerts int
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_alerts WHERE service_id=$1`, serviceID).Scan(&alerts); err != nil || alerts != 0 {
		t.Fatalf("alerts=%d err=%v", alerts, err)
	}
}

func TestStoreServiceMonitorDoesNotRecoverUnrelatedDegradation(t *testing.T) {
	ctx := context.Background()
	clock, database, store, serviceID := seedServiceMonitorTarget(t)
	if _, err := database.DB().ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='degraded',revision=revision+1 WHERE service_id=$1`, serviceID); err != nil {
		t.Fatal(err)
	}
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return "monitor-unrelated-degradation-lease" }
	monitor, found, err := store.ClaimServiceMonitor(ctx, "monitor-scheduler", time.Minute)
	if err != nil || !found || monitor.ServiceStatus != "degraded" {
		t.Fatalf("monitor=%#v found=%v err=%v", monitor, found, err)
	}
	if err = store.ScheduleServiceMonitor(ctx, monitor); err != nil {
		t.Fatal(err)
	}
	runServiceMonitorRound(t, store, &clock, "succeeded")
	var status string
	var revision int64
	if err = database.DB().QueryRowContext(ctx, `SELECT service_status,revision FROM p2p_cloud_services WHERE service_id=$1`, serviceID).Scan(&status, &revision); err != nil {
		t.Fatal(err)
	}
	if status != "degraded" || revision != 2 {
		t.Fatalf("unrelated degradation changed to %s/%d", status, revision)
	}
}

func seedServiceMonitorTarget(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, string) {
	t.Helper()
	ctx := context.Background()
	now, database, store, bootstrap := prepareExecutionProbeTask(t)
	manifest, jobID, _ := seedApprovedRecipeInstall(t, ctx, database, bootstrap, now)
	clock := now.Add(3 * time.Minute)
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return "seed-monitor-install-lease" }
	install, found, err := store.ClaimRecipeInstall(ctx, "seed-monitor-install", time.Minute)
	if err != nil || !found {
		t.Fatalf("install claim found=%v err=%v", found, err)
	}
	tx, err := database.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = ensureServiceReadinessTask(ctx, tx, install, clock.UnixMilli()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	serviceID := stableID("cloud_service_", manifest.DeploymentID, manifest.ExecutionID, install.ManifestDigest)
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status='succeeded',last_checkpoint=$1,lease_owner='',lease_token='',lease_until=0,updated_at=$2 WHERE execution_id=$3`, manifest.CheckpointSequence[len(manifest.CheckpointSequence)-1], clock.UnixMilli(), manifest.ExecutionID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET task_status='succeeded',checkpoint=$1,last_sequence=1,updated_at=$2 WHERE service_id=$3 AND purpose='install'`, runtime.ServiceReadinessVerified, clock.UnixMilli(), serviceID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='succeeded',checkpoint='readiness_verified',updated_at=$1 WHERE job_id=$2`, clock.UnixMilli(), jobID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployments SET execution_status='finished',outcome_status='succeeded',resource_status='active',updated_at=$1 WHERE deployment_id=$2`, clock.UnixMilli(), manifest.DeploymentID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at)
		VALUES($1,$2,'recipe-monitor-1','Monitor target','experimental','not_requested',1,$3,$3)`, serviceID, manifest.DeploymentID, clock.UnixMilli()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_worker_bootstrap_observations SET worker_session_state='active',worker_lease_epoch=3,worker_lease_expires_at=$1,updated_at=$2 WHERE deployment_id=$3`, clock.Add(time.Hour).UnixMilli(), clock.UnixMilli(), manifest.DeploymentID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return clock, database, store, serviceID
}

func runServiceMonitorRound(t *testing.T, store *Store, clock *time.Time, terminal string) runtime.ServiceReadinessClaim {
	t.Helper()
	ctx := context.Background()
	issue, found, err := store.ClaimServiceReadiness(ctx, "monitor-readiness-issue", time.Minute)
	if err != nil || !found || issue.Phase != runtime.ServiceReadinessPhaseIssue || issue.Purpose != "monitor" {
		t.Fatalf("issue=%#v found=%v err=%v", issue, found, err)
	}
	if err = store.PersistServiceReadinessCommand(ctx, issue, signedServiceReadinessCommand(t, issue, *clock)); err != nil {
		t.Fatal(err)
	}
	queued := runtime.ServiceReadinessResult{ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, TaskID: issue.TaskID, Status: "queued", Attempt: issue.TaskAttempt, UpdatedAt: *clock}
	if err = store.CommitServiceReadiness(ctx, issue, queued); err != nil {
		t.Fatal(err)
	}
	*clock = (*clock).Add(6 * time.Second)
	observe, found, err := store.ClaimServiceReadiness(ctx, "monitor-readiness-observe", time.Minute)
	if err != nil || !found || observe.Phase != runtime.ServiceReadinessPhaseObserve || observe.Purpose != "monitor" {
		t.Fatalf("observe=%#v found=%v err=%v", observe, found, err)
	}
	if err = store.PersistServiceReadinessCommand(ctx, observe, signedServiceReadinessCommand(t, observe, *clock)); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitServiceReadiness(ctx, observe, monitorReadinessResult(observe, *clock, terminal)); err != nil {
		t.Fatal(err)
	}
	return observe
}

func monitorReadinessResult(claim runtime.ServiceReadinessClaim, now time.Time, status string) runtime.ServiceReadinessResult {
	result := runtime.ServiceReadinessResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID, TaskID: claim.TaskID, Status: status, Attempt: claim.TaskAttempt, LastSequence: 1, UpdatedAt: now}
	if status == "succeeded" {
		challenge := "sha256:" + strings.Repeat("e", 64)
		semantic := cloudcontracts.FixedReadinessEvidenceDigestV1
		stack := "sha256:" + strings.Repeat("f", 64)
		result.Checkpoint, result.ChallengeDigest, result.SemanticEvidenceDigest, result.StackObservationDigest = runtime.ServiceReadinessVerified, &challenge, &semantic, &stack
	} else {
		code := "semantic_probe_failed"
		result.ErrorCode = &code
	}
	return result
}
