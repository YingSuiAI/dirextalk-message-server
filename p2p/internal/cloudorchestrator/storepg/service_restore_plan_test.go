package storepg

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	"testing"
	"time"
)

func TestServiceRestorePlanStoreCommitsExactBrokerReadback(t *testing.T) {
	now, database, store, backupClaim := prepareServiceBackupClaim(t)
	ctx := context.Background()
	snapshots, _ := json.Marshal([]broker.ServiceBackupSnapshot{{VolumeID: backupClaim.Request.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}})
	refs, _ := json.Marshal([]broker.ServiceRestoreSnapshotRef{{OriginalVolumeID: backupClaim.Request.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0"}})
	ts := now.UnixMilli()
	queries := []struct {
		q string
		a []any
	}{{`UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0 WHERE outbox_id=$2`, []any{ts, backupClaim.OutboxID}}, {`UPDATE p2p_cloud_service_backups SET backup_status='available',image_id='ami-0123456789abcdef0',snapshots_json=$1,revision=2 WHERE backup_id=$2`, []any{string(snapshots), backupClaim.BackupID}}, {`UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='succeeded' WHERE job_id=$1`, []any{backupClaim.JobID}}, {`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-restore-plan-runtime-0001','plan-backup-runtime-0001','deployment-backup-runtime-0001','restore_plan','queued','pending','restore_plan_queued','',1,$1,$1)`, []any{ts}}, {`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-restore-plan-runtime-0001','restore_plan','queued','queued','restore_plan_queued','',1,$1,$1)`, []any{ts}}, {`INSERT INTO p2p_cloud_service_restore_plans(restore_plan_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,backup_id,backup_revision,recipe_id,recipe_digest,instance_id,region,image_id,snapshot_refs_json,plan_status,job_id,idempotency_hash,request_digest,created_at,updated_at)VALUES('restore-plan-runtime-0001','@owner:example.com','service-backup-runtime-0001',2,'deployment-backup-runtime-0001',5,'plan-backup-runtime-0001','connection-backup-runtime-0001','backup-runtime-0001',2,'recipe-backup-runtime-0001','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','i-0123456789abcdef0','ap-south-1','ami-0123456789abcdef0',$1,'planning','job-restore-plan-runtime-0001','restore-plan-runtime-idem','restore-plan-runtime-request',$2,$2)`, []any{string(refs), ts}}, {`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at)VALUES('outbox-restore-plan-runtime-0001','cloud.service.restore.plan.requested','service_restore_plan','restore-plan-runtime-0001','{}',$1)`, []any{ts}}}
	for _, q := range queries {
		if _, e := database.DB().ExecContext(ctx, q.q, q.a...); e != nil {
			t.Fatal(e)
		}
	}
	claim, found, e := store.ClaimServiceRestorePlan(ctx, "restore-plan-runtime", time.Minute)
	if e != nil || !found {
		t.Fatalf("found=%v err=%v", found, e)
	}
	command, e := broker.NewServiceRestorePlanCommand(broker.ServiceRestorePlanCommandInput{ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: claim.Request, PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x64}, 32))})
	if e != nil {
		t.Fatal(e)
	}
	envelope, _ := json.Marshal(command)
	payload, _ := command.Request()
	payloadJSON, _ := json.Marshal(payload)
	signed := runtime.SignedServiceRestorePlanCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payloadJSON), PayloadSHA256: command.PayloadSHA256, RequestSHA256: command.RequestSHA256(), IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute)}
	if e = store.PersistServiceRestorePlanCommand(ctx, claim, signed); e != nil {
		t.Fatal(e)
	}
	plan := broker.ServiceRestorePlan{Schema: broker.ServiceRestorePlanSchema, RestorePlanID: claim.RestorePlanID, ConnectionID: claim.ConnectionID, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, BackupID: claim.BackupID, InstanceID: claim.Request.InstanceID, Region: claim.Region, AvailabilityZone: "ap-south-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "restore-quote-runtime-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuotedAt: "2026-07-15T18:00:00.000Z", ValidUntil: "2026-07-15T18:15:00.000Z", Unincluded: []string{"taxes"}, VolumeSwaps: []broker.ServiceRestoreVolumeSwap{{OriginalVolumeID: claim.Request.SnapshotRefs[0].OriginalVolumeID, SnapshotID: claim.Request.SnapshotRefs[0].SnapshotID, DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}
	receipt := broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: claim.ConnectionID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), Action: broker.ServiceRestorePlanAction}
	receiptJSON, _ := json.Marshal(receipt)
	result := runtime.ServiceRestorePlanResult{Status: "restore_plan_ready", Plan: plan, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), ReceiptJSON: string(receiptJSON)}
	if e = store.CompleteServiceRestorePlan(ctx, claim, result); e != nil {
		t.Fatal(e)
	}
	var status, jobExecution, jobOutcome, checkpoint string
	if e = database.DB().QueryRow(`SELECT restore.plan_status,job.execution_status,job.outcome_status,job.checkpoint FROM p2p_cloud_service_restore_plans restore JOIN p2p_cloud_jobs job ON job.job_id=restore.job_id WHERE restore.restore_plan_id=$1`, claim.RestorePlanID).Scan(&status, &jobExecution, &jobOutcome, &checkpoint); e != nil {
		t.Fatal(e)
	}
	if status != "ready_for_confirmation" || jobExecution != "finished" || jobOutcome != "succeeded" || checkpoint != "restore_plan_ready" {
		t.Fatalf("status=%s job=%s/%s checkpoint=%s", status, jobExecution, jobOutcome, checkpoint)
	}
}
