package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreServiceRestoreReusesSignedCommandAndQueuesSemanticVerification(t *testing.T) {
	now, database, store, claim := prepareServiceRestoreClaim(t, true)
	_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(nodePrivate, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceRestoreCommand(claim.Command, claim.Request, claim.Approval)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PersistServiceRestoreCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkServiceRestoreStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err = store.DeferServiceRestore(context.Background(), claim, "broker_timeout", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	store.cfg.Now = func() time.Time { return now.Add(2 * time.Second) }
	retry, found, err := store.ClaimServiceRestore(context.Background(), "restore-runtime-retry", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry claim found=%v err=%v", found, err)
	}
	if retry.Command.CommandID != claim.Command.CommandID || retry.Command.NodeCounter != claim.Command.NodeCounter || retry.Command.SignedEnvelope != signed.EnvelopeJSON {
		t.Fatalf("retry allocated a different command: first=%+v retry=%+v", claim.Command, retry.Command)
	}
	if err = store.MarkServiceRestoreStarted(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	replacement := broker.ServiceRestoreReplacementVolume{OriginalVolumeID: retry.Request.VolumeSwaps[0].OriginalVolumeID, ReplacementVolumeID: "vol-0fedcba9876543210", SnapshotID: retry.Request.VolumeSwaps[0].SnapshotID, DeviceName: retry.Request.VolumeSwaps[0].DeviceName, State: "attached_current", Encrypted: true, DeleteOnTermination: retry.Request.VolumeSwaps[0].DeleteOnTermination}
	result := serviceRestoreResult(t, retry, signed, "aws_restore_applied", "restored", "running", false, replacement)
	if err = store.CompleteServiceRestore(context.Background(), retry, result); err != nil {
		t.Fatal(err)
	}
	var restoreStatus, jobExecution, jobOutcome, checkpoint, volumesJSON, commandState, readinessStatus, readinessPurpose string
	if err = database.DB().QueryRow(`SELECT restore_status FROM p2p_cloud_service_restores WHERE restore_id=$1`, retry.RestoreID).Scan(&restoreStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT execution_status,outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, retry.JobID).Scan(&jobExecution, &jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT volume_ids_json FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, retry.DeploymentID).Scan(&volumesJSON); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT state FROM p2p_cloud_service_restore_commands WHERE command_id=$1`, retry.Command.CommandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT task_status,purpose FROM p2p_cloud_service_readiness_tasks WHERE restore_id=$1`, retry.RestoreID).Scan(&readinessStatus, &readinessPurpose); err != nil {
		t.Fatal(err)
	}
	if restoreStatus != "verifying" || jobExecution != "verifying" || jobOutcome != "pending" || checkpoint != "restore_readiness_queued" || volumesJSON != `["vol-0fedcba9876543210"]` || commandState != "accepted" || readinessStatus != "unissued" || readinessPurpose != "restore" {
		t.Fatalf("restore=%s job=%s/%s/%s volumes=%s command=%s readiness=%s/%s", restoreStatus, jobExecution, jobOutcome, checkpoint, volumesJSON, commandState, readinessStatus, readinessPurpose)
	}
	clock := now.Add(2 * time.Second)
	store.cfg.Now = func() time.Time { return clock }
	issue, found, err := store.ClaimServiceReadiness(context.Background(), "restore-readiness-issue", time.Minute)
	if err != nil || !found || issue.Purpose != "restore" || issue.RestoreID != retry.RestoreID || issue.JobID != retry.JobID {
		t.Fatalf("restore readiness issue found=%v claim=%+v err=%v", found, issue, err)
	}
	readinessSigned := signedServiceReadinessCommand(t, issue, clock)
	if err = store.MarkServiceReadinessStarted(context.Background(), issue); err != nil {
		t.Fatal(err)
	}
	if err = store.PersistServiceReadinessCommand(context.Background(), issue, readinessSigned); err != nil {
		t.Fatal(err)
	}
	queued := runtime.ServiceReadinessResult{ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, TaskID: issue.TaskID, Status: "queued", Attempt: 1, UpdatedAt: clock}
	if err = store.CommitServiceReadiness(context.Background(), issue, queued); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(6 * time.Second)
	observe, found, err := store.ClaimServiceReadiness(context.Background(), "restore-readiness-observe", time.Minute)
	if err != nil || !found {
		t.Fatalf("restore readiness observe found=%v err=%v", found, err)
	}
	readinessSigned = signedServiceReadinessCommand(t, observe, clock)
	if err = store.PersistServiceReadinessCommand(context.Background(), observe, readinessSigned); err != nil {
		t.Fatal(err)
	}
	challenge := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	semantic := observe.SemanticExpectationDigest
	stack := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	verified := runtime.ServiceReadinessResult{ExecutionID: observe.ExecutionID, DeploymentID: observe.DeploymentID, ServiceID: observe.ServiceID, TaskID: observe.TaskID, Status: "succeeded", Checkpoint: runtime.ServiceReadinessVerified, Attempt: 1, LastSequence: 1, ChallengeDigest: &challenge, SemanticEvidenceDigest: &semantic, StackObservationDigest: &stack, UpdatedAt: clock}
	if err = store.CommitServiceReadiness(context.Background(), observe, verified); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT restore_status FROM p2p_cloud_service_restores WHERE restore_id=$1`, retry.RestoreID).Scan(&restoreStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, retry.JobID).Scan(&jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if restoreStatus != "succeeded" || jobOutcome != "succeeded" || checkpoint != "restore_readiness_verified" {
		t.Fatalf("verified restore=%s outcome=%s checkpoint=%s", restoreStatus, jobOutcome, checkpoint)
	}
	services, err := database.ListCloudServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || len(services[0].Restores) != 1 || services[0].Restores[0].Status != "succeeded" || len(services[0].Restores[0].OriginalVolumeIDs) != 1 || len(services[0].Restores[0].ReplacementVolumeIDs) != 1 {
		t.Fatalf("service restore projection=%+v", services)
	}
}

func TestStoreServiceRestoreFallbackAndBlockedNeverSucceed(t *testing.T) {
	for _, tc := range []struct {
		name, status, outcome, instanceState, replacementState, expectedRestore, expectedResource string
		fallbackVerified                                                                          bool
	}{
		{name: "original restored", status: "aws_original_restored", outcome: "original_restored", instanceState: "running", replacementState: "retained_detached", expectedRestore: "failed", expectedResource: "active", fallbackVerified: true},
		{name: "fallback blocked", status: "restore_blocked", outcome: "restore_blocked", replacementState: "unknown", expectedRestore: "restore_blocked", expectedResource: "blocked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, database, store, claim := prepareServiceRestoreClaim(t, false)
			_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			transport, err := brokertransport.New(nodePrivate, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			signed, err := transport.BuildServiceRestoreCommand(claim.Command, claim.Request, claim.Approval)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.PersistServiceRestoreCommand(context.Background(), claim, signed); err != nil {
				t.Fatal(err)
			}
			if err = store.MarkServiceRestoreStarted(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
			replacement := broker.ServiceRestoreReplacementVolume{OriginalVolumeID: claim.Request.VolumeSwaps[0].OriginalVolumeID, ReplacementVolumeID: "vol-0fedcba9876543210", SnapshotID: claim.Request.VolumeSwaps[0].SnapshotID, DeviceName: claim.Request.VolumeSwaps[0].DeviceName, State: tc.replacementState, Encrypted: true, DeleteOnTermination: claim.Request.VolumeSwaps[0].DeleteOnTermination}
			result := serviceRestoreResult(t, claim, signed, tc.status, tc.outcome, tc.instanceState, tc.fallbackVerified, replacement)
			if err = store.CompleteServiceRestore(context.Background(), claim, result); err != nil {
				t.Fatal(err)
			}
			var restoreStatus, jobOutcome, resourceStatus string
			if err = database.DB().QueryRow(`SELECT restore_status FROM p2p_cloud_service_restores WHERE restore_id=$1`, claim.RestoreID).Scan(&restoreStatus); err != nil {
				t.Fatal(err)
			}
			if err = database.DB().QueryRow(`SELECT outcome_status FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&jobOutcome); err != nil {
				t.Fatal(err)
			}
			if err = database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&resourceStatus); err != nil {
				t.Fatal(err)
			}
			if restoreStatus != tc.expectedRestore || jobOutcome != "failed" || resourceStatus != tc.expectedResource {
				t.Fatalf("restore=%s outcome=%s resource=%s", restoreStatus, jobOutcome, resourceStatus)
			}
		})
	}
}

func prepareServiceRestoreClaim(t *testing.T, readiness bool) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.ServiceRestoreClaim) {
	t.Helper()
	now, database, store, backupClaim := prepareServiceBackupClaim(t)
	ctx := context.Background()
	if _, err := database.DB().ExecContext(ctx, `UPDATE p2p_cloud_service_backups SET backup_status='available',image_id='ami-0123456789abcdef0',snapshots_json='[{"volume_id":"vol-0123456789abcdef0","snapshot_id":"snap-0123456789abcdef0","state":"completed","encrypted":true}]' WHERE backup_id=$1`, backupClaim.BackupID); err != nil {
		t.Fatal(err)
	}
	_, devicePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	target := cloudcontracts.ServiceRestoreTargetV1{RestoreID: "restore-runtime-0001", ServiceID: backupClaim.ServiceID, ServiceRevision: uint64(backupClaim.ServiceRevision), DeploymentID: backupClaim.DeploymentID, DeploymentRevision: uint64(backupClaim.DeploymentRevision), CloudConnectionID: backupClaim.ConnectionID, BackupID: backupClaim.BackupID, BackupRevision: 1, RecipeID: "recipe-backup-runtime-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: backupClaim.Request.InstanceID, Region: "ap-south-1", AvailabilityZone: "ap-south-1a", RestoreMode: cloudcontracts.ServiceRestoreModeInPlace, DowntimeRequired: true, OriginalVolumeRetention: cloudcontracts.ServiceRestoreRetentionManual, FailurePolicy: cloudcontracts.ServiceRestoreFailureReattachOriginal, QuoteID: "quote-restore-runtime-0001", Currency: "USD", EstimatedHourlyMinor: 12, EstimatedThirtyDayMinor: 8640, QuoteValidUntil: now.Add(10 * time.Minute), VolumeSwaps: []cloudcontracts.ServiceRestoreVolumeSwapV1{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/sda1", VolumeType: "gp3", SizeGiB: 20, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}}}
	approval, err := cloudcontracts.NewServiceRestoreApprovalV1(target, "approval-restore-runtime-0001", "challenge-restore-runtime-0001", "device-restore-runtime-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(devicePrivate, now)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := approval
	unsigned.Signature = ""
	approvalJSON, _ := json.Marshal(unsigned)
	signingPayload, _ := approval.SigningPayload()
	swapsJSON, _ := json.Marshal(target.VolumeSwaps)
	ts := now.UnixMilli()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_service_restore_plans(restore_plan_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,backup_id,backup_revision,recipe_id,recipe_digest,instance_id,region,image_id,snapshot_refs_json,plan_status,availability_zone,quote_id,currency,estimated_hourly_minor,estimated_thirty_day_minor,quoted_at,valid_until,volume_swaps_json,job_id,idempotency_hash,request_digest,created_at,updated_at) VALUES($1,'@owner:example.com',$2,2,$3,5,'plan-backup-runtime-0001',$4,$5,1,$6,$7,$8,'ap-south-1','ami-0123456789abcdef0','[]','approved','ap-south-1a',$9,'USD',12,8640,$10,$11,$12,'job-restore-plan-runtime-0001','restore-plan-idem','restore-plan-request',$10,$10)`, []any{target.RestoreID, target.ServiceID, target.DeploymentID, target.CloudConnectionID, target.BackupID, target.RecipeID, target.RecipeDigest, target.InstanceID, target.QuoteID, ts, target.QuoteValidUntil.UnixMilli(), string(swapsJSON)}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES('job-restore-runtime-0001','plan-backup-runtime-0001',$1,'restore','queued','pending','restore_queued','',1,$2,$2)`, []any{target.DeploymentID, ts}},
		{`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES('job-restore-runtime-0001','restore','queued','queued','restore_queued','',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_service_restore_approvals(approval_id,challenge_id,owner_mxid,restore_plan_id,restore_plan_revision,service_id,service_revision,deployment_id,deployment_revision,backup_id,backup_revision,cloud_connection_id,signer_key_id,approval_json,signing_payload,service_json,deployment_json,restore_plan_json,status,prepare_idempotency_hash,prepare_request_digest,approve_idempotency_hash,approve_request_digest,signature,job_id,expires_at,created_at,updated_at) VALUES($1,$2,'@owner:example.com',$3,1,$4,2,$5,5,$6,1,$7,$8,$9,$10,'{}','{}','{}','approved','restore-prepare-idem','restore-prepare-request','restore-approve-idem','restore-approve-request',$11,'job-restore-runtime-0001',$12,$13,$13)`, []any{approval.ApprovalID, approval.ChallengeID, target.RestoreID, target.ServiceID, target.DeploymentID, target.BackupID, target.CloudConnectionID, approval.SignerKeyID, string(approvalJSON), signingPayload, approval.Signature, approval.ExpiresAt.UnixMilli(), ts}},
		{`INSERT INTO p2p_cloud_service_restores(restore_id,restore_plan_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,backup_id,backup_revision,plan_id,cloud_connection_id,instance_id,region,availability_zone,volume_swaps_json,original_volume_retention,failure_policy,job_id,restore_status,created_at,updated_at) VALUES($1,$1,$2,$3,2,$4,5,$5,1,'plan-backup-runtime-0001',$6,$7,'ap-south-1','ap-south-1a',$8,'manual','reattach_original','job-restore-runtime-0001','queued',$9,$9)`, []any{target.RestoreID, approval.ApprovalID, target.ServiceID, target.DeploymentID, target.BackupID, target.CloudConnectionID, target.InstanceID, string(swapsJSON), ts}},
		{`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at) VALUES('outbox-restore-runtime-0001','cloud.service.restore.requested','service_restore',$1,'{}',$2)`, []any{target.RestoreID, ts}},
	}
	if readiness {
		statements = append(statements, []struct {
			query string
			args  []any
		}{
			{`INSERT INTO p2p_cloud_recipe_execution_manifests(execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at) VALUES('execution-restore-runtime-0001',$1,'plan-backup-runtime-0001',4,'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',$2,$3,$4,'{}','approved',1,$5,$5)`, []any{target.DeploymentID, target.CloudConnectionID, target.RecipeDigest, []byte{0xa0}, ts}},
			{`INSERT INTO p2p_cloud_recipe_install_tasks(execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status,last_sequence,last_checkpoint,created_at,updated_at) VALUES('execution-restore-runtime-0001','install-task-restore-runtime-0001',$1,'plan-backup-runtime-0001',$2,$3,$4,'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','[]','succeeded',1,'service_ready',$5,$5)`, []any{target.DeploymentID, target.CloudConnectionID, target.InstanceID, target.RecipeDigest, ts}},
			{`INSERT INTO p2p_cloud_worker_bootstrap_observations(deployment_id,cloud_connection_id,instance_id,worker_session_state,worker_lease_epoch,worker_lease_expires_at,created_at,updated_at) VALUES($1,$2,$3,'active',1,$4,$5,$5)`, []any{target.DeploymentID, target.CloudConnectionID, target.InstanceID, now.Add(time.Hour).UnixMilli(), ts}},
			{`INSERT INTO p2p_cloud_service_readiness_tasks(task_id,execution_id,deployment_id,service_id,cloud_connection_id,instance_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest,task_status,purpose,job_id,created_at,updated_at) VALUES('readiness-install-restore-runtime-0001','execution-restore-runtime-0001',$1,$2,$3,$4,$5,$5,$6,'succeeded','install','',$7,$7)`, []any{target.DeploymentID, target.ServiceID, target.CloudConnectionID, target.InstanceID, target.RecipeDigest, cloudcontracts.FixedReadinessEvidenceDigestV1, ts}},
		}...)
	}
	for _, statement := range statements {
		if _, err = database.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed restore runtime: %v", err)
		}
	}
	store.cfg.Now = func() time.Time { return now }
	claim, found, err := store.ClaimServiceRestore(ctx, "restore-runtime", time.Minute)
	if err != nil || !found {
		t.Fatalf("restore claim found=%v err=%v", found, err)
	}
	return now, database, store, claim
}

func serviceRestoreResult(t *testing.T, claim runtime.ServiceRestoreClaim, signed runtime.SignedServiceRestoreCommand, status, outcome, instanceState string, fallback bool, replacement broker.ServiceRestoreReplacementVolume) runtime.ServiceRestoreResult {
	t.Helper()
	receipt, err := json.Marshal(broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: claim.ConnectionID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, Action: broker.ServiceRestoreAction})
	if err != nil {
		t.Fatal(err)
	}
	return runtime.ServiceRestoreResult{Status: status, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: string(receipt), Evidence: broker.ServiceRestoreAWSEvidence{RestoreID: claim.RestoreID, ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, BackupID: claim.BackupID, InstanceID: claim.Request.InstanceID, Region: claim.Request.Region, AvailabilityZone: claim.Request.AvailabilityZone, Outcome: outcome, InstanceState: instanceState, FallbackVerified: fallback, Replacements: []broker.ServiceRestoreReplacementVolume{replacement}}}
}
