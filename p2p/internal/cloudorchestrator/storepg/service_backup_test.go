package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreServiceBackupRejectsCurrentServiceRevisionDrift(t *testing.T) {
	_, database, store, claim := prepareServiceBackupClaim(t)
	if _, err := database.DB().Exec(`UPDATE p2p_cloud_services SET revision=revision+1 WHERE service_id=$1`, claim.ServiceID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkServiceBackupStarted(context.Background(), claim); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("drift error=%v, want %v", err, ErrLeaseLost)
	}
}

func TestStoreServiceBackupCommitsVerifiedReceiptWithoutMutatingServiceResourceAxes(t *testing.T) {
	now, database, store, claim := prepareServiceBackupClaim(t)
	_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(nodePrivate, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceBackupCommand(claim.Command, claim.Request, claim.Approval)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PersistServiceBackupCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkServiceBackupStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: claim.ConnectionID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, Action: broker.ServiceBackupAction})
	result := runtime.ServiceBackupResult{Status: "backup_available", BackupID: claim.BackupID, ServiceID: claim.ServiceID, DeploymentID: claim.DeploymentID, InstanceID: claim.Request.InstanceID, ImageID: "ami-0123456789abcdef0", ReceiptJSON: string(receipt), Snapshots: []broker.ServiceBackupSnapshot{{VolumeID: claim.Request.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256}
	if err = store.CompleteServiceBackup(context.Background(), claim, result); err != nil {
		t.Fatal(err)
	}
	var backupStatus, imageID, snapshotsJSON, jobExecution, jobOutcome, checkpoint, serviceStatus, resourceStatus, commandState string
	var serviceRevision int64
	if err = database.DB().QueryRow(`SELECT backup_status,image_id,snapshots_json FROM p2p_cloud_service_backups WHERE backup_id=$1`, claim.BackupID).Scan(&backupStatus, &imageID, &snapshotsJSON); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT execution_status,outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&jobExecution, &jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT service_status,revision FROM p2p_cloud_services WHERE service_id=$1`, claim.ServiceID).Scan(&serviceStatus, &serviceRevision); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, claim.DeploymentID).Scan(&resourceStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT state FROM p2p_cloud_service_backup_commands WHERE command_id=$1`, claim.Command.CommandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if backupStatus != "available" || imageID != result.ImageID || snapshotsJSON == "[]" || jobExecution != "finished" || jobOutcome != "succeeded" || checkpoint != "backup_available" || serviceStatus != "experimental" || serviceRevision != claim.ServiceRevision+1 || resourceStatus != "active" || commandState != "accepted" {
		t.Fatalf("backup=%s/%s/%s job=%s/%s/%s service=%s/%d resource=%s command=%s", backupStatus, imageID, snapshotsJSON, jobExecution, jobOutcome, checkpoint, serviceStatus, serviceRevision, resourceStatus, commandState)
	}
	var serviceEvents int
	if err = database.DB().QueryRow(`SELECT COUNT(*) FROM p2p_cloud_events WHERE type='cloud.service.changed' AND aggregate_id=$1 AND revision=$2`, claim.ServiceID, claim.ServiceRevision+1).Scan(&serviceEvents); err != nil {
		t.Fatal(err)
	}
	if serviceEvents != 1 {
		t.Fatalf("service backup terminal event count=%d, want 1", serviceEvents)
	}
}

func prepareServiceBackupClaim(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.ServiceBackupClaim) {
	t.Helper()
	ctx, database, closeDatabase := openMigratedStore(t)
	t.Cleanup(closeDatabase)
	now := time.Date(2026, time.July, 15, 18, 0, 0, 0, time.UTC)
	_, devicePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	target := cloudcontracts.ServiceBackupTargetV1{BackupID: "backup-runtime-0001", ServiceID: "service-backup-runtime-0001", ServiceRevision: 2, DeploymentID: "deployment-backup-runtime-0001", DeploymentRevision: 5, CloudConnectionID: "connection-backup-runtime-0001", RecipeID: "recipe-backup-runtime-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: cloudcontracts.ServiceBackupRetentionManual}
	approval, err := cloudcontracts.NewServiceBackupApprovalV1(target, "approval-backup-runtime-0001", "challenge-backup-runtime-0001", "device-backup-runtime-0001", now, now.Add(5*time.Minute))
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
	volumesJSON, _ := json.Marshal(target.VolumeIDs)
	signingPayload, _ := approval.SigningPayload()
	ts := now.UnixMilli()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_goals(goal_id,owner_mxid,prompt,cloud_connection_id,plan_id,status,idempotency_hash,request_digest,revision,created_at,updated_at)VALUES('goal-backup-runtime-0001','@owner:example.com','backup','connection-backup-runtime-0001','plan-backup-runtime-0001','planned','goal-backup-runtime-idem','goal-backup-runtime-request',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_plans(plan_id,goal_id,cloud_connection_id,status,title,summary,recipe_digest,quote_id,plan_hash,revision,created_at,updated_at)VALUES('plan-backup-runtime-0001','goal-backup-runtime-0001','connection-backup-runtime-0001','approved','Backup','safe',$1,'quote-backup-runtime-0001','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',4,$2,$2)`, []any{target.RecipeDigest, ts}},
		{`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at)VALUES('connection-backup-runtime-0001','aws','123456789012','ap-south-1','connection_stack_v2','active',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_connection_brokers(cloud_connection_id,broker_command_url,broker_region,connection_generation,node_key_id,next_node_counter,created_at,updated_at)VALUES('connection-backup-runtime-0001','https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands','ap-south-1',1,'node-backup-runtime-0001',0,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at)VALUES('recipe-backup-runtime-0001','Backup','v1',$1,'experimental',1,$2,$2)`, []any{target.RecipeDigest, ts}},
		{`INSERT INTO p2p_cloud_deployments(deployment_id,plan_id,cloud_connection_id,execution_status,outcome_status,resource_status,revision,created_at,updated_at)VALUES('deployment-backup-runtime-0001','plan-backup-runtime-0001','connection-backup-runtime-0001','finished','succeeded','active',5,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at)VALUES('service-backup-runtime-0001','deployment-backup-runtime-0001','recipe-backup-runtime-0001','Backup','experimental','not_requested',2,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_deployment_resources(deployment_id,cloud_connection_id,request_sha256,resource_status,instance_id,volume_ids_json,network_interface_ids_json,broker_receipt_json,created_at,updated_at)VALUES('deployment-backup-runtime-0001','connection-backup-runtime-0001','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','active',$1,$2,'[]','{}',$3,$3)`, []any{target.InstanceID, string(volumesJSON), ts}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-backup-runtime-0001','plan-backup-runtime-0001','deployment-backup-runtime-0001','backup','queued','pending','backup_queued','',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-backup-runtime-0001','backup','queued','queued','backup_queued','',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_service_backup_approvals(approval_id,challenge_id,owner_mxid,backup_id,service_id,service_revision,deployment_id,deployment_revision,cloud_connection_id,recipe_id,recipe_digest,instance_id,volume_ids_json,retention_policy,signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,prepare_idempotency_hash,prepare_request_digest,approve_idempotency_hash,approve_request_digest,signature,job_id,expires_at,created_at,updated_at)VALUES($1,$2,'@owner:example.com',$3,$4,2,$5,5,$6,$7,$8,$9,$10,'manual',$11,$12,$13,'{}','{}','approved','prepare-backup-runtime-idem','prepare-backup-runtime-request','approve-backup-runtime-idem','approve-backup-runtime-request',$14,'job-backup-runtime-0001',$15,$16,$16)`, []any{approval.ApprovalID, approval.ChallengeID, target.BackupID, target.ServiceID, target.DeploymentID, target.CloudConnectionID, target.RecipeID, target.RecipeDigest, target.InstanceID, string(volumesJSON), approval.SignerKeyID, string(approvalJSON), signingPayload, approval.Signature, approval.ExpiresAt.UnixMilli(), ts}},
		{`INSERT INTO p2p_cloud_service_backups(backup_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,instance_id,volume_ids_json,retention_policy,job_id,backup_status,created_at,updated_at)VALUES($1,$2,$3,2,$4,5,'plan-backup-runtime-0001',$5,$6,$7,'manual','job-backup-runtime-0001','queued',$8,$8)`, []any{target.BackupID, approval.ApprovalID, target.ServiceID, target.DeploymentID, target.CloudConnectionID, target.InstanceID, string(volumesJSON), ts}},
		{`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at)VALUES('outbox-backup-runtime-0001','cloud.service.backup.requested','service_backup',$1,'{}',$2)`, []any{target.BackupID, ts}},
	}
	for _, statement := range statements {
		if _, err = database.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed backup runtime: %v", err)
		}
	}
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimServiceBackup(ctx, "backup-runtime", time.Minute)
	if err != nil || !found {
		t.Fatalf("backup claim found=%v err=%v", found, err)
	}
	return now, database, store, claim
}
