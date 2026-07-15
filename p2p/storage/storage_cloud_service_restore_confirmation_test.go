package storage

import (
	"context"
	"encoding/json"
	"errors"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"testing"
	"time"
)

func TestDatabaseStoreRestoreApprovalBindsReadyPlanAndQueuesNoMutationParameters(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 15, 23, 0, 0, 0, time.UTC)
	private, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	snaps, _ := json.Marshal([]map[string]any{{"volume_id": "vol-0aaaaaaaaaaaaaaaa", "snapshot_id": "snap-0aaaaaaaaaaaaaaaa", "state": "completed", "encrypted": true}, {"volume_id": "vol-0bbbbbbbbbbbbbbbb", "snapshot_id": "snap-0bbbbbbbbbbbbbbbb", "state": "completed", "encrypted": true}})
	refs, _ := json.Marshal([]broker.ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0aaaaaaaaaaaaaaaa", SnapshotID: "snap-0aaaaaaaaaaaaaaaa"}, {OriginalVolumeID: "vol-0bbbbbbbbbbbbbbbb", SnapshotID: "snap-0bbbbbbbbbbbbbbbb"}})
	swaps, _ := json.Marshal([]broker.ServiceRestoreVolumeSwap{{OriginalVolumeID: "vol-0aaaaaaaaaaaaaaaa", SnapshotID: "snap-0aaaaaaaaaaaaaaaa", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}, {OriginalVolumeID: "vol-0bbbbbbbbbbbbbbbb", SnapshotID: "snap-0bbbbbbbbbbbbbbbb", DeviceName: "/dev/xvdb", VolumeType: "gp3", SizeGiB: 100, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: false}})
	ts := now.UnixMilli()
	statements := []struct {
		q string
		a []any
	}{{`INSERT INTO p2p_cloud_service_backups(backup_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,instance_id,volume_ids_json,retention_policy,job_id,backup_status,image_id,snapshots_json,revision,created_at,updated_at)VALUES('backup-restore-confirm-0001','approval-backup-seed-0001','service-destroy-0001',2,'deployment-destroy-0001',5,'plan-destroy-0001','connection-destroy-0001','i-0123456789abcdef0','["vol-0bbbbbbbbbbbbbbbb","vol-0aaaaaaaaaaaaaaaa"]','manual','job-backup-seed-0001','available','ami-0123456789abcdef0',$1,2,$2,$2)`, []any{string(snaps), ts}}, {`INSERT INTO p2p_cloud_service_restore_plans(restore_plan_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,backup_id,backup_revision,recipe_id,recipe_digest,instance_id,region,image_id,snapshot_refs_json,plan_status,availability_zone,quote_id,currency,estimated_hourly_minor,estimated_thirty_day_minor,quoted_at,valid_until,unincluded_json,volume_swaps_json,job_id,idempotency_hash,request_digest,revision,created_at,updated_at)VALUES('restore-plan-confirm-0001','@owner:example.com','service-destroy-0001',2,'deployment-destroy-0001',5,'plan-destroy-0001','connection-destroy-0001','backup-restore-confirm-0001',2,'recipe-destroy-0001','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','i-0123456789abcdef0','ap-south-1','ami-0123456789abcdef0',$1,'ready_for_confirmation','ap-south-1a','restore-quote-confirm-0001','USD',1,1440,$2,$3,'["taxes"]',$4,'job-restore-plan-seed-0001','restore-plan-seed-idem','restore-plan-seed-request',2,$2,$2)`, []any{string(refs), ts, now.Add(15 * time.Minute).UnixMilli(), string(swaps)}}}
	for _, s := range statements {
		if _, e := store.DB().ExecContext(ctx, s.q, s.a...); e != nil {
			t.Fatal(e)
		}
	}
	prepare := cloudmodule.PrepareServiceRestoreRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", RestorePlanID: "restore-plan-confirm-0001", ExpectedRevision: 2, IdempotencyHash: "restore-prepare-idem", RequestDigest: "restore-prepare-request", ApprovalID: "approval-restore-confirm-0001", ChallengeID: "challenge-restore-confirm-0001", CreatedAt: ts, ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, e := store.PrepareCloudServiceRestore(ctx, prepare)
	if e != nil || !prepared.Created || prepared.Confirmation.Approval.RestoreID != prepare.RestorePlanID || len(prepared.Confirmation.Approval.VolumeSwaps) != 2 {
		t.Fatalf("prepared=%#v err=%v", prepared, e)
	}
	approval, e := prepared.Confirmation.Approval.Sign(private, now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	tampered := approval
	tampered.VolumeSwaps = append([]cloudcontracts.ServiceRestoreVolumeSwapV1(nil), approval.VolumeSwaps...)
	tampered.VolumeSwaps[0].SnapshotID = "snap-0cccccccccccccccc"
	if _, e = store.ApproveCloudServiceRestore(ctx, restoreApproveRequest(tampered, now.Add(time.Minute).UnixMilli())); !errors.Is(e, cloudmodule.ErrServiceRestoreConfirmationInvalid) && !errors.Is(e, cloudmodule.ErrServiceRestoreApprovalSignature) {
		t.Fatalf("tampered=%v", e)
	}
	approved, e := store.ApproveCloudServiceRestore(ctx, restoreApproveRequest(approval, now.Add(time.Minute).UnixMilli()))
	if e != nil || !approved.Created || approved.Restore.Status != "queued" || approved.Job.Kind != "restore" || approved.Service.Revision != 2 {
		t.Fatalf("approved=%#v err=%v", approved, e)
	}
	var planStatus, restoreStatus, outboxKind, aggregate string
	if e = store.DB().QueryRow(`SELECT plan.plan_status,restore.restore_status,outbox.kind,outbox.aggregate_type FROM p2p_cloud_service_restore_plans plan JOIN p2p_cloud_service_restores restore ON restore.restore_plan_id=plan.restore_plan_id JOIN p2p_cloud_outbox outbox ON outbox.aggregate_id=restore.restore_id WHERE plan.restore_plan_id=$1`, prepare.RestorePlanID).Scan(&planStatus, &restoreStatus, &outboxKind, &aggregate); e != nil {
		t.Fatal(e)
	}
	if planStatus != "approved" || restoreStatus != "queued" || outboxKind != cloudmodule.OutboxKindServiceRestoreRequested || aggregate != "service_restore" {
		t.Fatalf("plan=%s restore=%s outbox=%s/%s", planStatus, restoreStatus, outboxKind, aggregate)
	}
	if _, e = store.PrepareCloudServiceOperation(ctx, cloudmodule.PrepareServiceOperationRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, Operation: cloudcontracts.ServiceOperationStop, IdempotencyHash: "restore-blocks-operation-idem", RequestDigest: "restore-blocks-operation-request", ApprovalID: "approval-restore-blocks-operation", ChallengeID: "challenge-restore-blocks-operation", CreatedAt: ts, ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}); !errors.Is(e, cloudmodule.ErrServiceOperationConfirmationInvalid) {
		t.Fatalf("active restore must block service operation, got %v", e)
	}
	if _, e = store.PrepareCloudServiceBackup(ctx, cloudmodule.PrepareServiceBackupRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "restore-blocks-backup-idem", RequestDigest: "restore-blocks-backup-request", BackupID: "backup-restore-blocked-0001", ApprovalID: "approval-restore-blocks-backup", ChallengeID: "challenge-restore-blocks-backup", CreatedAt: ts, ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}); !errors.Is(e, cloudmodule.ErrServiceBackupConfirmationInvalid) {
		t.Fatalf("active restore must block backup, got %v", e)
	}
	if _, e = store.PrepareCloudServiceDestroy(ctx, cloudmodule.PrepareServiceDestroyRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "restore-blocks-destroy-idem", RequestDigest: "restore-blocks-destroy-request", ApprovalID: "approval-restore-blocks-destroy", ChallengeID: "challenge-restore-blocks-destroy", CreatedAt: ts, ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}); !errors.Is(e, cloudmodule.ErrServiceDestroyConfirmationInvalid) {
		t.Fatalf("active restore must block destroy, got %v", e)
	}
	replay, e := store.ApproveCloudServiceRestore(ctx, restoreApproveRequest(approval, now.Add(time.Minute).UnixMilli()))
	if e != nil || replay.Created || replay.Job.JobID != approved.Job.JobID {
		t.Fatalf("replay=%#v err=%v", replay, e)
	}
}
func restoreApproveRequest(a cloudcontracts.ServiceRestoreApprovalV1, created int64) cloudmodule.ApproveServiceRestoreRequest {
	return cloudmodule.ApproveServiceRestoreRequest{OwnerMXID: "@owner:example.com", ServiceID: a.ServiceID, RestorePlanID: a.RestoreID, ExpectedRevision: int64(a.ServiceRevision), IdempotencyHash: "restore-approve-idem", Approval: a, JobID: "job-restore-confirm-0001", OutboxID: "outbox-restore-confirm-0001", JobEventID: "event-restore-confirm-0001", CreatedAt: created}
}
