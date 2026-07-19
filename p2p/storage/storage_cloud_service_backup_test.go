package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDatabaseStoreServiceBackupApprovalBindsTrackedVolumesAndLeavesServiceActive(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 15, 17, 0, 0, 0, time.UTC)
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, publicSPKI, now.UnixMilli())
	prepare := cloudmodule.PrepareServiceBackupRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "prepare-backup-idem", RequestDigest: "prepare-backup-request", BackupID: "backup-storage-0001", ApprovalID: "approval-backup-storage-0001", ChallengeID: "challenge-backup-storage-0001", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, e := store.PrepareCloudServiceBackup(ctx, prepare)
	if e != nil || !prepared.Created || len(prepared.Confirmation.Approval.VolumeIDs) != 2 {
		t.Fatalf("prepare=%#v err=%v", prepared, e)
	}
	approval, e := prepared.Confirmation.Approval.Sign(privateKey, now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	tampered := approval
	tampered.VolumeIDs = []string{"vol-0aaaaaaaaaaaaaaaa"}
	if _, e = store.ApproveCloudServiceBackup(ctx, backupApprovalRequest(tampered, now.Add(time.Minute).UnixMilli())); !errors.Is(e, cloudmodule.ErrServiceBackupConfirmationInvalid) && !errors.Is(e, cloudmodule.ErrServiceBackupApprovalSignature) {
		t.Fatalf("tampered approval=%v", e)
	}
	approval.VolumeIDs[0], approval.VolumeIDs[1] = approval.VolumeIDs[1], approval.VolumeIDs[0]
	request := backupApprovalRequest(approval, now.Add(time.Minute).UnixMilli())
	approved, e := store.ApproveCloudServiceBackup(ctx, request)
	if e != nil || !approved.Created || approved.Backup.Status != "queued" || approved.Service.Status != "experimental" || approved.Service.Revision != 2 || approved.Job.Kind != "backup" {
		t.Fatalf("approved=%#v err=%v", approved, e)
	}
	var status, resourceStatus, backupVolumes, resourceVolumes string
	if e = store.DB().QueryRowContext(ctx, `SELECT backup.backup_status,resource.resource_status,backup.volume_ids_json,resource.volume_ids_json FROM p2p_cloud_service_backups backup JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=backup.deployment_id WHERE backup.backup_id=$1`, approval.BackupID).Scan(&status, &resourceStatus, &backupVolumes, &resourceVolumes); e != nil || status != "queued" || resourceStatus != "active" || backupVolumes != resourceVolumes {
		t.Fatalf("ledger status=%s resource=%s backup_volumes=%s resource_volumes=%s err=%v", status, resourceStatus, backupVolumes, resourceVolumes, e)
	}
	replay, e := store.ApproveCloudServiceBackup(ctx, request)
	if e != nil || replay.Created || replay.Job.JobID != approved.Job.JobID {
		t.Fatalf("replay=%#v err=%v", replay, e)
	}
}
func backupApprovalRequest(a cloudcontracts.ServiceBackupApprovalV1, createdAt int64) cloudmodule.ApproveServiceBackupRequest {
	return cloudmodule.ApproveServiceBackupRequest{OwnerMXID: "@owner:example.com", ServiceID: a.ServiceID, ExpectedRevision: int64(a.ServiceRevision), IdempotencyHash: "approve-backup-idem", Approval: a, JobID: "job-backup-storage-0001", OutboxID: "outbox-backup-storage-0001", JobEventID: "event-backup-job-storage-0001", CreatedAt: createdAt}
}
