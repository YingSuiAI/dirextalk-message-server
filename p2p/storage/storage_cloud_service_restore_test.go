package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestDatabaseStoreRestorePlanDerivesExactRetainedBackupScope(t *testing.T) {
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 15, 21, 0, 0, 0, time.UTC)
	_, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	snapshots, _ := json.Marshal([]map[string]any{{"volume_id": "vol-0aaaaaaaaaaaaaaaa", "snapshot_id": "snap-0aaaaaaaaaaaaaaaa", "state": "completed", "encrypted": true}, {"volume_id": "vol-0bbbbbbbbbbbbbbbb", "snapshot_id": "snap-0bbbbbbbbbbbbbbbb", "state": "completed", "encrypted": true}})
	_, err := store.DB().Exec(`INSERT INTO p2p_cloud_service_backups(backup_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,instance_id,volume_ids_json,retention_policy,job_id,backup_status,image_id,snapshots_json,revision,created_at,updated_at)VALUES('backup-restore-storage-0001','approval-restore-seed-0001','service-destroy-0001',2,'deployment-destroy-0001',5,'plan-destroy-0001','connection-destroy-0001','i-0123456789abcdef0','["vol-0bbbbbbbbbbbbbbbb","vol-0aaaaaaaaaaaaaaaa"]','manual','job-restore-seed-0001','available','ami-0123456789abcdef0',$1,2,$2,$2)`, string(snapshots), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	r := cloudmodule.CreateServiceRestorePlanRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", BackupID: "backup-restore-storage-0001", ExpectedRevision: 2, IdempotencyHash: "restore-plan-idempotency", RequestDigest: "restore-plan-request", RestorePlanID: "restore-plan-storage-0001", JobID: "job-restore-plan-storage-0001", OutboxID: "outbox-restore-plan-storage-0001", JobEventID: "event-restore-plan-storage-0001", CreatedAt: now.UnixMilli()}
	created, err := store.CreateCloudServiceRestorePlan(context.Background(), r)
	if err != nil || !created.Created || created.Plan.Status != "planning" || created.Job.Kind != "restore_plan" {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	var instance, region, image, refs, kind, aggregate, status string
	if err = store.DB().QueryRow(`SELECT restore.instance_id,restore.region,restore.image_id,restore.snapshot_refs_json,outbox.kind,outbox.aggregate_type,restore.plan_status FROM p2p_cloud_service_restore_plans restore JOIN p2p_cloud_outbox outbox ON outbox.aggregate_id=restore.restore_plan_id WHERE restore.restore_plan_id=$1`, r.RestorePlanID).Scan(&instance, &region, &image, &refs, &kind, &aggregate, &status); err != nil {
		t.Fatal(err)
	}
	if instance != "i-0123456789abcdef0" || region != "ap-south-1" || image != "ami-0123456789abcdef0" || kind != cloudmodule.OutboxKindServiceRestorePlanRequested || aggregate != "service_restore_plan" || status != "planning" {
		t.Fatalf("instance=%s region=%s image=%s kind=%s aggregate=%s status=%s", instance, region, image, kind, aggregate, status)
	}
	var decoded []map[string]string
	if json.Unmarshal([]byte(refs), &decoded) != nil || len(decoded) != 2 || decoded[0]["original_volume_id"] != "vol-0aaaaaaaaaaaaaaaa" {
		t.Fatalf("snapshot refs=%s", refs)
	}
	replay, err := store.CreateCloudServiceRestorePlan(context.Background(), r)
	if err != nil || replay.Created || replay.Plan.RestorePlanID != created.Plan.RestorePlanID {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
}
