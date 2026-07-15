package store

import (
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	"strings"
	"testing"
)

func TestDynamoServiceBackupConsumesApprovalBeforeSnapshotAndFinalizesReceipt(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	request := contract.ServiceBackupRequest{Schema: contract.ServiceBackupSchema, BackupID: "backup-0001", ServiceID: "service-backup-0001", DeploymentID: "deployment-backup-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: "manual"}
	requestJSON, _ := json.Marshal(request)
	reservation := ServiceBackupReservation{ConnectionID: "connection-backup-0001", BackupID: request.BackupID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, CommandID: "command-backup-0001", RequestSHA256: strings.Repeat("a", 64), ExpectedGeneration: 2, NodeCounter: 11, ApprovalID: "approval-backup-0001", ChallengeID: "challenge-backup-0001", SignerKeyID: "device-backup-0001", RequestJSON: requestJSON, State: "reserved"}
	stored, created, e := repository.ReserveServiceBackup(t.Context(), reservation)
	if e != nil || !created || !stored.SameIdentity(reservation) {
		t.Fatalf("reserve=(%#v,%v,%v)", stored, created, e)
	}
	if len(client.transactInput.TransactItems) != 4 {
		t.Fatalf("reserve transaction=%#v", client.transactInput)
	}
	result := contract.ServiceBackupResult{Schema: contract.ServiceBackupResultSchema, Status: "backup_available", Receipt: contract.DeploymentCommandReceipt{Schema: contract.ReceiptSchema, Disposition: "committed", ConnectionID: reservation.ConnectionID, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, Action: contract.ActionServiceBackup}, Backup: contract.ServiceBackupEvidence{BackupID: request.BackupID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, InstanceID: request.InstanceID, RetentionPolicy: "manual", ImageID: "ami-0123456789abcdef0", Snapshots: []contract.ServiceBackupSnapshot{{VolumeID: request.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}}}
	raw, _ := json.Marshal(result)
	receipt := Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: contract.ActionServiceBackup, ResultJSON: raw}
	got, finalized, e := repository.FinalizeServiceBackup(t.Context(), reservation, receipt)
	if e != nil || !finalized || !got.SameIdentity(receipt) {
		t.Fatalf("finalize=(%#v,%v,%v)", got, finalized, e)
	}
	if len(client.transactInput.TransactItems) != 2 {
		t.Fatalf("finalize transaction=%#v", client.transactInput)
	}
}
