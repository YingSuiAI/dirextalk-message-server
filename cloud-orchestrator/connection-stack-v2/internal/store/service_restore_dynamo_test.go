package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestDynamoServiceRestoreConsumesApprovalBeforeVolumeCreationAndFinalizesReadBack(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	swap := contract.ServiceRestoreVolumeSwap{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}
	request := contract.ServiceRestoreRequest{Schema: contract.ServiceRestoreSchema, RestoreID: "restore-store-0001", ServiceID: "service-restore-0001", DeploymentID: "deployment-restore-0001", BackupID: "backup-restore-0001", InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", AvailabilityZone: "ap-south-1a", RestoreMode: "in_place", DowntimeRequired: true, OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "quote-restore-0001", QuoteValidUntil: "2026-07-15T16:15:00.000Z", VolumeSwaps: []contract.ServiceRestoreVolumeSwap{swap}}
	requestJSON, _ := json.Marshal(request)
	reservation := ServiceRestoreReservation{ConnectionID: "connection-restore-0001", RestoreID: request.RestoreID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, CommandID: "command-restore-0001", RequestSHA256: strings.Repeat("a", 64), ExpectedGeneration: 2, NodeCounter: 12, ApprovalID: "approval-restore-0001", ChallengeID: "challenge-restore-0001", SignerKeyID: "device-restore-0001", RequestJSON: requestJSON, State: "reserved"}
	stored, created, err := repository.ReserveServiceRestore(t.Context(), reservation)
	if err != nil || !created || !stored.SameIdentity(reservation) {
		t.Fatalf("reserve=(%#v,%v,%v)", stored, created, err)
	}
	if len(client.transactInput.TransactItems) != 4 {
		t.Fatalf("approval and counter must commit atomically before AWS mutation: %#v", client.transactInput)
	}
	result := contract.ServiceRestoreResult{Schema: contract.ServiceRestoreResultSchema, Status: "aws_restore_applied", Receipt: contract.DeploymentCommandReceipt{Schema: contract.ReceiptSchema, Disposition: "committed", ConnectionID: reservation.ConnectionID, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, Action: contract.ActionServiceRestore}, Restore: contract.ServiceRestoreAWSEvidence{RestoreID: request.RestoreID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, InstanceID: request.InstanceID, Region: request.Region, AvailabilityZone: request.AvailabilityZone, Outcome: "restored", InstanceState: "running", Replacements: []contract.ServiceRestoreReplacementVolume{{OriginalVolumeID: swap.OriginalVolumeID, ReplacementVolumeID: "vol-0fedcba9876543210", SnapshotID: swap.SnapshotID, DeviceName: swap.DeviceName, State: "attached_current", Encrypted: true, DeleteOnTermination: true}}}}
	raw, _ := json.Marshal(result)
	receipt := Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: contract.ActionServiceRestore, ResultJSON: raw}
	got, finalized, err := repository.FinalizeServiceRestore(t.Context(), reservation, receipt)
	if err != nil || !finalized || !got.SameIdentity(receipt) {
		t.Fatalf("finalize=(%#v,%v,%v)", got, finalized, err)
	}
	if len(client.transactInput.TransactItems) != 2 {
		t.Fatalf("result and receipt must finalize atomically: %#v", client.transactInput)
	}
}
