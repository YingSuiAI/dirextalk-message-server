package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type ServiceRestoreSpec struct {
	ConnectionID     string
	RestoreID        string
	ServiceID        string
	DeploymentID     string
	BackupID         string
	InstanceID       string
	Region           string
	AvailabilityZone string
	VolumeSwaps      []contract.ServiceRestoreVolumeSwap
}

type ServiceRestoreProvider interface {
	EnsureRestore(context.Context, ServiceRestoreSpec) (contract.ServiceRestoreAWSEvidence, bool, error)
}

func (broker Broker) executeServiceRestore(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
	if existing, found, err := broker.ServiceRestoreStore.Lookup(request.Context(), command.ConnectionID, command.CommandID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		broker.writeServiceRestoreReplay(response, command, identity, existing)
		return
	}
	restoreRequest, err := command.ServiceRestoreRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	proof, err := command.ServiceRestoreApproval()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	requestJSON, _ := json.Marshal(restoreRequest)
	reservation := commandstore.ServiceRestoreReservation{ConnectionID: command.ConnectionID, RestoreID: restoreRequest.RestoreID, ServiceID: restoreRequest.ServiceID, DeploymentID: restoreRequest.DeploymentID, BackupID: restoreRequest.BackupID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID, RequestJSON: requestJSON, State: "reserved"}
	if existing, found, err := broker.ServiceRestoreStore.LookupServiceRestore(request.Context(), command.ConnectionID, restoreRequest.RestoreID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		if !existing.SameIdentity(reservation) {
			writeError(response, http.StatusConflict, "service_restore_conflict")
			return
		}
		broker.resumeServiceRestore(response, request, command, identity, existing, restoreRequest)
		return
	}
	if err := command.ValidateAt(now); err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	approvalKey, ok := broker.ApprovalResolver.LookupApprovalKey(request.Context(), command.ConnectionID, proof.SignerKeyID)
	if !ok {
		writeError(response, http.StatusForbidden, "unknown_approval_key")
		return
	}
	if err := proof.Verify(approvalKey, now); err != nil {
		writeError(response, http.StatusForbidden, contract.Code(err))
		return
	}
	if err := broker.validateRestoreResources(request.Context(), command.ConnectionID, restoreRequest); err != nil {
		writeProviderError(response, err)
		return
	}
	stored, _, err := broker.ServiceRestoreStore.ReserveServiceRestore(request.Context(), reservation)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(reservation) {
		writeError(response, http.StatusConflict, "service_restore_conflict")
		return
	}
	broker.resumeServiceRestore(response, request, command, identity, stored, restoreRequest)
}

func (broker Broker) validateRestoreResources(ctx context.Context, connectionID string, request contract.ServiceRestoreRequest) error {
	created, found, err := broker.ServiceRestoreStore.LookupDeployment(ctx, connectionID, request.DeploymentID)
	if err != nil {
		return err
	}
	if !found || created.State != "finalized" {
		return NewError("deployment_not_restorable", http.StatusConflict)
	}
	createdEvidence, err := contract.StoredDeploymentReceipt(created.ResultJSON)
	if err != nil || createdEvidence.InstanceID != request.InstanceID {
		return NewError("deployment_resource_mismatch", http.StatusConflict)
	}
	originals := make([]string, 0, len(request.VolumeSwaps))
	for _, swap := range request.VolumeSwaps {
		originals = append(originals, swap.OriginalVolumeID)
	}
	if !sameSortedStrings(createdEvidence.VolumeIDs, originals) {
		return NewError("deployment_resource_mismatch", http.StatusConflict)
	}
	backup, found, err := broker.ServiceRestoreStore.LookupServiceBackup(ctx, connectionID, request.BackupID)
	if err != nil {
		return err
	}
	if !found || backup.State != "finalized" || backup.ServiceID != request.ServiceID || backup.DeploymentID != request.DeploymentID {
		return NewError("backup_not_restorable", http.StatusConflict)
	}
	var backupResult contract.ServiceBackupResult
	if json.Unmarshal(backup.ResultJSON, &backupResult) != nil || backupResult.Status != "backup_available" || backupResult.Backup.InstanceID != request.InstanceID {
		return NewError("backup_resource_mismatch", http.StatusConflict)
	}
	want := make([]string, 0, len(request.VolumeSwaps))
	for _, swap := range request.VolumeSwaps {
		want = append(want, swap.OriginalVolumeID+"\x00"+swap.SnapshotID)
	}
	got := make([]string, 0, len(backupResult.Backup.Snapshots))
	for _, snapshot := range backupResult.Backup.Snapshots {
		got = append(got, snapshot.VolumeID+"\x00"+snapshot.SnapshotID)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !sameSortedStrings(want, got) {
		return NewError("backup_resource_mismatch", http.StatusConflict)
	}
	return nil
}

func (broker Broker) resumeServiceRestore(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, reservation commandstore.ServiceRestoreReservation, restoreRequest contract.ServiceRestoreRequest) {
	evidence, complete, err := broker.ServiceRestoreProvider.EnsureRestore(request.Context(), ServiceRestoreSpec{ConnectionID: command.ConnectionID, RestoreID: restoreRequest.RestoreID, ServiceID: restoreRequest.ServiceID, DeploymentID: restoreRequest.DeploymentID, BackupID: restoreRequest.BackupID, InstanceID: restoreRequest.InstanceID, Region: restoreRequest.Region, AvailabilityZone: restoreRequest.AvailabilityZone, VolumeSwaps: restoreRequest.VolumeSwaps})
	if err != nil {
		writeProviderError(response, err)
		return
	}
	if !complete {
		writeError(response, http.StatusConflict, "service_restore_in_progress")
		return
	}
	resultJSON, err := contract.MarshalCommittedServiceRestoreResult(command, evidence)
	if err != nil {
		writeError(response, http.StatusBadGateway, "provider_readback_invalid")
		return
	}
	identity.ResultJSON = resultJSON
	stored, _, err := broker.ServiceRestoreStore.FinalizeServiceRestore(request.Context(), reservation, identity)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) || !bytes.Equal(stored.ResultJSON, resultJSON) {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	var result contract.ServiceRestoreResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateServiceRestoreResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func (broker Broker) writeServiceRestoreReplay(response http.ResponseWriter, command contract.Command, identity, stored commandstore.Record) {
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	var result contract.ServiceRestoreResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateServiceRestoreResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}
