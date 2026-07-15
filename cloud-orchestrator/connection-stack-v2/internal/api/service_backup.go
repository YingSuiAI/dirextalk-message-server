package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type ServiceBackupSpec struct {
	ConnectionID string
	BackupID     string
	ServiceID    string
	DeploymentID string
	InstanceID   string
	VolumeIDs    []string
}

type ServiceBackupProvider interface {
	EnsureBackup(context.Context, ServiceBackupSpec) (contract.ServiceBackupEvidence, bool, error)
}

func (broker Broker) executeServiceBackup(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
	if existing, found, err := broker.ServiceBackupStore.Lookup(request.Context(), command.ConnectionID, command.CommandID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		broker.writeServiceBackupReplay(response, command, identity, existing)
		return
	}
	backupRequest, err := command.ServiceBackupRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	proof, err := command.ServiceBackupApproval()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	requestJSON, _ := json.Marshal(backupRequest)
	reservation := commandstore.ServiceBackupReservation{ConnectionID: command.ConnectionID, BackupID: backupRequest.BackupID, ServiceID: backupRequest.ServiceID, DeploymentID: backupRequest.DeploymentID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID, RequestJSON: requestJSON, State: "reserved"}
	if existing, found, err := broker.ServiceBackupStore.LookupServiceBackup(request.Context(), command.ConnectionID, backupRequest.BackupID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		if !existing.SameIdentity(reservation) {
			writeError(response, http.StatusConflict, "service_backup_conflict")
			return
		}
		broker.resumeServiceBackup(response, request, command, identity, existing, backupRequest)
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
	created, found, err := broker.ServiceBackupStore.LookupDeployment(request.Context(), command.ConnectionID, backupRequest.DeploymentID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || created.State != "finalized" {
		writeError(response, http.StatusConflict, "deployment_not_backupable")
		return
	}
	createdEvidence, err := contract.StoredDeploymentReceipt(created.ResultJSON)
	if err != nil || createdEvidence.InstanceID != backupRequest.InstanceID || !sameSortedStrings(createdEvidence.VolumeIDs, backupRequest.VolumeIDs) {
		writeError(response, http.StatusConflict, "deployment_resource_mismatch")
		return
	}
	stored, _, err := broker.ServiceBackupStore.ReserveServiceBackup(request.Context(), reservation)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(reservation) {
		writeError(response, http.StatusConflict, "service_backup_conflict")
		return
	}
	broker.resumeServiceBackup(response, request, command, identity, stored, backupRequest)
}

func (broker Broker) resumeServiceBackup(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, reservation commandstore.ServiceBackupReservation, backupRequest contract.ServiceBackupRequest) {
	evidence, complete, err := broker.ServiceBackupProvider.EnsureBackup(request.Context(), ServiceBackupSpec{ConnectionID: command.ConnectionID, BackupID: backupRequest.BackupID, ServiceID: backupRequest.ServiceID, DeploymentID: backupRequest.DeploymentID, InstanceID: backupRequest.InstanceID, VolumeIDs: backupRequest.VolumeIDs})
	if err != nil {
		writeProviderError(response, err)
		return
	}
	if !complete {
		writeError(response, http.StatusConflict, "service_backup_in_progress")
		return
	}
	resultJSON, err := contract.MarshalCommittedServiceBackupResult(command, evidence)
	if err != nil {
		writeError(response, http.StatusBadGateway, "provider_readback_invalid")
		return
	}
	identity.ResultJSON = resultJSON
	stored, _, err := broker.ServiceBackupStore.FinalizeServiceBackup(request.Context(), reservation, identity)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) || !bytes.Equal(stored.ResultJSON, resultJSON) {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	var result contract.ServiceBackupResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateServiceBackupResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func (broker Broker) writeServiceBackupReplay(response http.ResponseWriter, command contract.Command, identity, stored commandstore.Record) {
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	var result contract.ServiceBackupResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateServiceBackupResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}
