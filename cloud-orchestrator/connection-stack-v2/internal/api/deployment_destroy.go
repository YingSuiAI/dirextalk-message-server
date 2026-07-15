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

type DeploymentDestroySpec struct {
	InstanceID          string
	VolumeIDs           []string
	NetworkInterfaceIDs []string
}

type DeploymentDestroyProvider interface {
	EnsureVerifiedDestroyed(context.Context, DeploymentDestroySpec) (bool, error)
}

func (broker Broker) executeDeploymentDestroy(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
	if existing, found, err := broker.DeploymentDestroyStore.Lookup(request.Context(), command.ConnectionID, command.CommandID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		broker.writeDeploymentDestroyReplay(response, command, identity, existing)
		return
	}
	destroyRequest, err := command.DeploymentDestroyRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	proof, err := command.ServiceDestroyApproval()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	requestJSON, _ := json.Marshal(destroyRequest)
	reservation := commandstore.DeploymentDestroyReservation{ConnectionID: command.ConnectionID, DeploymentID: destroyRequest.DeploymentID, ServiceID: destroyRequest.ServiceID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID, RequestJSON: requestJSON, State: "reserved"}
	if existing, found, err := broker.DeploymentDestroyStore.LookupDeploymentDestroy(request.Context(), command.ConnectionID, destroyRequest.DeploymentID); err != nil {
		writeStoreError(response, err)
		return
	} else if found {
		if !existing.SameIdentity(reservation) {
			writeError(response, http.StatusConflict, "deployment_destroy_conflict")
			return
		}
		broker.resumeDeploymentDestroy(response, request, command, identity, existing, destroyRequest)
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
	created, found, err := broker.DeploymentDestroyStore.LookupDeployment(request.Context(), command.ConnectionID, destroyRequest.DeploymentID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || created.State != "finalized" {
		writeError(response, http.StatusConflict, "deployment_not_destroyable")
		return
	}
	createdEvidence, err := contract.StoredDeploymentReceipt(created.ResultJSON)
	if err != nil || createdEvidence.InstanceID != destroyRequest.InstanceID || !sameSortedStrings(createdEvidence.VolumeIDs, destroyRequest.VolumeIDs) || !sameSortedStrings(createdEvidence.NetworkInterfaceIDs, destroyRequest.NetworkInterfaceIDs) {
		writeError(response, http.StatusConflict, "deployment_resource_mismatch")
		return
	}
	stored, _, err := broker.DeploymentDestroyStore.ReserveDeploymentDestroy(request.Context(), reservation)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(reservation) {
		writeError(response, http.StatusConflict, "deployment_destroy_conflict")
		return
	}
	broker.resumeDeploymentDestroy(response, request, command, identity, stored, destroyRequest)
}

func (broker Broker) resumeDeploymentDestroy(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, reservation commandstore.DeploymentDestroyReservation, destroyRequest contract.DeploymentDestroyRequest) {
	complete, err := broker.DeploymentDestroyProvider.EnsureVerifiedDestroyed(request.Context(), DeploymentDestroySpec{InstanceID: destroyRequest.InstanceID, VolumeIDs: destroyRequest.VolumeIDs, NetworkInterfaceIDs: destroyRequest.NetworkInterfaceIDs})
	if err != nil {
		writeProviderError(response, err)
		return
	}
	if !complete {
		writeError(response, http.StatusConflict, "deployment_destroy_in_progress")
		return
	}
	resultJSON, err := contract.MarshalCommittedDeploymentDestroyResult(command, contract.DeploymentDestroyEvidence{DeploymentID: destroyRequest.DeploymentID, InstanceID: destroyRequest.InstanceID, VolumeIDs: destroyRequest.VolumeIDs, NetworkInterfaceIDs: destroyRequest.NetworkInterfaceIDs})
	if err != nil {
		writeError(response, http.StatusBadGateway, "provider_readback_invalid")
		return
	}
	identity.ResultJSON = resultJSON
	stored, _, err := broker.DeploymentDestroyStore.FinalizeDeploymentDestroy(request.Context(), reservation, identity)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) || !bytes.Equal(stored.ResultJSON, resultJSON) {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	var result contract.DeploymentDestroyResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateDeploymentDestroyResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func (broker Broker) writeDeploymentDestroyReplay(response http.ResponseWriter, command contract.Command, identity, stored commandstore.Record) {
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	var result contract.DeploymentDestroyResult
	if json.Unmarshal(stored.ResultJSON, &result) != nil || contract.ValidateDeploymentDestroyResult(command, result) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func sameSortedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
