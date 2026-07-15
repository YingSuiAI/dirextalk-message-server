package api

import (
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func (b Broker) executeDeploymentObserve(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	if b.DeploymentStore == nil {
		writeError(response, http.StatusServiceUnavailable, "broker_not_configured")
		return
	}
	if err := command.ValidateAt(now); err != nil {
		status := http.StatusBadRequest
		if contract.Code(err) == "expired_command" {
			status = http.StatusUnauthorized
		}
		writeError(response, status, contract.Code(err))
		return
	}
	observeRequest, err := command.DeploymentObserveRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action,
	}
	existing, found, err := b.DeploymentStore.Lookup(request.Context(), command.ConnectionID, command.CommandID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if found && !existing.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	observation, err := b.currentDeploymentObservation(request, command.ConnectionID, observeRequest.DeploymentID, now)
	if err != nil {
		writeProviderError(response, err)
		return
	}
	resultJSON, err := contract.MarshalDeploymentObserveResult(command, observation, found)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	if found {
		if _, validationErr := contract.DecodeDeploymentObserveResult(command, existing.ResultJSON); validationErr != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		writeRawJSON(response, http.StatusOK, resultJSON)
		return
	}
	committedJSON, err := contract.MarshalDeploymentObserveResult(command, observation, false)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	identity.ResultJSON = committedJSON
	stored, created, err := b.DeploymentStore.Commit(request.Context(), identity, nil)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	if !created {
		if _, validationErr := contract.DecodeDeploymentObserveResult(command, stored.ResultJSON); validationErr != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		resultJSON, err = contract.MarshalDeploymentObserveResult(command, observation, true)
		if err != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
	}
	writeRawJSON(response, http.StatusOK, resultJSON)
}

func (b Broker) currentDeploymentObservation(request *http.Request, connectionID, deploymentID string, now time.Time) (contract.DeploymentObservation, error) {
	deployment, found, err := b.DeploymentStore.LookupDeployment(request.Context(), connectionID, deploymentID)
	if err != nil {
		return contract.DeploymentObservation{}, NewError("worker_bootstrap_unavailable", http.StatusServiceUnavailable)
	}
	if !found || deployment.State != "finalized" || len(deployment.ResultJSON) == 0 {
		return contract.DeploymentObservation{}, NewError("deployment_not_found", http.StatusNotFound)
	}
	receipt, err := contract.StoredDeploymentReceipt(deployment.ResultJSON)
	if err != nil || receipt.ConnectionID != connectionID || receipt.DeploymentID != deploymentID {
		return contract.DeploymentObservation{}, NewError("deployment_store_invalid", http.StatusInternalServerError)
	}
	session, found, err := b.DeploymentStore.LookupWorkerSession(request.Context(), deployment.BootstrapSessionID)
	if err != nil {
		return contract.DeploymentObservation{}, NewError("worker_bootstrap_unavailable", http.StatusServiceUnavailable)
	}
	if !found || session.ConnectionID != connectionID || session.DeploymentID != deploymentID || session.ExpectedInstanceID != receipt.InstanceID {
		return contract.DeploymentObservation{}, NewError("worker_bootstrap_unavailable", http.StatusConflict)
	}
	if session.State != "active" {
		return contract.DeploymentObservation{}, NewError("worker_bootstrap_unavailable", http.StatusConflict)
	}
	leaseExpiresAt, err := time.Parse("2006-01-02T15:04:05.000Z", session.LeaseExpiresAt)
	if err != nil || !leaseExpiresAt.After(now) {
		return contract.DeploymentObservation{}, NewError("worker_session_expired", http.StatusConflict)
	}
	lease := session.LeaseExpiresAt
	var lastEvent *string
	if session.LastSequence > 0 {
		value := session.LastEventAt
		lastEvent = &value
	}
	return contract.DeploymentObservation{
		Schema: contract.DeploymentObservationSchema, DeploymentID: deploymentID,
		Resource:   contract.DeploymentObservationResource{Status: "provisioning", InstanceID: receipt.InstanceID},
		Worker:     contract.DeploymentObservationWorker{BootstrapSessionState: "active", LeaseEpoch: session.LeaseEpoch, LeaseExpiresAt: &lease, LastSequence: session.LastSequence, LastEventAt: lastEvent},
		ObservedAt: contract.CanonicalInstant(now),
	}, nil
}
