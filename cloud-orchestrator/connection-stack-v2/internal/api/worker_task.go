package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func (b Broker) executeWorkerTask(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
	existing, found, err := b.DeploymentStore.Lookup(request.Context(), command.ConnectionID, command.CommandID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if found && !existing.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	if !found {
		if err := command.ValidateAt(now); err != nil {
			status := http.StatusBadRequest
			if contract.Code(err) == "expired_command" {
				status = http.StatusUnauthorized
			}
			writeError(response, status, contract.Code(err))
			return
		}
	}
	if found {
		if _, err := contract.DecodeWorkerTaskResult(command, existing.ResultJSON); err != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		identity.ResultJSON = append([]byte(nil), existing.ResultJSON...)
	}
	if command.Action == contract.ActionWorkerTaskIssue {
		b.executeWorkerTaskIssue(response, request, command, identity, found)
		return
	}
	b.executeWorkerTaskObserve(response, request, command, identity, found)
}

func (b Broker) executeWorkerTaskIssue(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, replay bool) {
	issue, err := command.WorkerTaskIssueRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	if replay {
		storedTask, taskFound, lookupErr := b.WorkerTasks.LookupWorkerTask(request.Context(), issue.DeploymentID, issue.TaskID)
		if lookupErr != nil {
			writeStoreError(response, lookupErr)
			return
		}
		if taskFound {
			if storedTask.ConnectionID != command.ConnectionID || storedTask.RequestSHA256 != identity.RequestSHA256 ||
				storedTask.TaskKind != issue.TaskKind || storedTask.ExecutionManifestDigest != issue.ExecutionManifestDigest ||
				storedTask.InputDigest != issue.InputDigest {
				writeError(response, http.StatusConflict, "worker_task_conflict")
				return
			}
			resultJSON, marshalErr := contract.MarshalWorkerTaskResult(command, workerTaskSummary(storedTask), true)
			if marshalErr != nil {
				writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
				return
			}
			writeRawJSON(response, http.StatusOK, resultJSON)
			return
		}
	}
	reservation, found, err := b.DeploymentStore.LookupDeployment(request.Context(), command.ConnectionID, issue.DeploymentID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || reservation.State != "finalized" || reservation.WorkerSession.ExpectedInstanceID == "" ||
		(reservation.WorkerSession.State != "bound" && reservation.WorkerSession.State != "active") {
		writeError(response, http.StatusConflict, "worker_session_not_ready")
		return
	}
	issuedAt, parseErr := time.Parse("2006-01-02T15:04:05.000Z", command.IssuedAt)
	if parseErr != nil {
		writeError(response, http.StatusBadRequest, "invalid_command")
		return
	}
	createdAt := issuedAt.UTC().Format("2006-01-02T15:04:05.000Z")
	task := commandstore.WorkerTaskRecord{ConnectionID: command.ConnectionID, DeploymentID: issue.DeploymentID, TaskID: issue.TaskID,
		RequestSHA256: identity.RequestSHA256, BootstrapSessionID: reservation.BootstrapSessionID,
		ExpectedInstanceID: reservation.WorkerSession.ExpectedInstanceID, TaskKind: issue.TaskKind,
		ExecutionManifestDigest: issue.ExecutionManifestDigest, InputDigest: issue.InputDigest,
		Status: "queued", Attempt: 1, CreatedAt: createdAt, UpdatedAt: createdAt}
	if !replay {
		queued := workerTaskSummary(task)
		resultJSON, marshalErr := contract.MarshalWorkerTaskResult(command, queued, false)
		if marshalErr != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		identity.ResultJSON = resultJSON
	}
	storedReceipt, storedTask, created, err := b.WorkerTasks.IssueWorkerTask(request.Context(), identity, task)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !storedReceipt.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	replay = !created
	resultJSON, err := contract.MarshalWorkerTaskResult(command, workerTaskSummary(storedTask), replay)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, resultJSON)
}

func (b Broker) executeWorkerTaskObserve(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, replay bool) {
	observe, err := command.WorkerTaskObserveRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	task, found, err := b.WorkerTasks.LookupWorkerTask(request.Context(), observe.DeploymentID, observe.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || task.ConnectionID != command.ConnectionID {
		writeError(response, http.StatusNotFound, "worker_task_not_found")
		return
	}
	resultJSON, err := contract.MarshalWorkerTaskResult(command, workerTaskSummary(task), replay)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
		return
	}
	if !replay {
		identity.ResultJSON = resultJSON
		stored, created, commitErr := b.DeploymentStore.Commit(request.Context(), identity, nil)
		if commitErr != nil {
			writeStoreError(response, commitErr)
			return
		}
		if !stored.SameIdentity(identity) {
			writeError(response, http.StatusConflict, "command_id_conflict")
			return
		}
		if !created {
			replay = true
			resultJSON, err = contract.MarshalWorkerTaskResult(command, workerTaskSummary(task), true)
			if err != nil {
				writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
				return
			}
		}
	}
	writeRawJSON(response, http.StatusOK, resultJSON)
}

func (b Broker) serveWorkerTaskClaim(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	claim, err := contract.ParseWorkerTaskClaimRequest(raw)
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(claim.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	task, found, _, err := b.WorkerTasks.ClaimWorkerTask(request.Context(), workerTaskAuthorization(authorization))
	if err != nil {
		writeStoreError(response, err)
		return
	}
	var claimed *contract.WorkerTask
	if found {
		value := workerTaskContract(task)
		claimed = &value
	}
	result, err := contract.MarshalWorkerTaskClaimResponse(claim.LeaseEpoch, claimed)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func (b Broker) serveWorkerTaskEvent(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	event, err := contract.ParseWorkerTaskEvent(raw)
	if err != nil || event.TaskID != route.taskID {
		writeError(response, http.StatusBadRequest, "invalid_worker_task_event")
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(event.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	task, found, err := b.WorkerTasks.LookupWorkerTask(request.Context(), authorization.Session.DeploymentID, event.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found {
		writeError(response, http.StatusNotFound, "worker_task_not_found")
		return
	}
	if err := event.ValidateFor(workerTaskContract(task)); err != nil {
		writeError(response, http.StatusConflict, contract.Code(err))
		return
	}
	digest := sha256.Sum256(raw)
	storeEvent := commandstore.WorkerTaskEvent{TaskID: event.TaskID, Attempt: int64(event.Attempt), LeaseEpoch: int64(event.LeaseEpoch),
		Sequence: int64(event.Sequence), Status: string(event.Status), OccurredAt: event.OccurredAt, EventSHA256: hex.EncodeToString(digest[:])}
	if event.Checkpoint != nil {
		storeEvent.Checkpoint = *event.Checkpoint
	}
	if event.ErrorCode != nil {
		storeEvent.ErrorCode = *event.ErrorCode
	}
	if event.EvidenceDigest != nil {
		storeEvent.EvidenceDigest = *event.EvidenceDigest
	}
	_, idempotent, err := b.WorkerTasks.RecordWorkerTaskEvent(request.Context(), workerTaskAuthorization(authorization), storeEvent)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	receipt, err := contract.NewWorkerTaskEventReceipt(event, idempotent)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
		return
	}
	result, err := json.Marshal(receipt)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func workerTaskAuthorization(auth workerAuthorization) commandstore.WorkerLeaseAuthorization {
	return commandstore.WorkerLeaseAuthorization{ConnectionID: auth.Session.ConnectionID, DeploymentID: auth.Session.DeploymentID,
		BootstrapSessionID: auth.Session.BootstrapSessionID, ExpectedInstanceID: auth.Session.ExpectedInstanceID,
		LeaseEpoch: auth.Session.LeaseEpoch, TokenSHA256: auth.TokenSHA256, Now: auth.Now.Format("2006-01-02T15:04:05.000Z")}
}

func workerTaskContract(task commandstore.WorkerTaskRecord) contract.WorkerTask {
	return contract.WorkerTask{Schema: contract.WorkerTaskSchema, TaskID: task.TaskID, DeploymentID: task.DeploymentID,
		TaskKind: task.TaskKind, ExecutionManifestDigest: task.ExecutionManifestDigest, InputDigest: task.InputDigest,
		Attempt: uint64(task.Attempt), LastSequence: uint64(task.LastSequence)}
}

func workerTaskSummary(task commandstore.WorkerTaskRecord) contract.WorkerTaskSummary {
	summary := contract.WorkerTaskSummary{TaskID: task.TaskID, DeploymentID: task.DeploymentID, Status: task.Status,
		Attempt: task.Attempt, LastSequence: task.LastSequence, UpdatedAt: task.UpdatedAt}
	if task.Checkpoint != "" {
		value := task.Checkpoint
		summary.Checkpoint = &value
	}
	if task.ErrorCode != "" {
		value := task.ErrorCode
		summary.ErrorCode = &value
	}
	if task.EvidenceDigest != "" {
		value := task.EvidenceDigest
		summary.EvidenceDigest = &value
	}
	return summary
}
