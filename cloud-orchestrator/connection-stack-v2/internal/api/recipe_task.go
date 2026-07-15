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

func (b Broker) executeRecipeTask(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
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
	} else {
		if _, err := contract.DecodeRecipeTaskResult(command, existing.ResultJSON); err != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		identity.ResultJSON = append([]byte(nil), existing.ResultJSON...)
	}
	if command.Action == contract.ActionWorkerRecipeTaskIssue {
		b.executeRecipeTaskIssue(response, request, command, identity, found)
		return
	}
	b.executeRecipeTaskObserve(response, request, command, identity, found)
}

func (b Broker) executeRecipeTaskIssue(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, replay bool) {
	issue, err := command.RecipeTaskIssueRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	manifestJSON, err := issue.Manifest.CanonicalJSON()
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_recipe_execution_manifest")
		return
	}
	if replay {
		stored, found, lookupErr := b.RecipeTasks.LookupRecipeTask(request.Context(), issue.DeploymentID, issue.TaskID)
		if lookupErr != nil {
			writeStoreError(response, lookupErr)
			return
		}
		if found {
			if !recipeTaskMatchesIssue(stored, command.ConnectionID, identity.RequestSHA256, issue, manifestJSON) {
				writeError(response, http.StatusConflict, "recipe_task_conflict")
				return
			}
			result, marshalErr := contract.MarshalRecipeTaskResult(command, recipeTaskSummary(stored), true)
			if marshalErr != nil {
				writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
				return
			}
			writeRawJSON(response, http.StatusOK, result)
			return
		}
	}
	reservation, found, err := b.DeploymentStore.LookupDeployment(request.Context(), command.ConnectionID, issue.DeploymentID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || reservation.State != "finalized" || reservation.WorkerSession.ExpectedInstanceID == "" || (reservation.WorkerSession.State != "bound" && reservation.WorkerSession.State != "active") {
		writeError(response, http.StatusConflict, "worker_session_not_ready")
		return
	}
	if issue.Manifest.WorkerResourceManifestDigest != reservation.WorkerSession.WorkerImageDigest {
		writeError(response, http.StatusConflict, "recipe_task_worker_binding_mismatch")
		return
	}
	issuedAt, parseErr := time.Parse("2006-01-02T15:04:05.000Z", command.IssuedAt)
	if parseErr != nil {
		writeError(response, http.StatusBadRequest, "invalid_command")
		return
	}
	createdAt := issuedAt.UTC().Format("2006-01-02T15:04:05.000Z")
	task := commandstore.RecipeTaskRecord{ConnectionID: command.ConnectionID, DeploymentID: issue.DeploymentID, TaskID: issue.TaskID, RequestSHA256: identity.RequestSHA256, BootstrapSessionID: reservation.BootstrapSessionID, ExpectedInstanceID: reservation.WorkerSession.ExpectedInstanceID, ExecutionID: issue.ExecutionID, TaskKind: issue.TaskKind, RecipeExecutionManifestDigest: issue.RecipeExecutionManifestDigest, InputDigest: issue.InputDigest, CheckpointSequence: append([]string(nil), issue.CheckpointSequence...), ManifestJSON: manifestJSON, Status: "queued", Attempt: 1, CreatedAt: createdAt, UpdatedAt: createdAt}
	if !replay {
		result, marshalErr := contract.MarshalRecipeTaskResult(command, recipeTaskSummary(task), false)
		if marshalErr != nil {
			writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
			return
		}
		identity.ResultJSON = result
	}
	storedReceipt, storedTask, created, err := b.RecipeTasks.IssueRecipeTask(request.Context(), identity, task)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !storedReceipt.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	result, err := contract.MarshalRecipeTaskResult(command, recipeTaskSummary(storedTask), !created)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func (b Broker) executeRecipeTaskObserve(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, replay bool) {
	observe, err := command.RecipeTaskObserveRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	task, found, err := b.RecipeTasks.LookupRecipeTask(request.Context(), observe.DeploymentID, observe.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || task.ConnectionID != command.ConnectionID {
		writeError(response, http.StatusNotFound, "recipe_task_not_found")
		return
	}
	result, err := contract.MarshalRecipeTaskResult(command, recipeTaskSummary(task), replay)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
		return
	}
	if !replay {
		identity.ResultJSON = result
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
			result, err = contract.MarshalRecipeTaskResult(command, recipeTaskSummary(task), true)
			if err != nil {
				writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
				return
			}
		}
	}
	writeRawJSON(response, http.StatusOK, result)
}

func (b Broker) serveRecipeTaskClaim(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	claim, err := contract.ParseRecipeTaskClaimRequest(raw)
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(claim.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	task, found, _, err := b.RecipeTasks.ClaimRecipeTask(request.Context(), workerTaskAuthorization(authorization))
	if err != nil {
		writeStoreError(response, err)
		return
	}
	var claimed *contract.RecipeTaskV1
	var manifest *contract.RecipeExecutionManifestV1
	if found {
		value := recipeTaskContract(task)
		claimed = &value
		parsed, parseErr := contract.ParseRecipeExecutionManifestJSON(task.ManifestJSON)
		if parseErr != nil {
			writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
			return
		}
		manifest = &parsed
	}
	result, err := contract.MarshalRecipeTaskClaimResponse(claim.LeaseEpoch, claimed, manifest)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func (b Broker) serveRecipeTaskEvent(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	event, err := contract.ParseRecipeTaskEventV1(raw)
	if err != nil || event.TaskID != route.taskID {
		writeError(response, http.StatusBadRequest, "invalid_recipe_task_event")
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(event.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	task, found, err := b.RecipeTasks.LookupRecipeTask(request.Context(), authorization.Session.DeploymentID, event.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found {
		writeError(response, http.StatusNotFound, "recipe_task_not_found")
		return
	}
	if int64(event.Sequence) != task.LastSequence || int64(event.Attempt) != task.Attempt {
		if err := contract.ValidateRecipeTaskAdvance(recipeTaskContract(task), task.Status, event, uint64(authorization.Session.LeaseEpoch)); err != nil {
			writeError(response, http.StatusConflict, contract.Code(err))
			return
		}
	}
	digest := sha256.Sum256(raw)
	storeEvent := commandstore.RecipeTaskEvent{TaskID: event.TaskID, Attempt: int64(event.Attempt), LeaseEpoch: int64(event.LeaseEpoch), Sequence: int64(event.Sequence), Status: event.Status, OccurredAt: event.OccurredAt, EventSHA256: hex.EncodeToString(digest[:])}
	if event.Checkpoint != nil {
		storeEvent.Checkpoint = *event.Checkpoint
	}
	if event.ErrorCode != nil {
		storeEvent.ErrorCode = *event.ErrorCode
	}
	if event.EvidenceDigest != nil {
		storeEvent.EvidenceDigest = *event.EvidenceDigest
	}
	_, idempotent, err := b.RecipeTasks.RecordRecipeTaskEvent(request.Context(), workerTaskAuthorization(authorization), storeEvent)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	receipt, err := contract.NewRecipeTaskEventReceipt(event, idempotent)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
		return
	}
	result, err := json.Marshal(receipt)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "recipe_task_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func recipeTaskContract(task commandstore.RecipeTaskRecord) contract.RecipeTaskV1 {
	return contract.RecipeTaskV1{Schema: contract.RecipeTaskV1Schema, TaskID: task.TaskID, ExecutionID: task.ExecutionID, DeploymentID: task.DeploymentID, TaskKind: task.TaskKind, RecipeExecutionManifestDigest: task.RecipeExecutionManifestDigest, InputDigest: task.InputDigest, CheckpointSequence: append([]string(nil), task.CheckpointSequence...), LastCheckpoint: task.LastCheckpoint, Attempt: uint64(task.Attempt), LastSequence: uint64(task.LastSequence)}
}

func recipeTaskSummary(task commandstore.RecipeTaskRecord) contract.RecipeTaskSummary {
	summary := contract.RecipeTaskSummary{TaskID: task.TaskID, ExecutionID: task.ExecutionID, DeploymentID: task.DeploymentID, Status: task.Status, Attempt: task.Attempt, LastSequence: task.LastSequence, LastCheckpoint: task.LastCheckpoint, UpdatedAt: task.UpdatedAt}
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

func recipeTaskMatchesIssue(task commandstore.RecipeTaskRecord, connectionID, requestSHA string, issue contract.RecipeTaskIssueRequest, manifestJSON []byte) bool {
	if task.ConnectionID != connectionID || task.RequestSHA256 != requestSHA || task.ExecutionID != issue.ExecutionID || task.TaskKind != issue.TaskKind || task.RecipeExecutionManifestDigest != issue.RecipeExecutionManifestDigest || task.InputDigest != issue.InputDigest || string(task.ManifestJSON) != string(manifestJSON) || len(task.CheckpointSequence) != len(issue.CheckpointSequence) {
		return false
	}
	for index := range task.CheckpointSequence {
		if task.CheckpointSequence[index] != issue.CheckpointSequence[index] {
			return false
		}
	}
	return true
}
