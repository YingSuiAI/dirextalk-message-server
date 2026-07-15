package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type ReadinessChallengeGenerator interface {
	Generate(time.Time) (contract.ServiceReadinessChallengeV1, error)
}
type CryptoReadinessChallengeGenerator struct{}

func (CryptoReadinessChallengeGenerator) Generate(now time.Time) (contract.ServiceReadinessChallengeV1, error) {
	raw := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return contract.ServiceReadinessChallengeV1{}, errors.New("readiness challenge unavailable")
	}
	return contract.NewServiceReadinessChallenge(raw, contract.CanonicalInstant(now.UTC().Add(2*time.Minute)))
}

func (b Broker) executeServiceReadiness(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
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
			writeError(response, http.StatusBadRequest, contract.Code(err))
			return
		}
	} else {
		if _, err := contract.DecodeServiceReadinessResult(command, existing.ResultJSON); err != nil {
			writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
			return
		}
		writeRawJSON(response, http.StatusOK, existing.ResultJSON)
		return
	}
	if command.Action == contract.ActionServiceReadinessIssue {
		b.executeServiceReadinessIssue(response, request, command, identity)
		return
	}
	b.executeServiceReadinessObserve(response, request, command, identity)
}

func (b Broker) executeServiceReadinessIssue(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record) {
	issue, err := command.ServiceReadinessIssueRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
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
	issuedAt, err := time.Parse("2006-01-02T15:04:05.000Z", command.IssuedAt)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_command")
		return
	}
	instant := issuedAt.UTC().Format("2006-01-02T15:04:05.000Z")
	task := commandstore.ServiceReadinessRecord{ConnectionID: command.ConnectionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, TaskID: issue.TaskID, RequestSHA256: identity.RequestSHA256, BootstrapSessionID: reservation.BootstrapSessionID, ExpectedInstanceID: reservation.WorkerSession.ExpectedInstanceID, ExecutionID: issue.ExecutionID, ProbeKind: issue.ProbeKind, RecipeExecutionManifestDigest: issue.RecipeExecutionManifestDigest, InstallEvidenceDigest: issue.InstallEvidenceDigest, SemanticExpectationDigest: issue.SemanticExpectationDigest, Status: "queued", Attempt: 1, CreatedAt: instant, UpdatedAt: instant}
	result, marshalErr := contract.MarshalServiceReadinessResult(command, readinessSummary(task), false)
	if marshalErr != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	identity.ResultJSON = result
	storedReceipt, _, _, err := b.ServiceReadiness.IssueServiceReadiness(request.Context(), identity, task)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !storedReceipt.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	if _, err := contract.DecodeServiceReadinessResult(command, storedReceipt.ResultJSON); err != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, storedReceipt.ResultJSON)
}

func (b Broker) executeServiceReadinessObserve(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record) {
	observe, err := command.ServiceReadinessObserveRequest()
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	task, found, err := b.ServiceReadiness.LookupServiceReadiness(request.Context(), observe.DeploymentID, observe.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || task.ConnectionID != command.ConnectionID || task.ServiceID != observe.ServiceID {
		writeError(response, http.StatusNotFound, "service_readiness_not_found")
		return
	}
	result, err := contract.MarshalServiceReadinessResult(command, readinessSummary(task), false)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	identity.ResultJSON = result
	stored, _, commitErr := b.DeploymentStore.Commit(request.Context(), identity, nil)
	if commitErr != nil {
		writeStoreError(response, commitErr)
		return
	}
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	if _, err := contract.DecodeServiceReadinessResult(command, stored.ResultJSON); err != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func (b Broker) serveServiceReadinessClaim(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	claim, err := contract.ParseServiceReadinessClaimRequest(raw)
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(claim.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	generator := b.ReadinessChallenges
	if generator == nil {
		generator = CryptoReadinessChallengeGenerator{}
	}
	challenge, err := generator.Generate(authorization.Now)
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "service_readiness_unavailable")
		return
	}
	task, found, err := b.ServiceReadiness.ClaimServiceReadiness(request.Context(), workerTaskAuthorization(authorization), commandstore.ServiceReadinessChallengeGrant{Digest: challenge.ChallengeDigest, ExpiresAt: challenge.ExpiresAt})
	if err != nil {
		writeStoreError(response, err)
		return
	}
	var wire *contract.ServiceReadinessTaskV1
	var delivered *contract.ServiceReadinessChallengeV1
	if found {
		value := readinessTaskContract(task)
		wire = &value
		delivered = &challenge
	}
	result, err := contract.MarshalServiceReadinessClaimResponse(claim.LeaseEpoch, wire, delivered)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func (b Broker) serveServiceReadinessEvent(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	event, err := contract.ParseServiceReadinessEvent(raw)
	if err != nil || event.TaskID != route.taskID {
		writeError(response, http.StatusBadRequest, "invalid_service_readiness_event")
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(event.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	task, found, err := b.ServiceReadiness.LookupServiceReadiness(request.Context(), authorization.Session.DeploymentID, event.TaskID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found {
		writeError(response, http.StatusNotFound, "service_readiness_not_found")
		return
	}
	digest := sha256.Sum256(raw)
	eventSHA := hex.EncodeToString(digest[:])
	storedEvent := commandstore.ServiceReadinessEvent{TaskID: event.TaskID, Attempt: int64(event.Attempt), LeaseEpoch: int64(event.LeaseEpoch), Sequence: int64(event.Sequence), Status: event.Status, OccurredAt: event.OccurredAt, EventSHA256: eventSHA}
	if event.ChallengeDigest != nil {
		storedEvent.ChallengeDigest = *event.ChallengeDigest
	}
	if event.SemanticEvidenceDigest != nil {
		storedEvent.SemanticEvidenceDigest = *event.SemanticEvidenceDigest
	}
	if event.ErrorCode != nil {
		storedEvent.ErrorCode = *event.ErrorCode
	}
	if event.Status == "succeeded" {
		sum := sha256.Sum256([]byte("dirextalk.service-readiness-stack-observation/v1\x00" + task.TaskID + "\x00" + task.ChallengeDigest + "\x00" + task.SemanticExpectationDigest + "\x00" + eventSHA))
		storedEvent.StackObservationDigest = "sha256:" + hex.EncodeToString(sum[:])
	}
	_, idempotent, err := b.ServiceReadiness.RecordServiceReadinessEvent(request.Context(), workerTaskAuthorization(authorization), storedEvent)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	receipt := contract.NewServiceReadinessEventReceipt(event, idempotent)
	result, err := json.Marshal(receipt)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "service_readiness_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}

func readinessTaskContract(task commandstore.ServiceReadinessRecord) contract.ServiceReadinessTaskV1 {
	return contract.ServiceReadinessTaskV1{Schema: contract.ServiceReadinessTaskSchema, TaskID: task.TaskID, ExecutionID: task.ExecutionID, DeploymentID: task.DeploymentID, ServiceID: task.ServiceID, ProbeKind: task.ProbeKind, RecipeExecutionManifestDigest: task.RecipeExecutionManifestDigest, InstallEvidenceDigest: task.InstallEvidenceDigest, SemanticExpectationDigest: task.SemanticExpectationDigest, Attempt: uint64(task.Attempt), LastSequence: uint64(task.LastSequence)}
}
func readinessSummary(task commandstore.ServiceReadinessRecord) contract.ServiceReadinessTaskSummary {
	summary := contract.ServiceReadinessTaskSummary{ExecutionID: task.ExecutionID, DeploymentID: task.DeploymentID, ServiceID: task.ServiceID, TaskID: task.TaskID, Status: task.Status, Checkpoint: task.Checkpoint, Attempt: task.Attempt, LastSequence: task.LastSequence, UpdatedAt: task.UpdatedAt}
	if task.ChallengeDigest != "" {
		summary.ChallengeDigest = &task.ChallengeDigest
	}
	if task.SemanticEvidenceDigest != "" {
		summary.SemanticEvidenceDigest = &task.SemanticEvidenceDigest
	}
	if task.StackObservationDigest != "" {
		summary.StackObservationDigest = &task.StackObservationDigest
	}
	if task.ErrorCode != "" {
		summary.ErrorCode = &task.ErrorCode
	}
	return summary
}
