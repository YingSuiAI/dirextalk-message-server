package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

const (
	ServiceReadinessIssueSchema         = "dirextalk.service-readiness-task-issue/v1"
	ServiceReadinessTaskSchema          = "dirextalk.service-readiness-task/v1"
	ServiceReadinessClaimSchema         = "dirextalk.service-readiness-task-claim/v1"
	ServiceReadinessClaimResponseSchema = "dirextalk.service-readiness-task-claim-response/v1"
	ServiceReadinessChallengeSchema     = "dirextalk.service-readiness-challenge/v1"
	ServiceReadinessEventSchema         = "dirextalk.service-readiness-task-event/v1"
	ServiceReadinessEventReceiptSchema  = "dirextalk.service-readiness-task-event-receipt/v1"
	ServiceReadinessProbeKind           = "stack_witnessed_fixed_worker_probe_v1"
)

type ServiceReadinessIssueRequest struct {
	Schema                        string `json:"schema"`
	ExecutionID                   string `json:"execution_id"`
	DeploymentID                  string `json:"deployment_id"`
	ServiceID                     string `json:"service_id"`
	TaskID                        string `json:"task_id"`
	ProbeKind                     string `json:"probe_kind"`
	RecipeExecutionManifestDigest string `json:"recipe_execution_manifest_digest"`
	InstallEvidenceDigest         string `json:"install_evidence_digest"`
	SemanticExpectationDigest     string `json:"semantic_expectation_digest"`
}

type ServiceReadinessObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	ServiceID    string `json:"service_id"`
	TaskID       string `json:"task_id"`
}

type ServiceReadinessTaskV1 struct {
	Schema                        string `json:"schema"`
	TaskID                        string `json:"task_id"`
	ExecutionID                   string `json:"execution_id"`
	DeploymentID                  string `json:"deployment_id"`
	ServiceID                     string `json:"service_id"`
	ProbeKind                     string `json:"probe_kind"`
	RecipeExecutionManifestDigest string `json:"recipe_execution_manifest_digest"`
	InstallEvidenceDigest         string `json:"install_evidence_digest"`
	SemanticExpectationDigest     string `json:"semantic_expectation_digest"`
	Attempt                       uint64 `json:"attempt"`
	LastSequence                  uint64 `json:"last_sequence"`
}

type ServiceReadinessChallengeV1 struct {
	Schema          string `json:"schema"`
	ChallengeB64    string `json:"challenge_b64"`
	ChallengeDigest string `json:"challenge_digest"`
	ExpiresAt       string `json:"expires_at"`
}

type ServiceReadinessClaimRequest struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

type ServiceReadinessClaimResponse struct {
	Schema     string                       `json:"schema"`
	Status     string                       `json:"status"`
	LeaseEpoch uint64                       `json:"lease_epoch"`
	Task       *ServiceReadinessTaskV1      `json:"task,omitempty"`
	Challenge  *ServiceReadinessChallengeV1 `json:"challenge,omitempty"`
}

type ServiceReadinessEventV1 struct {
	Schema                 string  `json:"schema"`
	TaskID                 string  `json:"task_id"`
	Attempt                uint64  `json:"attempt"`
	LeaseEpoch             uint64  `json:"lease_epoch"`
	Sequence               uint64  `json:"sequence"`
	Status                 string  `json:"status"`
	ChallengeDigest        *string `json:"challenge_digest"`
	SemanticEvidenceDigest *string `json:"semantic_evidence_digest"`
	ErrorCode              *string `json:"error_code"`
	OccurredAt             string  `json:"occurred_at"`
}

type ServiceReadinessTaskSummary struct {
	ExecutionID            string  `json:"execution_id"`
	DeploymentID           string  `json:"deployment_id"`
	ServiceID              string  `json:"service_id"`
	TaskID                 string  `json:"task_id"`
	Status                 string  `json:"status"`
	Checkpoint             string  `json:"checkpoint"`
	Attempt                int64   `json:"attempt"`
	LastSequence           int64   `json:"last_sequence"`
	ChallengeDigest        *string `json:"challenge_digest"`
	SemanticEvidenceDigest *string `json:"semantic_evidence_digest"`
	StackObservationDigest *string `json:"stack_observation_digest"`
	ErrorCode              *string `json:"error_code"`
	UpdatedAt              string  `json:"updated_at"`
}

type ServiceReadinessResult struct {
	Status  string                      `json:"status"`
	Receipt ServiceReadinessReceipt     `json:"receipt"`
	Task    ServiceReadinessTaskSummary `json:"task"`
}

type ServiceReadinessReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type ServiceReadinessEventReceipt struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

func (request ServiceReadinessIssueRequest) Validate() error {
	if request.Schema != ServiceReadinessIssueSchema || !recipeBindingIDPattern.MatchString(request.ExecutionID) ||
		!recipeBindingIDPattern.MatchString(request.DeploymentID) || !recipeBindingIDPattern.MatchString(request.ServiceID) ||
		!recipeTaskIDPattern.MatchString(request.TaskID) || request.ProbeKind != ServiceReadinessProbeKind ||
		!namedSHA256Pattern.MatchString(request.RecipeExecutionManifestDigest) || !namedSHA256Pattern.MatchString(request.InstallEvidenceDigest) ||
		!namedSHA256Pattern.MatchString(request.SemanticExpectationDigest) {
		return errCode("invalid_service_readiness_issue_request")
	}
	return nil
}

func ParseServiceReadinessIssueRequest(raw []byte) (ServiceReadinessIssueRequest, error) {
	var request ServiceReadinessIssueRequest
	fields := []string{"schema", "execution_id", "deployment_id", "service_id", "task_id", "probe_kind", "recipe_execution_manifest_digest", "install_evidence_digest", "semantic_expectation_digest"}
	if !strictRecipeTaskObject(raw, fields, &request) || request.Validate() != nil {
		return ServiceReadinessIssueRequest{}, errCode("invalid_service_readiness_issue_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(raw, canonical) {
		return ServiceReadinessIssueRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (command Command) ServiceReadinessIssueRequest() (ServiceReadinessIssueRequest, error) {
	if command.Action != ActionServiceReadinessIssue {
		return ServiceReadinessIssueRequest{}, errCode("invalid_service_readiness_issue_request")
	}
	payload, err := command.actionPayload()
	if err != nil {
		return ServiceReadinessIssueRequest{}, err
	}
	return ParseServiceReadinessIssueRequest(payload)
}

func ParseServiceReadinessObserveRequest(raw []byte) (ServiceReadinessObserveRequest, error) {
	var request ServiceReadinessObserveRequest
	if !strictRecipeTaskObject(raw, []string{"deployment_id", "service_id", "task_id"}, &request) ||
		!recipeBindingIDPattern.MatchString(request.DeploymentID) || !recipeBindingIDPattern.MatchString(request.ServiceID) || !recipeTaskIDPattern.MatchString(request.TaskID) {
		return ServiceReadinessObserveRequest{}, errCode("invalid_service_readiness_observe_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(raw, canonical) {
		return ServiceReadinessObserveRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (command Command) ServiceReadinessObserveRequest() (ServiceReadinessObserveRequest, error) {
	if command.Action != ActionServiceReadinessObserve {
		return ServiceReadinessObserveRequest{}, errCode("invalid_service_readiness_observe_request")
	}
	payload, err := command.actionPayload()
	if err != nil {
		return ServiceReadinessObserveRequest{}, err
	}
	return ParseServiceReadinessObserveRequest(payload)
}

func ParseServiceReadinessClaimRequest(raw []byte) (ServiceReadinessClaimRequest, error) {
	var request ServiceReadinessClaimRequest
	if !strictRecipeTaskObject(raw, []string{"schema", "lease_epoch"}, &request) || request.Schema != ServiceReadinessClaimSchema || !workerTaskPositive(request.LeaseEpoch) {
		return ServiceReadinessClaimRequest{}, errCode("invalid_service_readiness_claim")
	}
	return request, nil
}

func NewServiceReadinessChallenge(raw []byte, expiresAt string) (ServiceReadinessChallengeV1, error) {
	if len(raw) != 32 || !canonicalTaskInstant(expiresAt) {
		return ServiceReadinessChallengeV1{}, errCode("invalid_service_readiness_challenge")
	}
	sum := sha256.Sum256(raw)
	return ServiceReadinessChallengeV1{Schema: ServiceReadinessChallengeSchema, ChallengeB64: base64.StdEncoding.EncodeToString(raw), ChallengeDigest: "sha256:" + hex.EncodeToString(sum[:]), ExpiresAt: expiresAt}, nil
}

func (challenge ServiceReadinessChallengeV1) Validate() error {
	raw, err := base64.StdEncoding.DecodeString(challenge.ChallengeB64)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != challenge.ChallengeB64 || len(raw) != 32 {
		return errCode("invalid_service_readiness_challenge")
	}
	want, _ := NewServiceReadinessChallenge(raw, challenge.ExpiresAt)
	if challenge.Schema != want.Schema || challenge.ChallengeDigest != want.ChallengeDigest {
		return errCode("invalid_service_readiness_challenge")
	}
	return nil
}

func MarshalServiceReadinessClaimResponse(leaseEpoch uint64, task *ServiceReadinessTaskV1, challenge *ServiceReadinessChallengeV1) ([]byte, error) {
	response := ServiceReadinessClaimResponse{Schema: ServiceReadinessClaimResponseSchema, Status: "none", LeaseEpoch: leaseEpoch}
	if task != nil || challenge != nil {
		if task == nil || challenge == nil || task.Validate() != nil || challenge.Validate() != nil {
			return nil, errCode("invalid_service_readiness_claim_response")
		}
		response.Status, response.Task, response.Challenge = "claimed", task, challenge
	}
	if !workerTaskPositive(response.LeaseEpoch) {
		return nil, errCode("invalid_service_readiness_claim_response")
	}
	return json.Marshal(response)
}

func (task ServiceReadinessTaskV1) Validate() error {
	request := ServiceReadinessIssueRequest{Schema: ServiceReadinessIssueSchema, ExecutionID: task.ExecutionID, DeploymentID: task.DeploymentID, ServiceID: task.ServiceID, TaskID: task.TaskID, ProbeKind: task.ProbeKind, RecipeExecutionManifestDigest: task.RecipeExecutionManifestDigest, InstallEvidenceDigest: task.InstallEvidenceDigest, SemanticExpectationDigest: task.SemanticExpectationDigest}
	if task.Schema != ServiceReadinessTaskSchema || request.Validate() != nil || !workerTaskPositive(task.Attempt) || task.LastSequence > uint64(maxSafeInteger) {
		return errCode("invalid_service_readiness_task")
	}
	return nil
}

func ParseServiceReadinessEvent(raw []byte) (ServiceReadinessEventV1, error) {
	var event ServiceReadinessEventV1
	fields := []string{"schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "challenge_digest", "semantic_evidence_digest", "error_code", "occurred_at"}
	if !strictRecipeTaskObject(raw, fields, &event) || event.Validate() != nil {
		return ServiceReadinessEventV1{}, errCode("invalid_service_readiness_event")
	}
	return event, nil
}

func (event ServiceReadinessEventV1) Validate() error {
	if event.Schema != ServiceReadinessEventSchema || !recipeTaskIDPattern.MatchString(event.TaskID) || !workerTaskPositive(event.Attempt) ||
		!workerTaskPositive(event.LeaseEpoch) || event.Sequence != 1 || !canonicalTaskInstant(event.OccurredAt) {
		return errCode("invalid_service_readiness_event")
	}
	switch event.Status {
	case "succeeded":
		if event.ChallengeDigest == nil || event.SemanticEvidenceDigest == nil || event.ErrorCode != nil || !namedSHA256Pattern.MatchString(*event.ChallengeDigest) || !namedSHA256Pattern.MatchString(*event.SemanticEvidenceDigest) {
			return errCode("invalid_service_readiness_event")
		}
	case "failed", "interrupted":
		if event.ChallengeDigest != nil || event.SemanticEvidenceDigest != nil || event.ErrorCode == nil || !recipeTaskCodePattern.MatchString(*event.ErrorCode) {
			return errCode("invalid_service_readiness_event")
		}
	default:
		return errCode("invalid_service_readiness_event")
	}
	return nil
}

func NewServiceReadinessEventReceipt(event ServiceReadinessEventV1, idempotent bool) ServiceReadinessEventReceipt {
	disposition := "accepted"
	if idempotent {
		disposition = "idempotent"
	}
	return ServiceReadinessEventReceipt{Schema: ServiceReadinessEventReceiptSchema, TaskID: event.TaskID, Attempt: event.Attempt, LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: disposition}
}

func MarshalServiceReadinessResult(command Command, summary ServiceReadinessTaskSummary, idempotent bool) ([]byte, error) {
	if command.Action != ActionServiceReadinessIssue && command.Action != ActionServiceReadinessObserve {
		return nil, errCode("invalid_service_readiness_result")
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return nil, err
	}
	status := "service_readiness_issued"
	if command.Action == ActionServiceReadinessObserve {
		status = "service_readiness_observed"
	}
	disposition := "committed"
	if idempotent {
		status, disposition = "idempotent", "idempotent"
	}
	result := ServiceReadinessResult{Status: status, Receipt: ServiceReadinessReceipt{Schema: ReceiptSchema, Disposition: disposition,
		ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		CommandID: command.CommandID, RequestSHA256: requestSHA, Action: command.Action}, Task: summary}
	if err := ValidateServiceReadinessResult(command, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func DecodeServiceReadinessResult(command Command, raw []byte) (ServiceReadinessResult, error) {
	var result ServiceReadinessResult
	object, err := exactJSONObject(raw)
	receipt, receiptErr := exactJSONObject(object["receipt"])
	task, taskErr := exactJSONObject(object["task"])
	if err != nil || !exactFields(object, []string{"status", "receipt", "task"}) || receiptErr != nil ||
		!exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) || taskErr != nil ||
		!exactFields(task, []string{"execution_id", "deployment_id", "service_id", "task_id", "status", "checkpoint", "attempt", "last_sequence", "challenge_digest", "semantic_evidence_digest", "stack_observation_digest", "error_code", "updated_at"}) ||
		decodeSingle(raw, &result) != nil || ValidateServiceReadinessResult(command, result) != nil {
		return ServiceReadinessResult{}, errCode("invalid_service_readiness_result")
	}
	return result, nil
}

func ValidateServiceReadinessResult(command Command, result ServiceReadinessResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	wantStatus := "service_readiness_issued"
	var deploymentID, serviceID, taskID, executionID string
	if command.Action == ActionServiceReadinessIssue {
		request, requestErr := command.ServiceReadinessIssueRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, serviceID, taskID, executionID = request.DeploymentID, request.ServiceID, request.TaskID, request.ExecutionID
	} else if command.Action == ActionServiceReadinessObserve {
		wantStatus = "service_readiness_observed"
		request, requestErr := command.ServiceReadinessObserveRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, serviceID, taskID = request.DeploymentID, request.ServiceID, request.TaskID
	} else {
		return errCode("invalid_service_readiness_result")
	}
	wantDisposition := "committed"
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	} else if result.Status != wantStatus {
		return errCode("invalid_service_readiness_result")
	}
	if result.Receipt.Schema != ReceiptSchema || result.Receipt.Disposition != wantDisposition || result.Receipt.ConnectionID != command.ConnectionID ||
		result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID ||
		result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != command.Action || result.Task.DeploymentID != deploymentID || result.Task.ServiceID != serviceID || result.Task.TaskID != taskID ||
		(executionID != "" && result.Task.ExecutionID != executionID) || result.Task.Attempt < 1 || result.Task.LastSequence < 0 || !canonicalTaskInstant(result.Task.UpdatedAt) {
		return errCode("invalid_service_readiness_result")
	}
	if !recipeBindingIDPattern.MatchString(result.Task.ExecutionID) || !recipeBindingIDPattern.MatchString(result.Task.DeploymentID) || !recipeBindingIDPattern.MatchString(result.Task.ServiceID) || !recipeTaskIDPattern.MatchString(result.Task.TaskID) {
		return errCode("invalid_service_readiness_result")
	}
	validDigest := func(value *string) bool { return value != nil && namedSHA256Pattern.MatchString(*value) }
	switch result.Task.Status {
	case "queued":
		if result.Task.LastSequence != 0 || result.Task.Checkpoint != "" || result.Task.ChallengeDigest != nil || result.Task.SemanticEvidenceDigest != nil || result.Task.StackObservationDigest != nil || result.Task.ErrorCode != nil {
			return errCode("invalid_service_readiness_result")
		}
	case "running":
		if result.Task.LastSequence != 0 || result.Task.Checkpoint != "challenge_issued" || !validDigest(result.Task.ChallengeDigest) || result.Task.SemanticEvidenceDigest != nil || result.Task.StackObservationDigest != nil || result.Task.ErrorCode != nil {
			return errCode("invalid_service_readiness_result")
		}
	case "succeeded":
		if result.Task.LastSequence != 1 || result.Task.Checkpoint != "readiness_verified" || !validDigest(result.Task.ChallengeDigest) || !validDigest(result.Task.SemanticEvidenceDigest) || !validDigest(result.Task.StackObservationDigest) || *result.Task.StackObservationDigest == *result.Task.ChallengeDigest || *result.Task.StackObservationDigest == *result.Task.SemanticEvidenceDigest || result.Task.ErrorCode != nil {
			return errCode("invalid_service_readiness_result")
		}
	case "failed", "interrupted":
		if result.Task.LastSequence != 1 || result.Task.Checkpoint != "" || result.Task.ChallengeDigest != nil || result.Task.SemanticEvidenceDigest != nil || result.Task.StackObservationDigest != nil || result.Task.ErrorCode == nil || !recipeTaskCodePattern.MatchString(*result.Task.ErrorCode) {
			return errCode("invalid_service_readiness_result")
		}
	default:
		return errCode("invalid_service_readiness_result")
	}
	return nil
}
