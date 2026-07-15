package contract

import (
	"bytes"
	"encoding/json"
	"regexp"
)

const (
	WorkerTaskIssueSchema             = "dirextalk.worker-task-issue/v1"
	WorkerTaskKindExecutionProbe      = "execution_probe"
	WorkerTaskClaimSchema             = "dirextalk.worker-task-claim/v1"
	WorkerTaskClaimResponseSchema     = "dirextalk.worker-task-claim-response/v1"
	WorkerTaskSchema                  = "dirextalk.worker-task/v1"
	WorkerTaskEventSchema             = "dirextalk.worker-task-event/v1"
	WorkerTaskEventReceiptSchema      = "dirextalk.worker-task-event-receipt/v1"
	WorkerTaskProbeReceivedCheckpoint = "execution_manifest_received"
	WorkerTaskProbeVerifiedCheckpoint = "task_transport_verified"
	MaxWorkerTaskRequestBytes         = 64 * 1024
)

var workerTaskCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

type WorkerTaskIssueRequest struct {
	Schema                  string `json:"schema"`
	DeploymentID            string `json:"deployment_id"`
	TaskID                  string `json:"task_id"`
	TaskKind                string `json:"task_kind"`
	ExecutionManifestDigest string `json:"execution_manifest_digest"`
	InputDigest             string `json:"input_digest"`
}

type WorkerTaskObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}

type WorkerTask struct {
	Schema                  string `json:"schema"`
	TaskID                  string `json:"task_id"`
	DeploymentID            string `json:"deployment_id"`
	TaskKind                string `json:"task_kind"`
	ExecutionManifestDigest string `json:"execution_manifest_digest"`
	InputDigest             string `json:"input_digest"`
	Attempt                 uint64 `json:"attempt"`
	LastSequence            uint64 `json:"last_sequence"`
}

type WorkerTaskClaimRequest struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

type WorkerTaskClaimResponse struct {
	Schema     string      `json:"schema"`
	Status     string      `json:"status"`
	LeaseEpoch uint64      `json:"lease_epoch"`
	Task       *WorkerTask `json:"task,omitempty"`
}

type WorkerTaskStatus string

const (
	WorkerTaskStatusRunning     WorkerTaskStatus = "running"
	WorkerTaskStatusSucceeded   WorkerTaskStatus = "succeeded"
	WorkerTaskStatusFailed      WorkerTaskStatus = "failed"
	WorkerTaskStatusInterrupted WorkerTaskStatus = "interrupted"
)

type WorkerTaskEvent struct {
	Schema         string           `json:"schema"`
	TaskID         string           `json:"task_id"`
	Attempt        uint64           `json:"attempt"`
	LeaseEpoch     uint64           `json:"lease_epoch"`
	Sequence       uint64           `json:"sequence"`
	Status         WorkerTaskStatus `json:"status"`
	Checkpoint     *string          `json:"checkpoint"`
	ErrorCode      *string          `json:"error_code"`
	EvidenceDigest *string          `json:"evidence_digest"`
	OccurredAt     string           `json:"occurred_at"`
}

type WorkerTaskEventReceipt struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

type WorkerTaskReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type WorkerTaskSummary struct {
	TaskID         string  `json:"task_id"`
	DeploymentID   string  `json:"deployment_id"`
	Status         string  `json:"status"`
	Attempt        int64   `json:"attempt"`
	LastSequence   int64   `json:"last_sequence"`
	Checkpoint     *string `json:"checkpoint"`
	ErrorCode      *string `json:"error_code"`
	EvidenceDigest *string `json:"evidence_digest"`
	UpdatedAt      string  `json:"updated_at"`
}

type WorkerTaskResult struct {
	Status  string            `json:"status"`
	Receipt WorkerTaskReceipt `json:"receipt"`
	Task    WorkerTaskSummary `json:"task"`
}

func MarshalWorkerTaskResult(command Command, summary WorkerTaskSummary, idempotent bool) ([]byte, error) {
	if command.Action != ActionWorkerTaskIssue && command.Action != ActionWorkerTaskObserve {
		return nil, errCode("invalid_worker_task_result")
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return nil, err
	}
	status := "worker_task_issued"
	if command.Action == ActionWorkerTaskObserve {
		status = "worker_task_observed"
	}
	disposition := "committed"
	if idempotent {
		status, disposition = "idempotent", "idempotent"
	}
	result := WorkerTaskResult{Status: status, Receipt: WorkerTaskReceipt{
		Schema: ReceiptSchema, Disposition: disposition, ConnectionID: command.ConnectionID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		CommandID: command.CommandID, RequestSHA256: requestSHA, Action: command.Action,
	}, Task: summary}
	if err := ValidateWorkerTaskResult(command, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func DecodeWorkerTaskResult(command Command, raw []byte) (WorkerTaskResult, error) {
	var result WorkerTaskResult
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "task"}) {
		return WorkerTaskResult{}, errCode("invalid_worker_task_result")
	}
	receipt, receiptErr := exactJSONObject(object["receipt"])
	task, taskErr := exactJSONObject(object["task"])
	if receiptErr != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) ||
		taskErr != nil || !exactFields(task, []string{"task_id", "deployment_id", "status", "attempt", "last_sequence", "checkpoint", "error_code", "evidence_digest", "updated_at"}) ||
		decodeSingle(raw, &result) != nil || ValidateWorkerTaskResult(command, result) != nil {
		return WorkerTaskResult{}, errCode("invalid_worker_task_result")
	}
	return result, nil
}

func ValidateWorkerTaskResult(command Command, result WorkerTaskResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	wantStatus := "worker_task_issued"
	var deploymentID, taskID, manifestDigest string
	if command.Action == ActionWorkerTaskIssue {
		request, requestErr := command.WorkerTaskIssueRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, taskID, manifestDigest = request.DeploymentID, request.TaskID, request.ExecutionManifestDigest
	} else if command.Action == ActionWorkerTaskObserve {
		wantStatus = "worker_task_observed"
		request, requestErr := command.WorkerTaskObserveRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, taskID = request.DeploymentID, request.TaskID
	} else {
		return errCode("invalid_worker_task_result")
	}
	wantDisposition := "committed"
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	} else if result.Status != wantStatus {
		return errCode("invalid_broker_status")
	}
	r := result.Receipt
	if r.Schema != ReceiptSchema || r.Disposition != wantDisposition || r.ConnectionID != command.ConnectionID ||
		r.ExpectedGeneration != command.ExpectedGeneration || r.NodeCounter != command.NodeCounter || r.CommandID != command.CommandID ||
		r.RequestSHA256 != requestSHA || r.Action != command.Action {
		return errCode("invalid_worker_task_receipt")
	}
	if err := result.Task.Validate(deploymentID, taskID); err != nil {
		return err
	}
	if command.Action == ActionWorkerTaskIssue && (result.Task.Status == "running" || result.Task.Status == "succeeded") &&
		(result.Task.EvidenceDigest == nil || *result.Task.EvidenceDigest != manifestDigest) {
		return errCode("invalid_worker_task_summary")
	}
	return nil
}

func (c Command) WorkerTaskIssueRequest() (WorkerTaskIssueRequest, error) {
	if c.Action != ActionWorkerTaskIssue {
		return WorkerTaskIssueRequest{}, errCode("invalid_worker_task_issue_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return WorkerTaskIssueRequest{}, err
	}
	object, err := exactJSONObject(payload)
	fields := []string{"schema", "deployment_id", "task_id", "task_kind", "execution_manifest_digest", "input_digest"}
	if err != nil || !exactFields(object, fields) {
		return WorkerTaskIssueRequest{}, errCode("invalid_worker_task_issue_request")
	}
	var request WorkerTaskIssueRequest
	if decodeSingle(payload, &request) != nil || request.Validate() != nil {
		return WorkerTaskIssueRequest{}, errCode("invalid_worker_task_issue_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return WorkerTaskIssueRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (c Command) WorkerTaskObserveRequest() (WorkerTaskObserveRequest, error) {
	if c.Action != ActionWorkerTaskObserve {
		return WorkerTaskObserveRequest{}, errCode("invalid_worker_task_observe_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return WorkerTaskObserveRequest{}, err
	}
	object, err := exactJSONObject(payload)
	if err != nil || !exactFields(object, []string{"deployment_id", "task_id"}) {
		return WorkerTaskObserveRequest{}, errCode("invalid_worker_task_observe_request")
	}
	var request WorkerTaskObserveRequest
	if decodeSingle(payload, &request) != nil || !ValidID(request.DeploymentID) || !ValidID(request.TaskID) {
		return WorkerTaskObserveRequest{}, errCode("invalid_worker_task_observe_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return WorkerTaskObserveRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (r WorkerTaskIssueRequest) Validate() error {
	if r.Schema != WorkerTaskIssueSchema || !ValidID(r.DeploymentID) || !ValidID(r.TaskID) ||
		r.TaskKind != WorkerTaskKindExecutionProbe || !namedSHA256Pattern.MatchString(r.ExecutionManifestDigest) ||
		!namedSHA256Pattern.MatchString(r.InputDigest) {
		return errCode("invalid_worker_task_issue_request")
	}
	return nil
}

func NewWorkerTask(request WorkerTaskIssueRequest) (WorkerTask, error) {
	if err := request.Validate(); err != nil {
		return WorkerTask{}, err
	}
	return WorkerTask{Schema: WorkerTaskSchema, TaskID: request.TaskID, DeploymentID: request.DeploymentID,
		TaskKind: request.TaskKind, ExecutionManifestDigest: request.ExecutionManifestDigest,
		InputDigest: request.InputDigest, Attempt: 1, LastSequence: 0}, nil
}

func ParseWorkerTaskClaimRequest(raw []byte) (WorkerTaskClaimRequest, error) {
	var request WorkerTaskClaimRequest
	if !strictWorkerTaskObject(raw, []string{"schema", "lease_epoch"}, &request) || request.Validate() != nil {
		return WorkerTaskClaimRequest{}, errCode("invalid_worker_task_claim")
	}
	return request, nil
}

func (r WorkerTaskClaimRequest) Validate() error {
	if r.Schema != WorkerTaskClaimSchema || !workerTaskPositive(r.LeaseEpoch) {
		return errCode("invalid_worker_task_claim")
	}
	return nil
}

func MarshalWorkerTaskClaimResponse(leaseEpoch uint64, task *WorkerTask) ([]byte, error) {
	response := WorkerTaskClaimResponse{Schema: WorkerTaskClaimResponseSchema, Status: "none", LeaseEpoch: leaseEpoch}
	if task != nil {
		response.Status, response.Task = "claimed", task
	}
	if err := response.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func (r WorkerTaskClaimResponse) Validate() error {
	if r.Schema != WorkerTaskClaimResponseSchema || !workerTaskPositive(r.LeaseEpoch) {
		return errCode("invalid_worker_task_claim_response")
	}
	if r.Status == "none" && r.Task == nil {
		return nil
	}
	if r.Status != "claimed" || r.Task == nil || r.Task.Validate() != nil {
		return errCode("invalid_worker_task_claim_response")
	}
	return nil
}

func (t WorkerTask) Validate() error {
	if t.Schema != WorkerTaskSchema || !ValidID(t.TaskID) || !ValidID(t.DeploymentID) ||
		t.TaskKind != WorkerTaskKindExecutionProbe || !namedSHA256Pattern.MatchString(t.ExecutionManifestDigest) ||
		!namedSHA256Pattern.MatchString(t.InputDigest) || !workerTaskPositive(t.Attempt) || t.LastSequence > uint64(maxSafeInteger) {
		return errCode("invalid_worker_task")
	}
	return nil
}

func ParseWorkerTaskEvent(raw []byte) (WorkerTaskEvent, error) {
	var event WorkerTaskEvent
	fields := []string{"schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "checkpoint", "error_code", "evidence_digest", "occurred_at"}
	if !strictWorkerTaskObject(raw, fields, &event) {
		return WorkerTaskEvent{}, errCode("invalid_worker_task_event")
	}
	return event, nil
}

func (e WorkerTaskEvent) ValidateFor(task WorkerTask) error {
	if task.Validate() != nil || e.Schema != WorkerTaskEventSchema || e.TaskID != task.TaskID || e.Attempt != task.Attempt ||
		!workerTaskPositive(e.Attempt) || !workerTaskPositive(e.LeaseEpoch) || !workerTaskPositive(e.Sequence) ||
		!canonicalTaskInstant(e.OccurredAt) || !optionalWorkerTaskCode(e.Checkpoint) || !optionalWorkerTaskCode(e.ErrorCode) ||
		!optionalWorkerTaskDigest(e.EvidenceDigest) {
		return errCode("invalid_worker_task_event")
	}
	switch e.Status {
	case WorkerTaskStatusRunning:
		if e.Checkpoint == nil || *e.Checkpoint != WorkerTaskProbeReceivedCheckpoint || e.ErrorCode != nil || e.EvidenceDigest == nil || *e.EvidenceDigest != task.ExecutionManifestDigest {
			return errCode("invalid_worker_task_event")
		}
	case WorkerTaskStatusSucceeded:
		if e.Checkpoint == nil || *e.Checkpoint != WorkerTaskProbeVerifiedCheckpoint || e.ErrorCode != nil || e.EvidenceDigest == nil || *e.EvidenceDigest != task.ExecutionManifestDigest {
			return errCode("invalid_worker_task_event")
		}
	case WorkerTaskStatusFailed, WorkerTaskStatusInterrupted:
		if e.Checkpoint != nil || e.ErrorCode == nil || e.EvidenceDigest != nil {
			return errCode("invalid_worker_task_event")
		}
	default:
		return errCode("invalid_worker_task_event")
	}
	return nil
}

func NewWorkerTaskEventReceipt(event WorkerTaskEvent, idempotent bool) (WorkerTaskEventReceipt, error) {
	disposition := "accepted"
	if idempotent {
		disposition = "idempotent"
	}
	receipt := WorkerTaskEventReceipt{Schema: WorkerTaskEventReceiptSchema, TaskID: event.TaskID, Attempt: event.Attempt,
		LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: disposition}
	return receipt, receipt.ValidateFor(event)
}

func (r WorkerTaskEventReceipt) ValidateFor(event WorkerTaskEvent) error {
	if r.Schema != WorkerTaskEventReceiptSchema || r.TaskID != event.TaskID || r.Attempt != event.Attempt ||
		r.LeaseEpoch != event.LeaseEpoch || r.Sequence != event.Sequence || (r.Disposition != "accepted" && r.Disposition != "idempotent") {
		return errCode("invalid_worker_task_event_receipt")
	}
	return nil
}

func (s WorkerTaskSummary) Validate(deploymentID, taskID string) error {
	if s.DeploymentID != deploymentID || s.TaskID != taskID || s.Attempt <= 0 || s.Attempt > maxSafeInteger ||
		s.LastSequence < 0 || s.LastSequence > maxSafeInteger || !canonicalTaskInstant(s.UpdatedAt) ||
		!optionalWorkerTaskCode(s.Checkpoint) || !optionalWorkerTaskCode(s.ErrorCode) || !optionalWorkerTaskDigest(s.EvidenceDigest) {
		return errCode("invalid_worker_task_summary")
	}
	switch s.Status {
	case "queued":
		if s.Attempt != 1 || s.LastSequence != 0 || s.Checkpoint != nil || s.ErrorCode != nil || s.EvidenceDigest != nil {
			return errCode("invalid_worker_task_summary")
		}
	case "running":
		if s.LastSequence < 1 || s.Checkpoint == nil || *s.Checkpoint != WorkerTaskProbeReceivedCheckpoint || s.ErrorCode != nil || s.EvidenceDigest == nil {
			return errCode("invalid_worker_task_summary")
		}
	case "succeeded":
		if s.LastSequence < 1 || s.Checkpoint == nil || *s.Checkpoint != WorkerTaskProbeVerifiedCheckpoint || s.ErrorCode != nil || s.EvidenceDigest == nil {
			return errCode("invalid_worker_task_summary")
		}
	case "failed", "interrupted":
		if s.LastSequence < 1 || s.Checkpoint != nil || s.ErrorCode == nil || s.EvidenceDigest != nil {
			return errCode("invalid_worker_task_summary")
		}
	default:
		return errCode("invalid_worker_task_summary")
	}
	return nil
}

func workerTaskPositive(value uint64) bool { return value > 0 && value <= uint64(maxSafeInteger) }
func canonicalTaskInstant(value string) bool {
	_, err := parseCanonicalInstant(value)
	return err == nil
}
func optionalWorkerTaskCode(value *string) bool {
	return value == nil || workerTaskCodePattern.MatchString(*value)
}
func optionalWorkerTaskDigest(value *string) bool {
	return value == nil || namedSHA256Pattern.MatchString(*value)
}

func strictWorkerTaskObject(raw []byte, fields []string, target any) bool {
	if len(raw) == 0 || len(raw) > MaxWorkerTaskRequestBytes {
		return false
	}
	object, err := exactJSONObject(raw)
	return err == nil && exactFields(object, fields) && decodeSingle(raw, target) == nil
}
