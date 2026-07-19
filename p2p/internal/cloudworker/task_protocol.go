package cloudworker

import (
	"bytes"
	"encoding/json"
	"errors"
)

const (
	WorkerTaskClaimV1Schema          = "dirextalk.worker-task-claim/v1"
	WorkerTaskClaimResponseV1Schema  = "dirextalk.worker-task-claim-response/v1"
	WorkerTaskV1Schema               = "dirextalk.worker-task/v1"
	WorkerTaskEventV1Schema          = "dirextalk.worker-task-event/v1"
	WorkerTaskEventReceiptV1Schema   = "dirextalk.worker-task-event-receipt/v1"
	ExecutionProbeReceivedCheckpoint = "execution_manifest_received"
	ExecutionProbeVerifiedCheckpoint = "task_transport_verified"
	maxWorkerTaskSafeInteger         = uint64(9_007_199_254_740_991)
)

// TaskKind is deliberately closed for the first Worker execution transport.
// execution_probe verifies only that a separately trusted future executor can
// receive a digest-bound task; it contains no command, URL, secret, or cloud
// control instruction.
type TaskKind string

const TaskKindExecutionProbe TaskKind = "execution_probe"

// WorkerTask is a de-secreted, immutable task descriptor. It is not a Recipe
// or execution instruction and cannot carry arbitrary process input.
type WorkerTask struct {
	Schema                  string   `json:"schema"`
	TaskID                  string   `json:"task_id"`
	DeploymentID            string   `json:"deployment_id"`
	TaskKind                TaskKind `json:"task_kind"`
	ExecutionManifestDigest string   `json:"execution_manifest_digest"`
	InputDigest             string   `json:"input_digest"`
	Attempt                 uint64   `json:"attempt"`
	LastSequence            uint64   `json:"last_sequence"`
}

// ParseWorkerTask accepts only the closed task document returned by the
// Connection Stack. Parsing rejects unknown fields before it checks the
// deployment binding supplied by the immutable bootstrap manifest.
func ParseWorkerTask(raw []byte, manifest BootstrapManifest) (WorkerTask, error) {
	var task WorkerTask
	if err := decodeStrictObject(raw, &task); err != nil || requireTaskFields(raw,
		"schema", "task_id", "deployment_id", "task_kind", "execution_manifest_digest", "input_digest", "attempt", "last_sequence") != nil ||
		requireNonNullTaskFields(raw, "last_sequence") != nil {
		return WorkerTask{}, errors.New("worker task is invalid")
	}
	if err := task.ValidateFor(manifest); err != nil {
		return WorkerTask{}, err
	}
	return task, nil
}

func (task WorkerTask) ValidateFor(manifest BootstrapManifest) error {
	if task.Schema != WorkerTaskV1Schema || !validIdentifier(task.TaskID) ||
		task.DeploymentID != manifest.DeploymentID || task.TaskKind != TaskKindExecutionProbe ||
		!validNamedSHA256(task.ExecutionManifestDigest) || !validNamedSHA256(task.InputDigest) || !validTaskPositive(task.Attempt) ||
		!validTaskNonnegative(task.LastSequence) {
		return errors.New("worker task is invalid")
	}
	return nil
}

// TaskClaimRequest contains only the active lease epoch. The bearer token and
// route bind it to the unique bootstrap session; task identifiers and task
// material must never be supplied by the Worker.
type TaskClaimRequest struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

func NewTaskClaimRequest(leaseEpoch uint64) (TaskClaimRequest, error) {
	request := TaskClaimRequest{Schema: WorkerTaskClaimV1Schema, LeaseEpoch: leaseEpoch}
	return request, request.Validate()
}

func ParseTaskClaimRequest(raw []byte) (TaskClaimRequest, error) {
	var request TaskClaimRequest
	if err := decodeStrictObject(raw, &request); err != nil || requireTaskFields(raw, "schema", "lease_epoch") != nil {
		return TaskClaimRequest{}, errors.New("worker task claim request is invalid")
	}
	if err := request.Validate(); err != nil {
		return TaskClaimRequest{}, err
	}
	return request, nil
}

func (request TaskClaimRequest) Validate() error {
	if request.Schema != WorkerTaskClaimV1Schema || !validTaskPositive(request.LeaseEpoch) {
		return errors.New("worker task claim request is invalid")
	}
	return nil
}

// TaskClaimResponse carries either the current task or a bounded no-work
// result. It intentionally has no token, URL, secret, log, or recipe field.
type TaskClaimResponse struct {
	Schema     string      `json:"schema"`
	Status     string      `json:"status"`
	LeaseEpoch uint64      `json:"lease_epoch"`
	Task       *WorkerTask `json:"task,omitempty"`
}

func ParseTaskClaimResponse(raw []byte, manifest BootstrapManifest, expectedLeaseEpoch uint64) (TaskClaimResponse, error) {
	var response TaskClaimResponse
	if err := decodeStrictObject(raw, &response); err != nil || requireTaskFields(raw, "schema", "status", "lease_epoch") != nil {
		return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
	}
	fields, err := taskFieldSet(raw)
	if err != nil || response.Schema != WorkerTaskClaimResponseV1Schema || !validTaskPositive(response.LeaseEpoch) || response.LeaseEpoch != expectedLeaseEpoch {
		return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
	}
	switch response.Status {
	case "none":
		if response.Task != nil || fields["task"] {
			return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
		}
	case "claimed":
		rawTask, present, rawTaskErr := taskRawField(raw, "task")
		if response.Task == nil || !fields["task"] || !present || rawTaskErr != nil {
			return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
		}
		task, taskErr := ParseWorkerTask(rawTask, manifest)
		if taskErr != nil {
			return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
		}
		response.Task = &task
	default:
		return TaskClaimResponse{}, errors.New("worker task claim response is invalid")
	}
	return response, nil
}

type TaskStatus string

const (
	TaskStatusRunning     TaskStatus = "running"
	TaskStatusSucceeded   TaskStatus = "succeeded"
	TaskStatusFailed      TaskStatus = "failed"
	TaskStatusInterrupted TaskStatus = "interrupted"
)

// TaskEvent is bounded progress evidence for one claimed task. Nullable
// fields are emitted explicitly so the Connection Stack can distinguish an
// absent/invalid payload from a deliberate no-value without accepting free
// form output or operational commands.
type TaskEvent struct {
	Schema         string     `json:"schema"`
	TaskID         string     `json:"task_id"`
	Attempt        uint64     `json:"attempt"`
	LeaseEpoch     uint64     `json:"lease_epoch"`
	Sequence       uint64     `json:"sequence"`
	Status         TaskStatus `json:"status"`
	Checkpoint     *string    `json:"checkpoint"`
	ErrorCode      *string    `json:"error_code"`
	EvidenceDigest *string    `json:"evidence_digest"`
	OccurredAt     string     `json:"occurred_at"`
}

func ParseTaskEvent(raw []byte) (TaskEvent, error) {
	var event TaskEvent
	if err := decodeStrictObject(raw, &event); err != nil || requireTaskFields(raw,
		"schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "checkpoint", "error_code", "evidence_digest", "occurred_at") != nil {
		return TaskEvent{}, errors.New("worker task event is invalid")
	}
	return event, nil
}

func (event TaskEvent) ValidateFor(task WorkerTask) error {
	if event.Schema != WorkerTaskEventV1Schema || event.TaskID != task.TaskID || event.Attempt != task.Attempt ||
		!validTaskPositive(event.Attempt) || !validTaskPositive(event.LeaseEpoch) || !validTaskPositive(event.Sequence) {
		return errors.New("worker task event is invalid")
	}
	if _, err := parseCanonicalInstant(event.OccurredAt); err != nil || !validOptionalTaskCode(event.Checkpoint) ||
		!validOptionalTaskCode(event.ErrorCode) || !validOptionalTaskDigest(event.EvidenceDigest) {
		return errors.New("worker task event is invalid")
	}
	switch event.Status {
	case TaskStatusRunning:
		if event.Checkpoint == nil || event.ErrorCode != nil || event.EvidenceDigest == nil || *event.EvidenceDigest != task.ExecutionManifestDigest {
			return errors.New("worker task event is invalid")
		}
		if *event.Checkpoint != ExecutionProbeReceivedCheckpoint {
			return errors.New("worker task event is invalid")
		}
	case TaskStatusSucceeded:
		if event.Checkpoint == nil || event.ErrorCode != nil || event.EvidenceDigest == nil || *event.EvidenceDigest != task.ExecutionManifestDigest {
			return errors.New("worker task event is invalid")
		}
		if *event.Checkpoint != ExecutionProbeVerifiedCheckpoint {
			return errors.New("worker task event is invalid")
		}
	case TaskStatusFailed, TaskStatusInterrupted:
		if event.Checkpoint != nil || event.ErrorCode == nil || event.EvidenceDigest != nil {
			return errors.New("worker task event is invalid")
		}
	default:
		return errors.New("worker task event is invalid")
	}
	return nil
}

// TaskEventReceipt is the sole response that advances a task progress
// sequence. A lost response leaves the exact TaskEvent available for replay.
type TaskEventReceipt struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

func (receipt TaskEventReceipt) ValidateFor(event TaskEvent) error {
	if receipt.Schema != WorkerTaskEventReceiptV1Schema || receipt.TaskID != event.TaskID ||
		receipt.Attempt != event.Attempt || receipt.LeaseEpoch != event.LeaseEpoch || receipt.Sequence != event.Sequence ||
		(receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return errors.New("worker task event receipt is invalid")
	}
	return nil
}

func validOptionalTaskCode(value *string) bool {
	return value == nil || safeCodePattern.MatchString(*value)
}

func validTaskPositive(value uint64) bool {
	return value > 0 && value <= maxWorkerTaskSafeInteger
}

func validTaskNonnegative(value uint64) bool {
	return value <= maxWorkerTaskSafeInteger
}

func validOptionalTaskDigest(value *string) bool {
	return value == nil || validNamedSHA256(*value)
}

func taskString(value string) *string {
	return &value
}

func requireTaskFields(raw []byte, names ...string) error {
	fields, err := taskFieldSet(raw)
	if err != nil {
		return err
	}
	for _, name := range names {
		if !fields[name] {
			return errors.New("required worker task field is absent")
		}
	}
	return nil
}

func requireNonNullTaskFields(raw []byte, names ...string) error {
	fields, err := taskRawFields(raw)
	if err != nil {
		return err
	}
	for _, name := range names {
		value, present := fields[name]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("required worker task field is null")
		}
	}
	return nil
}

func taskFieldSet(raw []byte) (map[string]bool, error) {
	fields, err := taskRawFields(raw)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(fields))
	for name := range fields {
		set[name] = true
	}
	return set, nil
}

func taskRawField(raw []byte, name string) (json.RawMessage, bool, error) {
	fields, err := taskRawFields(raw)
	if err != nil {
		return nil, false, err
	}
	value, present := fields[name]
	return value, present, nil
}

func taskRawFields(raw []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("worker task object is invalid")
	}
	return fields, nil
}
