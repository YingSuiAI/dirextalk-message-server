package recipeexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	// TaskV1Schema identifies the separate transport document for a sealed
	// Recipe execution. It intentionally is not a cloud-worker V1 task: the
	// existing non-root worker remains restricted to execution_probe.
	TaskV1Schema  = "dirextalk.recipe-execution-task/v1"
	EventV1Schema = "dirextalk.recipe-execution-task-event/v1"

	maxTaskSafeInteger = uint64(9_007_199_254_740_991)
)

var (
	ErrTaskInvalid           = errors.New("recipe execution task is invalid")
	ErrTaskManifestBinding   = errors.New("recipe execution task does not match sealed manifest")
	ErrTaskCheckpointBinding = errors.New("recipe execution task does not match durable checkpoint")
	ErrTaskProgress          = errors.New("recipe execution task progress is invalid")
	ErrTaskTerminal          = errors.New("recipe execution task is terminal")
	taskIDPattern            = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	bindingIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	taskDigestPattern        = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	taskCodePattern          = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)
	taskInstantPattern       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	taskCanonicalTimeLayout  = "2006-01-02T15:04:05.000Z"
)

// TaskKind identifies the only task purpose represented by TaskV1. Keeping
// it closed prevents the document from becoming a generic instruction
// envelope even if it is later delivered through a different executor
// session.
type TaskKind string

const TaskKindRecipeExecution TaskKind = "recipe_execution"

// TaskV1 is a digest-only, resumable Recipe execution descriptor. It binds a
// separately delivered, sealed RecipeExecutionManifestV1 to the deployment
// and carries only its declared checkpoint sequence. It never carries command
// text, URLs, artifact bytes, host paths, secret values, or cloud controls.
//
// This type is deliberately not accepted by cloud-worker's existing
// execution_probe task session. A later isolated executor and Connection
// Stack approval path must opt into it explicitly.
type TaskV1 struct {
	Schema                        string   `json:"schema"`
	TaskID                        string   `json:"task_id"`
	ExecutionID                   string   `json:"execution_id"`
	DeploymentID                  string   `json:"deployment_id"`
	TaskKind                      TaskKind `json:"task_kind"`
	RecipeExecutionManifestDigest string   `json:"recipe_execution_manifest_digest"`
	InputDigest                   string   `json:"input_digest"`
	CheckpointSequence            []string `json:"checkpoint_sequence"`
	LastCheckpoint                string   `json:"last_checkpoint"`
	Attempt                       uint64   `json:"attempt"`
	LastSequence                  uint64   `json:"last_sequence"`
}

// ParseTaskV1 accepts only the closed recipe task document. Unknown fields
// are rejected before validation so no future caller can smuggle shell text,
// a URL, an artifact payload, or a credential through this protocol.
func ParseTaskV1(raw []byte) (TaskV1, error) {
	var task TaskV1
	if err := decodeStrictTaskObject(raw, &task); err != nil || requireTaskObjectFields(raw,
		"schema", "task_id", "execution_id", "deployment_id", "task_kind", "recipe_execution_manifest_digest",
		"input_digest", "checkpoint_sequence", "last_checkpoint", "attempt", "last_sequence") != nil ||
		requireNonNullTaskObjectFields(raw, "schema", "task_id", "execution_id", "deployment_id", "task_kind",
			"recipe_execution_manifest_digest", "input_digest", "checkpoint_sequence", "last_checkpoint", "attempt", "last_sequence") != nil {
		return TaskV1{}, ErrTaskInvalid
	}
	if err := task.Validate(); err != nil {
		return TaskV1{}, err
	}
	return task, nil
}

// Validate checks the standalone, de-secreted task shape. It cannot prove an
// authorization by itself; ValidateForManifest performs the binding check
// after a trusted manifest has been retrieved and verified independently.
func (task TaskV1) Validate() error {
	if task.Schema != TaskV1Schema || !validTaskID(task.TaskID) || !validBindingIdentifier(task.ExecutionID) ||
		!validBindingIdentifier(task.DeploymentID) || task.TaskKind != TaskKindRecipeExecution ||
		!validTaskDigest(task.RecipeExecutionManifestDigest) || !validTaskDigest(task.InputDigest) ||
		!validTaskPositive(task.Attempt) || !validTaskNonnegative(task.LastSequence) {
		return ErrTaskInvalid
	}
	if err := validateTaskCheckpoints(task.CheckpointSequence); err != nil {
		return err
	}
	if task.LastCheckpoint != "" && taskCheckpointIndex(task.CheckpointSequence, task.LastCheckpoint) < 0 {
		return ErrTaskInvalid
	}
	if task.LastCheckpoint != "" && task.LastSequence == 0 {
		return ErrTaskInvalid
	}
	return nil
}

// ValidateForManifest ties the independently parsed task to one exact sealed
// manifest. Both the digest and all semantic identity/checkpoint fields must
// agree; a task cannot substitute a different execution, deployment, or
// checkpoint order under the same Worker session.
func (task TaskV1) ValidateForManifest(manifest cloudorchestrator.RecipeExecutionManifestV1) error {
	if err := task.Validate(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return ErrTaskManifestBinding
	}
	manifestDigest, err := manifest.Digest()
	if err != nil || task.RecipeExecutionManifestDigest != manifestDigest || task.ExecutionID != manifest.ExecutionID ||
		task.DeploymentID != manifest.DeploymentID || !sameTaskCheckpoints(task.CheckpointSequence, manifest.CheckpointSequence) {
		return ErrTaskManifestBinding
	}
	return nil
}

// Equal compares immutable task bindings without relying on slice identity.
// It is intended for exact retry and receipt validation when a future session
// persists task documents across a reconnect.
func (task TaskV1) Equal(other TaskV1) bool {
	return task.Schema == other.Schema && task.TaskID == other.TaskID && task.ExecutionID == other.ExecutionID &&
		task.DeploymentID == other.DeploymentID && task.TaskKind == other.TaskKind &&
		task.RecipeExecutionManifestDigest == other.RecipeExecutionManifestDigest && task.InputDigest == other.InputDigest &&
		task.LastCheckpoint == other.LastCheckpoint && task.Attempt == other.Attempt && task.LastSequence == other.LastSequence &&
		sameTaskCheckpoints(task.CheckpointSequence, other.CheckpointSequence)
}

// TaskStatus is the bounded lifecycle evidence a trusted executor may report.
// Service readiness remains independent of task success and is never inferred
// from a local executor report.
type TaskStatus string

const (
	TaskStatusRunning     TaskStatus = "running"
	TaskStatusSucceeded   TaskStatus = "succeeded"
	TaskStatusFailed      TaskStatus = "failed"
	TaskStatusInterrupted TaskStatus = "interrupted"
)

// EventV1 is the closed progress record for one TaskV1 attempt. The event is
// deliberately metadata-only: evidence is the sealed manifest digest, errors
// are safe codes, and no log/output/command field exists.
type EventV1 struct {
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

func ParseEventV1(raw []byte) (EventV1, error) {
	var event EventV1
	if err := decodeStrictTaskObject(raw, &event); err != nil || requireTaskObjectFields(raw,
		"schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "checkpoint", "error_code", "evidence_digest", "occurred_at") != nil ||
		requireNonNullTaskObjectFields(raw, "schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "occurred_at") != nil {
		return EventV1{}, ErrTaskProgress
	}
	if err := event.Validate(); err != nil {
		return EventV1{}, err
	}
	return event, nil
}

func (event EventV1) Validate() error {
	if event.Schema != EventV1Schema || !validTaskID(event.TaskID) || !validTaskPositive(event.Attempt) ||
		!validTaskPositive(event.LeaseEpoch) || !validTaskPositive(event.Sequence) || !validTaskInstant(event.OccurredAt) ||
		!validOptionalTaskCode(event.Checkpoint) || !validOptionalTaskCode(event.ErrorCode) || !validOptionalTaskDigest(event.EvidenceDigest) {
		return ErrTaskProgress
	}
	switch event.Status {
	case TaskStatusRunning, TaskStatusSucceeded, TaskStatusFailed, TaskStatusInterrupted:
		return nil
	default:
		return ErrTaskProgress
	}
}

// Progress is the bounded local projection used to validate an exact event
// sequence after a reconnect. It copies the TaskV1 checkpoint slice so a
// caller cannot mutate the declared scope after progress has been accepted.
type Progress struct {
	Task           TaskV1
	LastCheckpoint string
	LastSequence   uint64
	Terminal       bool
}

func NewProgress(task TaskV1) (Progress, error) {
	if err := task.Validate(); err != nil {
		return Progress{}, err
	}
	checkpointIndex := taskCheckpointIndex(task.CheckpointSequence, task.LastCheckpoint)
	return Progress{
		Task:           cloneTask(task),
		LastCheckpoint: task.LastCheckpoint,
		LastSequence:   task.LastSequence,
		Terminal:       checkpointIndex == len(task.CheckpointSequence)-1,
	}, nil
}

// Advance accepts exactly one next event under the expected lease epoch. It
// enforces the declared checkpoint order and accepts success only at the
// terminal checkpoint. Failure/interruption records remain terminal but do
// not invent a local checkpoint.
func (progress Progress) Advance(event EventV1, expectedLeaseEpoch uint64) (Progress, error) {
	if err := progress.Task.Validate(); err != nil || !validTaskPositive(expectedLeaseEpoch) {
		return Progress{}, ErrTaskProgress
	}
	if progress.Terminal {
		return Progress{}, ErrTaskTerminal
	}
	if err := event.Validate(); err != nil || event.TaskID != progress.Task.TaskID || event.Attempt != progress.Task.Attempt ||
		event.LeaseEpoch != expectedLeaseEpoch || progress.LastSequence == maxTaskSafeInteger || event.Sequence != progress.LastSequence+1 {
		return Progress{}, ErrTaskProgress
	}

	next := Progress{
		Task:           cloneTask(progress.Task),
		LastCheckpoint: progress.LastCheckpoint,
		LastSequence:   event.Sequence,
	}
	nextCheckpointIndex := taskCheckpointIndex(progress.Task.CheckpointSequence, progress.LastCheckpoint) + 1
	switch event.Status {
	case TaskStatusRunning:
		if !eventMatchesNextCheckpoint(event, progress.Task, nextCheckpointIndex) ||
			nextCheckpointIndex == len(progress.Task.CheckpointSequence)-1 {
			return Progress{}, ErrTaskProgress
		}
		next.LastCheckpoint = *event.Checkpoint
	case TaskStatusSucceeded:
		if !eventMatchesNextCheckpoint(event, progress.Task, nextCheckpointIndex) ||
			nextCheckpointIndex != len(progress.Task.CheckpointSequence)-1 {
			return Progress{}, ErrTaskProgress
		}
		next.LastCheckpoint = *event.Checkpoint
		next.Terminal = true
	case TaskStatusFailed, TaskStatusInterrupted:
		if event.Checkpoint != nil || event.ErrorCode == nil || event.EvidenceDigest != nil {
			return Progress{}, ErrTaskProgress
		}
		next.Terminal = true
	default:
		return Progress{}, ErrTaskProgress
	}
	return next, nil
}

func eventMatchesNextCheckpoint(event EventV1, task TaskV1, nextIndex int) bool {
	return event.Checkpoint != nil && event.ErrorCode == nil && event.EvidenceDigest != nil &&
		*event.EvidenceDigest == task.RecipeExecutionManifestDigest && nextIndex >= 0 && nextIndex < len(task.CheckpointSequence) &&
		*event.Checkpoint == task.CheckpointSequence[nextIndex]
}

func validateTaskCheckpoints(checkpoints []string) error {
	if len(checkpoints) == 0 || len(checkpoints) > 32 {
		return ErrTaskInvalid
	}
	seen := make(map[string]struct{}, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if !taskCodePattern.MatchString(checkpoint) {
			return ErrTaskInvalid
		}
		if _, exists := seen[checkpoint]; exists {
			return ErrTaskInvalid
		}
		seen[checkpoint] = struct{}{}
	}
	return nil
}

func validTaskID(value string) bool {
	return taskIDPattern.MatchString(value)
}

func validBindingIdentifier(value string) bool {
	return bindingIdentifierPattern.MatchString(value)
}

func validTaskDigest(value string) bool {
	return taskDigestPattern.MatchString(value)
}

func validTaskPositive(value uint64) bool {
	return value > 0 && value <= maxTaskSafeInteger
}

func validTaskNonnegative(value uint64) bool {
	return value <= maxTaskSafeInteger
}

func validOptionalTaskCode(value *string) bool {
	return value == nil || taskCodePattern.MatchString(*value)
}

func validOptionalTaskDigest(value *string) bool {
	return value == nil || validTaskDigest(*value)
}

func validTaskInstant(value string) bool {
	if !taskInstantPattern.MatchString(value) {
		return false
	}
	parsed, err := time.Parse(taskCanonicalTimeLayout, value)
	return err == nil && parsed.UTC().Format(taskCanonicalTimeLayout) == value
}

func taskCheckpointIndex(checkpoints []string, checkpoint string) int {
	if checkpoint == "" {
		return -1
	}
	for index, candidate := range checkpoints {
		if candidate == checkpoint {
			return index
		}
	}
	return -1
}

func sameTaskCheckpoints(left, right []string) bool {
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

func cloneTask(task TaskV1) TaskV1 {
	clone := task
	clone.CheckpointSequence = append([]string(nil), task.CheckpointSequence...)
	return clone
}

func decodeStrictTaskObject(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("task document has trailing data")
	}
	return nil
}

func requireTaskObjectFields(raw []byte, names ...string) error {
	fields, err := taskObjectFields(raw)
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, present := fields[name]; !present {
			return errors.New("required task field is absent")
		}
	}
	return nil
}

func requireNonNullTaskObjectFields(raw []byte, names ...string) error {
	fields, err := taskObjectFields(raw)
	if err != nil {
		return err
	}
	for _, name := range names {
		value, present := fields[name]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("required task field is null")
		}
	}
	return nil
}

func taskObjectFields(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, errors.New("task document is not an object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok {
			return nil, errors.New("task document field is invalid")
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, errors.New("task document contains a duplicate field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("task document field value is invalid")
		}
		fields[name] = value
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return nil, errors.New("task document is not an object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("task document has trailing data")
	}
	return fields, nil
}
