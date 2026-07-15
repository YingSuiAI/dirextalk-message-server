package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"

	"github.com/fxamacker/cbor/v2"
)

const (
	ActionWorkerRecipeTaskIssue   = "worker.recipe_task.issue"
	ActionWorkerRecipeTaskObserve = "worker.recipe_task.observe"

	RecipeTaskIssueSchema         = "dirextalk.recipe-execution-task-issue/v1"
	RecipeTaskV1Schema            = "dirextalk.recipe-execution-task/v1"
	RecipeTaskEventV1Schema       = "dirextalk.recipe-execution-task-event/v1"
	RecipeTaskClaimSchema         = "dirextalk.recipe-execution-task-claim/v1"
	RecipeTaskClaimResponseSchema = "dirextalk.recipe-execution-task-claim-response/v1"
	RecipeTaskEventReceiptSchema  = "dirextalk.recipe-execution-task-event-receipt/v1"
	RecipeTaskKindExecution       = "recipe_execution"
	RecipeExecutionManifestSchema = "dirextalk.recipe-execution-manifest/v1"
	MaxRecipeManifestJSONBytes    = 64 * 1024
)

var (
	recipeTaskIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	recipeBindingIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	recipeTaskCodePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)
	recipeVolumeRefPattern = regexp.MustCompile(`^volume_ref:[A-Za-z0-9._/-]{1,120}$`)
	recipeDataRefPattern   = regexp.MustCompile(`^data_ref:[A-Za-z0-9._/-]{1,120}$`)
	recipeSecretRefPattern = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	recipeSecretPatterns   = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`), regexp.MustCompile(`(?i)aws_secret_access_key\s*[:=]`),
		regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`), regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
		regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`), regexp.MustCompile(`\b(?:sk|hf)_[A-Za-z0-9_-]{20,}\b`),
		regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{20,}\b`),
	}
)

func ValidRecipeTaskID(value string) bool { return recipeTaskIDPattern.MatchString(value) }

type RecipeVolumeSlotV1 struct {
	SlotID    string `json:"slot_id"`
	VolumeRef string `json:"volume_ref"`
	ReadOnly  bool   `json:"read_only"`
}
type RecipeDataSlotV1 struct {
	SlotID   string `json:"slot_id"`
	DataRef  string `json:"data_ref"`
	ReadOnly bool   `json:"read_only"`
}
type RecipeSecretSlotV1 struct {
	SlotID    string `json:"slot_id"`
	SecretRef string `json:"secret_ref"`
}

type RecipeExecutionManifestV1 struct {
	SchemaVersion                string               `json:"schema_version"`
	ExecutionID                  string               `json:"execution_id"`
	DeploymentID                 string               `json:"deployment_id"`
	PlanID                       string               `json:"plan_id"`
	PlanHash                     string               `json:"plan_hash"`
	PlanRevision                 uint64               `json:"plan_revision"`
	RecipeDigest                 string               `json:"recipe_digest"`
	WorkerResourceManifestDigest string               `json:"worker_resource_manifest_digest"`
	ArtifactDigest               string               `json:"artifact_digest"`
	ActionID                     string               `json:"action_id"`
	RootRequired                 bool                 `json:"root_required"`
	TimeoutSeconds               uint32               `json:"timeout_seconds"`
	CheckpointSequence           []string             `json:"checkpoint_sequence"`
	VolumeSlots                  []RecipeVolumeSlotV1 `json:"volume_slots,omitempty"`
	DataSlots                    []RecipeDataSlotV1   `json:"data_slots,omitempty"`
	SecretSlots                  []RecipeSecretSlotV1 `json:"secret_slots,omitempty"`
}

type RecipeTaskIssueRequest struct {
	Schema                        string                    `json:"schema"`
	ExecutionID                   string                    `json:"execution_id"`
	DeploymentID                  string                    `json:"deployment_id"`
	TaskID                        string                    `json:"task_id"`
	TaskKind                      string                    `json:"task_kind"`
	RecipeExecutionManifestDigest string                    `json:"recipe_execution_manifest_digest"`
	InputDigest                   string                    `json:"input_digest"`
	CheckpointSequence            []string                  `json:"checkpoint_sequence"`
	Manifest                      RecipeExecutionManifestV1 `json:"manifest"`
}

type RecipeTaskObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}

// RecipeTaskV1 is wire-compatible with cloudworker/recipeexec.TaskV1. It has
// no command, URL, path, artifact body, log, secret, or cloud-control field.
type RecipeTaskV1 struct {
	Schema                        string   `json:"schema"`
	TaskID                        string   `json:"task_id"`
	ExecutionID                   string   `json:"execution_id"`
	DeploymentID                  string   `json:"deployment_id"`
	TaskKind                      string   `json:"task_kind"`
	RecipeExecutionManifestDigest string   `json:"recipe_execution_manifest_digest"`
	InputDigest                   string   `json:"input_digest"`
	CheckpointSequence            []string `json:"checkpoint_sequence"`
	LastCheckpoint                string   `json:"last_checkpoint"`
	Attempt                       uint64   `json:"attempt"`
	LastSequence                  uint64   `json:"last_sequence"`
}

type RecipeTaskEventV1 struct {
	Schema         string  `json:"schema"`
	TaskID         string  `json:"task_id"`
	Attempt        uint64  `json:"attempt"`
	LeaseEpoch     uint64  `json:"lease_epoch"`
	Sequence       uint64  `json:"sequence"`
	Status         string  `json:"status"`
	Checkpoint     *string `json:"checkpoint"`
	ErrorCode      *string `json:"error_code"`
	EvidenceDigest *string `json:"evidence_digest"`
	OccurredAt     string  `json:"occurred_at"`
}

type RecipeTaskClaimRequest struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

type RecipeTaskClaimResponse struct {
	Schema         string                     `json:"schema"`
	Status         string                     `json:"status"`
	LeaseEpoch     uint64                     `json:"lease_epoch"`
	Task           *RecipeTaskV1              `json:"task,omitempty"`
	Manifest       *RecipeExecutionManifestV1 `json:"manifest,omitempty"`
	ArtifactAccess *ArtifactAccess            `json:"artifact_access,omitempty"`
}

type RecipeTaskEventReceipt struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

type RecipeTaskReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type RecipeTaskSummary struct {
	TaskID         string  `json:"task_id"`
	ExecutionID    string  `json:"execution_id"`
	DeploymentID   string  `json:"deployment_id"`
	Status         string  `json:"status"`
	Attempt        int64   `json:"attempt"`
	LastSequence   int64   `json:"last_sequence"`
	LastCheckpoint string  `json:"last_checkpoint"`
	ErrorCode      *string `json:"error_code"`
	EvidenceDigest *string `json:"evidence_digest"`
	UpdatedAt      string  `json:"updated_at"`
}

type RecipeTaskResult struct {
	Status  string            `json:"status"`
	Receipt RecipeTaskReceipt `json:"receipt"`
	Task    RecipeTaskSummary `json:"task"`
}

func ParseRecipeTaskIssueRequest(raw []byte) (RecipeTaskIssueRequest, error) {
	var request RecipeTaskIssueRequest
	fields := []string{"schema", "task_id", "execution_id", "deployment_id", "task_kind", "recipe_execution_manifest_digest", "input_digest", "checkpoint_sequence", "manifest"}
	object, objectErr := exactJSONObject(raw)
	if !strictRecipeTaskObject(raw, fields, &request) || objectErr != nil || validateRecipeManifestJSON(object["manifest"]) != nil || request.Validate() != nil {
		return RecipeTaskIssueRequest{}, errCode("invalid_recipe_task_issue_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(raw, canonical) {
		return RecipeTaskIssueRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func (command Command) RecipeTaskIssueRequest() (RecipeTaskIssueRequest, error) {
	if command.Action != ActionWorkerRecipeTaskIssue {
		return RecipeTaskIssueRequest{}, errCode("invalid_recipe_task_issue_request")
	}
	payload, err := command.actionPayload()
	if err != nil {
		return RecipeTaskIssueRequest{}, err
	}
	return ParseRecipeTaskIssueRequest(payload)
}

func (command Command) RecipeTaskObserveRequest() (RecipeTaskObserveRequest, error) {
	if command.Action != ActionWorkerRecipeTaskObserve {
		return RecipeTaskObserveRequest{}, errCode("invalid_recipe_task_observe_request")
	}
	payload, err := command.actionPayload()
	if err != nil {
		return RecipeTaskObserveRequest{}, err
	}
	return ParseRecipeTaskObserveRequest(payload)
}

func (request RecipeTaskIssueRequest) Validate() error {
	if request.Schema != RecipeTaskIssueSchema || !recipeTaskIDPattern.MatchString(request.TaskID) ||
		!recipeBindingIDPattern.MatchString(request.ExecutionID) || !recipeBindingIDPattern.MatchString(request.DeploymentID) ||
		request.TaskKind != RecipeTaskKindExecution || !namedSHA256Pattern.MatchString(request.RecipeExecutionManifestDigest) ||
		!namedSHA256Pattern.MatchString(request.InputDigest) || !validRecipeTaskCheckpoints(request.CheckpointSequence) ||
		request.Manifest.Validate() != nil || request.Manifest.ExecutionID != request.ExecutionID || request.Manifest.DeploymentID != request.DeploymentID ||
		!sameRecipeTaskCheckpoints(request.Manifest.CheckpointSequence, request.CheckpointSequence) {
		return errCode("invalid_recipe_task_issue_request")
	}
	digest, err := request.Manifest.Digest()
	if err != nil || digest != request.RecipeExecutionManifestDigest {
		return errCode("invalid_recipe_task_issue_request")
	}
	return nil
}

func ParseRecipeTaskObserveRequest(raw []byte) (RecipeTaskObserveRequest, error) {
	var request RecipeTaskObserveRequest
	if !strictRecipeTaskObject(raw, []string{"deployment_id", "task_id"}, &request) ||
		!recipeBindingIDPattern.MatchString(request.DeploymentID) || !recipeTaskIDPattern.MatchString(request.TaskID) {
		return RecipeTaskObserveRequest{}, errCode("invalid_recipe_task_observe_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(raw, canonical) {
		return RecipeTaskObserveRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func NewRecipeTaskV1(request RecipeTaskIssueRequest) (RecipeTaskV1, error) {
	if err := request.Validate(); err != nil {
		return RecipeTaskV1{}, err
	}
	return RecipeTaskV1{Schema: RecipeTaskV1Schema, TaskID: request.TaskID, ExecutionID: request.ExecutionID,
		DeploymentID: request.DeploymentID, TaskKind: request.TaskKind,
		RecipeExecutionManifestDigest: request.RecipeExecutionManifestDigest, InputDigest: request.InputDigest,
		CheckpointSequence: append([]string(nil), request.CheckpointSequence...), Attempt: 1}, nil
}

func ParseRecipeTaskV1(raw []byte) (RecipeTaskV1, error) {
	var task RecipeTaskV1
	fields := []string{"schema", "task_id", "execution_id", "deployment_id", "task_kind", "recipe_execution_manifest_digest", "input_digest", "checkpoint_sequence", "last_checkpoint", "attempt", "last_sequence"}
	if !strictRecipeTaskObject(raw, fields, &task) || task.Validate() != nil {
		return RecipeTaskV1{}, errCode("invalid_recipe_task")
	}
	return task, nil
}

func (task RecipeTaskV1) Validate() error {
	if task.Schema != RecipeTaskV1Schema || !recipeTaskIDPattern.MatchString(task.TaskID) ||
		!recipeBindingIDPattern.MatchString(task.ExecutionID) || !recipeBindingIDPattern.MatchString(task.DeploymentID) ||
		task.TaskKind != RecipeTaskKindExecution || !namedSHA256Pattern.MatchString(task.RecipeExecutionManifestDigest) ||
		!namedSHA256Pattern.MatchString(task.InputDigest) || !validRecipeTaskCheckpoints(task.CheckpointSequence) ||
		!workerTaskPositive(task.Attempt) || task.LastSequence > uint64(maxSafeInteger) ||
		(task.LastCheckpoint != "" && recipeCheckpointIndex(task.CheckpointSequence, task.LastCheckpoint) < 0) ||
		(task.LastCheckpoint != "" && task.LastSequence == 0) {
		return errCode("invalid_recipe_task")
	}
	return nil
}

func ParseRecipeTaskClaimRequest(raw []byte) (RecipeTaskClaimRequest, error) {
	var request RecipeTaskClaimRequest
	if !strictRecipeTaskObject(raw, []string{"schema", "lease_epoch"}, &request) ||
		request.Schema != RecipeTaskClaimSchema || !workerTaskPositive(request.LeaseEpoch) {
		return RecipeTaskClaimRequest{}, errCode("invalid_recipe_task_claim")
	}
	return request, nil
}

func MarshalRecipeTaskClaimResponse(leaseEpoch uint64, task *RecipeTaskV1, manifest *RecipeExecutionManifestV1) ([]byte, error) {
	return MarshalRecipeTaskClaimResponseWithArtifact(leaseEpoch, task, manifest, nil, false)
}

func MarshalRecipeTaskArtifactPending(leaseEpoch uint64) ([]byte, error) {
	if !workerTaskPositive(leaseEpoch) {
		return nil, errCode("invalid_recipe_task_claim_response")
	}
	return json.Marshal(RecipeTaskClaimResponse{Schema: RecipeTaskClaimResponseSchema, Status: "artifact_pending", LeaseEpoch: leaseEpoch})
}

func MarshalRecipeTaskClaimResponseWithArtifact(leaseEpoch uint64, task *RecipeTaskV1, manifest *RecipeExecutionManifestV1, access *ArtifactAccess, required bool) ([]byte, error) {
	response := RecipeTaskClaimResponse{Schema: RecipeTaskClaimResponseSchema, Status: "none", LeaseEpoch: leaseEpoch}
	if task != nil {
		copy := *task
		copy.CheckpointSequence = append([]string(nil), task.CheckpointSequence...)
		if manifest == nil || manifest.Validate() != nil {
			return nil, errCode("invalid_recipe_task_claim_response")
		}
		manifestCopy := manifest.normalized()
		digest, _ := manifestCopy.Digest()
		if copy.ExecutionID != manifestCopy.ExecutionID || copy.DeploymentID != manifestCopy.DeploymentID || copy.RecipeExecutionManifestDigest != digest || !sameRecipeTaskCheckpoints(copy.CheckpointSequence, manifestCopy.CheckpointSequence) {
			return nil, errCode("invalid_recipe_task_claim_response")
		}
		response.Status, response.Task, response.Manifest = "claimed", &copy, &manifestCopy
		if access != nil {
			value := *access
			response.ArtifactAccess = &value
		}
	}
	if !workerTaskPositive(response.LeaseEpoch) || (response.Status == "none" && response.Task != nil) ||
		(response.Status == "none" && response.Manifest != nil) || (response.Status == "claimed" && (response.Task == nil || response.Manifest == nil || response.Task.Validate() != nil)) ||
		(response.Status != "none" && response.Status != "claimed") || (required && response.Status == "claimed" && response.ArtifactAccess == nil) || (!required && response.ArtifactAccess != nil) {
		return nil, errCode("invalid_recipe_task_claim_response")
	}
	return json.Marshal(response)
}

func (manifest RecipeExecutionManifestV1) Validate() error {
	if manifest.SchemaVersion != RecipeExecutionManifestSchema || !recipeBindingIDPattern.MatchString(manifest.ExecutionID) || !recipeBindingIDPattern.MatchString(manifest.DeploymentID) || !recipeBindingIDPattern.MatchString(manifest.PlanID) || !recipeBindingIDPattern.MatchString(manifest.ActionID) || !namedSHA256Pattern.MatchString(manifest.PlanHash) || manifest.PlanRevision == 0 || manifest.PlanRevision > uint64(maxSafeInteger) || !namedSHA256Pattern.MatchString(manifest.RecipeDigest) || !namedSHA256Pattern.MatchString(manifest.WorkerResourceManifestDigest) || !namedSHA256Pattern.MatchString(manifest.ArtifactDigest) || manifest.TimeoutSeconds == 0 || manifest.TimeoutSeconds > 86400 || !validRecipeTaskCheckpoints(manifest.CheckpointSequence) {
		return errCode("invalid_recipe_execution_manifest")
	}
	if recipeContainsSecret(manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, manifest.ActionID) || !validRecipeVolumeSlots(manifest.VolumeSlots) || !validRecipeDataSlots(manifest.DataSlots) || !validRecipeSecretSlots(manifest.SecretSlots) {
		return errCode("invalid_recipe_execution_manifest")
	}
	return nil
}

func (manifest RecipeExecutionManifestV1) Digest() (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(manifest.normalized())
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return "", err
	}
	canonical, err := mode.Marshal(recipeJSONNumbers(value))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (manifest RecipeExecutionManifestV1) CanonicalJSON() ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(manifest.normalized())
	if err != nil || len(raw) > MaxRecipeManifestJSONBytes {
		return nil, errCode("invalid_recipe_execution_manifest")
	}
	return raw, nil
}

func ParseRecipeExecutionManifestJSON(raw []byte) (RecipeExecutionManifestV1, error) {
	if len(raw) == 0 || len(raw) > MaxRecipeManifestJSONBytes || validateRecipeManifestJSON(raw) != nil {
		return RecipeExecutionManifestV1{}, errCode("invalid_recipe_execution_manifest")
	}
	var manifest RecipeExecutionManifestV1
	if decodeSingle(raw, &manifest) != nil || manifest.Validate() != nil {
		return RecipeExecutionManifestV1{}, errCode("invalid_recipe_execution_manifest")
	}
	canonical, err := manifest.CanonicalJSON()
	if err != nil || !bytes.Equal(raw, canonical) {
		return RecipeExecutionManifestV1{}, errCode("invalid_recipe_execution_manifest")
	}
	return manifest, nil
}

func (manifest RecipeExecutionManifestV1) normalized() RecipeExecutionManifestV1 {
	value := manifest
	value.CheckpointSequence = append([]string(nil), manifest.CheckpointSequence...)
	value.VolumeSlots = append([]RecipeVolumeSlotV1(nil), manifest.VolumeSlots...)
	sort.Slice(value.VolumeSlots, func(i, j int) bool {
		if value.VolumeSlots[i].SlotID == value.VolumeSlots[j].SlotID {
			return value.VolumeSlots[i].VolumeRef < value.VolumeSlots[j].VolumeRef
		}
		return value.VolumeSlots[i].SlotID < value.VolumeSlots[j].SlotID
	})
	value.DataSlots = append([]RecipeDataSlotV1(nil), manifest.DataSlots...)
	sort.Slice(value.DataSlots, func(i, j int) bool {
		if value.DataSlots[i].SlotID == value.DataSlots[j].SlotID {
			return value.DataSlots[i].DataRef < value.DataSlots[j].DataRef
		}
		return value.DataSlots[i].SlotID < value.DataSlots[j].SlotID
	})
	value.SecretSlots = append([]RecipeSecretSlotV1(nil), manifest.SecretSlots...)
	sort.Slice(value.SecretSlots, func(i, j int) bool {
		if value.SecretSlots[i].SlotID == value.SecretSlots[j].SlotID {
			return value.SecretSlots[i].SecretRef < value.SecretSlots[j].SecretRef
		}
		return value.SecretSlots[i].SlotID < value.SecretSlots[j].SlotID
	})
	return value
}

func ParseRecipeTaskEventV1(raw []byte) (RecipeTaskEventV1, error) {
	var event RecipeTaskEventV1
	fields := []string{"schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "checkpoint", "error_code", "evidence_digest", "occurred_at"}
	if !strictRecipeTaskObject(raw, fields, &event) || event.Validate() != nil {
		return RecipeTaskEventV1{}, errCode("invalid_recipe_task_event")
	}
	return event, nil
}

func (event RecipeTaskEventV1) Validate() error {
	if event.Schema != RecipeTaskEventV1Schema || !recipeTaskIDPattern.MatchString(event.TaskID) ||
		!workerTaskPositive(event.Attempt) || !workerTaskPositive(event.LeaseEpoch) || !workerTaskPositive(event.Sequence) ||
		!canonicalTaskInstant(event.OccurredAt) || !optionalRecipeTaskCode(event.Checkpoint) ||
		!optionalRecipeTaskCode(event.ErrorCode) || !optionalWorkerTaskDigest(event.EvidenceDigest) {
		return errCode("invalid_recipe_task_event")
	}
	switch event.Status {
	case "running", "succeeded", "failed", "interrupted":
		return nil
	default:
		return errCode("invalid_recipe_task_event")
	}
}

// ValidateRecipeTaskAdvance applies the exact recipeexec.Progress rules to a
// durable task projection without accepting any executor-controlled scope.
func ValidateRecipeTaskAdvance(task RecipeTaskV1, currentStatus string, event RecipeTaskEventV1, expectedLeaseEpoch uint64) error {
	if task.Validate() != nil || event.Validate() != nil || event.TaskID != task.TaskID || event.Attempt != task.Attempt ||
		event.LeaseEpoch != expectedLeaseEpoch || event.Sequence != task.LastSequence+1 ||
		(currentStatus != "queued" && currentStatus != "running") {
		return errCode("invalid_recipe_task_event")
	}
	nextIndex := recipeCheckpointIndex(task.CheckpointSequence, task.LastCheckpoint) + 1
	switch event.Status {
	case "running":
		if !recipeEventMatchesCheckpoint(event, task, nextIndex) || nextIndex == len(task.CheckpointSequence)-1 {
			return errCode("invalid_recipe_task_event")
		}
	case "succeeded":
		if !recipeEventMatchesCheckpoint(event, task, nextIndex) || nextIndex != len(task.CheckpointSequence)-1 {
			return errCode("invalid_recipe_task_event")
		}
	case "failed", "interrupted":
		if event.Checkpoint != nil || event.ErrorCode == nil || event.EvidenceDigest != nil {
			return errCode("invalid_recipe_task_event")
		}
	default:
		return errCode("invalid_recipe_task_event")
	}
	return nil
}

func NewRecipeTaskEventReceipt(event RecipeTaskEventV1, idempotent bool) (RecipeTaskEventReceipt, error) {
	disposition := "accepted"
	if idempotent {
		disposition = "idempotent"
	}
	receipt := RecipeTaskEventReceipt{Schema: RecipeTaskEventReceiptSchema, TaskID: event.TaskID, Attempt: event.Attempt,
		LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: disposition}
	if event.Validate() != nil || (receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return RecipeTaskEventReceipt{}, errCode("invalid_recipe_task_event_receipt")
	}
	return receipt, nil
}

func MarshalRecipeTaskResult(command Command, summary RecipeTaskSummary, idempotent bool) ([]byte, error) {
	if command.Action != ActionWorkerRecipeTaskIssue && command.Action != ActionWorkerRecipeTaskObserve {
		return nil, errCode("invalid_recipe_task_result")
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return nil, err
	}
	status := "recipe_task_issued"
	if command.Action == ActionWorkerRecipeTaskObserve {
		status = "recipe_task_observed"
	}
	disposition := "committed"
	if idempotent {
		status, disposition = "idempotent", "idempotent"
	}
	result := RecipeTaskResult{Status: status, Receipt: RecipeTaskReceipt{Schema: ReceiptSchema, Disposition: disposition,
		ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		CommandID: command.CommandID, RequestSHA256: requestSHA, Action: command.Action}, Task: summary}
	if err := ValidateRecipeTaskResult(command, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func DecodeRecipeTaskResult(command Command, raw []byte) (RecipeTaskResult, error) {
	var result RecipeTaskResult
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "task"}) {
		return RecipeTaskResult{}, errCode("invalid_recipe_task_result")
	}
	receipt, receiptErr := exactJSONObject(object["receipt"])
	task, taskErr := exactJSONObject(object["task"])
	if receiptErr != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) ||
		taskErr != nil || !exactFields(task, []string{"task_id", "execution_id", "deployment_id", "status", "attempt", "last_sequence", "last_checkpoint", "error_code", "evidence_digest", "updated_at"}) ||
		decodeSingle(raw, &result) != nil || ValidateRecipeTaskResult(command, result) != nil {
		return RecipeTaskResult{}, errCode("invalid_recipe_task_result")
	}
	return result, nil
}

func ValidateRecipeTaskResult(command Command, result RecipeTaskResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	wantStatus := "recipe_task_issued"
	var deploymentID, taskID, executionID, manifestDigest string
	var checkpoints []string
	if command.Action == ActionWorkerRecipeTaskIssue {
		request, requestErr := command.RecipeTaskIssueRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, taskID, executionID, manifestDigest, checkpoints = request.DeploymentID, request.TaskID, request.ExecutionID, request.RecipeExecutionManifestDigest, request.CheckpointSequence
	} else if command.Action == ActionWorkerRecipeTaskObserve {
		wantStatus = "recipe_task_observed"
		request, requestErr := command.RecipeTaskObserveRequest()
		if requestErr != nil {
			return requestErr
		}
		deploymentID, taskID = request.DeploymentID, request.TaskID
	} else {
		return errCode("invalid_recipe_task_result")
	}
	wantDisposition := "committed"
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	} else if result.Status != wantStatus {
		return errCode("invalid_broker_status")
	}
	receipt := result.Receipt
	if receipt.Schema != ReceiptSchema || receipt.Disposition != wantDisposition || receipt.ConnectionID != command.ConnectionID ||
		receipt.ExpectedGeneration != command.ExpectedGeneration || receipt.NodeCounter != command.NodeCounter ||
		receipt.CommandID != command.CommandID || receipt.RequestSHA256 != requestSHA || receipt.Action != command.Action ||
		result.Task.DeploymentID != deploymentID || result.Task.TaskID != taskID ||
		(command.Action == ActionWorkerRecipeTaskIssue && result.Task.ExecutionID != executionID) ||
		!validRecipeTaskSummary(result.Task, checkpoints) {
		return errCode("invalid_recipe_task_result")
	}
	if command.Action == ActionWorkerRecipeTaskIssue && (result.Task.Status == "running" || result.Task.Status == "succeeded") &&
		(result.Task.EvidenceDigest == nil || *result.Task.EvidenceDigest != manifestDigest) {
		return errCode("invalid_recipe_task_result")
	}
	return nil
}

func validRecipeTaskSummary(summary RecipeTaskSummary, checkpoints []string) bool {
	if !recipeTaskIDPattern.MatchString(summary.TaskID) || !recipeBindingIDPattern.MatchString(summary.ExecutionID) ||
		!recipeBindingIDPattern.MatchString(summary.DeploymentID) || summary.Attempt < 1 || summary.Attempt > maxSafeInteger ||
		summary.LastSequence < 0 || summary.LastSequence > maxSafeInteger || !canonicalTaskInstant(summary.UpdatedAt) ||
		(summary.LastCheckpoint != "" && !recipeTaskCodePattern.MatchString(summary.LastCheckpoint)) ||
		!optionalRecipeTaskCode(summary.ErrorCode) || !optionalWorkerTaskDigest(summary.EvidenceDigest) ||
		(len(checkpoints) > 0 && summary.LastCheckpoint != "" && recipeCheckpointIndex(checkpoints, summary.LastCheckpoint) < 0) {
		return false
	}
	switch summary.Status {
	case "queued":
		return summary.Attempt == 1 && summary.LastSequence == 0 && summary.LastCheckpoint == "" && summary.ErrorCode == nil && summary.EvidenceDigest == nil
	case "running":
		return summary.LastSequence > 0 && summary.LastCheckpoint != "" && summary.ErrorCode == nil && summary.EvidenceDigest != nil
	case "succeeded":
		return summary.LastSequence > 0 && summary.LastCheckpoint != "" && summary.ErrorCode == nil && summary.EvidenceDigest != nil
	case "failed", "interrupted":
		return summary.LastSequence > 0 && summary.ErrorCode != nil && summary.EvidenceDigest == nil
	default:
		return false
	}
}

func validRecipeTaskCheckpoints(checkpoints []string) bool {
	if len(checkpoints) == 0 || len(checkpoints) > 32 {
		return false
	}
	seen := make(map[string]struct{}, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if !recipeTaskCodePattern.MatchString(checkpoint) {
			return false
		}
		if _, duplicate := seen[checkpoint]; duplicate {
			return false
		}
		seen[checkpoint] = struct{}{}
	}
	return true
}

func recipeCheckpointIndex(checkpoints []string, checkpoint string) int {
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

func recipeEventMatchesCheckpoint(event RecipeTaskEventV1, task RecipeTaskV1, index int) bool {
	return event.Checkpoint != nil && event.ErrorCode == nil && event.EvidenceDigest != nil &&
		*event.EvidenceDigest == task.RecipeExecutionManifestDigest && index >= 0 && index < len(task.CheckpointSequence) &&
		*event.Checkpoint == task.CheckpointSequence[index]
}

func optionalRecipeTaskCode(value *string) bool {
	return value == nil || recipeTaskCodePattern.MatchString(*value)
}

func strictRecipeTaskObject(raw []byte, fields []string, target any) bool {
	object, err := exactJSONObject(raw)
	return err == nil && exactFields(object, fields) && decodeSingle(raw, target) == nil
}

func validateRecipeManifestJSON(raw []byte) error {
	object, err := exactJSONObject(raw)
	base := []string{"schema_version", "execution_id", "deployment_id", "plan_id", "plan_hash", "plan_revision", "recipe_digest", "worker_resource_manifest_digest", "artifact_digest", "action_id", "root_required", "timeout_seconds", "checkpoint_sequence"}
	if err != nil {
		return errCode("invalid_recipe_execution_manifest")
	}
	allowed := make(map[string]bool, len(base)+3)
	for _, field := range base {
		allowed[field] = true
	}
	allowed["volume_slots"], allowed["data_slots"], allowed["secret_slots"] = true, true, true
	for _, field := range base {
		if _, found := object[field]; !found {
			return errCode("invalid_recipe_execution_manifest")
		}
	}
	for field := range object {
		if !allowed[field] {
			return errCode("invalid_recipe_execution_manifest")
		}
	}
	for field, slotFields := range map[string][]string{"volume_slots": {"slot_id", "volume_ref", "read_only"}, "data_slots": {"slot_id", "data_ref", "read_only"}, "secret_slots": {"slot_id", "secret_ref"}} {
		rawSlots, found := object[field]
		if !found {
			continue
		}
		var slots []json.RawMessage
		if json.Unmarshal(rawSlots, &slots) != nil {
			return errCode("invalid_recipe_execution_manifest")
		}
		for _, slot := range slots {
			value, e := exactJSONObject(slot)
			if e != nil || !exactFields(value, slotFields) {
				return errCode("invalid_recipe_execution_manifest")
			}
		}
	}
	return nil
}

func validRecipeVolumeSlots(slots []RecipeVolumeSlotV1) bool {
	if len(slots) > 64 {
		return false
	}
	ids, refs := map[string]bool{}, map[string]bool{}
	for _, slot := range slots {
		if !recipeBindingIDPattern.MatchString(slot.SlotID) || !recipeVolumeRefPattern.MatchString(slot.VolumeRef) || recipeContainsSecret(slot.SlotID, slot.VolumeRef) || ids[slot.SlotID] || refs[slot.VolumeRef] {
			return false
		}
		ids[slot.SlotID], refs[slot.VolumeRef] = true, true
	}
	return true
}
func validRecipeDataSlots(slots []RecipeDataSlotV1) bool {
	if len(slots) > 64 {
		return false
	}
	ids, refs := map[string]bool{}, map[string]bool{}
	for _, slot := range slots {
		if !recipeBindingIDPattern.MatchString(slot.SlotID) || !recipeDataRefPattern.MatchString(slot.DataRef) || recipeContainsSecret(slot.SlotID, slot.DataRef) || ids[slot.SlotID] || refs[slot.DataRef] {
			return false
		}
		ids[slot.SlotID], refs[slot.DataRef] = true, true
	}
	return true
}
func validRecipeSecretSlots(slots []RecipeSecretSlotV1) bool {
	if len(slots) > 64 {
		return false
	}
	ids, refs := map[string]bool{}, map[string]bool{}
	for _, slot := range slots {
		if !recipeBindingIDPattern.MatchString(slot.SlotID) || !recipeSecretRefPattern.MatchString(slot.SecretRef) || recipeContainsSecret(slot.SlotID, slot.SecretRef) || ids[slot.SlotID] || refs[slot.SecretRef] {
			return false
		}
		ids[slot.SlotID], refs[slot.SecretRef] = true, true
	}
	return true
}
func recipeContainsSecret(values ...string) bool {
	for _, value := range values {
		for _, pattern := range recipeSecretPatterns {
			if pattern.MatchString(value) {
				return true
			}
		}
	}
	return false
}
func sameRecipeTaskCheckpoints(left, right []string) bool {
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
func recipeJSONNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if signed, err := strconv.ParseInt(typed.String(), 10, 64); err == nil {
			return signed
		}
		unsigned, _ := strconv.ParseUint(typed.String(), 10, 64)
		return unsigned
	case []any:
		for index := range typed {
			typed[index] = recipeJSONNumbers(typed[index])
		}
		return typed
	case map[string]any:
		for key := range typed {
			typed[key] = recipeJSONNumbers(typed[key])
		}
		return typed
	default:
		return value
	}
}
