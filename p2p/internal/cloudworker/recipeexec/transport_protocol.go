package recipeexec

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	TaskClaimV1Schema         = "dirextalk.recipe-execution-task-claim/v1"
	TaskClaimResponseV1Schema = "dirextalk.recipe-execution-task-claim-response/v1"
	EventReceiptV1Schema      = "dirextalk.recipe-execution-task-event-receipt/v1"
)

type TaskClaimRequestV1 struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

func NewTaskClaimRequestV1(leaseEpoch uint64) (TaskClaimRequestV1, error) {
	request := TaskClaimRequestV1{Schema: TaskClaimV1Schema, LeaseEpoch: leaseEpoch}
	if request.LeaseEpoch == 0 || request.LeaseEpoch > maxTaskSafeInteger {
		return TaskClaimRequestV1{}, ErrTaskInvalid
	}
	return request, nil
}

type TaskClaimResponseV1 struct {
	Schema     string                                       `json:"schema"`
	Status     string                                       `json:"status"`
	LeaseEpoch uint64                                       `json:"lease_epoch"`
	Task       *TaskV1                                      `json:"task,omitempty"`
	Manifest   *cloudorchestrator.RecipeExecutionManifestV1 `json:"manifest,omitempty"`
}

func ParseTaskClaimResponseV1(raw []byte, expectedLeaseEpoch uint64) (TaskClaimResponseV1, error) {
	type wireResponse struct {
		Schema     string          `json:"schema"`
		Status     string          `json:"status"`
		LeaseEpoch uint64          `json:"lease_epoch"`
		Task       json.RawMessage `json:"task,omitempty"`
		Manifest   json.RawMessage `json:"manifest,omitempty"`
	}
	var wire wireResponse
	if err := decodeStrictTaskObject(raw, &wire); err != nil || requireTaskObjectFields(raw, "schema", "status", "lease_epoch") != nil ||
		wire.Schema != TaskClaimResponseV1Schema || wire.LeaseEpoch != expectedLeaseEpoch || !validTaskPositive(wire.LeaseEpoch) {
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
	fields, err := taskObjectFields(raw)
	if err != nil {
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
	result := TaskClaimResponseV1{Schema: wire.Schema, Status: wire.Status, LeaseEpoch: wire.LeaseEpoch}
	switch wire.Status {
	case "none":
		if _, present := fields["task"]; present {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if _, present := fields["manifest"]; present {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		return result, nil
	case "claimed":
		if len(wire.Task) == 0 || len(wire.Manifest) == 0 || bytes.Equal(bytes.TrimSpace(wire.Task), []byte("null")) || bytes.Equal(bytes.TrimSpace(wire.Manifest), []byte("null")) {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		task, err := ParseTaskV1(wire.Task)
		if err != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		var manifest cloudorchestrator.RecipeExecutionManifestV1
		if _, err := taskObjectFields(wire.Manifest); err != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if err := decodeStrictTaskObject(wire.Manifest, &manifest); err != nil || manifest.Validate() != nil || task.ValidateForManifest(manifest) != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		result.Task, result.Manifest = &task, &manifest
		return result, nil
	default:
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
}

type EventReceiptV1 struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

func ParseEventReceiptV1(raw []byte, event EventV1) (EventReceiptV1, error) {
	var receipt EventReceiptV1
	if err := decodeStrictTaskObject(raw, &receipt); err != nil || requireTaskObjectFields(raw,
		"schema", "task_id", "attempt", "lease_epoch", "sequence", "disposition") != nil ||
		receipt.Schema != EventReceiptV1Schema || receipt.TaskID != event.TaskID || receipt.Attempt != event.Attempt ||
		receipt.LeaseEpoch != event.LeaseEpoch || receipt.Sequence != event.Sequence ||
		(receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return EventReceiptV1{}, errors.New("recipe task event receipt is invalid")
	}
	return receipt, nil
}
