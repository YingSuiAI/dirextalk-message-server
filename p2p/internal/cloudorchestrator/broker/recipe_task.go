package broker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	RecipeTaskIssueAction   = "worker.recipe_task.issue"
	RecipeTaskObserveAction = "worker.recipe_task.observe"
	RecipeTaskIssueSchema   = "dirextalk.recipe-execution-task-issue/v1"
)

type RecipeTaskIssueRequest struct {
	Schema             string                                   `json:"schema"`
	ExecutionID        string                                   `json:"execution_id"`
	DeploymentID       string                                   `json:"deployment_id"`
	TaskID             string                                   `json:"task_id"`
	TaskKind           string                                   `json:"task_kind"`
	ManifestDigest     string                                   `json:"recipe_execution_manifest_digest"`
	InputDigest        string                                   `json:"input_digest"`
	CheckpointSequence []string                                 `json:"checkpoint_sequence"`
	Manifest           cloudcontracts.RecipeExecutionManifestV1 `json:"manifest"`
}
type RecipeTaskObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}
type RecipeTaskCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Action                             string
	Issue                              RecipeTaskIssueRequest
	Observe                            RecipeTaskObserveRequest
	PrivateKey                         ed25519.PrivateKey
}
type RecipeTaskCommand struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	CommandID          string `json:"command_id"`
	NodeKeyID          string `json:"node_key_id"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	Action             string `json:"action"`
	PayloadB64         string `json:"payload_b64"`
	PayloadSHA256      string `json:"payload_sha256"`
	SignatureB64       string `json:"signature_b64"`
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
type RecipeTaskResult struct {
	Status  string            `json:"status"`
	Receipt RecipeTaskReceipt `json:"receipt"`
	Task    RecipeTaskSummary `json:"task"`
}

func NewRecipeTaskCommand(input RecipeTaskCommandInput) (RecipeTaskCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return RecipeTaskCommand{}, newError("invalid_node_private_key", nil)
	}
	var payload []byte
	var err error
	if input.Action == RecipeTaskIssueAction {
		payload, err = json.Marshal(input.Issue)
	} else if input.Action == RecipeTaskObserveAction {
		payload, err = json.Marshal(input.Observe)
	} else {
		return RecipeTaskCommand{}, newError("invalid_recipe_task_action", nil)
	}
	if err != nil {
		return RecipeTaskCommand{}, err
	}
	c := RecipeTaskCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: canonicalInstant(input.IssuedAt), ExpiresAt: canonicalInstant(input.ExpiresAt), ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: input.Action, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload)}
	if err = c.validate(false); err != nil {
		return RecipeTaskCommand{}, err
	}
	c.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(c.SignatureBase())))
	return c, c.validate(true)
}
func ParseRecipeTaskCommand(raw []byte) (RecipeTaskCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return RecipeTaskCommand{}, newError("invalid_command", err)
	}
	var c RecipeTaskCommand
	if err := decodeStrictJSON(raw, &c); err != nil {
		return c, err
	}
	return c, c.validate(true)
}
func (c RecipeTaskCommand) validate(signature bool) error {
	if c.Schema != CommandSchema || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) || !keyIDPattern.MatchString(c.NodeKeyID) || (c.Action != RecipeTaskIssueAction && c.Action != RecipeTaskObserveAction) || !safePositive(c.ExpectedGeneration) || !safeNonnegative(c.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issued, e := parseCanonicalInstant(c.IssuedAt)
	expires, x := parseCanonicalInstant(c.ExpiresAt)
	if e != nil || x != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", nil)
	}
	payload, e := decodeCanonicalBase64(c.PayloadB64)
	if e != nil || sha256Hex(payload) != c.PayloadSHA256 || len(payload) > 64*1024 {
		return newError("invalid_payload", e)
	}
	var canonical []byte
	if c.Action == RecipeTaskIssueAction {
		var r RecipeTaskIssueRequest
		if e = decodeStrictJSON(payload, &r); e != nil || r.Schema != RecipeTaskIssueSchema || !idPattern.MatchString(r.ExecutionID) || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.TaskID) || r.TaskKind != "recipe_execution" || !namedSHA256Pattern.MatchString(r.ManifestDigest) || !namedSHA256Pattern.MatchString(r.InputDigest) || len(r.CheckpointSequence) == 0 || r.Manifest.Validate() != nil || r.Manifest.ExecutionID != r.ExecutionID || r.Manifest.DeploymentID != r.DeploymentID || r.Manifest.VerifyDigest(r.ManifestDigest) != nil {
			return newError("invalid_recipe_task_request", e)
		}
		canonical, _ = json.Marshal(r)
	} else {
		var r RecipeTaskObserveRequest
		if e = decodeStrictJSON(payload, &r); e != nil || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.TaskID) {
			return newError("invalid_recipe_task_request", e)
		}
		canonical, _ = json.Marshal(r)
	}
	if !bytes.Equal(payload, canonical) {
		return newError("noncanonical_payload", nil)
	}
	if signature {
		raw, e := decodeCanonicalBase64(c.SignatureB64)
		if e != nil || len(raw) != ed25519.SignatureSize {
			return newError("invalid_command", e)
		}
	}
	return nil
}
func (c RecipeTaskCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{Schema: c.Schema, ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, Action: c.Action, PayloadSHA256: c.PayloadSHA256})
}
func (c RecipeTaskCommand) RequestSHA256() string { return sha256Hex([]byte(c.SignatureBase())) }

func (client *Client) SubmitRecipeTask(ctx context.Context, command RecipeTaskCommand) (RecipeTaskResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return RecipeTaskResult{}, newError("broker_client_unavailable", nil)
	}
	if err := command.validate(true); err != nil {
		return RecipeTaskResult{}, err
	}
	body, _ := json.Marshal(command)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return RecipeTaskResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return RecipeTaskResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return RecipeTaskResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return RecipeTaskResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		return RecipeTaskResult{}, newError("invalid_broker_content_type", err)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponseBytes+1))
	if err != nil || int64(len(raw)) > client.maxResponseBytes {
		return RecipeTaskResult{}, newError("broker_response_unavailable", err)
	}
	var result RecipeTaskResult
	if err = decodeStrictJSON(raw, &result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	if err = ValidateRecipeTaskResult(command, result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	return result, nil
}

// ValidateRecipeTaskResult binds the Stack receipt and de-secreted task
// summary to the exact signed issue/observe command. It deliberately accepts
// only the closed Connection Stack V2 response contract.
func ValidateRecipeTaskResult(command RecipeTaskCommand, result RecipeTaskResult) error {
	if err := command.validate(true); err != nil {
		return err
	}
	wantStatus := "recipe_task_issued"
	wantDisposition := "committed"
	var deploymentID, taskID, executionID, manifestDigest string
	var checkpoints []string
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		return errors.New("invalid recipe task command payload")
	}
	if command.Action == RecipeTaskIssueAction {
		var request RecipeTaskIssueRequest
		if err = decodeStrictJSON(payload, &request); err != nil {
			return errors.New("invalid recipe task issue binding")
		}
		deploymentID, taskID, executionID, manifestDigest = request.DeploymentID, request.TaskID, request.ExecutionID, request.ManifestDigest
		checkpoints = request.CheckpointSequence
	} else if command.Action == RecipeTaskObserveAction {
		wantStatus = "recipe_task_observed"
		var request RecipeTaskObserveRequest
		if err = decodeStrictJSON(payload, &request); err != nil {
			return errors.New("invalid recipe task observe binding")
		}
		deploymentID, taskID = request.DeploymentID, request.TaskID
	} else {
		return errors.New("invalid recipe task action")
	}
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	} else if result.Status != wantStatus {
		return errors.New("invalid recipe task result status")
	}
	receipt := result.Receipt
	if receipt.Schema != ReceiptSchema || receipt.Disposition != wantDisposition || receipt.ConnectionID != command.ConnectionID ||
		receipt.ExpectedGeneration != command.ExpectedGeneration || receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID ||
		receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != command.Action {
		return errors.New("invalid recipe task receipt binding")
	}
	if result.Task.DeploymentID != deploymentID || result.Task.TaskID != taskID ||
		(command.Action == RecipeTaskIssueAction && result.Task.ExecutionID != executionID) ||
		!validRecipeTaskSummary(result.Task, checkpoints) {
		return errors.New("invalid recipe task summary binding")
	}
	if command.Action == RecipeTaskIssueAction && (result.Task.Status == "running" || result.Task.Status == "succeeded") &&
		(result.Task.EvidenceDigest == nil || *result.Task.EvidenceDigest != manifestDigest) {
		return errors.New("invalid recipe task evidence binding")
	}
	return nil
}

func validRecipeTaskSummary(summary RecipeTaskSummary, checkpoints []string) bool {
	if !idPattern.MatchString(summary.TaskID) || !idPattern.MatchString(summary.ExecutionID) || !idPattern.MatchString(summary.DeploymentID) ||
		!safePositive(summary.Attempt) || !safeNonnegative(summary.LastSequence) {
		return false
	}
	if _, err := parseCanonicalInstant(summary.UpdatedAt); err != nil {
		return false
	}
	if summary.LastCheckpoint != "" && !itemPattern.MatchString(summary.LastCheckpoint) {
		return false
	}
	if summary.ErrorCode != nil && !brokerErrorCodePattern.MatchString(*summary.ErrorCode) {
		return false
	}
	if summary.EvidenceDigest != nil && !namedSHA256Pattern.MatchString(*summary.EvidenceDigest) {
		return false
	}
	if len(checkpoints) > 0 && summary.LastCheckpoint != "" {
		found := false
		for _, checkpoint := range checkpoints {
			if checkpoint == summary.LastCheckpoint {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	switch summary.Status {
	case "queued":
		return summary.Attempt == 1 && summary.LastSequence == 0 && summary.LastCheckpoint == "" && summary.ErrorCode == nil && summary.EvidenceDigest == nil
	case "running", "succeeded":
		return summary.LastSequence > 0 && summary.LastCheckpoint != "" && summary.ErrorCode == nil && summary.EvidenceDigest != nil
	case "failed", "interrupted":
		return summary.LastSequence > 0 && summary.ErrorCode != nil && summary.EvidenceDigest == nil
	default:
		return false
	}
}
