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
)

const (
	ServiceReadinessIssueAction   = "worker.service_readiness.issue"
	ServiceReadinessObserveAction = "worker.service_readiness.observe"
	ServiceReadinessIssueSchema   = "dirextalk.service-readiness-task-issue/v1"
	ServiceReadinessProbeKind     = "stack_witnessed_fixed_worker_probe_v1"
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
type ServiceReadinessCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Action                             string
	Issue                              ServiceReadinessIssueRequest
	Observe                            ServiceReadinessObserveRequest
	PrivateKey                         ed25519.PrivateKey
}
type ServiceReadinessCommand struct {
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
type ServiceReadinessSummary struct {
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
	Status  string                  `json:"status"`
	Receipt RecipeTaskReceipt       `json:"receipt"`
	Task    ServiceReadinessSummary `json:"task"`
}

var serviceReadinessResultFields = []string{"status", "receipt", "task"}
var serviceReadinessSummaryFields = []string{"task_id", "execution_id", "deployment_id", "service_id", "status", "attempt", "last_sequence", "checkpoint", "challenge_digest", "semantic_evidence_digest", "stack_observation_digest", "error_code", "updated_at"}
var serviceReadinessReceiptFields = []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}

func NewServiceReadinessCommand(input ServiceReadinessCommandInput) (ServiceReadinessCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return ServiceReadinessCommand{}, newError("invalid_node_private_key", nil)
	}
	var payload []byte
	var err error
	if input.Action == ServiceReadinessIssueAction {
		payload, err = json.Marshal(input.Issue)
	} else if input.Action == ServiceReadinessObserveAction {
		payload, err = json.Marshal(input.Observe)
	} else {
		return ServiceReadinessCommand{}, newError("invalid_service_readiness_action", nil)
	}
	if err != nil {
		return ServiceReadinessCommand{}, err
	}
	c := ServiceReadinessCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: canonicalInstant(input.IssuedAt), ExpiresAt: canonicalInstant(input.ExpiresAt), ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: input.Action, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload)}
	if err = c.validate(false); err != nil {
		return c, err
	}
	c.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(c.SignatureBase())))
	return c, c.validate(true)
}

func ParseServiceReadinessCommand(raw []byte) (ServiceReadinessCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return ServiceReadinessCommand{}, newError("invalid_command", err)
	}
	var c ServiceReadinessCommand
	if err := decodeStrictJSON(raw, &c); err != nil {
		return c, err
	}
	return c, c.validate(true)
}
func (c ServiceReadinessCommand) validate(signature bool) error {
	if c.Schema != CommandSchema || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) || !keyIDPattern.MatchString(c.NodeKeyID) || !safePositive(c.ExpectedGeneration) || !safePositive(c.NodeCounter) || (c.Action != ServiceReadinessIssueAction && c.Action != ServiceReadinessObserveAction) {
		return newError("invalid_command", nil)
	}
	issued, e := parseCanonicalInstant(c.IssuedAt)
	expires, x := parseCanonicalInstant(c.ExpiresAt)
	if e != nil || x != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", nil)
	}
	payload, e := decodeCanonicalBase64(c.PayloadB64)
	if e != nil || sha256Hex(payload) != c.PayloadSHA256 || len(payload) > 16*1024 {
		return newError("invalid_payload", e)
	}
	var canonical []byte
	if c.Action == ServiceReadinessIssueAction {
		var r ServiceReadinessIssueRequest
		if e = decodeStrictJSON(payload, &r); e != nil || r.Schema != ServiceReadinessIssueSchema || !idPattern.MatchString(r.ExecutionID) || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.ServiceID) || !idPattern.MatchString(r.TaskID) || r.ProbeKind != ServiceReadinessProbeKind || !namedSHA256Pattern.MatchString(r.RecipeExecutionManifestDigest) || !namedSHA256Pattern.MatchString(r.InstallEvidenceDigest) || !namedSHA256Pattern.MatchString(r.SemanticExpectationDigest) {
			return newError("invalid_service_readiness_request", e)
		}
		canonical, _ = json.Marshal(r)
	} else {
		var r ServiceReadinessObserveRequest
		if e = decodeStrictJSON(payload, &r); e != nil || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.ServiceID) || !idPattern.MatchString(r.TaskID) {
			return newError("invalid_service_readiness_request", e)
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
func (c ServiceReadinessCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{Schema: c.Schema, ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, Action: c.Action, PayloadSHA256: c.PayloadSHA256})
}
func (c ServiceReadinessCommand) RequestSHA256() string { return sha256Hex([]byte(c.SignatureBase())) }

func (client *Client) SubmitServiceReadiness(ctx context.Context, command ServiceReadinessCommand) (ServiceReadinessResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return ServiceReadinessResult{}, newError("broker_client_unavailable", nil)
	}
	if err := command.validate(true); err != nil {
		return ServiceReadinessResult{}, err
	}
	body, _ := json.Marshal(command)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return ServiceReadinessResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return ServiceReadinessResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return ServiceReadinessResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return ServiceReadinessResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		return ServiceReadinessResult{}, newError("invalid_broker_content_type", err)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponseBytes+1))
	if err != nil || int64(len(raw)) > client.maxResponseBytes {
		return ServiceReadinessResult{}, newError("broker_response_unavailable", err)
	}
	object, shapeErr := exactJSONObject(raw, serviceReadinessResultFields)
	if shapeErr != nil {
		return ServiceReadinessResult{}, newError("invalid_broker_response", shapeErr)
	}
	if _, shapeErr = exactJSONObject(object["receipt"], serviceReadinessReceiptFields); shapeErr != nil {
		return ServiceReadinessResult{}, newError("invalid_broker_response", shapeErr)
	}
	if _, shapeErr = exactJSONObject(object["task"], serviceReadinessSummaryFields); shapeErr != nil {
		return ServiceReadinessResult{}, newError("invalid_broker_response", shapeErr)
	}
	var result ServiceReadinessResult
	if err = decodeStrictJSON(raw, &result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	if err = ValidateServiceReadinessResult(command, result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	return result, nil
}

func ValidateServiceReadinessResult(command ServiceReadinessCommand, result ServiceReadinessResult) error {
	if err := command.validate(true); err != nil {
		return err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	var deployment, service, task, execution, semantic string
	wantStatus := "service_readiness_issued"
	disposition := "committed"
	if command.Action == ServiceReadinessIssueAction {
		var r ServiceReadinessIssueRequest
		if decodeStrictJSON(payload, &r) != nil {
			return errors.New("invalid readiness issue binding")
		}
		deployment, service, task, execution, semantic = r.DeploymentID, r.ServiceID, r.TaskID, r.ExecutionID, r.SemanticExpectationDigest
	} else {
		var r ServiceReadinessObserveRequest
		if decodeStrictJSON(payload, &r) != nil {
			return errors.New("invalid readiness observe binding")
		}
		deployment, service, task = r.DeploymentID, r.ServiceID, r.TaskID
		wantStatus = "service_readiness_observed"
	}
	if result.Status == "idempotent" {
		disposition = "idempotent"
	} else if result.Status != wantStatus {
		return errors.New("invalid readiness result status")
	}
	r := result.Receipt
	if r.Schema != ReceiptSchema || r.Disposition != disposition || r.ConnectionID != command.ConnectionID || r.ExpectedGeneration != command.ExpectedGeneration || r.NodeCounter != command.NodeCounter || r.CommandID != command.CommandID || r.RequestSHA256 != command.RequestSHA256() || r.Action != command.Action {
		return errors.New("invalid readiness receipt binding")
	}
	t := result.Task
	if t.DeploymentID != deployment || t.ServiceID != service || t.TaskID != task || (command.Action == ServiceReadinessIssueAction && t.ExecutionID != execution) || !safePositive(t.Attempt) || !safeNonnegative(t.LastSequence) {
		return errors.New("invalid readiness summary binding")
	}
	digest := func(v *string) bool { return v != nil && namedSHA256Pattern.MatchString(*v) }
	switch t.Status {
	case "queued":
		if t.LastSequence != 0 || t.Checkpoint != "" || t.ChallengeDigest != nil || t.SemanticEvidenceDigest != nil || t.StackObservationDigest != nil || t.ErrorCode != nil {
			return errors.New("invalid queued readiness")
		}
	case "running":
		if t.LastSequence < 0 || t.Checkpoint != "challenge_issued" || !digest(t.ChallengeDigest) || t.SemanticEvidenceDigest != nil || t.StackObservationDigest != nil || t.ErrorCode != nil {
			return errors.New("invalid running readiness")
		}
	case "succeeded":
		if t.LastSequence < 1 || t.Checkpoint != "readiness_verified" || !digest(t.ChallengeDigest) || !digest(t.SemanticEvidenceDigest) || !digest(t.StackObservationDigest) || t.ErrorCode != nil {
			return errors.New("invalid succeeded readiness")
		}
		if command.Action == ServiceReadinessIssueAction && *t.SemanticEvidenceDigest != semantic {
			return errors.New("invalid semantic readiness evidence")
		}
	case "failed", "interrupted":
		if t.LastSequence < 1 || t.Checkpoint != "" || t.ChallengeDigest != nil || t.ErrorCode == nil || t.SemanticEvidenceDigest != nil || t.StackObservationDigest != nil {
			return errors.New("invalid terminal readiness")
		}
	default:
		return errors.New("invalid readiness status")
	}
	if _, err := parseCanonicalInstant(t.UpdatedAt); err != nil {
		return errors.New("invalid readiness timestamp")
	}
	return nil
}
