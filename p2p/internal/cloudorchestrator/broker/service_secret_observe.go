package broker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	ServiceSecretObserveAction = "service.secret.observe"
	ServiceSecretObserveSchema = "dirextalk.service-secret-observation/v1"
)

var (
	serviceSecretRefPattern     = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	serviceSecretBindingPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	serviceSecretTaskPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	serviceSecretVersionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
)

type ServiceSecretObserveRequest struct {
	SessionID, DeploymentID, TaskID, ExecutionID string
	ManifestDigest, SecretRef, ContextDigest     string
}

func (r ServiceSecretObserveRequest) MarshalJSON() ([]byte, error) {
	type wire struct {
		SessionID      string `json:"session_id"`
		DeploymentID   string `json:"deployment_id"`
		TaskID         string `json:"task_id"`
		ExecutionID    string `json:"execution_id"`
		ManifestDigest string `json:"manifest_digest"`
		SecretRef      string `json:"secret_ref"`
		ContextDigest  string `json:"context_digest"`
	}
	return json.Marshal(wire(r))
}

type ServiceSecretObserveCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceSecretObserveRequest
	PrivateKey                         ed25519.PrivateKey
}

type ServiceSecretObserveCommand struct {
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

type ServiceSecretObservation struct {
	Schema          string `json:"schema"`
	SessionID       string `json:"session_id"`
	Status          string `json:"status"`
	ProviderVersion string `json:"provider_version,omitempty"`
	BindingDigest   string `json:"binding_digest"`
	UpdatedMarker   string `json:"updated_marker"`
}

func NewServiceSecretObserveCommand(input ServiceSecretObserveCommandInput) (ServiceSecretObserveCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize || validateServiceSecretObserveRequest(input.Request) != nil {
		return ServiceSecretObserveCommand{}, newError("invalid_service_secret_observe_command", nil)
	}
	payload, _ := json.Marshal(input.Request)
	c := ServiceSecretObserveCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: canonicalInstant(input.IssuedAt), ExpiresAt: canonicalInstant(input.ExpiresAt), ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: ServiceSecretObserveAction, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload)}
	if err := c.validate(false); err != nil {
		return ServiceSecretObserveCommand{}, err
	}
	c.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(c.SignatureBase())))
	return c, c.validate(true)
}

func ParseServiceSecretObserveCommand(raw []byte) (ServiceSecretObserveCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return ServiceSecretObserveCommand{}, newError("invalid_command", err)
	}
	var c ServiceSecretObserveCommand
	if err := decodeStrictJSON(raw, &c); err != nil {
		return c, err
	}
	return c, c.validate(true)
}

func (c ServiceSecretObserveCommand) validate(signature bool) error {
	if c.Schema != CommandSchema || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) || !keyIDPattern.MatchString(c.NodeKeyID) || c.Action != ServiceSecretObserveAction || !safePositive(c.ExpectedGeneration) || !safeNonnegative(c.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issued, ie := parseCanonicalInstant(c.IssuedAt)
	expires, ee := parseCanonicalInstant(c.ExpiresAt)
	if ie != nil || ee != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", nil)
	}
	payload, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil || sha256Hex(payload) != c.PayloadSHA256 || len(payload) > 8*1024 {
		return newError("invalid_payload", err)
	}
	request, err := decodeServiceSecretObserveRequest(payload)
	if err != nil {
		return err
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return newError("noncanonical_payload", nil)
	}
	if signature {
		decoded, e := decodeCanonicalBase64(c.SignatureB64)
		if e != nil || len(decoded) != ed25519.SignatureSize {
			return newError("invalid_node_signature", e)
		}
	}
	return nil
}

func (c ServiceSecretObserveCommand) Request() (ServiceSecretObserveRequest, error) {
	payload, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil {
		return ServiceSecretObserveRequest{}, err
	}
	return decodeServiceSecretObserveRequest(payload)
}
func (c ServiceSecretObserveCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{Schema: c.Schema, ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, Action: c.Action, PayloadSHA256: c.PayloadSHA256})
}
func (c ServiceSecretObserveCommand) RequestSHA256() string {
	return sha256Hex([]byte(c.SignatureBase()))
}

func decodeServiceSecretObserveRequest(raw []byte) (ServiceSecretObserveRequest, error) {
	fields := []string{"session_id", "deployment_id", "task_id", "execution_id", "manifest_digest", "secret_ref", "context_digest"}
	if _, err := exactJSONObject(raw, fields); err != nil {
		return ServiceSecretObserveRequest{}, newError("invalid_service_secret_observe_request", err)
	}
	var wire struct {
		SessionID      string `json:"session_id"`
		DeploymentID   string `json:"deployment_id"`
		TaskID         string `json:"task_id"`
		ExecutionID    string `json:"execution_id"`
		ManifestDigest string `json:"manifest_digest"`
		SecretRef      string `json:"secret_ref"`
		ContextDigest  string `json:"context_digest"`
	}
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return ServiceSecretObserveRequest{}, err
	}
	r := ServiceSecretObserveRequest(wire)
	return r, validateServiceSecretObserveRequest(r)
}
func validateServiceSecretObserveRequest(r ServiceSecretObserveRequest) error {
	if !idPattern.MatchString(r.SessionID) || !idPattern.MatchString(r.DeploymentID) || !serviceSecretTaskPattern.MatchString(r.TaskID) || !serviceSecretBindingPattern.MatchString(r.ExecutionID) || !namedSHA256Pattern.MatchString(r.ManifestDigest) || !serviceSecretRefPattern.MatchString(r.SecretRef) || !namedSHA256Pattern.MatchString(r.ContextDigest) {
		return newError("invalid_service_secret_observe_request", nil)
	}
	return nil
}

func (client *Client) SubmitServiceSecretObserve(ctx context.Context, command ServiceSecretObserveCommand) (ServiceSecretObservation, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil || ctx == nil {
		return ServiceSecretObservation{}, newError("broker_client_unavailable", nil)
	}
	if err := command.validate(true); err != nil {
		return ServiceSecretObservation{}, err
	}
	body, _ := json.Marshal(command)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return ServiceSecretObservation{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ServiceSecretObservation{}, newError("broker_timeout", err)
		}
		return ServiceSecretObservation{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return ServiceSecretObservation{}, newHTTPError(code, response.StatusCode, nil)
		}
		return ServiceSecretObservation{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		return ServiceSecretObservation{}, newError("invalid_broker_content_type", err)
	}
	raw, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return ServiceSecretObservation{}, newError("invalid_broker_response", err)
	}
	observation, err := decodeServiceSecretObservation(raw)
	if err != nil || validateServiceSecretObservation(command, observation) != nil {
		return ServiceSecretObservation{}, newError("invalid_broker_response", err)
	}
	return observation, nil
}

func decodeServiceSecretObservation(raw []byte) (ServiceSecretObservation, error) {
	var probe struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return ServiceSecretObservation{}, newError("invalid_broker_response", nil)
	}
	fields := []string{"schema", "session_id", "status", "binding_digest", "updated_marker"}
	if probe.Status == "uploaded" || probe.Status == "completed" {
		fields = append(fields, "provider_version")
	}
	if _, err := exactJSONObject(raw, fields); err != nil {
		return ServiceSecretObservation{}, err
	}
	var result ServiceSecretObservation
	if err := decodeStrictJSON(raw, &result); err != nil {
		return result, err
	}
	return result, nil
}
func validateServiceSecretObservation(c ServiceSecretObserveCommand, r ServiceSecretObservation) error {
	request, err := c.Request()
	if err != nil || r.Schema != ServiceSecretObserveSchema || r.SessionID != request.SessionID || r.BindingDigest != request.ContextDigest || !sha256Pattern.MatchString(r.UpdatedMarker) {
		return newError("invalid_service_secret_observation", err)
	}
	switch r.Status {
	case "pending_upload", "processing", "expired":
		if r.ProviderVersion != "" {
			return newError("invalid_service_secret_observation", nil)
		}
	case "uploaded", "completed":
		if !serviceSecretVersionPattern.MatchString(r.ProviderVersion) {
			return newError("invalid_service_secret_observation", nil)
		}
	default:
		return newError("invalid_service_secret_observation", nil)
	}
	return nil
}
