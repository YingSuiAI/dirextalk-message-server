package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"time"
)

const (
	// DeploymentObserveAction is the fixed read-only command used to obtain
	// Stack-verified bootstrap evidence for one already-created Worker.
	DeploymentObserveAction = "deployment.observe"
	// DeploymentObservationSchema is private Broker-to-Orchestrator evidence.
	// It is never returned through ProductCore, MCP, Matrix, or a Worker route.
	DeploymentObservationSchema = "dirextalk.aws.deployment-observation/v1"
)

var (
	deploymentObserveRequestFields = []string{"deployment_id"}
	deploymentObserveResultFields  = []string{"status", "receipt", "observation"}
	deploymentObserveReceiptFields = []string{
		"schema",
		"disposition",
		"connection_id",
		"expected_generation",
		"node_counter",
		"command_id",
		"request_sha256",
		"action",
	}
	deploymentObservationFields = []string{
		"schema",
		"deployment_id",
		"resource",
		"worker",
		"observed_at",
	}
	deploymentObservationResourceFields = []string{"status", "instance_id"}
	deploymentObservationWorkerFields   = []string{
		"bootstrap_session_state",
		"lease_epoch",
		"lease_expires_at",
		"last_sequence",
		"last_event_at",
	}
)

// DeploymentObserveRequest is deliberately the only payload admitted by the
// signed observation command. It has no AWS operation, session identifier,
// bearer, endpoint, or worker-controlled input.
type DeploymentObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
}

// DeploymentObserveCommandInput is the private, durable identity used to
// create one signed read-only observation envelope.
type DeploymentObserveCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentObserveRequest
	PrivateKey         ed25519.PrivateKey
}

// DeploymentObserveCommand is a strict Connection Stack V2 envelope. The
// envelope intentionally omits every approval/binding/proof JSON field; the
// four corresponding empty digest lines remain part of its signature base.
type DeploymentObserveCommand struct {
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

// DeploymentObserveCommandBinding is the immutable durable identity required
// before an observation envelope may be replayed after a timeout.
type DeploymentObserveCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentObserveRequest
}

// DeploymentObserveReceipt is the action-neutral durable Stack command
// receipt. The observation itself remains a fresh read after receipt handling,
// so an idempotent receipt never freezes worker state.
type DeploymentObserveReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

// DeploymentObservationResource is Stack-owned private resource evidence.
// InstanceID never reaches public projections or Worker processes.
type DeploymentObservationResource struct {
	Status     string `json:"status"`
	InstanceID string `json:"instance_id"`
}

// DeploymentObservationWorker excludes bootstrap_session_id, bearer hashes,
// token, endpoint, instance identity document, and raw Worker event. Nil time
// values are encoded explicitly as JSON null when a claim/event has not yet
// occurred.
type DeploymentObservationWorker struct {
	BootstrapSessionState string  `json:"bootstrap_session_state"`
	LeaseEpoch            int64   `json:"lease_epoch"`
	LeaseExpiresAt        *string `json:"lease_expires_at"`
	LastSequence          int64   `json:"last_sequence"`
	LastEventAt           *string `json:"last_event_at"`
}

// DeploymentObservation is the single de-secreted evidence shape accepted
// from the Connection Stack after it independently verifies Worker identity.
type DeploymentObservation struct {
	Schema       string                        `json:"schema"`
	DeploymentID string                        `json:"deployment_id"`
	Resource     DeploymentObservationResource `json:"resource"`
	Worker       DeploymentObservationWorker   `json:"worker"`
	ObservedAt   string                        `json:"observed_at"`
}

// DeploymentObserveResult is the sole successful response shape for a
// deployment.observe command. Its status intentionally remains distinct from
// receipt disposition: a replayed receipt can still contain fresh evidence.
type DeploymentObserveResult struct {
	Status      string                   `json:"status"`
	Receipt     DeploymentObserveReceipt `json:"receipt"`
	Observation DeploymentObservation    `json:"observation"`
}

// NewDeploymentObserveCommand builds one canonical signed, read-only V2
// envelope. It performs no network I/O and never retains the node key.
func NewDeploymentObserveCommand(input DeploymentObserveCommandInput) (DeploymentObserveCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return DeploymentObserveCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateDeploymentObserveRequest(input.Request); err != nil {
		return DeploymentObserveCommand{}, err
	}
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return DeploymentObserveCommand{}, newError("invalid_deployment_observe_request", err)
	}
	command := DeploymentObserveCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           canonicalInstant(input.IssuedAt),
		ExpiresAt:          canonicalInstant(input.ExpiresAt),
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             DeploymentObserveAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
	}
	if err := validateDeploymentObserveCommand(command, false); err != nil {
		return DeploymentObserveCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return DeploymentObserveCommand{}, err
	}
	return command, nil
}

// Validate validates a persisted/replayed command. Expired envelopes remain
// structurally valid so an explicit Stack expired_command response can retire
// the durable command; the runtime never treats local expiry as evidence.
func (command DeploymentObserveCommand) Validate() error {
	return validateDeploymentObserveCommand(command, true)
}

// ValidateBinding proves a stored envelope still expresses exactly one
// deployment observation request before it is retried.
func (command DeploymentObserveCommand) ValidateBinding(binding DeploymentObserveCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if binding.IssuedAt.IsZero() || binding.ExpiresAt.IsZero() || !binding.ExpiresAt.After(binding.IssuedAt) ||
		command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID ||
		command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter ||
		command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateDeploymentObserveRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.DeploymentObserveRequest()
	if err != nil {
		return err
	}
	if request != binding.Request {
		return newError("invalid_deployment_observe_request", nil)
	}
	return nil
}

// VerifySignature checks the exact envelope against the registered node key.
func (command DeploymentObserveCommand) VerifySignature(publicKey ed25519.PublicKey) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return newError("invalid_node_public_key", nil)
	}
	signature, _ := base64.StdEncoding.DecodeString(command.SignatureB64)
	if !ed25519.Verify(publicKey, []byte(command.SignatureBase()), signature) {
		return newError("invalid_node_signature", nil)
	}
	return nil
}

// SignatureBase exactly mirrors Connection Stack V2 buildNodeSignatureBase,
// including its fixed empty approval-proof digest line for a read-only action.
func (command DeploymentObserveCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{
		Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID,
		NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		Action: command.Action, PayloadSHA256: command.PayloadSHA256,
	})
}

// RequestSHA256 is the durable request identity calculated from the exact V2
// signature base, rather than from mutable HTTP JSON serialization.
func (command DeploymentObserveCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// DeploymentObserveRequest decodes the fixed payload bound into the command.
func (command DeploymentObserveCommand) DeploymentObserveRequest() (DeploymentObserveRequest, error) {
	if err := command.Validate(); err != nil {
		return DeploymentObserveRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeDeploymentObserveRequestJSON(payload)
	if err != nil {
		return DeploymentObserveRequest{}, newError("invalid_payload", err)
	}
	if err := validateDeploymentObserveRequest(request); err != nil {
		return DeploymentObserveRequest{}, err
	}
	return request, nil
}

// ParseDeploymentObserveCommand strictly parses one persisted envelope before
// a retry. Unknown, duplicate, approval, or incompatible action fields fail
// before any request reaches the Connection Stack.
func ParseDeploymentObserveCommand(raw []byte) (DeploymentObserveCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return DeploymentObserveCommand{}, newError("invalid_command", err)
	}
	var command DeploymentObserveCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return DeploymentObserveCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return DeploymentObserveCommand{}, err
	}
	return command, nil
}

// ValidateDeploymentObserveResult validates receipt binding, resource binding,
// freshness, and the de-secreted worker state before the Orchestrator can
// persist a verified bootstrap observation.
func ValidateDeploymentObserveResult(command DeploymentObserveCommand, result DeploymentObserveResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "deployment_observed" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	if err := validateDeploymentObserveReceipt(command, result.Receipt); err != nil {
		return err
	}
	if (result.Status == "deployment_observed" && result.Receipt.Disposition != "committed") ||
		(result.Status == "idempotent" && result.Receipt.Disposition != "idempotent") {
		return newError("invalid_deployment_observe_receipt", nil)
	}
	request, err := command.DeploymentObserveRequest()
	if err != nil {
		return err
	}
	return validateDeploymentObservation(command, request, result.Observation)
}

func validateDeploymentObserveCommand(command DeploymentObserveCommand, requireSignature bool) error {
	if command.Schema != CommandSchema || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) ||
		!keyIDPattern.MatchString(command.NodeKeyID) || command.Action != DeploymentObserveAction ||
		!safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, err := parseCanonicalInstant(command.IssuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expiresAt, err := parseCanonicalInstant(command.ExpiresAt)
	if err != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime || !sha256Pattern.MatchString(command.PayloadSHA256) {
		return newError("invalid_command", err)
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeDeploymentObserveRequestJSON(payload)
	if err != nil {
		return newError("invalid_payload", err)
	}
	if err := validateDeploymentObserveRequest(request); err != nil {
		return err
	}
	canonicalPayload, err := json.Marshal(request)
	if err != nil || !bytes.Equal(payload, canonicalPayload) {
		return newError("noncanonical_payload", err)
	}
	if requireSignature {
		signature, err := decodeCanonicalBase64(command.SignatureB64)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func validateDeploymentObserveRequest(request DeploymentObserveRequest) error {
	if !idPattern.MatchString(request.DeploymentID) {
		return newError("invalid_deployment_observe_request", nil)
	}
	return nil
}

func validateDeploymentObserveReceipt(command DeploymentObserveCommand, receipt DeploymentObserveReceipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") ||
		receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration ||
		receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID ||
		receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != DeploymentObserveAction {
		return newError("invalid_deployment_observe_receipt", nil)
	}
	return nil
}

func validateDeploymentObservation(command DeploymentObserveCommand, request DeploymentObserveRequest, observation DeploymentObservation) error {
	if observation.Schema != DeploymentObservationSchema || observation.DeploymentID != request.DeploymentID ||
		observation.Resource.Status != "provisioning" || !instanceIDPattern.MatchString(observation.Resource.InstanceID) ||
		!safeNonnegative(observation.Worker.LastSequence) {
		return newError("invalid_deployment_observation", nil)
	}
	observedAt, err := parseCanonicalInstant(observation.ObservedAt)
	if err != nil {
		return newError("invalid_deployment_observation", err)
	}
	issuedAt, _ := parseCanonicalInstant(command.IssuedAt)
	expiresAt, _ := parseCanonicalInstant(command.ExpiresAt)
	if observedAt.Before(issuedAt) || observedAt.After(expiresAt) {
		return newError("invalid_deployment_observation", nil)
	}
	leaseExpiresAt, hasLease, err := optionalCanonicalInstant(observation.Worker.LeaseExpiresAt)
	if err != nil {
		return newError("invalid_deployment_observation", err)
	}
	lastEventAt, hasLastEvent, err := optionalCanonicalInstant(observation.Worker.LastEventAt)
	if err != nil {
		return newError("invalid_deployment_observation", err)
	}
	switch observation.Worker.BootstrapSessionState {
	case "issued", "bound":
		if observation.Worker.LeaseEpoch != 0 || hasLease || observation.Worker.LastSequence != 0 || hasLastEvent {
			return newError("invalid_deployment_observation", nil)
		}
	case "active":
		if !safePositive(observation.Worker.LeaseEpoch) || !hasLease || !leaseExpiresAt.After(observedAt) ||
			(observation.Worker.LastSequence == 0 && hasLastEvent) ||
			(observation.Worker.LastSequence > 0 && (!hasLastEvent || lastEventAt.After(observedAt))) {
			return newError("invalid_deployment_observation", nil)
		}
	default:
		return newError("invalid_deployment_observation", nil)
	}
	return nil
}

func optionalCanonicalInstant(value *string) (time.Time, bool, error) {
	if value == nil {
		return time.Time{}, false, nil
	}
	parsed, err := parseCanonicalInstant(*value)
	if err != nil {
		return time.Time{}, false, err
	}
	return parsed, true, nil
}

func decodeDeploymentObserveRequestJSON(raw []byte) (DeploymentObserveRequest, error) {
	if _, err := exactJSONObject(raw, deploymentObserveRequestFields); err != nil {
		return DeploymentObserveRequest{}, err
	}
	var request DeploymentObserveRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return DeploymentObserveRequest{}, err
	}
	return request, nil
}

func decodeDeploymentObserveResultJSON(raw []byte) (DeploymentObserveResult, error) {
	if err := validateDeploymentObserveResultJSONShape(raw); err != nil {
		return DeploymentObserveResult{}, err
	}
	var result DeploymentObserveResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return DeploymentObserveResult{}, err
	}
	return result, nil
}

func validateDeploymentObserveResultJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, deploymentObserveResultFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(object["receipt"], deploymentObserveReceiptFields); err != nil {
		return err
	}
	observation, err := exactJSONObject(object["observation"], deploymentObservationFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(observation["resource"], deploymentObservationResourceFields); err != nil {
		return err
	}
	_, err = exactJSONObject(observation["worker"], deploymentObservationWorkerFields)
	return err
}
