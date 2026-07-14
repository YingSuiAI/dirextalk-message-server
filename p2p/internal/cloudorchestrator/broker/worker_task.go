package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"time"
)

const (
	// WorkerTaskIssueAction creates or returns one exact, predeclared Worker
	// task. It cannot carry shell, image, network, secret, or AWS API input.
	WorkerTaskIssueAction = "worker.task.issue"
	// WorkerTaskObserveAction reads the current de-secreted summary for one
	// exact Worker task. It has no mutation capability.
	WorkerTaskObserveAction = "worker.task.observe"
	// WorkerTaskIssueSchema is the only issue payload accepted by the
	// Connection Stack. A task kind is deliberately fixed in this first slice.
	WorkerTaskIssueSchema = "dirextalk.worker-task-issue/v1"
	// WorkerTaskKindExecutionProbe is the sole task kind allowed before the
	// durable Worker executor and Recipe task vocabulary are introduced.
	WorkerTaskKindExecutionProbe = "execution_probe"
	// WorkerTaskExecutionProbeReceivedCheckpoint is the only nonterminal
	// execution_probe evidence accepted from an untrusted Worker transport.
	WorkerTaskExecutionProbeReceivedCheckpoint = "execution_manifest_received"
	// WorkerTaskExecutionProbeVerifiedCheckpoint is the only terminal success
	// evidence for the fixed transport probe. It is not service readiness.
	WorkerTaskExecutionProbeVerifiedCheckpoint = "task_transport_verified"
)

var (
	workerTaskIssueRequestFields = []string{
		"schema",
		"deployment_id",
		"task_id",
		"task_kind",
		"execution_manifest_digest",
		"input_digest",
	}
	workerTaskObserveRequestFields = []string{"deployment_id", "task_id"}
	workerTaskIssueResultFields    = []string{"status", "receipt", "task"}
	workerTaskObserveResultFields  = []string{"status", "receipt", "task"}
	workerTaskReceiptFields        = []string{
		"schema",
		"disposition",
		"connection_id",
		"expected_generation",
		"node_counter",
		"command_id",
		"request_sha256",
		"action",
	}
	workerTaskSummaryFields = []string{
		"task_id",
		"deployment_id",
		"status",
		"attempt",
		"last_sequence",
		"checkpoint",
		"error_code",
		"evidence_digest",
		"updated_at",
	}
	workerTaskCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)
)

// WorkerTaskIssueRequest is the canonical, non-secret intent for the only
// Worker command enabled by this stage. Digest references are opaque and
// bind previously sealed execution/input artifacts; neither contents nor
// credentials enter the Broker envelope.
type WorkerTaskIssueRequest struct {
	Schema                  string `json:"schema"`
	DeploymentID            string `json:"deployment_id"`
	TaskID                  string `json:"task_id"`
	TaskKind                string `json:"task_kind"`
	ExecutionManifestDigest string `json:"execution_manifest_digest"`
	InputDigest             string `json:"input_digest"`
}

// WorkerTaskObserveRequest identifies one already-issued task. It has no
// task execution, retry, cancellation, artifact, or secret control surface.
type WorkerTaskObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}

// WorkerTaskIssueCommandInput creates one durable, signed issue envelope.
// PrivateKey is never retained by the returned command or sent over HTTP.
type WorkerTaskIssueCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            WorkerTaskIssueRequest
	PrivateKey         ed25519.PrivateKey
}

// WorkerTaskObserveCommandInput creates one durable, signed observation
// envelope. PrivateKey is never retained by the returned command or sent over
// HTTP.
type WorkerTaskObserveCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            WorkerTaskObserveRequest
	PrivateKey         ed25519.PrivateKey
}

// WorkerTaskIssueCommand is the exact, approval-free Connection Stack V2
// envelope for worker.task.issue. Its signature base still includes the four
// empty approval digest lines required by the common Stack verifier.
type WorkerTaskIssueCommand struct {
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

// WorkerTaskObserveCommand is the exact, approval-free Connection Stack V2
// envelope for worker.task.observe.
type WorkerTaskObserveCommand struct {
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

// WorkerTaskIssueCommandBinding is the immutable logical identity a
// persisted issue command must retain before an HTTP retry.
type WorkerTaskIssueCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            WorkerTaskIssueRequest
}

// WorkerTaskObserveCommandBinding is the immutable logical identity a
// persisted observation command must retain before an HTTP retry.
type WorkerTaskObserveCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            WorkerTaskObserveRequest
}

// WorkerTaskReceipt is the action-neutral durable receipt returned by the
// Connection Stack for both issue and observe commands.
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

// WorkerTaskSummary is the only Worker task state accepted at this Broker
// boundary. It deliberately excludes command payloads, task documents,
// output/log data, URLs, bearer material, Worker identity, and raw evidence.
// Nil optional values are encoded as JSON null.
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

// WorkerTaskIssueResult is the sole successful worker.task.issue response.
type WorkerTaskIssueResult struct {
	Status  string            `json:"status"`
	Receipt WorkerTaskReceipt `json:"receipt"`
	Task    WorkerTaskSummary `json:"task"`
}

// WorkerTaskObserveResult is the sole successful worker.task.observe
// response. A replayed command receipt may still carry a newer task summary.
type WorkerTaskObserveResult struct {
	Status  string            `json:"status"`
	Receipt WorkerTaskReceipt `json:"receipt"`
	Task    WorkerTaskSummary `json:"task"`
}

// NewWorkerTaskIssueCommand creates one canonical worker.task.issue command.
// It performs no network I/O and never retains the signing key.
func NewWorkerTaskIssueCommand(input WorkerTaskIssueCommandInput) (WorkerTaskIssueCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return WorkerTaskIssueCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateWorkerTaskIssueRequest(input.Request); err != nil {
		return WorkerTaskIssueCommand{}, err
	}
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return WorkerTaskIssueCommand{}, newError("invalid_worker_task_issue_request", err)
	}
	command := WorkerTaskIssueCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           canonicalInstant(input.IssuedAt),
		ExpiresAt:          canonicalInstant(input.ExpiresAt),
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             WorkerTaskIssueAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
	}
	if err := validateWorkerTaskIssueCommand(command, false); err != nil {
		return WorkerTaskIssueCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return WorkerTaskIssueCommand{}, err
	}
	return command, nil
}

// NewWorkerTaskObserveCommand creates one canonical worker.task.observe
// command. It performs no network I/O and never retains the signing key.
func NewWorkerTaskObserveCommand(input WorkerTaskObserveCommandInput) (WorkerTaskObserveCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return WorkerTaskObserveCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateWorkerTaskObserveRequest(input.Request); err != nil {
		return WorkerTaskObserveCommand{}, err
	}
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return WorkerTaskObserveCommand{}, newError("invalid_worker_task_observe_request", err)
	}
	command := WorkerTaskObserveCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           canonicalInstant(input.IssuedAt),
		ExpiresAt:          canonicalInstant(input.ExpiresAt),
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             WorkerTaskObserveAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
	}
	if err := validateWorkerTaskObserveCommand(command, false); err != nil {
		return WorkerTaskObserveCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return WorkerTaskObserveCommand{}, err
	}
	return command, nil
}

// Validate permits an existing issue envelope to be replayed after expiry so
// the Stack can return its durable receipt. The Stack alone can issue work.
func (command WorkerTaskIssueCommand) Validate() error {
	return validateWorkerTaskIssueCommand(command, true)
}

// Validate permits an existing observation envelope to be replayed after
// expiry so the Stack can return its durable receipt.
func (command WorkerTaskObserveCommand) Validate() error {
	return validateWorkerTaskObserveCommand(command, true)
}

// ValidateBinding proves a persisted issue envelope is unchanged before retry.
func (command WorkerTaskIssueCommand) ValidateBinding(binding WorkerTaskIssueCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if !sameWorkerTaskCommandBinding(command.ConnectionID, command.CommandID, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, command.IssuedAt, command.ExpiresAt,
		binding.ConnectionID, binding.CommandID, binding.NodeKeyID, binding.ExpectedGeneration, binding.NodeCounter, binding.IssuedAt, binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateWorkerTaskIssueRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.WorkerTaskIssueRequest()
	if err != nil {
		return err
	}
	if request != binding.Request {
		return newError("invalid_worker_task_issue_request", nil)
	}
	return nil
}

// ValidateBinding proves a persisted observation envelope is unchanged before
// retry.
func (command WorkerTaskObserveCommand) ValidateBinding(binding WorkerTaskObserveCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if !sameWorkerTaskCommandBinding(command.ConnectionID, command.CommandID, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, command.IssuedAt, command.ExpiresAt,
		binding.ConnectionID, binding.CommandID, binding.NodeKeyID, binding.ExpectedGeneration, binding.NodeCounter, binding.IssuedAt, binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateWorkerTaskObserveRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.WorkerTaskObserveRequest()
	if err != nil {
		return err
	}
	if request != binding.Request {
		return newError("invalid_worker_task_observe_request", nil)
	}
	return nil
}

// VerifySignature verifies an issue envelope against the registered node key.
func (command WorkerTaskIssueCommand) VerifySignature(publicKey ed25519.PublicKey) error {
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

// VerifySignature verifies an observation envelope against the registered
// node key.
func (command WorkerTaskObserveCommand) VerifySignature(publicKey ed25519.PublicKey) error {
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

// SignatureBase mirrors Connection Stack V2 buildNodeSignatureBase. Read-only
// and non-approval commands carry empty (not omitted) approval digest lines.
func (command WorkerTaskIssueCommand) SignatureBase() string {
	return workerTaskSignatureBase(
		command.Schema, command.ConnectionID, command.CommandID, command.NodeKeyID,
		command.IssuedAt, command.ExpiresAt, command.ExpectedGeneration, command.NodeCounter,
		command.Action, command.PayloadSHA256,
	)
}

// SignatureBase mirrors Connection Stack V2 buildNodeSignatureBase.
func (command WorkerTaskObserveCommand) SignatureBase() string {
	return workerTaskSignatureBase(
		command.Schema, command.ConnectionID, command.CommandID, command.NodeKeyID,
		command.IssuedAt, command.ExpiresAt, command.ExpectedGeneration, command.NodeCounter,
		command.Action, command.PayloadSHA256,
	)
}

// RequestSHA256 is the durable idempotency identity, based on the exact
// signature base rather than mutable outer HTTP JSON serialization.
func (command WorkerTaskIssueCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// RequestSHA256 is the durable idempotency identity for observation commands.
func (command WorkerTaskObserveCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// WorkerTaskIssueRequest decodes the exact canonical issue payload bound into
// an envelope.
func (command WorkerTaskIssueCommand) WorkerTaskIssueRequest() (WorkerTaskIssueRequest, error) {
	if err := command.Validate(); err != nil {
		return WorkerTaskIssueRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeWorkerTaskIssueRequestJSON(payload)
	if err != nil {
		return WorkerTaskIssueRequest{}, newError("invalid_payload", err)
	}
	if err := validateWorkerTaskIssueRequest(request); err != nil {
		return WorkerTaskIssueRequest{}, err
	}
	return request, nil
}

// WorkerTaskObserveRequest decodes the exact canonical observation payload
// bound into an envelope.
func (command WorkerTaskObserveCommand) WorkerTaskObserveRequest() (WorkerTaskObserveRequest, error) {
	if err := command.Validate(); err != nil {
		return WorkerTaskObserveRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeWorkerTaskObserveRequestJSON(payload)
	if err != nil {
		return WorkerTaskObserveRequest{}, newError("invalid_payload", err)
	}
	if err := validateWorkerTaskObserveRequest(request); err != nil {
		return WorkerTaskObserveRequest{}, err
	}
	return request, nil
}

// ParseWorkerTaskIssueCommand strictly parses a persisted issue envelope
// before retrying it. Unknown, duplicate, approval, or action-drift fields are
// rejected before any request reaches the Connection Stack.
func ParseWorkerTaskIssueCommand(raw []byte) (WorkerTaskIssueCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return WorkerTaskIssueCommand{}, newError("invalid_command", err)
	}
	var command WorkerTaskIssueCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return WorkerTaskIssueCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return WorkerTaskIssueCommand{}, err
	}
	return command, nil
}

// ParseWorkerTaskObserveCommand strictly parses a persisted observation
// envelope before retrying it.
func ParseWorkerTaskObserveCommand(raw []byte) (WorkerTaskObserveCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return WorkerTaskObserveCommand{}, newError("invalid_command", err)
	}
	var command WorkerTaskObserveCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return WorkerTaskObserveCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return WorkerTaskObserveCommand{}, err
	}
	return command, nil
}

// ValidateWorkerTaskIssueResult validates exact receipt/request binding and a
// de-secreted Stack task summary before durable state may change.
func ValidateWorkerTaskIssueResult(command WorkerTaskIssueCommand, result WorkerTaskIssueResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "worker_task_issued" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	if err := validateWorkerTaskReceipt(command.ConnectionID, command.CommandID, command.ExpectedGeneration, command.NodeCounter, command.RequestSHA256(), WorkerTaskIssueAction, result.Receipt); err != nil {
		return err
	}
	if (result.Status == "worker_task_issued" && result.Receipt.Disposition != "committed") ||
		(result.Status == "idempotent" && result.Receipt.Disposition != "idempotent") {
		return newError("invalid_worker_task_receipt", nil)
	}
	request, err := command.WorkerTaskIssueRequest()
	if err != nil {
		return err
	}
	if err := validateWorkerTaskSummary(request.DeploymentID, request.TaskID, result.Task); err != nil {
		return err
	}
	if (result.Task.Status == "running" || result.Task.Status == "succeeded") &&
		(result.Task.EvidenceDigest == nil || *result.Task.EvidenceDigest != request.ExecutionManifestDigest) {
		return newError("invalid_worker_task_summary", nil)
	}
	return nil
}

// ValidateWorkerTaskObserveResult validates exact receipt/request binding and
// a de-secreted Stack task summary before durable state may change.
func ValidateWorkerTaskObserveResult(command WorkerTaskObserveCommand, result WorkerTaskObserveResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "worker_task_observed" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	if err := validateWorkerTaskReceipt(command.ConnectionID, command.CommandID, command.ExpectedGeneration, command.NodeCounter, command.RequestSHA256(), WorkerTaskObserveAction, result.Receipt); err != nil {
		return err
	}
	if (result.Status == "worker_task_observed" && result.Receipt.Disposition != "committed") ||
		(result.Status == "idempotent" && result.Receipt.Disposition != "idempotent") {
		return newError("invalid_worker_task_receipt", nil)
	}
	request, err := command.WorkerTaskObserveRequest()
	if err != nil {
		return err
	}
	return validateWorkerTaskSummary(request.DeploymentID, request.TaskID, result.Task)
}

func validateWorkerTaskIssueCommand(command WorkerTaskIssueCommand, requireSignature bool) error {
	if err := validateWorkerTaskEnvelope(
		command.Schema, command.ConnectionID, command.CommandID, command.NodeKeyID, command.IssuedAt, command.ExpiresAt,
		command.ExpectedGeneration, command.NodeCounter, command.Action, command.PayloadB64, command.PayloadSHA256, command.SignatureB64,
		WorkerTaskIssueAction, canonicalWorkerTaskIssuePayload, requireSignature,
	); err != nil {
		return err
	}
	return nil
}

func validateWorkerTaskObserveCommand(command WorkerTaskObserveCommand, requireSignature bool) error {
	if err := validateWorkerTaskEnvelope(
		command.Schema, command.ConnectionID, command.CommandID, command.NodeKeyID, command.IssuedAt, command.ExpiresAt,
		command.ExpectedGeneration, command.NodeCounter, command.Action, command.PayloadB64, command.PayloadSHA256, command.SignatureB64,
		WorkerTaskObserveAction, canonicalWorkerTaskObservePayload, requireSignature,
	); err != nil {
		return err
	}
	return nil
}

func validateWorkerTaskEnvelope(
	schema, connectionID, commandID, nodeKeyID, issuedAtValue, expiresAtValue string,
	expectedGeneration, nodeCounter int64,
	action, payloadB64, payloadSHA256, signatureB64, expectedAction string,
	canonicalizePayload func([]byte) ([]byte, error),
	requireSignature bool,
) error {
	if schema != CommandSchema || !idPattern.MatchString(connectionID) || !idPattern.MatchString(commandID) ||
		!keyIDPattern.MatchString(nodeKeyID) || action != expectedAction || !safePositive(expectedGeneration) || !safeNonnegative(nodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, err := parseCanonicalInstant(issuedAtValue)
	if err != nil {
		return newError("invalid_command", err)
	}
	expiresAt, err := parseCanonicalInstant(expiresAtValue)
	if err != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime || !sha256Pattern.MatchString(payloadSHA256) {
		return newError("invalid_command", err)
	}
	payload, err := decodeCanonicalBase64(payloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != payloadSHA256 {
		return newError("invalid_payload", err)
	}
	canonicalPayload, err := canonicalizePayload(payload)
	if err != nil {
		if _, isBrokerError := err.(*Error); isBrokerError {
			return err
		}
		return newError("invalid_payload", err)
	}
	if !bytes.Equal(payload, canonicalPayload) {
		return newError("noncanonical_payload", err)
	}
	if requireSignature {
		signature, err := decodeCanonicalBase64(signatureB64)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func canonicalWorkerTaskIssuePayload(payload []byte) ([]byte, error) {
	request, err := decodeWorkerTaskIssueRequestJSON(payload)
	if err != nil {
		return nil, err
	}
	if err := validateWorkerTaskIssueRequest(request); err != nil {
		return nil, err
	}
	return json.Marshal(request)
}

func canonicalWorkerTaskObservePayload(payload []byte) ([]byte, error) {
	request, err := decodeWorkerTaskObserveRequestJSON(payload)
	if err != nil {
		return nil, err
	}
	if err := validateWorkerTaskObserveRequest(request); err != nil {
		return nil, err
	}
	return json.Marshal(request)
}

func validateWorkerTaskIssueRequest(request WorkerTaskIssueRequest) error {
	if request.Schema != WorkerTaskIssueSchema || !idPattern.MatchString(request.DeploymentID) || !idPattern.MatchString(request.TaskID) ||
		request.TaskKind != WorkerTaskKindExecutionProbe || !namedSHA256Pattern.MatchString(request.ExecutionManifestDigest) || !namedSHA256Pattern.MatchString(request.InputDigest) {
		return newError("invalid_worker_task_issue_request", nil)
	}
	return nil
}

func validateWorkerTaskObserveRequest(request WorkerTaskObserveRequest) error {
	if !idPattern.MatchString(request.DeploymentID) || !idPattern.MatchString(request.TaskID) {
		return newError("invalid_worker_task_observe_request", nil)
	}
	return nil
}

func validateWorkerTaskReceipt(connectionID, commandID string, expectedGeneration, nodeCounter int64, requestSHA256, action string, receipt WorkerTaskReceipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") ||
		receipt.ConnectionID != connectionID || receipt.ExpectedGeneration != expectedGeneration || receipt.NodeCounter != nodeCounter ||
		receipt.CommandID != commandID || receipt.RequestSHA256 != requestSHA256 || receipt.Action != action {
		return newError("invalid_worker_task_receipt", nil)
	}
	return nil
}

func validateWorkerTaskSummary(deploymentID, taskID string, summary WorkerTaskSummary) error {
	if summary.DeploymentID != deploymentID || summary.TaskID != taskID || !safePositive(summary.Attempt) || !safeNonnegative(summary.LastSequence) {
		return newError("invalid_worker_task_summary", nil)
	}
	if _, err := parseCanonicalInstant(summary.UpdatedAt); err != nil {
		return newError("invalid_worker_task_summary", err)
	}
	if !optionalWorkerTaskCode(summary.Checkpoint) || !optionalWorkerTaskCode(summary.ErrorCode) || !optionalNamedSHA256(summary.EvidenceDigest) {
		return newError("invalid_worker_task_summary", nil)
	}
	switch summary.Status {
	case "queued":
		if summary.Attempt != 1 || summary.LastSequence != 0 || summary.Checkpoint != nil || summary.ErrorCode != nil || summary.EvidenceDigest != nil {
			return newError("invalid_worker_task_summary", nil)
		}
	case "running":
		if summary.LastSequence < 1 || summary.Checkpoint == nil || *summary.Checkpoint != WorkerTaskExecutionProbeReceivedCheckpoint ||
			summary.ErrorCode != nil || summary.EvidenceDigest == nil {
			return newError("invalid_worker_task_summary", nil)
		}
	case "succeeded":
		if summary.LastSequence < 1 || summary.Checkpoint == nil || *summary.Checkpoint != WorkerTaskExecutionProbeVerifiedCheckpoint ||
			summary.ErrorCode != nil || summary.EvidenceDigest == nil {
			return newError("invalid_worker_task_summary", nil)
		}
	case "failed", "interrupted":
		if summary.LastSequence < 1 || summary.Checkpoint != nil || summary.ErrorCode == nil || summary.EvidenceDigest != nil {
			return newError("invalid_worker_task_summary", nil)
		}
	default:
		return newError("invalid_worker_task_summary", nil)
	}
	return nil
}

func optionalWorkerTaskCode(value *string) bool {
	return value == nil || workerTaskCodePattern.MatchString(*value)
}

func optionalNamedSHA256(value *string) bool {
	return value == nil || namedSHA256Pattern.MatchString(*value)
}

func sameWorkerTaskCommandBinding(
	connectionID, commandID, nodeKeyID string,
	expectedGeneration, nodeCounter int64,
	issuedAtValue, expiresAtValue string,
	bindingConnectionID, bindingCommandID, bindingNodeKeyID string,
	bindingExpectedGeneration, bindingNodeCounter int64,
	bindingIssuedAt, bindingExpiresAt time.Time,
) bool {
	return !bindingIssuedAt.IsZero() && !bindingExpiresAt.IsZero() && bindingExpiresAt.After(bindingIssuedAt) &&
		connectionID == bindingConnectionID && commandID == bindingCommandID && nodeKeyID == bindingNodeKeyID &&
		expectedGeneration == bindingExpectedGeneration && nodeCounter == bindingNodeCounter &&
		issuedAtValue == canonicalInstant(bindingIssuedAt) && expiresAtValue == canonicalInstant(bindingExpiresAt)
}

func workerTaskSignatureBase(
	schema, connectionID, commandID, nodeKeyID, issuedAt, expiresAt string,
	expectedGeneration, nodeCounter int64,
	action, payloadSHA256 string,
) string {
	return nodeSignatureBase(nodeSignatureFields{
		Schema: schema, ConnectionID: connectionID, CommandID: commandID, NodeKeyID: nodeKeyID,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, ExpectedGeneration: expectedGeneration, NodeCounter: nodeCounter,
		Action: action, PayloadSHA256: payloadSHA256,
	})
}

func decodeWorkerTaskIssueRequestJSON(raw []byte) (WorkerTaskIssueRequest, error) {
	if _, err := exactJSONObject(raw, workerTaskIssueRequestFields); err != nil {
		return WorkerTaskIssueRequest{}, err
	}
	var request WorkerTaskIssueRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return WorkerTaskIssueRequest{}, err
	}
	return request, nil
}

func decodeWorkerTaskObserveRequestJSON(raw []byte) (WorkerTaskObserveRequest, error) {
	if _, err := exactJSONObject(raw, workerTaskObserveRequestFields); err != nil {
		return WorkerTaskObserveRequest{}, err
	}
	var request WorkerTaskObserveRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return WorkerTaskObserveRequest{}, err
	}
	return request, nil
}

func decodeWorkerTaskIssueResultJSON(raw []byte) (WorkerTaskIssueResult, error) {
	if err := validateWorkerTaskResultJSONShape(raw, workerTaskIssueResultFields); err != nil {
		return WorkerTaskIssueResult{}, err
	}
	var result WorkerTaskIssueResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return WorkerTaskIssueResult{}, err
	}
	return result, nil
}

func decodeWorkerTaskObserveResultJSON(raw []byte) (WorkerTaskObserveResult, error) {
	if err := validateWorkerTaskResultJSONShape(raw, workerTaskObserveResultFields); err != nil {
		return WorkerTaskObserveResult{}, err
	}
	var result WorkerTaskObserveResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return WorkerTaskObserveResult{}, err
	}
	return result, nil
}

func validateWorkerTaskResultJSONShape(raw []byte, resultFields []string) error {
	object, err := exactJSONObject(raw, resultFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(object["receipt"], workerTaskReceiptFields); err != nil {
		return err
	}
	_, err = exactJSONObject(object["task"], workerTaskSummaryFields)
	return err
}
