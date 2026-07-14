package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"regexp"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	// DeploymentAction is the one billable broker mutation exposed to this
	// package. It is deliberately not a generic provider-action capability.
	DeploymentAction = "deployment.create"
	// DeploymentCreateSchema is the fixed, canonical payload schema accepted by
	// the user-owned Connection Stack.
	DeploymentCreateSchema = "dirextalk.aws.deployment-create/v1"
	// DeploymentReceiptSchema is private Broker-to-Orchestrator evidence. It
	// must never be projected through ProductCore or MCP.
	DeploymentReceiptSchema = "dirextalk.aws.deployment-receipt/v1"
)

var (
	amiIDPattern       = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	vpcIDPattern       = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	subnetIDPattern    = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	instanceIDPattern  = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	volumeIDPattern    = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	interfaceIDPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)

	deploymentCommandFields = []string{
		"schema",
		"connection_id",
		"command_id",
		"node_key_id",
		"issued_at",
		"expires_at",
		"expected_generation",
		"node_counter",
		"action",
		"payload_b64",
		"payload_sha256",
		"approval_proof",
		"signature_b64",
	}
	deploymentRequestFields = []string{
		"schema",
		"deployment_id",
		"connection_generation",
		"plan_hash",
		"plan_revision",
		"quote_id",
		"quote_digest",
		"candidate_id",
		"resource_manifest_digest",
		"worker_artifact",
		"network",
	}
	deploymentWorkerArtifactFields = []string{"kind", "ami_id"}
	deploymentNetworkFields        = []string{"vpc_id", "subnet_id", "availability_zone"}
	deploymentResultFields         = []string{"status", "receipt", "deployment"}
	deploymentCommandReceiptFields = []string{
		"schema",
		"disposition",
		"connection_id",
		"expected_generation",
		"node_counter",
		"command_id",
		"request_sha256",
		"action",
	}
	deploymentReceiptFields = []string{
		"schema",
		"connection_id",
		"deployment_id",
		"request_sha256",
		"resource_status",
		"instance_id",
		"volume_ids",
		"network_interface_ids",
	}
)

// DeploymentWorkerArtifact has no arbitrary image, user-data, bootstrap URL,
// SSH key, instance profile, or secret field. The Stack verifies this fixed
// AMI against its trusted worker artifact registration.
type DeploymentWorkerArtifact struct {
	Kind  string `json:"kind"`
	AMIID string `json:"ami_id"`
}

// DeploymentNetwork is a Stack-owned private placement reference. The closed
// type deliberately cannot request public ingress or mutate security groups.
type DeploymentNetwork struct {
	VPCID            string `json:"vpc_id"`
	SubnetID         string `json:"subnet_id"`
	AvailabilityZone string `json:"availability_zone"`
}

// DeploymentRequest is the exact canonical payload sent in deployment.create.
// Field declaration order is the Connection Stack canonical JSON order.
type DeploymentRequest struct {
	Schema                 string                   `json:"schema"`
	DeploymentID           string                   `json:"deployment_id"`
	ConnectionGeneration   int64                    `json:"connection_generation"`
	PlanHash               string                   `json:"plan_hash"`
	PlanRevision           uint64                   `json:"plan_revision"`
	QuoteID                string                   `json:"quote_id"`
	QuoteDigest            string                   `json:"quote_digest"`
	CandidateID            string                   `json:"candidate_id"`
	ResourceManifestDigest string                   `json:"resource_manifest_digest"`
	WorkerArtifact         DeploymentWorkerArtifact `json:"worker_artifact"`
	Network                DeploymentNetwork        `json:"network"`
}

// DeploymentCommandInput is all private data necessary to create one signed,
// approval-bound command. ApprovalProof is persisted only inside the sealed
// Broker envelope; callers must not put it in product projections or events.
type DeploymentCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentRequest
	ApprovalProof      cloudcontracts.ApprovalV1
	PrivateKey         ed25519.PrivateKey
}

// DeploymentCommand is an exact typed Connection Stack V2 envelope. It has
// no AWS credential, EC2 parameter passthrough, or generic approval surface.
// approval_proof is the existing Flutter-signed ApprovalV1 (deterministic
// CBOR), kept private in the durable signed command only.
type DeploymentCommand struct {
	Schema             string                    `json:"schema"`
	ConnectionID       string                    `json:"connection_id"`
	CommandID          string                    `json:"command_id"`
	NodeKeyID          string                    `json:"node_key_id"`
	IssuedAt           string                    `json:"issued_at"`
	ExpiresAt          string                    `json:"expires_at"`
	ExpectedGeneration int64                     `json:"expected_generation"`
	NodeCounter        int64                     `json:"node_counter"`
	Action             string                    `json:"action"`
	PayloadB64         string                    `json:"payload_b64"`
	PayloadSHA256      string                    `json:"payload_sha256"`
	ApprovalProof      cloudcontracts.ApprovalV1 `json:"approval_proof"`
	SignatureB64       string                    `json:"signature_b64"`
}

// DeploymentCommandBinding is the immutable identity a persisted create
// envelope must retain for every replay. It is private control-plane state.
type DeploymentCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentRequest
	ApprovalProof      cloudcontracts.ApprovalV1
}

// DeploymentCommandReceipt is the action-neutral durable Broker receipt.
type DeploymentCommandReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

// DeploymentReceipt holds the private EC2 resource identity resulting from
// an accepted launch. It intentionally contains no bootstrap material,
// user-data, service credential, Worker log, or public URL.
type DeploymentReceipt struct {
	Schema              string   `json:"schema"`
	ConnectionID        string   `json:"connection_id"`
	DeploymentID        string   `json:"deployment_id"`
	RequestSHA256       string   `json:"request_sha256"`
	ResourceStatus      string   `json:"resource_status"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
}

// DeploymentResult is the sole successful deployment.create response shape.
type DeploymentResult struct {
	Status     string                   `json:"status"`
	Receipt    DeploymentCommandReceipt `json:"receipt"`
	Deployment DeploymentReceipt        `json:"deployment"`
}

// NewDeploymentCommand creates a canonical, private approval-bound command.
// It performs no network call and never retains the node signing key.
func NewDeploymentCommand(input DeploymentCommandInput) (DeploymentCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return DeploymentCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateDeploymentRequest(input.Request); err != nil {
		return DeploymentCommand{}, err
	}
	issuedAt := canonicalInstant(input.IssuedAt)
	expiresAt := canonicalInstant(input.ExpiresAt)
	if err := validateDeploymentApprovalProof(input.ApprovalProof, input.Request, input.ConnectionID, issuedAt, expiresAt, true); err != nil {
		return DeploymentCommand{}, err
	}
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return DeploymentCommand{}, newError("invalid_deployment_request", err)
	}
	command := DeploymentCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           issuedAt,
		ExpiresAt:          expiresAt,
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             DeploymentAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
		ApprovalProof:      input.ApprovalProof,
	}
	if err := validateDeploymentCommand(command, false); err != nil {
		return DeploymentCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return DeploymentCommand{}, err
	}
	return command, nil
}

// Validate permits a persisted command to be replayed after expiry so the
// Stack can return its durable idempotent receipt. Only the Stack decides
// whether an expired command can issue a new cloud mutation.
func (command DeploymentCommand) Validate() error {
	return validateDeploymentCommand(command, true)
}

// ValidateBinding proves a persisted envelope is byte-for-byte bound to the
// original approved deployment request and device approval proof before it is
// allowed to make an HTTP retry.
func (command DeploymentCommand) ValidateBinding(binding DeploymentCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if binding.IssuedAt.IsZero() || binding.ExpiresAt.IsZero() || !binding.ExpiresAt.After(binding.IssuedAt) ||
		command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID ||
		command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter ||
		command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateDeploymentRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.DeploymentRequest()
	if err != nil {
		return err
	}
	if request != binding.Request || !reflect.DeepEqual(command.ApprovalProof, binding.ApprovalProof) {
		return newError("invalid_deployment_request", nil)
	}
	return nil
}

// VerifySignature verifies the node signature against an explicit registered
// public key. It does not re-verify the device key; the Connection Stack owns
// that check immediately before its one-time approval consumption.
func (command DeploymentCommand) VerifySignature(publicKey ed25519.PublicKey) error {
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

// ApprovalProofPayloadSHA256 is the exact deterministic-CBOR digest of the
// existing ApprovalV1 signing payload. The node signature covers it, so a
// proxy cannot substitute a different valid device proof after signing.
func (command DeploymentCommand) ApprovalProofPayloadSHA256() (string, error) {
	payload, err := command.ApprovalProof.SigningPayload()
	if err != nil {
		return "", newError("invalid_approval_proof", err)
	}
	return sha256Hex(payload), nil
}

// SignatureBase mirrors the fixed Connection Stack V2 deployment command
// verifier. Existing v2 approval_binding/approval fields are deliberately
// absent: deployment.create carries exactly one ApprovalV1 proof instead.
func (command DeploymentCommand) SignatureBase() string {
	proofDigest, err := command.ApprovalProofPayloadSHA256()
	if err != nil {
		return ""
	}
	return nodeSignatureBase(nodeSignatureFields{
		Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID,
		NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		Action: command.Action, PayloadSHA256: command.PayloadSHA256,
		ApprovalProofPayloadSHA256: proofDigest,
	})
}

// RequestSHA256 is the durable idempotency identity derived from the signed
// command base, not the outer HTTP JSON bytes.
func (command DeploymentCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// DeploymentRequest returns the exact canonical payload bound into command.
func (command DeploymentCommand) DeploymentRequest() (DeploymentRequest, error) {
	if err := command.Validate(); err != nil {
		return DeploymentRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeDeploymentRequestJSON(payload)
	if err != nil {
		return DeploymentRequest{}, newError("invalid_payload", err)
	}
	if err := validateDeploymentRequest(request); err != nil {
		return DeploymentRequest{}, err
	}
	return request, nil
}

// ParseDeploymentCommand strictly parses an exact persisted envelope before a
// retry. It rejects legacy v2 approval_binding/approval fields, unknown keys,
// duplicate keys and payload shape drift before any network I/O.
func ParseDeploymentCommand(raw []byte) (DeploymentCommand, error) {
	if _, err := exactJSONObject(raw, deploymentCommandFields); err != nil {
		return DeploymentCommand{}, newError("invalid_command", err)
	}
	var command DeploymentCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return DeploymentCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return DeploymentCommand{}, err
	}
	return command, nil
}

// ParseApprovalProof parses the existing private ApprovalV1 JSON without
// logging or reserializing its signature. The caller supplies this only from
// the durable approved-plan record while creating the signed Broker envelope.
func ParseApprovalProof(raw []byte) (cloudcontracts.ApprovalV1, error) {
	if len(raw) == 0 || len(raw) > maxPayloadBytes {
		return cloudcontracts.ApprovalV1{}, newError("invalid_approval_proof", nil)
	}
	var proof cloudcontracts.ApprovalV1
	if err := decodeStrictJSON(raw, &proof); err != nil {
		return cloudcontracts.ApprovalV1{}, newError("invalid_approval_proof", err)
	}
	if err := proof.Validate(); err != nil || proof.Signature == "" {
		return cloudcontracts.ApprovalV1{}, newError("invalid_approval_proof", err)
	}
	return proof, nil
}

// ValidateDeploymentResult validates all private Broker receipt fields before
// an Orchestrator store may mark a Deployment as provisioning.
func ValidateDeploymentResult(command DeploymentCommand, result DeploymentResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "deployment_created" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	if err := validateDeploymentCommandReceipt(command, result.Receipt); err != nil {
		return err
	}
	if result.Status == "deployment_created" && result.Receipt.Disposition != "committed" {
		return newError("invalid_deployment_receipt", nil)
	}
	if result.Status == "idempotent" && result.Receipt.Disposition != "idempotent" {
		return newError("invalid_deployment_receipt", nil)
	}
	request, err := command.DeploymentRequest()
	if err != nil {
		return err
	}
	return validateDeploymentReceipt(command, request, result.Deployment)
}

func validateDeploymentCommand(command DeploymentCommand, requireSignature bool) error {
	if command.Schema != CommandSchema || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) || !keyIDPattern.MatchString(command.NodeKeyID) || command.Action != DeploymentAction {
		return newError("invalid_command", nil)
	}
	if !safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, err := parseCanonicalInstant(command.IssuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expiresAt, err := parseCanonicalInstant(command.ExpiresAt)
	if err != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime {
		return newError("invalid_command", err)
	}
	if !sha256Pattern.MatchString(command.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeDeploymentRequestJSON(payload)
	if err != nil {
		return newError("invalid_payload", err)
	}
	if err := validateDeploymentRequest(request); err != nil {
		return err
	}
	if request.ConnectionGeneration != command.ExpectedGeneration {
		return newError("invalid_deployment_request", nil)
	}
	canonicalPayload, err := json.Marshal(request)
	if err != nil || !bytes.Equal(payload, canonicalPayload) {
		return newError("noncanonical_payload", err)
	}
	if err := validateDeploymentApprovalProof(command.ApprovalProof, request, command.ConnectionID, command.IssuedAt, command.ExpiresAt, false); err != nil {
		return err
	}
	if requireSignature {
		signature, err := decodeCanonicalBase64(command.SignatureB64)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func validateDeploymentRequest(request DeploymentRequest) error {
	if request.Schema != DeploymentCreateSchema || !idPattern.MatchString(request.DeploymentID) || !safePositive(request.ConnectionGeneration) ||
		!namedSHA256Pattern.MatchString(request.PlanHash) || request.PlanRevision == 0 || request.PlanRevision > uint64(maxSafeInteger) ||
		!idPattern.MatchString(request.QuoteID) || !namedSHA256Pattern.MatchString(request.QuoteDigest) || !idPattern.MatchString(request.CandidateID) ||
		!namedSHA256Pattern.MatchString(request.ResourceManifestDigest) || request.WorkerArtifact.Kind != "fixed_ami" ||
		!amiIDPattern.MatchString(request.WorkerArtifact.AMIID) || !vpcIDPattern.MatchString(request.Network.VPCID) ||
		!subnetIDPattern.MatchString(request.Network.SubnetID) || !availabilityZonePattern.MatchString(request.Network.AvailabilityZone) {
		return newError("invalid_deployment_request", nil)
	}
	return nil
}

func validateDeploymentApprovalProof(proof cloudcontracts.ApprovalV1, request DeploymentRequest, connectionID, issuedAt, expiresAt string, requireUnexpired bool) error {
	if err := proof.Validate(); err != nil || proof.Signature == "" {
		return newError("invalid_approval_proof", err)
	}
	issued, err := parseCanonicalInstant(issuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expires, err := parseCanonicalInstant(expiresAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	if !proof.ExpiresAt.After(issued) || proof.ExpiresAt.Before(expires) ||
		proof.CloudConnectionID != connectionID || proof.PlanHash != request.PlanHash || proof.PlanRevision != request.PlanRevision ||
		proof.QuoteID != request.QuoteID || proof.QuoteDigest != request.QuoteDigest {
		return newError("approval_proof_mismatch", nil)
	}
	if requireUnexpired && !proof.ExpiresAt.After(issued) {
		return newError("approval_expired", nil)
	}
	return nil
}

func validateDeploymentCommandReceipt(command DeploymentCommand, receipt DeploymentCommandReceipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") ||
		receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration || receipt.NodeCounter != command.NodeCounter ||
		receipt.CommandID != command.CommandID || receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != DeploymentAction {
		return newError("invalid_deployment_receipt", nil)
	}
	return nil
}

func validateDeploymentReceipt(command DeploymentCommand, request DeploymentRequest, receipt DeploymentReceipt) error {
	if receipt.Schema != DeploymentReceiptSchema || receipt.ConnectionID != command.ConnectionID || receipt.DeploymentID != request.DeploymentID ||
		receipt.RequestSHA256 != command.RequestSHA256() || receipt.ResourceStatus != "provisioning" || !instanceIDPattern.MatchString(receipt.InstanceID) ||
		!canonicalStrings(receipt.VolumeIDs, volumeIDPattern, true) || !canonicalStrings(receipt.NetworkInterfaceIDs, interfaceIDPattern, true) {
		return newError("invalid_deployment_receipt", nil)
	}
	return nil
}

func decodeDeploymentRequestJSON(raw []byte) (DeploymentRequest, error) {
	if err := validateDeploymentRequestJSONShape(raw); err != nil {
		return DeploymentRequest{}, err
	}
	var request DeploymentRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return DeploymentRequest{}, err
	}
	return request, nil
}

func decodeDeploymentResultJSON(raw []byte) (DeploymentResult, error) {
	if err := validateDeploymentResultJSONShape(raw); err != nil {
		return DeploymentResult{}, err
	}
	var result DeploymentResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return DeploymentResult{}, err
	}
	return result, nil
}

func validateDeploymentRequestJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, deploymentRequestFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(object["worker_artifact"], deploymentWorkerArtifactFields); err != nil {
		return err
	}
	_, err = exactJSONObject(object["network"], deploymentNetworkFields)
	return err
}

func validateDeploymentResultJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, deploymentResultFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(object["receipt"], deploymentCommandReceiptFields); err != nil {
		return err
	}
	deployment, err := exactJSONObject(object["deployment"], deploymentReceiptFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONArray(deployment["volume_ids"]); err != nil {
		return err
	}
	_, err = exactJSONArray(deployment["network_interface_ids"])
	return err
}
