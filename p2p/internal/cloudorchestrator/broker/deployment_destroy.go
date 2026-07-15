package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"sort"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	DeploymentDestroyAction       = "deployment.destroy"
	DeploymentDestroySchema       = "dirextalk.aws.deployment-destroy/v1"
	DeploymentDestroyResultSchema = "dirextalk.aws.deployment-destroy-result/v1"
)

var (
	destroyCommandFields  = []string{"schema", "connection_id", "command_id", "node_key_id", "issued_at", "expires_at", "expected_generation", "node_counter", "action", "payload_b64", "payload_sha256", "approval_proof", "signature_b64"}
	destroyRequestFields  = []string{"schema", "service_id", "deployment_id", "instance_id", "volume_ids", "network_interface_ids"}
	destroyResultFields   = []string{"schema", "status", "receipt", "deployment"}
	destroyEvidenceFields = []string{"deployment_id", "instance_id", "volume_ids", "network_interface_ids"}
)

type DeploymentDestroyRequest struct {
	Schema              string   `json:"schema"`
	ServiceID           string   `json:"service_id"`
	DeploymentID        string   `json:"deployment_id"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
}

type DeploymentDestroyCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentDestroyRequest
	ApprovalProof      cloudcontracts.ServiceDestroyApprovalV1
	PrivateKey         ed25519.PrivateKey
}

type DeploymentDestroyCommand struct {
	Schema             string                                  `json:"schema"`
	ConnectionID       string                                  `json:"connection_id"`
	CommandID          string                                  `json:"command_id"`
	NodeKeyID          string                                  `json:"node_key_id"`
	IssuedAt           string                                  `json:"issued_at"`
	ExpiresAt          string                                  `json:"expires_at"`
	ExpectedGeneration int64                                   `json:"expected_generation"`
	NodeCounter        int64                                   `json:"node_counter"`
	Action             string                                  `json:"action"`
	PayloadB64         string                                  `json:"payload_b64"`
	PayloadSHA256      string                                  `json:"payload_sha256"`
	ApprovalProof      cloudcontracts.ServiceDestroyApprovalV1 `json:"approval_proof"`
	SignatureB64       string                                  `json:"signature_b64"`
}

type DeploymentDestroyCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            DeploymentDestroyRequest
	ApprovalProof      cloudcontracts.ServiceDestroyApprovalV1
}

type DeploymentDestroyEvidence struct {
	DeploymentID        string   `json:"deployment_id"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
}

type DeploymentDestroyResult struct {
	Schema     string                    `json:"schema"`
	Status     string                    `json:"status"`
	Receipt    DeploymentCommandReceipt  `json:"receipt"`
	Deployment DeploymentDestroyEvidence `json:"deployment"`
}

func NewDeploymentDestroyCommand(input DeploymentDestroyCommandInput) (DeploymentDestroyCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return DeploymentDestroyCommand{}, newError("invalid_node_private_key", nil)
	}
	input.Request = normalizeDestroyRequest(input.Request)
	issuedAt, expiresAt := canonicalInstant(input.IssuedAt), canonicalInstant(input.ExpiresAt)
	if err := validateDestroyRequest(input.Request); err != nil {
		return DeploymentDestroyCommand{}, err
	}
	if err := validateDestroyApproval(input.ApprovalProof, input.Request, input.ConnectionID, issuedAt, expiresAt); err != nil {
		return DeploymentDestroyCommand{}, err
	}
	payload, _ := json.Marshal(input.Request)
	command := DeploymentDestroyCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: issuedAt, ExpiresAt: expiresAt, ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: DeploymentDestroyAction, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload), ApprovalProof: input.ApprovalProof}
	if err := validateDestroyCommand(command, false); err != nil {
		return DeploymentDestroyCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return DeploymentDestroyCommand{}, err
	}
	return command, nil
}

func (command DeploymentDestroyCommand) Validate() error {
	return validateDestroyCommand(command, true)
}

func (command DeploymentDestroyCommand) ValidateBinding(binding DeploymentDestroyCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	request, err := command.Request()
	if err != nil || binding.IssuedAt.IsZero() || binding.ExpiresAt.IsZero() || command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID || command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter || command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) || !reflect.DeepEqual(request, normalizeDestroyRequest(binding.Request)) || !reflect.DeepEqual(command.ApprovalProof, binding.ApprovalProof) {
		return newError("invalid_deployment_destroy_request", err)
	}
	return nil
}

func (command DeploymentDestroyCommand) SignatureBase() string {
	payload, err := command.ApprovalProof.SigningPayload()
	if err != nil {
		return ""
	}
	return nodeSignatureBase(nodeSignatureFields{Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action, PayloadSHA256: command.PayloadSHA256, ApprovalProofPayloadSHA256: sha256Hex(payload)})
}

func (command DeploymentDestroyCommand) VerifySignature(publicKey ed25519.PublicKey) error {
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

func (command DeploymentDestroyCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

func (command DeploymentDestroyCommand) Request() (DeploymentDestroyRequest, error) {
	if err := command.Validate(); err != nil {
		return DeploymentDestroyRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	return decodeDestroyRequest(payload)
}

func ParseDeploymentDestroyCommand(raw []byte) (DeploymentDestroyCommand, error) {
	if _, err := exactJSONObject(raw, destroyCommandFields); err != nil {
		return DeploymentDestroyCommand{}, newError("invalid_command", err)
	}
	var command DeploymentDestroyCommand
	if decodeStrictJSON(raw, &command) != nil || command.Validate() != nil {
		return DeploymentDestroyCommand{}, newError("invalid_command", nil)
	}
	return command, nil
}

func ValidateDeploymentDestroyResult(command DeploymentDestroyCommand, result DeploymentDestroyResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Schema != DeploymentDestroyResultSchema || result.Status != "verified_destroyed" || result.Receipt.Disposition != "committed" || result.Receipt.Schema != ReceiptSchema || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != command.RequestSHA256() || result.Receipt.Action != DeploymentDestroyAction {
		return newError("invalid_deployment_destroy_receipt", nil)
	}
	request, err := command.Request()
	result.Deployment.VolumeIDs = sortedCopy(result.Deployment.VolumeIDs)
	result.Deployment.NetworkInterfaceIDs = sortedCopy(result.Deployment.NetworkInterfaceIDs)
	if err != nil || result.Deployment.DeploymentID != request.DeploymentID || result.Deployment.InstanceID != request.InstanceID || !reflect.DeepEqual(result.Deployment.VolumeIDs, request.VolumeIDs) || !reflect.DeepEqual(result.Deployment.NetworkInterfaceIDs, request.NetworkInterfaceIDs) {
		return newError("invalid_deployment_destroy_receipt", err)
	}
	return nil
}

func validateDestroyCommand(command DeploymentDestroyCommand, requireSignature bool) error {
	if command.Schema != CommandSchema || command.Action != DeploymentDestroyAction || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) || !keyIDPattern.MatchString(command.NodeKeyID) || !safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) || !sha256Pattern.MatchString(command.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	issued, err := parseCanonicalInstant(command.IssuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expires, err := parseCanonicalInstant(command.ExpiresAt)
	if err != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", err)
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeDestroyRequest(payload)
	if err != nil {
		return newError("invalid_payload", err)
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(canonical, payload) {
		return newError("noncanonical_payload", nil)
	}
	if err := validateDestroyApproval(command.ApprovalProof, request, command.ConnectionID, command.IssuedAt, command.ExpiresAt); err != nil {
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

func validateDestroyRequest(request DeploymentDestroyRequest) error {
	if request.Schema != DeploymentDestroySchema || !idPattern.MatchString(request.ServiceID) || !idPattern.MatchString(request.DeploymentID) || !instanceIDPattern.MatchString(request.InstanceID) || !canonicalStrings(request.VolumeIDs, volumeIDPattern, true) || !canonicalStrings(request.NetworkInterfaceIDs, interfaceIDPattern, true) {
		return newError("invalid_deployment_destroy_request", nil)
	}
	return nil
}

func validateDestroyApproval(proof cloudcontracts.ServiceDestroyApprovalV1, request DeploymentDestroyRequest, connectionID, issuedAt, expiresAt string) error {
	if proof.Validate() != nil || proof.Signature == "" {
		return newError("invalid_approval_proof", nil)
	}
	issued, err := parseCanonicalInstant(issuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expires, err := parseCanonicalInstant(expiresAt)
	if err != nil || !proof.ExpiresAt.After(issued) || proof.ExpiresAt.Before(expires) || proof.CloudConnectionID != connectionID || proof.ServiceID != request.ServiceID || proof.DeploymentID != request.DeploymentID || proof.InstanceID != request.InstanceID || !reflect.DeepEqual(sortedCopy(proof.VolumeIDs), request.VolumeIDs) || !reflect.DeepEqual(sortedCopy(proof.NetworkInterfaceIDs), request.NetworkInterfaceIDs) {
		return newError("approval_proof_mismatch", err)
	}
	return nil
}

func decodeDestroyRequest(raw []byte) (DeploymentDestroyRequest, error) {
	object, err := exactJSONObject(raw, destroyRequestFields)
	if err != nil {
		return DeploymentDestroyRequest{}, err
	}
	if _, err := exactJSONArray(object["volume_ids"]); err != nil {
		return DeploymentDestroyRequest{}, err
	}
	if _, err := exactJSONArray(object["network_interface_ids"]); err != nil {
		return DeploymentDestroyRequest{}, err
	}
	var request DeploymentDestroyRequest
	if decodeStrictJSON(raw, &request) != nil {
		return DeploymentDestroyRequest{}, newError("invalid_payload", nil)
	}
	request = normalizeDestroyRequest(request)
	return request, validateDestroyRequest(request)
}

func decodeDestroyResult(raw []byte) (DeploymentDestroyResult, error) {
	object, err := exactJSONObject(raw, destroyResultFields)
	if err != nil {
		return DeploymentDestroyResult{}, err
	}
	if _, err := exactJSONObject(object["receipt"], deploymentCommandReceiptFields); err != nil {
		return DeploymentDestroyResult{}, err
	}
	evidence, err := exactJSONObject(object["deployment"], destroyEvidenceFields)
	if err != nil {
		return DeploymentDestroyResult{}, err
	}
	if _, err := exactJSONArray(evidence["volume_ids"]); err != nil {
		return DeploymentDestroyResult{}, err
	}
	if _, err := exactJSONArray(evidence["network_interface_ids"]); err != nil {
		return DeploymentDestroyResult{}, err
	}
	var result DeploymentDestroyResult
	if decodeStrictJSON(raw, &result) != nil {
		return DeploymentDestroyResult{}, newError("invalid_broker_response", nil)
	}
	return result, nil
}

func normalizeDestroyRequest(request DeploymentDestroyRequest) DeploymentDestroyRequest {
	request.VolumeIDs = sortedCopy(request.VolumeIDs)
	request.NetworkInterfaceIDs = sortedCopy(request.NetworkInterfaceIDs)
	return request
}

func sortedCopy(values []string) []string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return copyValues
}
