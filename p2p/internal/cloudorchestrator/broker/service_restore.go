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

const ServiceRestoreAction = "service.restore"
const ServiceRestoreSchema = "dirextalk.aws.service-restore/v1"
const ServiceRestoreResultSchema = "dirextalk.aws.service-restore-result/v1"

type ServiceRestoreRequest struct {
	Schema                  string                     `json:"schema"`
	RestoreID               string                     `json:"restore_id"`
	ServiceID               string                     `json:"service_id"`
	DeploymentID            string                     `json:"deployment_id"`
	BackupID                string                     `json:"backup_id"`
	InstanceID              string                     `json:"instance_id"`
	Region                  string                     `json:"region"`
	AvailabilityZone        string                     `json:"availability_zone"`
	RestoreMode             string                     `json:"restore_mode"`
	DowntimeRequired        bool                       `json:"downtime_required"`
	OriginalVolumeRetention string                     `json:"original_volume_retention"`
	FailurePolicy           string                     `json:"failure_policy"`
	QuoteID                 string                     `json:"quote_id"`
	QuoteValidUntil         string                     `json:"quote_valid_until"`
	VolumeSwaps             []ServiceRestoreVolumeSwap `json:"volume_swaps"`
}

type ServiceRestoreCommand struct {
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
	ApprovalProof      cloudcontracts.ServiceRestoreApprovalV1 `json:"approval_proof"`
	SignatureB64       string                                  `json:"signature_b64"`
}

type ServiceRestoreCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceRestoreRequest
	ApprovalProof                      cloudcontracts.ServiceRestoreApprovalV1
	PrivateKey                         ed25519.PrivateKey
}

type ServiceRestoreCommandBinding struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceRestoreRequest
	ApprovalProof                      cloudcontracts.ServiceRestoreApprovalV1
}

type ServiceRestoreReplacementVolume struct {
	OriginalVolumeID    string `json:"original_volume_id"`
	ReplacementVolumeID string `json:"replacement_volume_id"`
	SnapshotID          string `json:"snapshot_id"`
	DeviceName          string `json:"device_name"`
	State               string `json:"state"`
	Encrypted           bool   `json:"encrypted"`
	DeleteOnTermination bool   `json:"delete_on_termination"`
}

type ServiceRestoreAWSEvidence struct {
	RestoreID        string                            `json:"restore_id"`
	ServiceID        string                            `json:"service_id"`
	DeploymentID     string                            `json:"deployment_id"`
	BackupID         string                            `json:"backup_id"`
	InstanceID       string                            `json:"instance_id"`
	Region           string                            `json:"region"`
	AvailabilityZone string                            `json:"availability_zone"`
	Outcome          string                            `json:"outcome"`
	InstanceState    string                            `json:"instance_state"`
	FallbackVerified bool                              `json:"fallback_verified"`
	Replacements     []ServiceRestoreReplacementVolume `json:"replacements"`
}

type ServiceRestoreResult struct {
	Schema  string                    `json:"schema"`
	Status  string                    `json:"status"`
	Receipt DeploymentCommandReceipt  `json:"receipt"`
	Restore ServiceRestoreAWSEvidence `json:"restore"`
}

func NewServiceRestoreCommand(input ServiceRestoreCommandInput) (ServiceRestoreCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return ServiceRestoreCommand{}, newError("invalid_node_private_key", nil)
	}
	input.Request = normalizeServiceRestoreRequest(input.Request)
	issued, expires := canonicalInstant(input.IssuedAt), canonicalInstant(input.ExpiresAt)
	if validateServiceRestoreRequest(input.Request) != nil || validateServiceRestoreApproval(input.ApprovalProof, input.Request, input.ConnectionID, issued, expires) != nil {
		return ServiceRestoreCommand{}, newError("approval_proof_mismatch", nil)
	}
	payload, _ := json.Marshal(input.Request)
	command := ServiceRestoreCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: issued, ExpiresAt: expires, ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: ServiceRestoreAction, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload), ApprovalProof: input.ApprovalProof}
	if validateServiceRestoreCommand(command, false) != nil {
		return ServiceRestoreCommand{}, newError("invalid_command", nil)
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	return command, command.Validate()
}

func (command ServiceRestoreCommand) Validate() error {
	return validateServiceRestoreCommand(command, true)
}

func (command ServiceRestoreCommand) SignatureBase() string {
	payload, err := command.ApprovalProof.SigningPayload()
	if err != nil {
		return ""
	}
	return nodeSignatureBase(nodeSignatureFields{Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action, PayloadSHA256: command.PayloadSHA256, ApprovalProofPayloadSHA256: sha256Hex(payload)})
}

func (command ServiceRestoreCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

func (command ServiceRestoreCommand) Request() (ServiceRestoreRequest, error) {
	if command.Validate() != nil {
		return ServiceRestoreRequest{}, newError("invalid_command", nil)
	}
	raw, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	return decodeServiceRestoreRequest(raw)
}

func (command ServiceRestoreCommand) ValidateBinding(binding ServiceRestoreCommandBinding) error {
	request, err := command.Request()
	if err != nil || command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID || command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter || command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) || !reflect.DeepEqual(request, normalizeServiceRestoreRequest(binding.Request)) || !reflect.DeepEqual(command.ApprovalProof, binding.ApprovalProof) {
		return newError("invalid_service_restore_request", err)
	}
	return nil
}

func ParseServiceRestoreCommand(raw []byte) (ServiceRestoreCommand, error) {
	if _, err := exactJSONObject(raw, []string{"schema", "connection_id", "command_id", "node_key_id", "issued_at", "expires_at", "expected_generation", "node_counter", "action", "payload_b64", "payload_sha256", "approval_proof", "signature_b64"}); err != nil {
		return ServiceRestoreCommand{}, newError("invalid_command", err)
	}
	var command ServiceRestoreCommand
	if decodeStrictJSON(raw, &command) != nil || command.Validate() != nil {
		return command, newError("invalid_command", nil)
	}
	return command, nil
}

func validateServiceRestoreCommand(command ServiceRestoreCommand, signature bool) error {
	if command.Schema != CommandSchema || command.Action != ServiceRestoreAction || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) || !keyIDPattern.MatchString(command.NodeKeyID) || !safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) || !sha256Pattern.MatchString(command.PayloadSHA256) {
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
	raw, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || sha256Hex(raw) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeServiceRestoreRequest(raw)
	if err != nil {
		return err
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(raw, canonical) {
		return newError("noncanonical_payload", nil)
	}
	if err = validateServiceRestoreApproval(command.ApprovalProof, request, command.ConnectionID, command.IssuedAt, command.ExpiresAt); err != nil {
		return err
	}
	if signature {
		signatureBytes, err := decodeCanonicalBase64(command.SignatureB64)
		if err != nil || len(signatureBytes) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func validateServiceRestoreRequest(request ServiceRestoreRequest) error {
	if request.Schema != ServiceRestoreSchema || !idPattern.MatchString(request.RestoreID) || !idPattern.MatchString(request.ServiceID) || !idPattern.MatchString(request.DeploymentID) || !idPattern.MatchString(request.BackupID) || !instanceIDPattern.MatchString(request.InstanceID) || request.Region == "" || request.AvailabilityZone == "" || request.RestoreMode != cloudcontracts.ServiceRestoreModeInPlace || !request.DowntimeRequired || request.OriginalVolumeRetention != cloudcontracts.ServiceRestoreRetentionManual || request.FailurePolicy != cloudcontracts.ServiceRestoreFailureReattachOriginal || !idPattern.MatchString(request.QuoteID) || len(request.VolumeSwaps) == 0 {
		return newError("invalid_service_restore_request", nil)
	}
	validUntil, err := parseCanonicalInstant(request.QuoteValidUntil)
	if err != nil || validUntil.IsZero() {
		return newError("invalid_service_restore_request", err)
	}
	seenOriginal, seenSnapshot, seenDevice := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, swap := range request.VolumeSwaps {
		if !volumeIDPattern.MatchString(swap.OriginalVolumeID) || !backupSnapshotIDPattern.MatchString(swap.SnapshotID) || swap.DeviceName == "" || seenOriginal[swap.OriginalVolumeID] || seenSnapshot[swap.SnapshotID] || seenDevice[swap.DeviceName] || !swap.Encrypted || swap.SizeGiB <= 0 || swap.IOPS < 0 || swap.ThroughputMiB < 0 {
			return newError("invalid_service_restore_request", nil)
		}
		seenOriginal[swap.OriginalVolumeID], seenSnapshot[swap.SnapshotID], seenDevice[swap.DeviceName] = true, true, true
	}
	return nil
}

func validateServiceRestoreApproval(approval cloudcontracts.ServiceRestoreApprovalV1, request ServiceRestoreRequest, connection, issuedRaw, expiresRaw string) error {
	issued, err := parseCanonicalInstant(issuedRaw)
	if err != nil {
		return err
	}
	expires, err := parseCanonicalInstant(expiresRaw)
	target := approval.ServiceRestoreTargetV1
	if err != nil || approval.Validate() != nil || approval.Signature == "" || !approval.ExpiresAt.After(issued) || approval.ExpiresAt.Before(expires) || target.CloudConnectionID != connection || target.RestoreID != request.RestoreID || target.ServiceID != request.ServiceID || target.DeploymentID != request.DeploymentID || target.BackupID != request.BackupID || target.InstanceID != request.InstanceID || target.Region != request.Region || target.AvailabilityZone != request.AvailabilityZone || target.RestoreMode != request.RestoreMode || target.DowntimeRequired != request.DowntimeRequired || target.OriginalVolumeRetention != request.OriginalVolumeRetention || target.FailurePolicy != request.FailurePolicy || target.QuoteID != request.QuoteID || canonicalInstant(target.QuoteValidUntil) != request.QuoteValidUntil || !restoreSwapsEqual(target.VolumeSwaps, request.VolumeSwaps) {
		return newError("approval_proof_mismatch", err)
	}
	return nil
}

func restoreSwapsEqual(left []cloudcontracts.ServiceRestoreVolumeSwapV1, right []ServiceRestoreVolumeSwap) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]cloudcontracts.ServiceRestoreVolumeSwapV1(nil), left...)
	sort.Slice(left, func(i, j int) bool { return left[i].DeviceName < left[j].DeviceName })
	for index, value := range left {
		other := right[index]
		if value.OriginalVolumeID != other.OriginalVolumeID || value.SnapshotID != other.SnapshotID || value.DeviceName != other.DeviceName || value.VolumeType != other.VolumeType || value.SizeGiB != other.SizeGiB || value.IOPS != other.IOPS || value.ThroughputMiB != other.ThroughputMiB || value.Encrypted != other.Encrypted || value.DeleteOnTermination != other.DeleteOnTermination {
			return false
		}
	}
	return true
}

func decodeServiceRestoreRequest(raw []byte) (ServiceRestoreRequest, error) {
	object, err := exactJSONObject(raw, []string{"schema", "restore_id", "service_id", "deployment_id", "backup_id", "instance_id", "region", "availability_zone", "restore_mode", "downtime_required", "original_volume_retention", "failure_policy", "quote_id", "quote_valid_until", "volume_swaps"})
	if err != nil {
		return ServiceRestoreRequest{}, err
	}
	if _, err = exactJSONArray(object["volume_swaps"]); err != nil {
		return ServiceRestoreRequest{}, err
	}
	var request ServiceRestoreRequest
	if decodeStrictJSON(raw, &request) != nil {
		return request, newError("invalid_payload", nil)
	}
	request = normalizeServiceRestoreRequest(request)
	return request, validateServiceRestoreRequest(request)
}

func normalizeServiceRestoreRequest(request ServiceRestoreRequest) ServiceRestoreRequest {
	request.VolumeSwaps = normalizeRestoreSwaps(request.VolumeSwaps)
	return request
}

func normalizeRestoreSwaps(swaps []ServiceRestoreVolumeSwap) []ServiceRestoreVolumeSwap {
	result := append([]ServiceRestoreVolumeSwap(nil), swaps...)
	sort.Slice(result, func(i, j int) bool { return result[i].DeviceName < result[j].DeviceName })
	return result
}

func decodeServiceRestoreResult(raw []byte) (ServiceRestoreResult, error) {
	object, err := exactJSONObject(raw, []string{"schema", "status", "receipt", "restore"})
	if err != nil {
		return ServiceRestoreResult{}, err
	}
	if _, err = exactJSONObject(object["receipt"], deploymentCommandReceiptFields); err != nil {
		return ServiceRestoreResult{}, err
	}
	restore, err := exactJSONObject(object["restore"], []string{"restore_id", "service_id", "deployment_id", "backup_id", "instance_id", "region", "availability_zone", "outcome", "instance_state", "fallback_verified", "replacements"})
	if err != nil {
		return ServiceRestoreResult{}, err
	}
	if _, err = exactJSONArray(restore["replacements"]); err != nil {
		return ServiceRestoreResult{}, err
	}
	var result ServiceRestoreResult
	if decodeStrictJSON(raw, &result) != nil {
		return result, newError("invalid_broker_response", nil)
	}
	return result, nil
}

func ValidateServiceRestoreResult(command ServiceRestoreCommand, result ServiceRestoreResult) error {
	if command.Validate() != nil || result.Schema != ServiceRestoreResultSchema || (result.Status != "aws_restore_applied" && result.Status != "aws_original_restored" && result.Status != "restore_blocked") || result.Receipt.Schema != ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != command.RequestSHA256() || result.Receipt.Action != ServiceRestoreAction || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter {
		return newError("invalid_service_restore_receipt", nil)
	}
	request, err := command.Request()
	if err != nil || result.Restore.RestoreID != request.RestoreID || result.Restore.ServiceID != request.ServiceID || result.Restore.DeploymentID != request.DeploymentID || result.Restore.BackupID != request.BackupID || result.Restore.InstanceID != request.InstanceID || result.Restore.Region != request.Region || result.Restore.AvailabilityZone != request.AvailabilityZone || len(result.Restore.Replacements) != len(request.VolumeSwaps) {
		return newError("invalid_service_restore_receipt", err)
	}
	expectedOutcome, expectedState, fallback := "restored", "attached_current", false
	if result.Status == "aws_original_restored" {
		expectedOutcome, expectedState, fallback = "original_restored", "retained_detached", true
	} else if result.Status == "restore_blocked" {
		expectedOutcome, expectedState = "restore_blocked", ""
	}
	if result.Restore.Outcome != expectedOutcome || (result.Status != "restore_blocked" && (result.Restore.InstanceState != "running" || result.Restore.FallbackVerified != fallback)) {
		return newError("invalid_service_restore_receipt", nil)
	}
	sort.Slice(result.Restore.Replacements, func(i, j int) bool {
		return result.Restore.Replacements[i].DeviceName < result.Restore.Replacements[j].DeviceName
	})
	for index, replacement := range result.Restore.Replacements {
		swap := request.VolumeSwaps[index]
		if replacement.OriginalVolumeID != swap.OriginalVolumeID || !volumeIDPattern.MatchString(replacement.ReplacementVolumeID) || replacement.SnapshotID != swap.SnapshotID || replacement.DeviceName != swap.DeviceName || !replacement.Encrypted || replacement.DeleteOnTermination != swap.DeleteOnTermination || (expectedState != "" && replacement.State != expectedState) || (expectedState == "" && replacement.State != "attached_current" && replacement.State != "retained_detached" && replacement.State != "unknown") {
			return newError("invalid_service_restore_receipt", nil)
		}
	}
	return nil
}
