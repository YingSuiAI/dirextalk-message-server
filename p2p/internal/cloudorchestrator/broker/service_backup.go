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

const ServiceBackupAction = "service.backup"
const ServiceBackupSchema = "dirextalk.aws.service-backup/v1"
const ServiceBackupResultSchema = "dirextalk.aws.service-backup-result/v1"

var backupSnapshotIDPattern = regexp.MustCompile(`^snap-[0-9a-f]{8,17}$`)
var backupImageIDPattern = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)

type ServiceBackupRequest struct {
	Schema          string   `json:"schema"`
	BackupID        string   `json:"backup_id"`
	ServiceID       string   `json:"service_id"`
	DeploymentID    string   `json:"deployment_id"`
	InstanceID      string   `json:"instance_id"`
	VolumeIDs       []string `json:"volume_ids"`
	RetentionPolicy string   `json:"retention_policy"`
}
type ServiceBackupCommand struct {
	Schema             string                                 `json:"schema"`
	ConnectionID       string                                 `json:"connection_id"`
	CommandID          string                                 `json:"command_id"`
	NodeKeyID          string                                 `json:"node_key_id"`
	IssuedAt           string                                 `json:"issued_at"`
	ExpiresAt          string                                 `json:"expires_at"`
	ExpectedGeneration int64                                  `json:"expected_generation"`
	NodeCounter        int64                                  `json:"node_counter"`
	Action             string                                 `json:"action"`
	PayloadB64         string                                 `json:"payload_b64"`
	PayloadSHA256      string                                 `json:"payload_sha256"`
	ApprovalProof      cloudcontracts.ServiceBackupApprovalV1 `json:"approval_proof"`
	SignatureB64       string                                 `json:"signature_b64"`
}
type ServiceBackupCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceBackupRequest
	ApprovalProof                      cloudcontracts.ServiceBackupApprovalV1
	PrivateKey                         ed25519.PrivateKey
}
type ServiceBackupCommandBinding struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceBackupRequest
	ApprovalProof                      cloudcontracts.ServiceBackupApprovalV1
}
type ServiceBackupSnapshot struct {
	VolumeID   string `json:"volume_id"`
	SnapshotID string `json:"snapshot_id"`
	State      string `json:"state"`
	Encrypted  bool   `json:"encrypted"`
}
type ServiceBackupEvidence struct {
	BackupID        string                  `json:"backup_id"`
	ServiceID       string                  `json:"service_id"`
	DeploymentID    string                  `json:"deployment_id"`
	InstanceID      string                  `json:"instance_id"`
	RetentionPolicy string                  `json:"retention_policy"`
	ImageID         string                  `json:"image_id"`
	Snapshots       []ServiceBackupSnapshot `json:"snapshots"`
}
type ServiceBackupResult struct {
	Schema  string                   `json:"schema"`
	Status  string                   `json:"status"`
	Receipt DeploymentCommandReceipt `json:"receipt"`
	Backup  ServiceBackupEvidence    `json:"backup"`
}

func NewServiceBackupCommand(input ServiceBackupCommandInput) (ServiceBackupCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return ServiceBackupCommand{}, newError("invalid_node_private_key", nil)
	}
	input.Request.VolumeIDs = sortedCopy(input.Request.VolumeIDs)
	issued, expires := canonicalInstant(input.IssuedAt), canonicalInstant(input.ExpiresAt)
	if validateServiceBackupRequest(input.Request) != nil {
		return ServiceBackupCommand{}, newError("invalid_service_backup_request", nil)
	}
	if validateServiceBackupApproval(input.ApprovalProof, input.Request, input.ConnectionID, issued, expires) != nil {
		return ServiceBackupCommand{}, newError("approval_proof_mismatch", nil)
	}
	payload, _ := json.Marshal(input.Request)
	c := ServiceBackupCommand{Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID, NodeKeyID: input.NodeKeyID, IssuedAt: issued, ExpiresAt: expires, ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: ServiceBackupAction, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload), ApprovalProof: input.ApprovalProof}
	if validateServiceBackupCommand(c, false) != nil {
		return ServiceBackupCommand{}, newError("invalid_command", nil)
	}
	c.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(c.SignatureBase())))
	return c, c.Validate()
}
func (c ServiceBackupCommand) Validate() error { return validateServiceBackupCommand(c, true) }
func (c ServiceBackupCommand) SignatureBase() string {
	p, err := c.ApprovalProof.SigningPayload()
	if err != nil {
		return ""
	}
	return nodeSignatureBase(nodeSignatureFields{Schema: c.Schema, ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, Action: c.Action, PayloadSHA256: c.PayloadSHA256, ApprovalProofPayloadSHA256: sha256Hex(p)})
}
func (c ServiceBackupCommand) RequestSHA256() string { return sha256Hex([]byte(c.SignatureBase())) }
func (c ServiceBackupCommand) Request() (ServiceBackupRequest, error) {
	if c.Validate() != nil {
		return ServiceBackupRequest{}, newError("invalid_command", nil)
	}
	raw, _ := base64.StdEncoding.DecodeString(c.PayloadB64)
	return decodeServiceBackupRequest(raw)
}
func (c ServiceBackupCommand) ValidateBinding(b ServiceBackupCommandBinding) error {
	r, err := c.Request()
	if err != nil || c.ConnectionID != b.ConnectionID || c.CommandID != b.CommandID || c.NodeKeyID != b.NodeKeyID || c.ExpectedGeneration != b.ExpectedGeneration || c.NodeCounter != b.NodeCounter || c.IssuedAt != canonicalInstant(b.IssuedAt) || c.ExpiresAt != canonicalInstant(b.ExpiresAt) || !reflect.DeepEqual(r, normalizeServiceBackupRequest(b.Request)) || !reflect.DeepEqual(c.ApprovalProof, b.ApprovalProof) {
		return newError("invalid_service_backup_request", err)
	}
	return nil
}
func ParseServiceBackupCommand(raw []byte) (ServiceBackupCommand, error) {
	if _, err := exactJSONObject(raw, []string{"schema", "connection_id", "command_id", "node_key_id", "issued_at", "expires_at", "expected_generation", "node_counter", "action", "payload_b64", "payload_sha256", "approval_proof", "signature_b64"}); err != nil {
		return ServiceBackupCommand{}, newError("invalid_command", err)
	}
	var c ServiceBackupCommand
	if decodeStrictJSON(raw, &c) != nil || c.Validate() != nil {
		return c, newError("invalid_command", nil)
	}
	return c, nil
}
func validateServiceBackupCommand(c ServiceBackupCommand, signature bool) error {
	if c.Schema != CommandSchema || c.Action != ServiceBackupAction || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) || !keyIDPattern.MatchString(c.NodeKeyID) || !safePositive(c.ExpectedGeneration) || !safeNonnegative(c.NodeCounter) || !sha256Pattern.MatchString(c.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	issued, e := parseCanonicalInstant(c.IssuedAt)
	if e != nil {
		return newError("invalid_command", e)
	}
	expires, e := parseCanonicalInstant(c.ExpiresAt)
	if e != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", e)
	}
	raw, e := decodeCanonicalBase64(c.PayloadB64)
	if e != nil || sha256Hex(raw) != c.PayloadSHA256 {
		return newError("invalid_payload", e)
	}
	r, e := decodeServiceBackupRequest(raw)
	if e != nil {
		return e
	}
	canonical, _ := json.Marshal(r)
	if !bytes.Equal(raw, canonical) {
		return newError("noncanonical_payload", nil)
	}
	if e = validateServiceBackupApproval(c.ApprovalProof, r, c.ConnectionID, c.IssuedAt, c.ExpiresAt); e != nil {
		return e
	}
	if signature {
		sig, e := decodeCanonicalBase64(c.SignatureB64)
		if e != nil || len(sig) != ed25519.SignatureSize {
			return newError("invalid_command", e)
		}
	}
	return nil
}
func validateServiceBackupRequest(r ServiceBackupRequest) error {
	if r.Schema != ServiceBackupSchema || !idPattern.MatchString(r.BackupID) || !idPattern.MatchString(r.ServiceID) || !idPattern.MatchString(r.DeploymentID) || !instanceIDPattern.MatchString(r.InstanceID) || !canonicalStrings(r.VolumeIDs, volumeIDPattern, true) || r.RetentionPolicy != cloudcontracts.ServiceBackupRetentionManual {
		return newError("invalid_service_backup_request", nil)
	}
	return nil
}
func validateServiceBackupApproval(a cloudcontracts.ServiceBackupApprovalV1, r ServiceBackupRequest, connection, issuedRaw, expiresRaw string) error {
	issued, e := parseCanonicalInstant(issuedRaw)
	if e != nil {
		return e
	}
	expires, e := parseCanonicalInstant(expiresRaw)
	if e != nil || a.Validate() != nil || a.Signature == "" || !a.ExpiresAt.After(issued) || a.ExpiresAt.Before(expires) || a.CloudConnectionID != connection || a.BackupID != r.BackupID || a.ServiceID != r.ServiceID || a.DeploymentID != r.DeploymentID || a.InstanceID != r.InstanceID || a.RetentionPolicy != r.RetentionPolicy || !reflect.DeepEqual(sortedCopy(a.VolumeIDs), r.VolumeIDs) {
		return newError("approval_proof_mismatch", e)
	}
	return nil
}
func decodeServiceBackupRequest(raw []byte) (ServiceBackupRequest, error) {
	object, e := exactJSONObject(raw, []string{"schema", "backup_id", "service_id", "deployment_id", "instance_id", "volume_ids", "retention_policy"})
	if e != nil {
		return ServiceBackupRequest{}, e
	}
	if _, e = exactJSONArray(object["volume_ids"]); e != nil {
		return ServiceBackupRequest{}, e
	}
	var r ServiceBackupRequest
	if decodeStrictJSON(raw, &r) != nil {
		return r, newError("invalid_payload", nil)
	}
	r = normalizeServiceBackupRequest(r)
	return r, validateServiceBackupRequest(r)
}
func normalizeServiceBackupRequest(r ServiceBackupRequest) ServiceBackupRequest {
	r.VolumeIDs = sortedCopy(r.VolumeIDs)
	return r
}
func decodeServiceBackupResult(raw []byte) (ServiceBackupResult, error) {
	object, e := exactJSONObject(raw, []string{"schema", "status", "receipt", "backup"})
	if e != nil {
		return ServiceBackupResult{}, e
	}
	if _, e = exactJSONObject(object["receipt"], deploymentCommandReceiptFields); e != nil {
		return ServiceBackupResult{}, e
	}
	backup, e := exactJSONObject(object["backup"], []string{"backup_id", "service_id", "deployment_id", "instance_id", "retention_policy", "image_id", "snapshots"})
	if e != nil {
		return ServiceBackupResult{}, e
	}
	if _, e = exactJSONArray(backup["snapshots"]); e != nil {
		return ServiceBackupResult{}, e
	}
	var result ServiceBackupResult
	if decodeStrictJSON(raw, &result) != nil {
		return result, newError("invalid_broker_response", nil)
	}
	return result, nil
}
func ValidateServiceBackupResult(c ServiceBackupCommand, result ServiceBackupResult) error {
	if c.Validate() != nil || result.Schema != ServiceBackupResultSchema || result.Status != "backup_available" || result.Receipt.Schema != ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != c.ConnectionID || result.Receipt.CommandID != c.CommandID || result.Receipt.RequestSHA256 != c.RequestSHA256() || result.Receipt.Action != ServiceBackupAction || result.Receipt.ExpectedGeneration != c.ExpectedGeneration || result.Receipt.NodeCounter != c.NodeCounter {
		return newError("invalid_service_backup_receipt", nil)
	}
	r, e := c.Request()
	if e != nil || result.Backup.BackupID != r.BackupID || result.Backup.ServiceID != r.ServiceID || result.Backup.DeploymentID != r.DeploymentID || result.Backup.InstanceID != r.InstanceID || result.Backup.RetentionPolicy != r.RetentionPolicy || !backupImageIDPattern.MatchString(result.Backup.ImageID) || len(result.Backup.Snapshots) != len(r.VolumeIDs) {
		return newError("invalid_service_backup_receipt", e)
	}
	byVolume := map[string]ServiceBackupSnapshot{}
	for _, s := range result.Backup.Snapshots {
		if !backupSnapshotIDPattern.MatchString(s.SnapshotID) || s.State != "completed" || !s.Encrypted {
			return newError("invalid_service_backup_receipt", nil)
		}
		byVolume[s.VolumeID] = s
	}
	for _, v := range r.VolumeIDs {
		if byVolume[v].VolumeID != v {
			return newError("invalid_service_backup_receipt", nil)
		}
	}
	return nil
}
