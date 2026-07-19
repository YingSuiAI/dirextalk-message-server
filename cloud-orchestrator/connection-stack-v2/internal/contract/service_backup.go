package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"time"
)

const (
	ServiceBackupSchema                 = "dirextalk.aws.service-backup/v1"
	ServiceBackupResultSchema           = "dirextalk.aws.service-backup-result/v1"
	serviceBackupApprovalIntent         = "service_backup"
	serviceBackupApprovalPayloadVersion = "service-backup-approval-signing-payload/v1"
	serviceBackupRetentionManual        = "manual"
)

var snapshotIDPattern = regexp.MustCompile(`^snap-[0-9a-f]{8,17}$`)
var backupImageIDPattern = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)

type ServiceBackupApprovalProof struct {
	SchemaVersion      string    `json:"schema_version"`
	Intent             string    `json:"intent"`
	ApprovalID         string    `json:"approval_id"`
	ChallengeID        string    `json:"challenge_id"`
	SignerKeyID        string    `json:"signer_key_id"`
	BackupID           string    `json:"backup_id"`
	ServiceID          string    `json:"service_id"`
	ServiceRevision    uint64    `json:"service_revision"`
	DeploymentID       string    `json:"deployment_id"`
	DeploymentRevision uint64    `json:"deployment_revision"`
	CloudConnectionID  string    `json:"cloud_connection_id"`
	RecipeID           string    `json:"recipe_id"`
	RecipeDigest       string    `json:"recipe_digest"`
	InstanceID         string    `json:"instance_id"`
	VolumeIDs          []string  `json:"volume_ids"`
	RetentionPolicy    string    `json:"retention_policy"`
	IssuedAt           time.Time `json:"issued_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Signature          string    `json:"signature"`
}

type serviceBackupApprovalSigningPayload struct {
	SchemaVersion      string    `json:"schema_version"`
	PayloadVersion     string    `json:"payload_version"`
	HashAlgorithm      string    `json:"hash_algorithm"`
	Intent             string    `json:"intent"`
	ApprovalID         string    `json:"approval_id"`
	ChallengeID        string    `json:"challenge_id"`
	SignerKeyID        string    `json:"signer_key_id"`
	BackupID           string    `json:"backup_id"`
	ServiceID          string    `json:"service_id"`
	ServiceRevision    uint64    `json:"service_revision"`
	DeploymentID       string    `json:"deployment_id"`
	DeploymentRevision uint64    `json:"deployment_revision"`
	CloudConnectionID  string    `json:"cloud_connection_id"`
	RecipeID           string    `json:"recipe_id"`
	RecipeDigest       string    `json:"recipe_digest"`
	InstanceID         string    `json:"instance_id"`
	VolumeIDs          []string  `json:"volume_ids"`
	RetentionPolicy    string    `json:"retention_policy"`
	IssuedAt           time.Time `json:"issued_at"`
	ExpiresAt          time.Time `json:"expires_at"`
}

type ServiceBackupRequest struct {
	Schema          string   `json:"schema"`
	BackupID        string   `json:"backup_id"`
	ServiceID       string   `json:"service_id"`
	DeploymentID    string   `json:"deployment_id"`
	InstanceID      string   `json:"instance_id"`
	VolumeIDs       []string `json:"volume_ids"`
	RetentionPolicy string   `json:"retention_policy"`
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

func ParseServiceBackupApprovalProof(raw []byte) (ServiceBackupApprovalProof, error) {
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, []string{"schema_version", "intent", "approval_id", "challenge_id", "signer_key_id", "backup_id", "service_id", "service_revision", "deployment_id", "deployment_revision", "cloud_connection_id", "recipe_id", "recipe_digest", "instance_id", "volume_ids", "retention_policy", "issued_at", "expires_at", "signature"}) {
		return ServiceBackupApprovalProof{}, errCode("invalid_approval_proof")
	}
	var proof ServiceBackupApprovalProof
	if decodeSingle(raw, &proof) != nil || proof.validate() != nil {
		return ServiceBackupApprovalProof{}, errCode("invalid_approval_proof")
	}
	return proof, nil
}
func (p ServiceBackupApprovalProof) validate() error {
	if p.SchemaVersion != approvalSchemaVersion || p.Intent != serviceBackupApprovalIntent || !approvalIdentifierPattern.MatchString(p.ApprovalID) || !approvalIdentifierPattern.MatchString(p.ChallengeID) || !approvalIdentifierPattern.MatchString(p.SignerKeyID) || !approvalIdentifierPattern.MatchString(p.BackupID) || !approvalIdentifierPattern.MatchString(p.ServiceID) || !approvalIdentifierPattern.MatchString(p.DeploymentID) || !approvalIdentifierPattern.MatchString(p.CloudConnectionID) || !approvalIdentifierPattern.MatchString(p.RecipeID) || !namedSHA256Pattern.MatchString(p.RecipeDigest) || p.ServiceRevision == 0 || p.DeploymentRevision == 0 || p.ServiceRevision > uint64(maxSafeInteger) || p.DeploymentRevision > uint64(maxSafeInteger) || !destroyInstanceIDPattern.MatchString(p.InstanceID) || !validDestroyResourceIDs(p.VolumeIDs, destroyVolumeIDPattern) || p.RetentionPolicy != serviceBackupRetentionManual {
		return errCode("invalid_approval_proof")
	}
	if p.IssuedAt.IsZero() || p.ExpiresAt.IsZero() || p.IssuedAt.Location() != time.UTC || p.ExpiresAt.Location() != time.UTC || !p.ExpiresAt.After(p.IssuedAt) || p.ExpiresAt.Sub(p.IssuedAt) > 5*time.Minute {
		return errCode("invalid_approval_proof")
	}
	signature, err := base64.RawURLEncoding.DecodeString(p.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errCode("invalid_approval_proof")
	}
	return nil
}
func (p ServiceBackupApprovalProof) SigningPayload() ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	p = normalizeServiceBackupProof(p)
	return deterministicCBOR(serviceBackupApprovalSigningPayload{SchemaVersion: p.SchemaVersion, PayloadVersion: serviceBackupApprovalPayloadVersion, HashAlgorithm: approvalHashAlgorithm, Intent: p.Intent, ApprovalID: p.ApprovalID, ChallengeID: p.ChallengeID, SignerKeyID: p.SignerKeyID, BackupID: p.BackupID, ServiceID: p.ServiceID, ServiceRevision: p.ServiceRevision, DeploymentID: p.DeploymentID, DeploymentRevision: p.DeploymentRevision, CloudConnectionID: p.CloudConnectionID, RecipeID: p.RecipeID, RecipeDigest: p.RecipeDigest, InstanceID: p.InstanceID, VolumeIDs: p.VolumeIDs, RetentionPolicy: p.RetentionPolicy, IssuedAt: p.IssuedAt, ExpiresAt: p.ExpiresAt})
}
func (p ServiceBackupApprovalProof) PayloadSHA256() (string, error) {
	payload, err := p.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
func (p ServiceBackupApprovalProof) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	if !p.ExpiresAt.After(now.UTC()) {
		return errCode("approval_expired")
	}
	payload, err := p.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(p.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errCode("invalid_approval_signature")
	}
	return nil
}
func normalizeServiceBackupProof(p ServiceBackupApprovalProof) ServiceBackupApprovalProof {
	p.IssuedAt, p.ExpiresAt = p.IssuedAt.UTC(), p.ExpiresAt.UTC()
	p.VolumeIDs = append([]string(nil), p.VolumeIDs...)
	sort.Strings(p.VolumeIDs)
	return p
}
func (c Command) ServiceBackupApproval() (ServiceBackupApprovalProof, error) {
	if c.Action != ActionServiceBackup || len(c.ApprovalProof) == 0 {
		return ServiceBackupApprovalProof{}, errCode("invalid_approval_proof")
	}
	return ParseServiceBackupApprovalProof(c.ApprovalProof)
}
func (c Command) ServiceBackupApprovalPayloadSHA256() (string, error) {
	if err := c.ValidateServiceBackupBinding(); err != nil {
		return "", err
	}
	p, _ := c.ServiceBackupApproval()
	return p.PayloadSHA256()
}

func (c Command) ServiceBackupRequest() (ServiceBackupRequest, error) {
	if c.Action != ActionServiceBackup {
		return ServiceBackupRequest{}, errCode("invalid_payload")
	}
	payload, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil {
		return ServiceBackupRequest{}, errCode("invalid_payload")
	}
	fields, err := exactJSONObject(payload)
	if err != nil || !exactFields(fields, []string{"schema", "backup_id", "service_id", "deployment_id", "instance_id", "volume_ids", "retention_policy"}) {
		return ServiceBackupRequest{}, errCode("invalid_payload")
	}
	var request ServiceBackupRequest
	if decodeSingle(payload, &request) != nil || request.validate() != nil {
		return ServiceBackupRequest{}, errCode("invalid_payload")
	}
	request.VolumeIDs = append([]string(nil), request.VolumeIDs...)
	sort.Strings(request.VolumeIDs)
	return request, nil
}
func (r ServiceBackupRequest) validate() error {
	if r.Schema != ServiceBackupSchema || !approvalIdentifierPattern.MatchString(r.BackupID) || !approvalIdentifierPattern.MatchString(r.ServiceID) || !approvalIdentifierPattern.MatchString(r.DeploymentID) || !destroyInstanceIDPattern.MatchString(r.InstanceID) || !validDestroyResourceIDs(r.VolumeIDs, destroyVolumeIDPattern) || r.RetentionPolicy != serviceBackupRetentionManual {
		return errCode("invalid_payload")
	}
	return nil
}
func (r ServiceBackupRequest) Validate() error { return r.validate() }
func (c Command) ValidateServiceBackupBinding() error {
	request, err := c.ServiceBackupRequest()
	if err != nil {
		return err
	}
	proof, err := c.ServiceBackupApproval()
	if err != nil {
		return err
	}
	proof = normalizeServiceBackupProof(proof)
	if proof.CloudConnectionID != c.ConnectionID || proof.BackupID != request.BackupID || proof.ServiceID != request.ServiceID || proof.DeploymentID != request.DeploymentID || proof.InstanceID != request.InstanceID || proof.RetentionPolicy != request.RetentionPolicy || !sameStrings(proof.VolumeIDs, request.VolumeIDs) {
		return errCode("approval_scope_mismatch")
	}
	return nil
}

func MarshalCommittedServiceBackupResult(c Command, evidence ServiceBackupEvidence) ([]byte, error) {
	if err := c.ValidateServiceBackupBinding(); err != nil {
		return nil, err
	}
	request, _ := c.ServiceBackupRequest()
	normalizeBackupEvidence(&evidence)
	if validateBackupEvidence(request, evidence) != nil {
		return nil, errCode("provider_readback_invalid")
	}
	requestSHA, _ := c.RequestSHA256()
	return json.Marshal(ServiceBackupResult{Schema: ServiceBackupResultSchema, Status: "backup_available", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: c.ConnectionID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, CommandID: c.CommandID, RequestSHA256: requestSHA, Action: ActionServiceBackup}, Backup: evidence})
}
func ValidateServiceBackupResult(c Command, result ServiceBackupResult) error {
	requestSHA, err := c.RequestSHA256()
	if err != nil || result.Schema != ServiceBackupResultSchema || result.Status != "backup_available" || result.Receipt.Schema != ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != c.ConnectionID || result.Receipt.ExpectedGeneration != c.ExpectedGeneration || result.Receipt.NodeCounter != c.NodeCounter || result.Receipt.CommandID != c.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionServiceBackup {
		return errCode("invalid_result")
	}
	request, err := c.ServiceBackupRequest()
	if err != nil {
		return err
	}
	normalizeBackupEvidence(&result.Backup)
	return validateBackupEvidence(request, result.Backup)
}
func normalizeBackupEvidence(e *ServiceBackupEvidence) {
	sort.Slice(e.Snapshots, func(i, j int) bool { return e.Snapshots[i].VolumeID < e.Snapshots[j].VolumeID })
}
func validateBackupEvidence(request ServiceBackupRequest, e ServiceBackupEvidence) error {
	if e.BackupID != request.BackupID || e.ServiceID != request.ServiceID || e.DeploymentID != request.DeploymentID || e.InstanceID != request.InstanceID || e.RetentionPolicy != request.RetentionPolicy || !backupImageIDPattern.MatchString(e.ImageID) || len(e.Snapshots) != len(request.VolumeIDs) {
		return errCode("invalid_result")
	}
	for i, s := range e.Snapshots {
		if s.VolumeID != request.VolumeIDs[i] || !snapshotIDPattern.MatchString(s.SnapshotID) || s.State != "completed" || !s.Encrypted {
			return errCode("invalid_result")
		}
	}
	return nil
}
