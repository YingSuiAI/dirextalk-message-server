package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"time"
)

const (
	ServiceRestoreSchema                 = "dirextalk.aws.service-restore/v1"
	ServiceRestoreResultSchema           = "dirextalk.aws.service-restore-result/v1"
	serviceRestoreApprovalIntent         = "service_restore"
	serviceRestoreApprovalPayloadVersion = "service-restore-approval-signing-payload/v1"
)

type ServiceRestoreApprovalProof struct {
	SchemaVersion           string                     `json:"schema_version"`
	Intent                  string                     `json:"intent"`
	ApprovalID              string                     `json:"approval_id"`
	ChallengeID             string                     `json:"challenge_id"`
	SignerKeyID             string                     `json:"signer_key_id"`
	RestoreID               string                     `json:"restore_id"`
	ServiceID               string                     `json:"service_id"`
	ServiceRevision         uint64                     `json:"service_revision"`
	DeploymentID            string                     `json:"deployment_id"`
	DeploymentRevision      uint64                     `json:"deployment_revision"`
	CloudConnectionID       string                     `json:"cloud_connection_id"`
	BackupID                string                     `json:"backup_id"`
	BackupRevision          uint64                     `json:"backup_revision"`
	RecipeID                string                     `json:"recipe_id"`
	RecipeDigest            string                     `json:"recipe_digest"`
	InstanceID              string                     `json:"instance_id"`
	Region                  string                     `json:"region"`
	AvailabilityZone        string                     `json:"availability_zone"`
	RestoreMode             string                     `json:"restore_mode"`
	DowntimeRequired        bool                       `json:"downtime_required"`
	OriginalVolumeRetention string                     `json:"original_volume_retention"`
	FailurePolicy           string                     `json:"failure_policy"`
	QuoteID                 string                     `json:"quote_id"`
	Currency                string                     `json:"currency"`
	EstimatedHourlyMinor    int64                      `json:"estimated_hourly_minor"`
	EstimatedThirtyDayMinor int64                      `json:"estimated_thirty_day_minor"`
	QuoteValidUntil         time.Time                  `json:"quote_valid_until"`
	Unincluded              []string                   `json:"unincluded"`
	VolumeSwaps             []ServiceRestoreVolumeSwap `json:"volume_swaps"`
	IssuedAt                time.Time                  `json:"issued_at"`
	ExpiresAt               time.Time                  `json:"expires_at"`
	Signature               string                     `json:"signature"`
}
type serviceRestoreApprovalSigningPayload struct {
	SchemaVersion           string                     `json:"schema_version"`
	PayloadVersion          string                     `json:"payload_version"`
	HashAlgorithm           string                     `json:"hash_algorithm"`
	Intent                  string                     `json:"intent"`
	ApprovalID              string                     `json:"approval_id"`
	ChallengeID             string                     `json:"challenge_id"`
	SignerKeyID             string                     `json:"signer_key_id"`
	RestoreID               string                     `json:"restore_id"`
	ServiceID               string                     `json:"service_id"`
	ServiceRevision         uint64                     `json:"service_revision"`
	DeploymentID            string                     `json:"deployment_id"`
	DeploymentRevision      uint64                     `json:"deployment_revision"`
	CloudConnectionID       string                     `json:"cloud_connection_id"`
	BackupID                string                     `json:"backup_id"`
	BackupRevision          uint64                     `json:"backup_revision"`
	RecipeID                string                     `json:"recipe_id"`
	RecipeDigest            string                     `json:"recipe_digest"`
	InstanceID              string                     `json:"instance_id"`
	Region                  string                     `json:"region"`
	AvailabilityZone        string                     `json:"availability_zone"`
	RestoreMode             string                     `json:"restore_mode"`
	DowntimeRequired        bool                       `json:"downtime_required"`
	OriginalVolumeRetention string                     `json:"original_volume_retention"`
	FailurePolicy           string                     `json:"failure_policy"`
	QuoteID                 string                     `json:"quote_id"`
	Currency                string                     `json:"currency"`
	EstimatedHourlyMinor    int64                      `json:"estimated_hourly_minor"`
	EstimatedThirtyDayMinor int64                      `json:"estimated_thirty_day_minor"`
	QuoteValidUntil         time.Time                  `json:"quote_valid_until"`
	Unincluded              []string                   `json:"unincluded"`
	VolumeSwaps             []ServiceRestoreVolumeSwap `json:"volume_swaps"`
	IssuedAt                time.Time                  `json:"issued_at"`
	ExpiresAt               time.Time                  `json:"expires_at"`
}
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

var restoreApprovalFields = []string{"schema_version", "intent", "approval_id", "challenge_id", "signer_key_id", "restore_id", "service_id", "service_revision", "deployment_id", "deployment_revision", "cloud_connection_id", "backup_id", "backup_revision", "recipe_id", "recipe_digest", "instance_id", "region", "availability_zone", "restore_mode", "downtime_required", "original_volume_retention", "failure_policy", "quote_id", "currency", "estimated_hourly_minor", "estimated_thirty_day_minor", "quote_valid_until", "unincluded", "volume_swaps", "issued_at", "expires_at", "signature"}

func ParseServiceRestoreApprovalProof(raw []byte) (ServiceRestoreApprovalProof, error) {
	fields, e := exactJSONObject(raw)
	if e != nil || !exactFields(fields, restoreApprovalFields) {
		return ServiceRestoreApprovalProof{}, errCode("invalid_approval_proof")
	}
	var p ServiceRestoreApprovalProof
	if decodeSingle(raw, &p) != nil || p.validate() != nil {
		return p, errCode("invalid_approval_proof")
	}
	return normalizeServiceRestoreProof(p), nil
}
func (p ServiceRestoreApprovalProof) validate() error {
	if p.SchemaVersion != approvalSchemaVersion || p.Intent != serviceRestoreApprovalIntent || !approvalIdentifierPattern.MatchString(p.ApprovalID) || !approvalIdentifierPattern.MatchString(p.ChallengeID) || !approvalIdentifierPattern.MatchString(p.SignerKeyID) || !approvalIdentifierPattern.MatchString(p.RestoreID) || !approvalIdentifierPattern.MatchString(p.ServiceID) || p.ServiceRevision == 0 || !approvalIdentifierPattern.MatchString(p.DeploymentID) || p.DeploymentRevision == 0 || !approvalIdentifierPattern.MatchString(p.CloudConnectionID) || !approvalIdentifierPattern.MatchString(p.BackupID) || p.BackupRevision == 0 || !approvalIdentifierPattern.MatchString(p.RecipeID) || !namedSHA256Pattern.MatchString(p.RecipeDigest) || !destroyInstanceIDPattern.MatchString(p.InstanceID) || !regionPattern.MatchString(p.Region) || !ValidAvailabilityZone(p.Region, p.AvailabilityZone) || p.RestoreMode != "in_place" || !p.DowntimeRequired || p.OriginalVolumeRetention != "manual" || p.FailurePolicy != "reattach_original" || !approvalIdentifierPattern.MatchString(p.QuoteID) || p.Currency != "USD" || p.EstimatedHourlyMinor < 0 || p.EstimatedThirtyDayMinor <= 0 || p.QuoteValidUntil.IsZero() || len(p.VolumeSwaps) == 0 {
		return errCode("invalid_approval_proof")
	}
	if p.IssuedAt.IsZero() || p.ExpiresAt.IsZero() || p.IssuedAt.Location() != time.UTC || p.ExpiresAt.Location() != time.UTC || !p.ExpiresAt.After(p.IssuedAt) || p.ExpiresAt.Sub(p.IssuedAt) > 5*time.Minute || p.ExpiresAt.After(p.QuoteValidUntil) {
		return errCode("invalid_approval_proof")
	}
	if !validRestoreSwaps(p.VolumeSwaps) {
		return errCode("invalid_approval_proof")
	}
	sig, e := base64.RawURLEncoding.DecodeString(p.Signature)
	if e != nil || len(sig) != ed25519.SignatureSize {
		return errCode("invalid_approval_proof")
	}
	return nil
}
func normalizeServiceRestoreProof(p ServiceRestoreApprovalProof) ServiceRestoreApprovalProof {
	p.IssuedAt, p.ExpiresAt, p.QuoteValidUntil = p.IssuedAt.UTC(), p.ExpiresAt.UTC(), p.QuoteValidUntil.UTC()
	p.Unincluded = append([]string(nil), p.Unincluded...)
	sort.Strings(p.Unincluded)
	p.VolumeSwaps = append([]ServiceRestoreVolumeSwap(nil), p.VolumeSwaps...)
	sort.Slice(p.VolumeSwaps, func(i, j int) bool { return p.VolumeSwaps[i].DeviceName < p.VolumeSwaps[j].DeviceName })
	return p
}
func (p ServiceRestoreApprovalProof) SigningPayload() ([]byte, error) {
	if e := p.validate(); e != nil {
		return nil, e
	}
	p = normalizeServiceRestoreProof(p)
	return deterministicCBOR(serviceRestoreApprovalSigningPayload{p.SchemaVersion, serviceRestoreApprovalPayloadVersion, approvalHashAlgorithm, p.Intent, p.ApprovalID, p.ChallengeID, p.SignerKeyID, p.RestoreID, p.ServiceID, p.ServiceRevision, p.DeploymentID, p.DeploymentRevision, p.CloudConnectionID, p.BackupID, p.BackupRevision, p.RecipeID, p.RecipeDigest, p.InstanceID, p.Region, p.AvailabilityZone, p.RestoreMode, p.DowntimeRequired, p.OriginalVolumeRetention, p.FailurePolicy, p.QuoteID, p.Currency, p.EstimatedHourlyMinor, p.EstimatedThirtyDayMinor, p.QuoteValidUntil, p.Unincluded, p.VolumeSwaps, p.IssuedAt, p.ExpiresAt})
}
func (p ServiceRestoreApprovalProof) PayloadSHA256() (string, error) {
	raw, e := p.SigningPayload()
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func (p ServiceRestoreApprovalProof) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	if !p.ExpiresAt.After(now.UTC()) || !p.QuoteValidUntil.After(now.UTC()) {
		return errCode("approval_expired")
	}
	payload, e := p.SigningPayload()
	if e != nil {
		return e
	}
	sig, _ := base64.RawURLEncoding.DecodeString(p.Signature)
	if !ed25519.Verify(key, payload, sig) {
		return errCode("invalid_approval_signature")
	}
	return nil
}
func (c Command) ServiceRestoreApproval() (ServiceRestoreApprovalProof, error) {
	if c.Action != ActionServiceRestore || len(c.ApprovalProof) == 0 {
		return ServiceRestoreApprovalProof{}, errCode("invalid_approval_proof")
	}
	return ParseServiceRestoreApprovalProof(c.ApprovalProof)
}
func (c Command) ServiceRestoreApprovalPayloadSHA256() (string, error) {
	if e := c.ValidateServiceRestoreBinding(); e != nil {
		return "", e
	}
	p, _ := c.ServiceRestoreApproval()
	return p.PayloadSHA256()
}
func (c Command) ServiceRestoreRequest() (ServiceRestoreRequest, error) {
	if c.Action != ActionServiceRestore {
		return ServiceRestoreRequest{}, errCode("invalid_payload")
	}
	raw, e := decodeCanonicalBase64(c.PayloadB64)
	if e != nil {
		return ServiceRestoreRequest{}, errCode("invalid_payload")
	}
	fields, e := exactJSONObject(raw)
	if e != nil || !exactFields(fields, []string{"schema", "restore_id", "service_id", "deployment_id", "backup_id", "instance_id", "region", "availability_zone", "restore_mode", "downtime_required", "original_volume_retention", "failure_policy", "quote_id", "quote_valid_until", "volume_swaps"}) {
		return ServiceRestoreRequest{}, errCode("invalid_payload")
	}
	var r ServiceRestoreRequest
	if decodeSingle(raw, &r) != nil || r.validate() != nil {
		return r, errCode("invalid_payload")
	}
	return normalizeRestoreRequest(r), nil
}
func (r ServiceRestoreRequest) validate() error {
	if r.Schema != ServiceRestoreSchema || !approvalIdentifierPattern.MatchString(r.RestoreID) || !approvalIdentifierPattern.MatchString(r.ServiceID) || !approvalIdentifierPattern.MatchString(r.DeploymentID) || !approvalIdentifierPattern.MatchString(r.BackupID) || !destroyInstanceIDPattern.MatchString(r.InstanceID) || !regionPattern.MatchString(r.Region) || !ValidAvailabilityZone(r.Region, r.AvailabilityZone) || r.RestoreMode != "in_place" || !r.DowntimeRequired || r.OriginalVolumeRetention != "manual" || r.FailurePolicy != "reattach_original" || !approvalIdentifierPattern.MatchString(r.QuoteID) || len(r.VolumeSwaps) == 0 || !validRestoreSwaps(r.VolumeSwaps) {
		return errCode("invalid_payload")
	}
	valid, e := parseCanonicalInstant(r.QuoteValidUntil)
	if e != nil || valid.IsZero() {
		return errCode("invalid_payload")
	}
	return nil
}

func (r ServiceRestoreRequest) Validate() error { return r.validate() }
func normalizeRestoreRequest(r ServiceRestoreRequest) ServiceRestoreRequest {
	r.VolumeSwaps = append([]ServiceRestoreVolumeSwap(nil), r.VolumeSwaps...)
	sort.Slice(r.VolumeSwaps, func(i, j int) bool { return r.VolumeSwaps[i].DeviceName < r.VolumeSwaps[j].DeviceName })
	return r
}
func validRestoreSwaps(swaps []ServiceRestoreVolumeSwap) bool {
	originals, snaps, devices := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, s := range swaps {
		if !destroyVolumeIDPattern.MatchString(s.OriginalVolumeID) || !restorePlanSnapshotPattern.MatchString(s.SnapshotID) || !restorePlanDevicePattern.MatchString(s.DeviceName) || !validRestorePlanVolumeType(s.VolumeType) || s.SizeGiB <= 0 || s.IOPS < 0 || s.ThroughputMiB < 0 || !s.Encrypted || originals[s.OriginalVolumeID] || snaps[s.SnapshotID] || devices[s.DeviceName] {
			return false
		}
		originals[s.OriginalVolumeID], snaps[s.SnapshotID], devices[s.DeviceName] = true, true, true
	}
	return true
}
func (c Command) ValidateServiceRestoreBinding() error {
	r, e := c.ServiceRestoreRequest()
	if e != nil {
		return e
	}
	p, e := c.ServiceRestoreApproval()
	if e != nil {
		return e
	}
	p = normalizeServiceRestoreProof(p)
	valid, _ := parseCanonicalInstant(r.QuoteValidUntil)
	if p.CloudConnectionID != c.ConnectionID || p.RestoreID != r.RestoreID || p.ServiceID != r.ServiceID || p.DeploymentID != r.DeploymentID || p.BackupID != r.BackupID || p.InstanceID != r.InstanceID || p.Region != r.Region || p.AvailabilityZone != r.AvailabilityZone || p.RestoreMode != r.RestoreMode || p.DowntimeRequired != r.DowntimeRequired || p.OriginalVolumeRetention != r.OriginalVolumeRetention || p.FailurePolicy != r.FailurePolicy || p.QuoteID != r.QuoteID || !p.QuoteValidUntil.Equal(valid) || !reflect.DeepEqual(p.VolumeSwaps, r.VolumeSwaps) {
		return errCode("approval_scope_mismatch")
	}
	return nil
}
func MarshalCommittedServiceRestoreResult(c Command, evidence ServiceRestoreAWSEvidence) ([]byte, error) {
	if c.ValidateServiceRestoreBinding() != nil {
		return nil, errCode("invalid_result")
	}
	request, _ := c.ServiceRestoreRequest()
	normalizeRestoreEvidence(&evidence)
	if validateRestoreEvidence(request, evidence) != nil {
		return nil, errCode("provider_readback_invalid")
	}
	requestSHA, _ := c.RequestSHA256()
	status := "aws_restore_applied"
	if evidence.Outcome == "original_restored" {
		status = "aws_original_restored"
	} else if evidence.Outcome == "restore_blocked" {
		status = "restore_blocked"
	}
	return json.Marshal(ServiceRestoreResult{Schema: ServiceRestoreResultSchema, Status: status, Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: c.ConnectionID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, CommandID: c.CommandID, RequestSHA256: requestSHA, Action: ActionServiceRestore}, Restore: evidence})
}
func ValidateServiceRestoreResult(c Command, r ServiceRestoreResult) error {
	requestSHA, e := c.RequestSHA256()
	if e != nil || r.Schema != ServiceRestoreResultSchema || (r.Status != "aws_restore_applied" && r.Status != "aws_original_restored" && r.Status != "restore_blocked") || r.Receipt.Schema != ReceiptSchema || (r.Receipt.Disposition != "committed" && r.Receipt.Disposition != "idempotent") || r.Receipt.ConnectionID != c.ConnectionID || r.Receipt.ExpectedGeneration != c.ExpectedGeneration || r.Receipt.NodeCounter != c.NodeCounter || r.Receipt.CommandID != c.CommandID || r.Receipt.RequestSHA256 != requestSHA || r.Receipt.Action != ActionServiceRestore {
		return errCode("invalid_result")
	}
	request, e := c.ServiceRestoreRequest()
	if e != nil {
		return e
	}
	normalizeRestoreEvidence(&r.Restore)
	return validateRestoreEvidence(request, r.Restore)
}
func normalizeRestoreEvidence(e *ServiceRestoreAWSEvidence) {
	sort.Slice(e.Replacements, func(i, j int) bool { return e.Replacements[i].DeviceName < e.Replacements[j].DeviceName })
}
func validateRestoreEvidence(r ServiceRestoreRequest, e ServiceRestoreAWSEvidence) error {
	if e.RestoreID != r.RestoreID || e.ServiceID != r.ServiceID || e.DeploymentID != r.DeploymentID || e.BackupID != r.BackupID || e.InstanceID != r.InstanceID || e.Region != r.Region || e.AvailabilityZone != r.AvailabilityZone || (e.Outcome != "restored" && e.Outcome != "original_restored" && e.Outcome != "restore_blocked") || len(e.Replacements) != len(r.VolumeSwaps) {
		return errCode("invalid_result")
	}
	if e.Outcome == "restored" && (e.InstanceState != "running" || e.FallbackVerified) {
		return errCode("invalid_result")
	}
	if e.Outcome == "original_restored" && (e.InstanceState != "running" || !e.FallbackVerified) {
		return errCode("invalid_result")
	}
	for i, x := range e.Replacements {
		swap := r.VolumeSwaps[i]
		expectedState := "attached_current"
		if e.Outcome == "original_restored" {
			expectedState = "retained_detached"
		}
		if x.OriginalVolumeID != swap.OriginalVolumeID || x.SnapshotID != swap.SnapshotID || x.DeviceName != swap.DeviceName || !destroyVolumeIDPattern.MatchString(x.ReplacementVolumeID) || !x.Encrypted || x.DeleteOnTermination != swap.DeleteOnTermination || (e.Outcome != "restore_blocked" && x.State != expectedState) || (e.Outcome == "restore_blocked" && x.State != "attached_current" && x.State != "retained_detached" && x.State != "unknown") {
			return errCode("invalid_result")
		}
	}
	return nil
}

func ValidateServiceRestoreEvidence(request ServiceRestoreRequest, evidence ServiceRestoreAWSEvidence) error {
	request = normalizeRestoreRequest(request)
	normalizeRestoreEvidence(&evidence)
	if request.validate() != nil {
		return errCode("invalid_payload")
	}
	return validateRestoreEvidence(request, evidence)
}
