package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ServiceRestoreApprovalIntent          = "service_restore"
	ServiceRestoreModeInPlace             = "in_place"
	ServiceRestoreRetentionManual         = "manual"
	ServiceRestoreFailureReattachOriginal = "reattach_original"
	maxServiceRestoreApprovalLifetime     = 5 * time.Minute
)

var (
	serviceRestoreSnapshotIDPattern = regexp.MustCompile(`^snap-[0-9a-f]{8,17}$`)
	serviceRestoreDevicePattern     = regexp.MustCompile(`^/dev/(?:xvd|sd)[a-z][0-9]*$`)
	serviceRestoreRegionPattern     = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-\d$`)
	serviceRestoreAZPattern         = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-\d[a-z]$`)
	serviceRestoreCurrencyPattern   = regexp.MustCompile(`^[A-Z]{3}$`)
)

type ServiceRestoreVolumeSwapV1 struct {
	OriginalVolumeID    string `json:"original_volume_id"`
	SnapshotID          string `json:"snapshot_id"`
	DeviceName          string `json:"device_name"`
	VolumeType          string `json:"volume_type"`
	SizeGiB             int64  `json:"size_gib"`
	IOPS                int64  `json:"iops"`
	ThroughputMiB       int64  `json:"throughput_mib"`
	Encrypted           bool   `json:"encrypted"`
	DeleteOnTermination bool   `json:"delete_on_termination"`
}

type ServiceRestoreTargetV1 struct {
	RestoreID               string                       `json:"restore_id"`
	ServiceID               string                       `json:"service_id"`
	ServiceRevision         uint64                       `json:"service_revision"`
	DeploymentID            string                       `json:"deployment_id"`
	DeploymentRevision      uint64                       `json:"deployment_revision"`
	CloudConnectionID       string                       `json:"cloud_connection_id"`
	BackupID                string                       `json:"backup_id"`
	BackupRevision          uint64                       `json:"backup_revision"`
	RecipeID                string                       `json:"recipe_id"`
	RecipeDigest            string                       `json:"recipe_digest"`
	InstanceID              string                       `json:"instance_id"`
	Region                  string                       `json:"region"`
	AvailabilityZone        string                       `json:"availability_zone"`
	RestoreMode             string                       `json:"restore_mode"`
	DowntimeRequired        bool                         `json:"downtime_required"`
	OriginalVolumeRetention string                       `json:"original_volume_retention"`
	FailurePolicy           string                       `json:"failure_policy"`
	QuoteID                 string                       `json:"quote_id"`
	Currency                string                       `json:"currency"`
	EstimatedHourlyMinor    int64                        `json:"estimated_hourly_minor"`
	EstimatedThirtyDayMinor int64                        `json:"estimated_thirty_day_minor"`
	QuoteValidUntil         time.Time                    `json:"quote_valid_until"`
	Unincluded              []string                     `json:"unincluded"`
	VolumeSwaps             []ServiceRestoreVolumeSwapV1 `json:"volume_swaps"`
}

type ServiceRestoreApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	ServiceRestoreTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type serviceRestoreApprovalPayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	ServiceRestoreTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

var ErrServiceRestoreApprovalBinding = errors.New("service restore approval does not match the planned in-place volume swap")

func (target ServiceRestoreTargetV1) Validate() error { return target.ValidateAt(time.Time{}) }

func (target ServiceRestoreTargetV1) ValidateAt(now time.Time) error {
	for label, value := range map[string]string{"restore_id": target.RestoreID, "service_id": target.ServiceID, "deployment_id": target.DeploymentID, "cloud_connection_id": target.CloudConnectionID, "backup_id": target.BackupID, "recipe_id": target.RecipeID, "quote_id": target.QuoteID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if target.ServiceRevision == 0 || target.DeploymentRevision == 0 || target.BackupRevision == 0 {
		return errors.New("service restore revisions must be positive")
	}
	if validateDigest("recipe_digest", target.RecipeDigest) != nil || !ec2InstanceIDPattern.MatchString(target.InstanceID) {
		return errors.New("service restore binding is invalid")
	}
	if !serviceRestoreRegionPattern.MatchString(target.Region) || !serviceRestoreAZPattern.MatchString(target.AvailabilityZone) || len(target.AvailabilityZone) != len(target.Region)+1 || !strings.HasPrefix(target.AvailabilityZone, target.Region) {
		return errors.New("service restore location is invalid")
	}
	if target.RestoreMode != ServiceRestoreModeInPlace || !target.DowntimeRequired || target.OriginalVolumeRetention != ServiceRestoreRetentionManual || target.FailurePolicy != ServiceRestoreFailureReattachOriginal {
		return errors.New("service restore rollback policy is unsafe")
	}
	if !serviceRestoreCurrencyPattern.MatchString(target.Currency) || target.EstimatedHourlyMinor < 0 || target.EstimatedThirtyDayMinor <= 0 || target.QuoteValidUntil.IsZero() || (!now.IsZero() && (!target.QuoteValidUntil.After(now.UTC()) || target.QuoteValidUntil.Sub(now.UTC()) > 15*time.Minute)) {
		return errors.New("service restore quote is invalid")
	}
	if len(target.VolumeSwaps) == 0 {
		return errors.New("service restore volume swaps are empty")
	}
	originals, snapshots, devices := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, swap := range target.VolumeSwaps {
		if !ebsVolumeIDPattern.MatchString(swap.OriginalVolumeID) || !serviceRestoreSnapshotIDPattern.MatchString(swap.SnapshotID) || !serviceRestoreDevicePattern.MatchString(swap.DeviceName) || originals[swap.OriginalVolumeID] || snapshots[swap.SnapshotID] || devices[swap.DeviceName] {
			return errors.New("service restore volume identity is invalid")
		}
		if !validRestoreVolumeType(swap.VolumeType) || swap.SizeGiB <= 0 || swap.IOPS < 0 || swap.ThroughputMiB < 0 || !swap.Encrypted {
			return errors.New("service restore volume specification is invalid")
		}
		originals[swap.OriginalVolumeID], snapshots[swap.SnapshotID], devices[swap.DeviceName] = true, true, true
	}
	return nil
}

func validRestoreVolumeType(value string) bool {
	switch value {
	case "gp2", "gp3", "io1", "io2", "st1", "sc1", "standard":
		return true
	default:
		return false
	}
}

func NewServiceRestoreApprovalV1(target ServiceRestoreTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (ServiceRestoreApprovalV1, error) {
	target = normalizeServiceRestoreTarget(target)
	if err := target.ValidateAt(issuedAt); err != nil {
		return ServiceRestoreApprovalV1{}, fmt.Errorf("invalid service restore target: %w", err)
	}
	a := ServiceRestoreApprovalV1{SchemaVersion: SchemaVersionV1, Intent: ServiceRestoreApprovalIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, ServiceRestoreTargetV1: target, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	if err := a.Validate(); err != nil {
		return ServiceRestoreApprovalV1{}, err
	}
	return a, nil
}

func (a ServiceRestoreApprovalV1) Validate() error { return a.validate(false) }
func (a ServiceRestoreApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) || !a.QuoteValidUntil.After(now.UTC()) {
		return errors.New("service restore approval or quote has expired")
	}
	return nil
}
func (a ServiceRestoreApprovalV1) validate(requireSignature bool) error {
	if validateSchema(a.SchemaVersion) != nil || a.Intent != ServiceRestoreApprovalIntent {
		return errors.New("service restore approval schema or intent is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := a.ServiceRestoreTargetV1.ValidateAt(a.IssuedAt); err != nil {
		return err
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxServiceRestoreApprovalLifetime || a.ExpiresAt.After(a.QuoteValidUntil) {
		return errors.New("service restore approval expiry is invalid")
	}
	if requireSignature || a.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("service restore approval signature is invalid")
		}
	}
	return nil
}
func (a ServiceRestoreApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	a.ServiceRestoreTargetV1 = normalizeServiceRestoreTarget(a.ServiceRestoreTargetV1)
	return canonicalCBOR(serviceRestoreApprovalPayloadV1{SchemaVersion: a.SchemaVersion, PayloadVersion: "service-restore-approval-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: a.Intent, ApprovalID: a.ApprovalID, ChallengeID: a.ChallengeID, SignerKeyID: a.SignerKeyID, ServiceRestoreTargetV1: a.ServiceRestoreTargetV1, IssuedAt: a.IssuedAt.UTC(), ExpiresAt: a.ExpiresAt.UTC()})
}
func (a ServiceRestoreApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (ServiceRestoreApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || a.validate(false) != nil || !a.ExpiresAt.After(now.UTC()) {
		return ServiceRestoreApprovalV1{}, errors.New("service restore approval cannot be signed")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ServiceRestoreApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}
func (a ServiceRestoreApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("service restore approval key is invalid")
	}
	if err := a.ValidateAt(now); err != nil {
		return err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(a.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("service restore approval signature is invalid")
	}
	return nil
}
func (a ServiceRestoreApprovalV1) ValidateAgainst(target ServiceRestoreTargetV1, now time.Time) error {
	if a.ValidateAt(now) != nil || target.ValidateAt(now) != nil || !reflect.DeepEqual(normalizeServiceRestoreTarget(a.ServiceRestoreTargetV1), normalizeServiceRestoreTarget(target)) {
		return ErrServiceRestoreApprovalBinding
	}
	return nil
}
func normalizeServiceRestoreTarget(target ServiceRestoreTargetV1) ServiceRestoreTargetV1 {
	target.QuoteValidUntil = target.QuoteValidUntil.UTC()
	target.Unincluded = append([]string(nil), target.Unincluded...)
	sort.Strings(target.Unincluded)
	target.VolumeSwaps = append([]ServiceRestoreVolumeSwapV1(nil), target.VolumeSwaps...)
	sort.Slice(target.VolumeSwaps, func(i, j int) bool { return target.VolumeSwaps[i].DeviceName < target.VolumeSwaps[j].DeviceName })
	return target
}
