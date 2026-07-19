package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

const (
	ServiceBackupApprovalIntent      = "service_backup"
	ServiceBackupRetentionManual     = "manual"
	maxServiceBackupApprovalLifetime = 5 * time.Minute
)

type ServiceBackupTargetV1 struct {
	BackupID           string   `json:"backup_id"`
	ServiceID          string   `json:"service_id"`
	ServiceRevision    uint64   `json:"service_revision"`
	DeploymentID       string   `json:"deployment_id"`
	DeploymentRevision uint64   `json:"deployment_revision"`
	CloudConnectionID  string   `json:"cloud_connection_id"`
	RecipeID           string   `json:"recipe_id"`
	RecipeDigest       string   `json:"recipe_digest"`
	InstanceID         string   `json:"instance_id"`
	VolumeIDs          []string `json:"volume_ids"`
	RetentionPolicy    string   `json:"retention_policy"`
}

type ServiceBackupApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	ServiceBackupTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type serviceBackupApprovalPayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	ServiceBackupTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

var ErrServiceBackupApprovalBinding = errors.New("service backup approval does not match the tracked service volumes")

func (target ServiceBackupTargetV1) Validate() error {
	for label, value := range map[string]string{"backup_id": target.BackupID, "service_id": target.ServiceID, "deployment_id": target.DeploymentID, "cloud_connection_id": target.CloudConnectionID, "recipe_id": target.RecipeID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if target.ServiceRevision == 0 || target.DeploymentRevision == 0 {
		return errors.New("service backup revisions must be positive")
	}
	if validateDigest("recipe_digest", target.RecipeDigest) != nil || !ec2InstanceIDPattern.MatchString(target.InstanceID) {
		return errors.New("service backup binding is invalid")
	}
	if err := validateProviderResourceIDs("volume_ids", target.VolumeIDs, ebsVolumeIDPattern); err != nil {
		return err
	}
	if target.RetentionPolicy != ServiceBackupRetentionManual {
		return errors.New("service backup retention policy is invalid")
	}
	return nil
}

func NewServiceBackupApprovalV1(target ServiceBackupTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (ServiceBackupApprovalV1, error) {
	target = normalizeServiceBackupTarget(target)
	if err := target.Validate(); err != nil {
		return ServiceBackupApprovalV1{}, fmt.Errorf("invalid service backup target: %w", err)
	}
	a := ServiceBackupApprovalV1{SchemaVersion: SchemaVersionV1, Intent: ServiceBackupApprovalIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, ServiceBackupTargetV1: target, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	if err := a.Validate(); err != nil {
		return ServiceBackupApprovalV1{}, err
	}
	return a, nil
}

func (a ServiceBackupApprovalV1) Validate() error { return a.validate(false) }
func (a ServiceBackupApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("service backup approval has expired")
	}
	return nil
}
func (a ServiceBackupApprovalV1) validate(requireSignature bool) error {
	if validateSchema(a.SchemaVersion) != nil || a.Intent != ServiceBackupApprovalIntent {
		return errors.New("service backup approval schema or intent is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := a.ServiceBackupTargetV1.Validate(); err != nil {
		return err
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxServiceBackupApprovalLifetime {
		return errors.New("service backup approval expiry is invalid")
	}
	if requireSignature || a.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("service backup approval signature is invalid")
		}
	}
	return nil
}

func (a ServiceBackupApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	a.ServiceBackupTargetV1 = normalizeServiceBackupTarget(a.ServiceBackupTargetV1)
	return canonicalCBOR(serviceBackupApprovalPayloadV1{SchemaVersion: a.SchemaVersion, PayloadVersion: "service-backup-approval-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: a.Intent, ApprovalID: a.ApprovalID, ChallengeID: a.ChallengeID, SignerKeyID: a.SignerKeyID, ServiceBackupTargetV1: a.ServiceBackupTargetV1, IssuedAt: a.IssuedAt.UTC(), ExpiresAt: a.ExpiresAt.UTC()})
}
func (a ServiceBackupApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (ServiceBackupApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || a.validate(false) != nil || !a.ExpiresAt.After(now.UTC()) {
		return ServiceBackupApprovalV1{}, errors.New("service backup approval cannot be signed")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ServiceBackupApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}
func (a ServiceBackupApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("service backup approval key is invalid")
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
		return errors.New("service backup approval signature is invalid")
	}
	return nil
}
func (a ServiceBackupApprovalV1) ValidateAgainst(target ServiceBackupTargetV1, now time.Time) error {
	if a.ValidateAt(now) != nil || target.Validate() != nil || !reflect.DeepEqual(normalizeServiceBackupTarget(a.ServiceBackupTargetV1), normalizeServiceBackupTarget(target)) {
		return ErrServiceBackupApprovalBinding
	}
	return nil
}
func normalizeServiceBackupTarget(target ServiceBackupTargetV1) ServiceBackupTargetV1 {
	target.VolumeIDs = append([]string(nil), target.VolumeIDs...)
	sort.Strings(target.VolumeIDs)
	return target
}
