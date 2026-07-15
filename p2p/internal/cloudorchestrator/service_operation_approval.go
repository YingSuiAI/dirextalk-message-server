package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const ServiceOperationApprovalIntent = "service_operation"
const maxServiceOperationApprovalLifetime = 5 * time.Minute

type ServiceOperation string

const (
	ServiceOperationStart   ServiceOperation = "start"
	ServiceOperationStop    ServiceOperation = "stop"
	ServiceOperationRestart ServiceOperation = "restart"
)

type ServiceOperationTargetV1 struct {
	Operation               ServiceOperation `json:"operation"`
	ServiceID               string           `json:"service_id"`
	ServiceRevision         uint64           `json:"service_revision"`
	ExpectedServiceStatus   string           `json:"expected_service_status"`
	DeploymentID            string           `json:"deployment_id"`
	DeploymentRevision      uint64           `json:"deployment_revision"`
	CloudConnectionID       string           `json:"cloud_connection_id"`
	RecipeID                string           `json:"recipe_id"`
	RecipeDigest            string           `json:"recipe_digest"`
	InstalledManifestDigest string           `json:"installed_manifest_digest"`
	ArtifactDigest          string           `json:"artifact_digest"`
	ActionID                string           `json:"action_id"`
	RootRequired            bool             `json:"root_required"`
	TimeoutSeconds          uint32           `json:"timeout_seconds"`
	CheckpointSequence      []string         `json:"checkpoint_sequence"`
}

type ServiceOperationApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	ServiceOperationTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type serviceOperationApprovalPayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	ServiceOperationTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

var ErrServiceOperationApprovalBinding = errors.New("service operation approval does not match the current service capability")

func (target ServiceOperationTargetV1) Validate() error {
	if target.Operation != ServiceOperationStart && target.Operation != ServiceOperationStop && target.Operation != ServiceOperationRestart {
		return errors.New("service operation is invalid")
	}
	for label, value := range map[string]string{"service_id": target.ServiceID, "deployment_id": target.DeploymentID, "cloud_connection_id": target.CloudConnectionID, "recipe_id": target.RecipeID, "action_id": target.ActionID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	for label, value := range map[string]string{"recipe_digest": target.RecipeDigest, "installed_manifest_digest": target.InstalledManifestDigest, "artifact_digest": target.ArtifactDigest} {
		if err := validateDigest(label, value); err != nil {
			return err
		}
	}
	if target.ServiceRevision == 0 || target.DeploymentRevision == 0 {
		return errors.New("service operation revisions must be positive")
	}
	if !validServiceOperationStatus(target.Operation, target.ExpectedServiceStatus) {
		return errors.New("service operation is not valid for the expected service status")
	}
	if !target.RootRequired {
		return errors.New("first service operation requires the approved root capability")
	}
	if target.TimeoutSeconds == 0 || target.TimeoutSeconds > 3600 {
		return errors.New("service operation timeout must be between 1 and 3600 seconds")
	}
	return validateCheckpointSequence(target.CheckpointSequence)
}

func validServiceOperationStatus(operation ServiceOperation, status string) bool {
	switch operation {
	case ServiceOperationStart:
		return status == "stopped" || status == "degraded"
	case ServiceOperationStop, ServiceOperationRestart:
		return status == "experimental" || status == "active" || status == "degraded"
	default:
		return false
	}
}

func NewServiceOperationApprovalV1(target ServiceOperationTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (ServiceOperationApprovalV1, error) {
	if err := target.Validate(); err != nil {
		return ServiceOperationApprovalV1{}, fmt.Errorf("invalid service operation target: %w", err)
	}
	a := ServiceOperationApprovalV1{SchemaVersion: SchemaVersionV1, Intent: ServiceOperationApprovalIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, ServiceOperationTargetV1: cloneServiceOperationTarget(target), IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	if err := a.Validate(); err != nil {
		return ServiceOperationApprovalV1{}, err
	}
	return a, nil
}

func (a ServiceOperationApprovalV1) Validate() error { return a.validate(false) }
func (a ServiceOperationApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("service operation approval has expired")
	}
	return nil
}
func (a ServiceOperationApprovalV1) validate(requireSignature bool) error {
	if validateSchema(a.SchemaVersion) != nil || a.Intent != ServiceOperationApprovalIntent {
		return errors.New("service operation approval schema or intent is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := a.ServiceOperationTargetV1.Validate(); err != nil {
		return err
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxServiceOperationApprovalLifetime {
		return errors.New("service operation approval expiry is invalid")
	}
	if requireSignature || a.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("service operation approval signature is invalid")
		}
	}
	return nil
}

func (a ServiceOperationApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	a.ServiceOperationTargetV1 = cloneServiceOperationTarget(a.ServiceOperationTargetV1)
	return canonicalCBOR(serviceOperationApprovalPayloadV1{SchemaVersion: a.SchemaVersion, PayloadVersion: "service-operation-approval-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: a.Intent, ApprovalID: a.ApprovalID, ChallengeID: a.ChallengeID, SignerKeyID: a.SignerKeyID, ServiceOperationTargetV1: a.ServiceOperationTargetV1, IssuedAt: a.IssuedAt.UTC(), ExpiresAt: a.ExpiresAt.UTC()})
}
func (a ServiceOperationApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (ServiceOperationApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || a.validate(false) != nil || !a.ExpiresAt.After(now.UTC()) {
		return ServiceOperationApprovalV1{}, errors.New("service operation approval cannot be signed")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ServiceOperationApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}
func (a ServiceOperationApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("service operation approval key is invalid")
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
		return errors.New("service operation approval signature is invalid")
	}
	return nil
}
func (a ServiceOperationApprovalV1) ValidateAgainst(target ServiceOperationTargetV1, now time.Time) error {
	if a.ValidateAt(now) != nil || target.Validate() != nil || !reflect.DeepEqual(cloneServiceOperationTarget(a.ServiceOperationTargetV1), cloneServiceOperationTarget(target)) {
		return ErrServiceOperationApprovalBinding
	}
	return nil
}
func cloneServiceOperationTarget(target ServiceOperationTargetV1) ServiceOperationTargetV1 {
	target.CheckpointSequence = append([]string(nil), target.CheckpointSequence...)
	return target
}
