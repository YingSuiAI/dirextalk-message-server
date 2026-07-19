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
	DeploymentDestroyApprovalIntent      = "deployment_destroy"
	maxDeploymentDestroyApprovalLifetime = 5 * time.Minute
)

// DeploymentDestroyTargetV1 is resolved from the private provider resource
// ledger. It deliberately does not require a Service, readiness evidence, or
// a successful deployment outcome: retained resources must remain destroyable
// after a failed, interrupted, or canceled workload.
type DeploymentDestroyTargetV1 struct {
	DeploymentID        string   `json:"deployment_id"`
	DeploymentRevision  uint64   `json:"deployment_revision"`
	PlanID              string   `json:"plan_id"`
	CloudConnectionID   string   `json:"cloud_connection_id"`
	ResourceStatus      string   `json:"resource_status"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
	SecretRefs          []string `json:"secret_refs,omitempty"`
}

// DeploymentDestroyApprovalV1 is the sole owner authorization for destroying
// the exact tracked resources of one Deployment without manufacturing a
// Service fact. It cannot authorize replacement resources or arbitrary AWS
// operations.
type DeploymentDestroyApprovalV1 struct {
	SchemaVersion       string    `json:"schema_version"`
	Intent              string    `json:"intent"`
	ApprovalID          string    `json:"approval_id"`
	ChallengeID         string    `json:"challenge_id"`
	SignerKeyID         string    `json:"signer_key_id"`
	DeploymentID        string    `json:"deployment_id"`
	DeploymentRevision  uint64    `json:"deployment_revision"`
	PlanID              string    `json:"plan_id"`
	CloudConnectionID   string    `json:"cloud_connection_id"`
	ResourceStatus      string    `json:"resource_status"`
	InstanceID          string    `json:"instance_id"`
	VolumeIDs           []string  `json:"volume_ids"`
	NetworkInterfaceIDs []string  `json:"network_interface_ids"`
	SecretRefs          []string  `json:"secret_refs,omitempty"`
	IssuedAt            time.Time `json:"issued_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	Signature           string    `json:"signature,omitempty"`
}

type deploymentDestroyApprovalSigningPayloadV1 struct {
	SchemaVersion       string    `json:"schema_version"`
	PayloadVersion      string    `json:"payload_version"`
	HashAlgorithm       string    `json:"hash_algorithm"`
	Intent              string    `json:"intent"`
	ApprovalID          string    `json:"approval_id"`
	ChallengeID         string    `json:"challenge_id"`
	SignerKeyID         string    `json:"signer_key_id"`
	DeploymentID        string    `json:"deployment_id"`
	DeploymentRevision  uint64    `json:"deployment_revision"`
	PlanID              string    `json:"plan_id"`
	CloudConnectionID   string    `json:"cloud_connection_id"`
	ResourceStatus      string    `json:"resource_status"`
	InstanceID          string    `json:"instance_id"`
	VolumeIDs           []string  `json:"volume_ids"`
	NetworkInterfaceIDs []string  `json:"network_interface_ids"`
	SecretRefs          []string  `json:"secret_refs,omitempty"`
	IssuedAt            time.Time `json:"issued_at"`
	ExpiresAt           time.Time `json:"expires_at"`
}

var ErrDeploymentDestroyApprovalBinding = errors.New("deployment destroy approval does not match the tracked deployment resources")

func NewDeploymentDestroyApprovalV1(target DeploymentDestroyTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (DeploymentDestroyApprovalV1, error) {
	target = normalizeDeploymentDestroyTarget(target)
	if err := target.Validate(); err != nil {
		return DeploymentDestroyApprovalV1{}, fmt.Errorf("invalid deployment destroy target: %w", err)
	}
	approval := DeploymentDestroyApprovalV1{
		SchemaVersion: SchemaVersionV1, Intent: DeploymentDestroyApprovalIntent,
		ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID,
		DeploymentID: target.DeploymentID, DeploymentRevision: target.DeploymentRevision,
		PlanID: target.PlanID, CloudConnectionID: target.CloudConnectionID, ResourceStatus: target.ResourceStatus,
		InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs, SecretRefs: target.SecretRefs,
		IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC(),
	}
	if err := approval.Validate(); err != nil {
		return DeploymentDestroyApprovalV1{}, fmt.Errorf("invalid deployment destroy approval challenge: %w", err)
	}
	return approval, nil
}

func (target DeploymentDestroyTargetV1) Validate() error {
	for label, value := range map[string]string{
		"deployment_id": target.DeploymentID, "plan_id": target.PlanID, "cloud_connection_id": target.CloudConnectionID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if target.DeploymentRevision == 0 {
		return errors.New("deployment destroy revision must be positive")
	}
	switch target.ResourceStatus {
	case "active", "retained_tracked", "blocked", "orphaned":
	default:
		return errors.New("deployment destroy resource status is invalid")
	}
	if !ec2InstanceIDPattern.MatchString(target.InstanceID) || rejectSecretMaterial("instance_id", target.InstanceID) != nil {
		return errors.New("instance_id is invalid")
	}
	if err := validateProviderResourceIDs("volume_ids", target.VolumeIDs, ebsVolumeIDPattern); err != nil {
		return err
	}
	if err := validateProviderResourceIDs("network_interface_ids", target.NetworkInterfaceIDs, ec2NetworkInterfaceIDPattern); err != nil {
		return err
	}
	return validateServiceDestroySecretRefs(target.SecretRefs)
}

func (approval DeploymentDestroyApprovalV1) Validate() error { return approval.validate(false) }

func (approval DeploymentDestroyApprovalV1) ValidateAt(now time.Time) error {
	if err := approval.validate(true); err != nil {
		return err
	}
	now = now.UTC()
	if approval.IssuedAt.After(now) {
		return errors.New("deployment destroy approval has not been issued yet")
	}
	if !approval.ExpiresAt.After(now) {
		return errors.New("deployment destroy approval has expired")
	}
	return nil
}

func (approval DeploymentDestroyApprovalV1) validate(requireSignature bool) error {
	if err := validateSchema(approval.SchemaVersion); err != nil {
		return err
	}
	if approval.Intent != DeploymentDestroyApprovalIntent {
		return errors.New("deployment destroy approval intent is invalid")
	}
	for label, value := range map[string]string{
		"approval_id": approval.ApprovalID, "challenge_id": approval.ChallengeID, "signer_key_id": approval.SignerKeyID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := approval.Target().Validate(); err != nil {
		return err
	}
	if approval.IssuedAt.IsZero() || approval.ExpiresAt.IsZero() || !approval.ExpiresAt.After(approval.IssuedAt) || approval.ExpiresAt.Sub(approval.IssuedAt) > maxDeploymentDestroyApprovalLifetime {
		return errors.New("deployment destroy approval expiry must be within five minutes of issuance")
	}
	if requireSignature || approval.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("deployment destroy approval signature must be a base64url Ed25519 signature")
		}
	}
	return nil
}

func (approval DeploymentDestroyApprovalV1) Target() DeploymentDestroyTargetV1 {
	return normalizeDeploymentDestroyTarget(DeploymentDestroyTargetV1{
		DeploymentID: approval.DeploymentID, DeploymentRevision: approval.DeploymentRevision,
		PlanID: approval.PlanID, CloudConnectionID: approval.CloudConnectionID, ResourceStatus: approval.ResourceStatus,
		InstanceID: approval.InstanceID, VolumeIDs: approval.VolumeIDs, NetworkInterfaceIDs: approval.NetworkInterfaceIDs, SecretRefs: approval.SecretRefs,
	})
}

func (approval DeploymentDestroyApprovalV1) SigningPayload() ([]byte, error) {
	if err := approval.Validate(); err != nil {
		return nil, err
	}
	approval = normalizeDeploymentDestroyApproval(approval)
	return canonicalCBOR(deploymentDestroyApprovalSigningPayloadV1{
		SchemaVersion: approval.SchemaVersion, PayloadVersion: "deployment-destroy-approval-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256,
		Intent: approval.Intent, ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
		DeploymentID: approval.DeploymentID, DeploymentRevision: approval.DeploymentRevision,
		PlanID: approval.PlanID, CloudConnectionID: approval.CloudConnectionID, ResourceStatus: approval.ResourceStatus,
		InstanceID: approval.InstanceID, VolumeIDs: approval.VolumeIDs, NetworkInterfaceIDs: approval.NetworkInterfaceIDs, SecretRefs: approval.SecretRefs,
		IssuedAt: approval.IssuedAt, ExpiresAt: approval.ExpiresAt,
	})
}

func (approval DeploymentDestroyApprovalV1) Sign(privateKey ed25519.PrivateKey, now time.Time) (DeploymentDestroyApprovalV1, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return DeploymentDestroyApprovalV1{}, errors.New("deployment destroy approval signing key is not an Ed25519 private key")
	}
	now = now.UTC()
	if err := approval.validate(false); err != nil {
		return DeploymentDestroyApprovalV1{}, err
	}
	if approval.IssuedAt.After(now) || !approval.ExpiresAt.After(now) {
		return DeploymentDestroyApprovalV1{}, errors.New("deployment destroy approval is not currently signable")
	}
	approval = normalizeDeploymentDestroyApproval(approval)
	payload, err := approval.SigningPayload()
	if err != nil {
		return DeploymentDestroyApprovalV1{}, err
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return approval, nil
}

func (approval DeploymentDestroyApprovalV1) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("deployment destroy approval verification key is not an Ed25519 public key")
	}
	if err := approval.ValidateAt(now); err != nil {
		return err
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(approval.Signature)
	if !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("deployment destroy approval signature is invalid")
	}
	return nil
}

func (approval DeploymentDestroyApprovalV1) ValidateAgainst(target DeploymentDestroyTargetV1, now time.Time) error {
	if err := approval.ValidateAt(now); err != nil {
		return err
	}
	target = normalizeDeploymentDestroyTarget(target)
	if err := target.Validate(); err != nil || !reflect.DeepEqual(approval.Target(), target) {
		return ErrDeploymentDestroyApprovalBinding
	}
	return nil
}

func normalizeDeploymentDestroyTarget(target DeploymentDestroyTargetV1) DeploymentDestroyTargetV1 {
	target.VolumeIDs = append([]string(nil), target.VolumeIDs...)
	target.NetworkInterfaceIDs = append([]string(nil), target.NetworkInterfaceIDs...)
	target.SecretRefs = append([]string(nil), target.SecretRefs...)
	sort.Strings(target.VolumeIDs)
	sort.Strings(target.NetworkInterfaceIDs)
	sort.Strings(target.SecretRefs)
	return target
}

func normalizeDeploymentDestroyApproval(approval DeploymentDestroyApprovalV1) DeploymentDestroyApprovalV1 {
	approval.IssuedAt = approval.IssuedAt.UTC()
	approval.ExpiresAt = approval.ExpiresAt.UTC()
	target := approval.Target()
	approval.VolumeIDs = target.VolumeIDs
	approval.NetworkInterfaceIDs = target.NetworkInterfaceIDs
	approval.SecretRefs = target.SecretRefs
	return approval
}
