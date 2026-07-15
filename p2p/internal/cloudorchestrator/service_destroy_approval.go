package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"time"
)

const (
	ServiceDestroyApprovalIntent      = "service_destroy"
	maxServiceDestroyApprovalLifetime = 5 * time.Minute
)

var (
	ec2InstanceIDPattern         = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	ebsVolumeIDPattern           = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	ec2NetworkInterfaceIDPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
)

// ServiceDestroyTargetV1 is resolved from tracked provider read-back facts.
// No public API may supply or widen this resource set. Every identifier is
// repeated in the device-signed approval so a stale service revision cannot
// delete a newly replaced resource.
type ServiceDestroyTargetV1 struct {
	ServiceID           string   `json:"service_id"`
	ServiceRevision     uint64   `json:"service_revision"`
	DeploymentID        string   `json:"deployment_id"`
	DeploymentRevision  uint64   `json:"deployment_revision"`
	CloudConnectionID   string   `json:"cloud_connection_id"`
	RecipeID            string   `json:"recipe_id"`
	RecipeDigest        string   `json:"recipe_digest"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
}

// ServiceDestroyApprovalV1 is the sole owner authorization for destroying
// the exact tracked resources of one Service. It cannot authorize ingress,
// arbitrary AWS APIs, a replacement resource, or implicit retention.
type ServiceDestroyApprovalV1 struct {
	SchemaVersion       string    `json:"schema_version"`
	Intent              string    `json:"intent"`
	ApprovalID          string    `json:"approval_id"`
	ChallengeID         string    `json:"challenge_id"`
	SignerKeyID         string    `json:"signer_key_id"`
	ServiceID           string    `json:"service_id"`
	ServiceRevision     uint64    `json:"service_revision"`
	DeploymentID        string    `json:"deployment_id"`
	DeploymentRevision  uint64    `json:"deployment_revision"`
	CloudConnectionID   string    `json:"cloud_connection_id"`
	RecipeID            string    `json:"recipe_id"`
	RecipeDigest        string    `json:"recipe_digest"`
	InstanceID          string    `json:"instance_id"`
	VolumeIDs           []string  `json:"volume_ids"`
	NetworkInterfaceIDs []string  `json:"network_interface_ids"`
	IssuedAt            time.Time `json:"issued_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	Signature           string    `json:"signature,omitempty"`
}

type serviceDestroyApprovalSigningPayloadV1 struct {
	SchemaVersion       string    `json:"schema_version"`
	PayloadVersion      string    `json:"payload_version"`
	HashAlgorithm       string    `json:"hash_algorithm"`
	Intent              string    `json:"intent"`
	ApprovalID          string    `json:"approval_id"`
	ChallengeID         string    `json:"challenge_id"`
	SignerKeyID         string    `json:"signer_key_id"`
	ServiceID           string    `json:"service_id"`
	ServiceRevision     uint64    `json:"service_revision"`
	DeploymentID        string    `json:"deployment_id"`
	DeploymentRevision  uint64    `json:"deployment_revision"`
	CloudConnectionID   string    `json:"cloud_connection_id"`
	RecipeID            string    `json:"recipe_id"`
	RecipeDigest        string    `json:"recipe_digest"`
	InstanceID          string    `json:"instance_id"`
	VolumeIDs           []string  `json:"volume_ids"`
	NetworkInterfaceIDs []string  `json:"network_interface_ids"`
	IssuedAt            time.Time `json:"issued_at"`
	ExpiresAt           time.Time `json:"expires_at"`
}

var ErrServiceDestroyApprovalBinding = errors.New("service destroy approval does not match the tracked service resources")

func NewServiceDestroyApprovalV1(target ServiceDestroyTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (ServiceDestroyApprovalV1, error) {
	target = normalizeServiceDestroyTarget(target)
	if err := target.Validate(); err != nil {
		return ServiceDestroyApprovalV1{}, fmt.Errorf("invalid service destroy target: %w", err)
	}
	approval := ServiceDestroyApprovalV1{
		SchemaVersion: SchemaVersionV1, Intent: ServiceDestroyApprovalIntent,
		ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID,
		ServiceID: target.ServiceID, ServiceRevision: target.ServiceRevision,
		DeploymentID: target.DeploymentID, DeploymentRevision: target.DeploymentRevision,
		CloudConnectionID: target.CloudConnectionID, RecipeID: target.RecipeID, RecipeDigest: target.RecipeDigest,
		InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs,
		IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC(),
	}
	if err := approval.Validate(); err != nil {
		return ServiceDestroyApprovalV1{}, fmt.Errorf("invalid service destroy approval challenge: %w", err)
	}
	return approval, nil
}

func (target ServiceDestroyTargetV1) Validate() error {
	for label, value := range map[string]string{
		"service_id": target.ServiceID, "deployment_id": target.DeploymentID,
		"cloud_connection_id": target.CloudConnectionID, "recipe_id": target.RecipeID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if target.ServiceRevision == 0 || target.DeploymentRevision == 0 {
		return errors.New("service destroy revisions must be positive")
	}
	if err := validateDigest("recipe_digest", target.RecipeDigest); err != nil {
		return err
	}
	if !ec2InstanceIDPattern.MatchString(target.InstanceID) || rejectSecretMaterial("instance_id", target.InstanceID) != nil {
		return errors.New("instance_id is invalid")
	}
	if err := validateProviderResourceIDs("volume_ids", target.VolumeIDs, ebsVolumeIDPattern); err != nil {
		return err
	}
	return validateProviderResourceIDs("network_interface_ids", target.NetworkInterfaceIDs, ec2NetworkInterfaceIDPattern)
}

func validateProviderResourceIDs(label string, values []string, pattern *regexp.Regexp) error {
	if len(values) == 0 || len(values) > 64 {
		return fmt.Errorf("%s must contain 1 to 64 entries", label)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !pattern.MatchString(value) || rejectSecretMaterial(label, value) != nil {
			return fmt.Errorf("%s contains an invalid identifier", label)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s must not contain duplicates", label)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func (approval ServiceDestroyApprovalV1) Validate() error { return approval.validate(false) }

func (approval ServiceDestroyApprovalV1) ValidateAt(now time.Time) error {
	if err := approval.validate(true); err != nil {
		return err
	}
	if !approval.ExpiresAt.After(now.UTC()) {
		return errors.New("service destroy approval has expired")
	}
	return nil
}

func (approval ServiceDestroyApprovalV1) validate(requireSignature bool) error {
	if err := validateSchema(approval.SchemaVersion); err != nil {
		return err
	}
	if approval.Intent != ServiceDestroyApprovalIntent {
		return errors.New("service destroy approval intent is invalid")
	}
	for label, value := range map[string]string{
		"approval_id": approval.ApprovalID, "challenge_id": approval.ChallengeID, "signer_key_id": approval.SignerKeyID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	target := approval.Target()
	if err := target.Validate(); err != nil {
		return err
	}
	if approval.IssuedAt.IsZero() || approval.ExpiresAt.IsZero() || !approval.ExpiresAt.After(approval.IssuedAt) || approval.ExpiresAt.Sub(approval.IssuedAt) > maxServiceDestroyApprovalLifetime {
		return errors.New("service destroy approval expiry must be within five minutes of issuance")
	}
	if requireSignature || approval.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("service destroy approval signature must be a base64url Ed25519 signature")
		}
	}
	return nil
}

func (approval ServiceDestroyApprovalV1) Target() ServiceDestroyTargetV1 {
	return normalizeServiceDestroyTarget(ServiceDestroyTargetV1{
		ServiceID: approval.ServiceID, ServiceRevision: approval.ServiceRevision,
		DeploymentID: approval.DeploymentID, DeploymentRevision: approval.DeploymentRevision,
		CloudConnectionID: approval.CloudConnectionID, RecipeID: approval.RecipeID, RecipeDigest: approval.RecipeDigest,
		InstanceID: approval.InstanceID, VolumeIDs: approval.VolumeIDs, NetworkInterfaceIDs: approval.NetworkInterfaceIDs,
	})
}

func (approval ServiceDestroyApprovalV1) SigningPayload() ([]byte, error) {
	if err := approval.Validate(); err != nil {
		return nil, err
	}
	approval = normalizeServiceDestroyApproval(approval)
	return canonicalCBOR(serviceDestroyApprovalSigningPayloadV1{
		SchemaVersion: approval.SchemaVersion, PayloadVersion: "service-destroy-approval-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256,
		Intent: approval.Intent, ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
		ServiceID: approval.ServiceID, ServiceRevision: approval.ServiceRevision,
		DeploymentID: approval.DeploymentID, DeploymentRevision: approval.DeploymentRevision,
		CloudConnectionID: approval.CloudConnectionID, RecipeID: approval.RecipeID, RecipeDigest: approval.RecipeDigest,
		InstanceID: approval.InstanceID, VolumeIDs: approval.VolumeIDs, NetworkInterfaceIDs: approval.NetworkInterfaceIDs,
		IssuedAt: approval.IssuedAt, ExpiresAt: approval.ExpiresAt,
	})
}

func (approval ServiceDestroyApprovalV1) Sign(privateKey ed25519.PrivateKey, now time.Time) (ServiceDestroyApprovalV1, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return ServiceDestroyApprovalV1{}, errors.New("service destroy approval signing key is not an Ed25519 private key")
	}
	if err := approval.validate(false); err != nil {
		return ServiceDestroyApprovalV1{}, err
	}
	if !approval.ExpiresAt.After(now.UTC()) {
		return ServiceDestroyApprovalV1{}, errors.New("service destroy approval has expired")
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		return ServiceDestroyApprovalV1{}, err
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return approval, nil
}

func (approval ServiceDestroyApprovalV1) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("service destroy approval verification key is not an Ed25519 public key")
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
		return errors.New("service destroy approval signature is invalid")
	}
	return nil
}

func (approval ServiceDestroyApprovalV1) ValidateAgainst(target ServiceDestroyTargetV1, now time.Time) error {
	if err := approval.ValidateAt(now); err != nil {
		return err
	}
	target = normalizeServiceDestroyTarget(target)
	if err := target.Validate(); err != nil || !reflect.DeepEqual(approval.Target(), target) {
		return ErrServiceDestroyApprovalBinding
	}
	return nil
}

func normalizeServiceDestroyTarget(target ServiceDestroyTargetV1) ServiceDestroyTargetV1 {
	target.VolumeIDs = append([]string(nil), target.VolumeIDs...)
	target.NetworkInterfaceIDs = append([]string(nil), target.NetworkInterfaceIDs...)
	sort.Strings(target.VolumeIDs)
	sort.Strings(target.NetworkInterfaceIDs)
	return target
}

func normalizeServiceDestroyApproval(approval ServiceDestroyApprovalV1) ServiceDestroyApprovalV1 {
	approval.IssuedAt = approval.IssuedAt.UTC()
	approval.ExpiresAt = approval.ExpiresAt.UTC()
	target := approval.Target()
	approval.VolumeIDs = target.VolumeIDs
	approval.NetworkInterfaceIDs = target.NetworkInterfaceIDs
	return approval
}
