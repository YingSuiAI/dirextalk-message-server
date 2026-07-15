package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"sort"
	"time"
)

const (
	serviceDestroyApprovalIntent         = "service_destroy"
	serviceDestroyApprovalPayloadVersion = "service-destroy-approval-signing-payload/v1"
)

var (
	destroyInstanceIDPattern         = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	destroyVolumeIDPattern           = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	destroyNetworkInterfaceIDPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
)

type ServiceDestroyApprovalProof struct {
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
	Signature           string    `json:"signature"`
}

type serviceDestroyApprovalSigningPayload struct {
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

func ParseServiceDestroyApprovalProof(raw []byte) (ServiceDestroyApprovalProof, error) {
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, []string{"schema_version", "intent", "approval_id", "challenge_id", "signer_key_id", "service_id", "service_revision", "deployment_id", "deployment_revision", "cloud_connection_id", "recipe_id", "recipe_digest", "instance_id", "volume_ids", "network_interface_ids", "issued_at", "expires_at", "signature"}) {
		return ServiceDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	var proof ServiceDestroyApprovalProof
	if decodeSingle(raw, &proof) != nil || proof.validate() != nil {
		return ServiceDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	return proof, nil
}

func (proof ServiceDestroyApprovalProof) validate() error {
	if proof.SchemaVersion != approvalSchemaVersion || proof.Intent != serviceDestroyApprovalIntent || !approvalIdentifierPattern.MatchString(proof.ApprovalID) || !approvalIdentifierPattern.MatchString(proof.ChallengeID) || !approvalIdentifierPattern.MatchString(proof.SignerKeyID) || !approvalIdentifierPattern.MatchString(proof.ServiceID) || !approvalIdentifierPattern.MatchString(proof.DeploymentID) || !approvalIdentifierPattern.MatchString(proof.CloudConnectionID) || !approvalIdentifierPattern.MatchString(proof.RecipeID) || !namedSHA256Pattern.MatchString(proof.RecipeDigest) || proof.ServiceRevision == 0 || proof.DeploymentRevision == 0 || proof.ServiceRevision > uint64(maxSafeInteger) || proof.DeploymentRevision > uint64(maxSafeInteger) || !destroyInstanceIDPattern.MatchString(proof.InstanceID) {
		return errCode("invalid_approval_proof")
	}
	if !validDestroyResourceIDs(proof.VolumeIDs, destroyVolumeIDPattern) || !validDestroyResourceIDs(proof.NetworkInterfaceIDs, destroyNetworkInterfaceIDPattern) {
		return errCode("invalid_approval_proof")
	}
	if proof.IssuedAt.IsZero() || proof.ExpiresAt.IsZero() || proof.IssuedAt.Location() != time.UTC || proof.ExpiresAt.Location() != time.UTC || !proof.ExpiresAt.After(proof.IssuedAt) || proof.ExpiresAt.Sub(proof.IssuedAt) > 5*time.Minute {
		return errCode("invalid_approval_proof")
	}
	signature, err := base64.RawURLEncoding.DecodeString(proof.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errCode("invalid_approval_proof")
	}
	return nil
}

func validDestroyResourceIDs(values []string, pattern *regexp.Regexp) bool {
	if len(values) == 0 || len(values) > 64 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !pattern.MatchString(value) {
			return false
		}
		if _, ok := seen[value]; ok {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func (proof ServiceDestroyApprovalProof) SigningPayload() ([]byte, error) {
	if err := proof.validate(); err != nil {
		return nil, err
	}
	proof = normalizeServiceDestroyApprovalProof(proof)
	return deterministicCBOR(serviceDestroyApprovalSigningPayload{
		SchemaVersion: proof.SchemaVersion, PayloadVersion: serviceDestroyApprovalPayloadVersion, HashAlgorithm: approvalHashAlgorithm,
		Intent: proof.Intent, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID,
		ServiceID: proof.ServiceID, ServiceRevision: proof.ServiceRevision, DeploymentID: proof.DeploymentID, DeploymentRevision: proof.DeploymentRevision,
		CloudConnectionID: proof.CloudConnectionID, RecipeID: proof.RecipeID, RecipeDigest: proof.RecipeDigest,
		InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs,
		IssuedAt: proof.IssuedAt, ExpiresAt: proof.ExpiresAt,
	})
}

func (proof ServiceDestroyApprovalProof) PayloadSHA256() (string, error) {
	payload, err := proof.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (proof ServiceDestroyApprovalProof) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	if !proof.ExpiresAt.After(now.UTC()) {
		return errCode("approval_expired")
	}
	payload, err := proof.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(proof.Signature)
	if !ed25519.Verify(publicKey, payload, signature) {
		return errCode("invalid_approval_signature")
	}
	return nil
}

func normalizeServiceDestroyApprovalProof(proof ServiceDestroyApprovalProof) ServiceDestroyApprovalProof {
	proof.IssuedAt, proof.ExpiresAt = proof.IssuedAt.UTC(), proof.ExpiresAt.UTC()
	proof.VolumeIDs = append([]string(nil), proof.VolumeIDs...)
	proof.NetworkInterfaceIDs = append([]string(nil), proof.NetworkInterfaceIDs...)
	sort.Strings(proof.VolumeIDs)
	sort.Strings(proof.NetworkInterfaceIDs)
	return proof
}

func (command Command) ServiceDestroyApproval() (ServiceDestroyApprovalProof, error) {
	if command.Action != ActionDeploymentDestroy || len(command.ApprovalProof) == 0 {
		return ServiceDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	return ParseServiceDestroyApprovalProof(command.ApprovalProof)
}
