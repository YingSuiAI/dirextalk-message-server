package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

const (
	deploymentDestroyApprovalIntent         = "deployment_destroy"
	deploymentDestroyApprovalPayloadVersion = "deployment-destroy-approval-signing-payload/v1"
)

type DeploymentDestroyApprovalProof struct {
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
	Signature           string    `json:"signature"`
}

type deploymentDestroyApprovalSigningPayload struct {
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
	SecretRefs          []string  `json:"secret_refs,omitempty" cbor:"secret_refs,omitempty"`
	IssuedAt            time.Time `json:"issued_at"`
	ExpiresAt           time.Time `json:"expires_at"`
}

// DestroyApprovalMetadata is the common one-time-consumption identity for
// legacy Service and service-free Deployment destroy approvals.
type DestroyApprovalMetadata struct {
	Intent      string
	ApprovalID  string
	ChallengeID string
	SignerKeyID string
}

func ParseDeploymentDestroyApprovalProof(raw []byte) (DeploymentDestroyApprovalProof, error) {
	fields, err := exactJSONObject(raw)
	baseFields := []string{"schema_version", "intent", "approval_id", "challenge_id", "signer_key_id", "deployment_id", "deployment_revision", "plan_id", "cloud_connection_id", "resource_status", "instance_id", "volume_ids", "network_interface_ids", "issued_at", "expires_at", "signature"}
	if err != nil || (!exactFields(fields, baseFields) && !exactFields(fields, append(baseFields, "secret_refs"))) {
		return DeploymentDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	var proof DeploymentDestroyApprovalProof
	if decodeSingle(raw, &proof) != nil || proof.validate() != nil {
		return DeploymentDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	if _, present := fields["secret_refs"]; present && len(proof.SecretRefs) == 0 {
		return DeploymentDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	return proof, nil
}

func (proof DeploymentDestroyApprovalProof) validate() error {
	if proof.SchemaVersion != approvalSchemaVersion || proof.Intent != deploymentDestroyApprovalIntent ||
		!approvalIdentifierPattern.MatchString(proof.ApprovalID) || !approvalIdentifierPattern.MatchString(proof.ChallengeID) || !approvalIdentifierPattern.MatchString(proof.SignerKeyID) ||
		!approvalIdentifierPattern.MatchString(proof.DeploymentID) || !approvalIdentifierPattern.MatchString(proof.PlanID) || !approvalIdentifierPattern.MatchString(proof.CloudConnectionID) ||
		proof.DeploymentRevision == 0 || proof.DeploymentRevision > uint64(maxSafeInteger) || !validDeploymentDestroyResourceStatus(proof.ResourceStatus) || !destroyInstanceIDPattern.MatchString(proof.InstanceID) {
		return errCode("invalid_approval_proof")
	}
	if !validDestroyResourceIDs(proof.VolumeIDs, destroyVolumeIDPattern) || !validDestroyResourceIDs(proof.NetworkInterfaceIDs, destroyNetworkInterfaceIDPattern) || !validOptionalDestroySecretRefs(proof.SecretRefs) {
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

func validDeploymentDestroyResourceStatus(value string) bool {
	return value == "active" || value == "retained_tracked" || value == "blocked" || value == "orphaned"
}

func (proof DeploymentDestroyApprovalProof) SigningPayload() ([]byte, error) {
	if err := proof.validate(); err != nil {
		return nil, err
	}
	proof = normalizeDeploymentDestroyApprovalProof(proof)
	return deterministicCBOR(deploymentDestroyApprovalSigningPayload{
		SchemaVersion: proof.SchemaVersion, PayloadVersion: deploymentDestroyApprovalPayloadVersion, HashAlgorithm: approvalHashAlgorithm,
		Intent: proof.Intent, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID,
		DeploymentID: proof.DeploymentID, DeploymentRevision: proof.DeploymentRevision,
		PlanID: proof.PlanID, CloudConnectionID: proof.CloudConnectionID, ResourceStatus: proof.ResourceStatus,
		InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs, SecretRefs: proof.SecretRefs,
		IssuedAt: proof.IssuedAt, ExpiresAt: proof.ExpiresAt,
	})
}

func (proof DeploymentDestroyApprovalProof) PayloadSHA256() (string, error) {
	payload, err := proof.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (proof DeploymentDestroyApprovalProof) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	now = now.UTC()
	if len(publicKey) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	if proof.IssuedAt.After(now) {
		return errCode("future_approval")
	}
	if !proof.ExpiresAt.After(now) {
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

func normalizeDeploymentDestroyApprovalProof(proof DeploymentDestroyApprovalProof) DeploymentDestroyApprovalProof {
	proof.IssuedAt, proof.ExpiresAt = proof.IssuedAt.UTC(), proof.ExpiresAt.UTC()
	proof.VolumeIDs = append([]string(nil), proof.VolumeIDs...)
	proof.NetworkInterfaceIDs = append([]string(nil), proof.NetworkInterfaceIDs...)
	proof.SecretRefs = append([]string(nil), proof.SecretRefs...)
	sort.Strings(proof.VolumeIDs)
	sort.Strings(proof.NetworkInterfaceIDs)
	sort.Strings(proof.SecretRefs)
	return proof
}

func (command Command) DeploymentDestroyApproval() (DeploymentDestroyApprovalProof, error) {
	if command.Action != ActionDeploymentDestroy || len(command.ApprovalProof) == 0 {
		return DeploymentDestroyApprovalProof{}, errCode("invalid_approval_proof")
	}
	return ParseDeploymentDestroyApprovalProof(command.ApprovalProof)
}

func (command Command) DestroyApprovalMetadata() (DestroyApprovalMetadata, error) {
	intent, err := command.destroyApprovalIntent()
	if err != nil {
		return DestroyApprovalMetadata{}, err
	}
	switch intent {
	case serviceDestroyApprovalIntent:
		proof, parseErr := command.ServiceDestroyApproval()
		if parseErr != nil {
			return DestroyApprovalMetadata{}, parseErr
		}
		return DestroyApprovalMetadata{Intent: proof.Intent, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID}, nil
	case deploymentDestroyApprovalIntent:
		proof, parseErr := command.DeploymentDestroyApproval()
		if parseErr != nil {
			return DestroyApprovalMetadata{}, parseErr
		}
		return DestroyApprovalMetadata{Intent: proof.Intent, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID}, nil
	default:
		return DestroyApprovalMetadata{}, errCode("invalid_approval_proof")
	}
}

func (command Command) VerifyDestroyApproval(publicKey ed25519.PublicKey, now time.Time) error {
	metadata, err := command.DestroyApprovalMetadata()
	if err != nil {
		return err
	}
	if metadata.Intent == serviceDestroyApprovalIntent {
		proof, _ := command.ServiceDestroyApproval()
		return proof.Verify(publicKey, now)
	}
	proof, _ := command.DeploymentDestroyApproval()
	return proof.Verify(publicKey, now)
}

func (command Command) DestroyApprovalPayloadSHA256() (string, error) {
	if err := command.ValidateDeploymentDestroyBinding(); err != nil {
		return "", err
	}
	metadata, err := command.DestroyApprovalMetadata()
	if err != nil {
		return "", err
	}
	if metadata.Intent == serviceDestroyApprovalIntent {
		proof, _ := command.ServiceDestroyApproval()
		return proof.PayloadSHA256()
	}
	proof, _ := command.DeploymentDestroyApproval()
	return proof.PayloadSHA256()
}

func (command Command) destroyApprovalIntent() (string, error) {
	if command.Action != ActionDeploymentDestroy || len(command.ApprovalProof) == 0 {
		return "", errCode("invalid_approval_proof")
	}
	fields, err := exactJSONObject(command.ApprovalProof)
	if err != nil {
		return "", errCode("invalid_approval_proof")
	}
	var intent string
	if json.Unmarshal(fields["intent"], &intent) != nil {
		return "", errCode("invalid_approval_proof")
	}
	return intent, nil
}
