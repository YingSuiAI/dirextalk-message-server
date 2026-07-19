package contract

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"time"
)

const (
	ServiceSecretApprovalIntent         = "service_secret"
	serviceSecretApprovalPayloadVersion = "service-secret-approval-signing-payload/v1"
	MaxServiceSecretApprovalBytes       = 16 * 1024
)

type ServiceSecretApprovalProof struct {
	SchemaVersion  string    `json:"schema_version"`
	Intent         string    `json:"intent"`
	ApprovalID     string    `json:"approval_id"`
	ChallengeID    string    `json:"challenge_id"`
	SignerKeyID    string    `json:"signer_key_id"`
	SessionID      string    `json:"session_id"`
	ConnectionID   string    `json:"connection_id"`
	DeploymentID   string    `json:"deployment_id"`
	TaskID         string    `json:"task_id"`
	ExecutionID    string    `json:"execution_id"`
	ManifestDigest string    `json:"manifest_digest"`
	RecipeDigest   string    `json:"recipe_digest"`
	ArtifactDigest string    `json:"artifact_digest"`
	SlotID         string    `json:"slot_id"`
	SecretRef      string    `json:"secret_ref"`
	Purpose        string    `json:"purpose"`
	Delivery       string    `json:"delivery"`
	ContextDigest  string    `json:"context_digest"`
	IssuedAt       time.Time `json:"issued_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Signature      string    `json:"signature"`
}

type serviceSecretApprovalSigningPayload struct {
	SchemaVersion  string    `json:"schema_version" cbor:"schema_version"`
	PayloadVersion string    `json:"payload_version" cbor:"payload_version"`
	HashAlgorithm  string    `json:"hash_algorithm" cbor:"hash_algorithm"`
	Intent         string    `json:"intent" cbor:"intent"`
	ApprovalID     string    `json:"approval_id" cbor:"approval_id"`
	ChallengeID    string    `json:"challenge_id" cbor:"challenge_id"`
	SignerKeyID    string    `json:"signer_key_id" cbor:"signer_key_id"`
	SessionID      string    `json:"session_id" cbor:"session_id"`
	ConnectionID   string    `json:"connection_id" cbor:"connection_id"`
	DeploymentID   string    `json:"deployment_id" cbor:"deployment_id"`
	TaskID         string    `json:"task_id" cbor:"task_id"`
	ExecutionID    string    `json:"execution_id" cbor:"execution_id"`
	ManifestDigest string    `json:"manifest_digest" cbor:"manifest_digest"`
	RecipeDigest   string    `json:"recipe_digest" cbor:"recipe_digest"`
	ArtifactDigest string    `json:"artifact_digest" cbor:"artifact_digest"`
	SlotID         string    `json:"slot_id" cbor:"slot_id"`
	SecretRef      string    `json:"secret_ref" cbor:"secret_ref"`
	Purpose        string    `json:"purpose" cbor:"purpose"`
	Delivery       string    `json:"delivery" cbor:"delivery"`
	ContextDigest  string    `json:"context_digest" cbor:"context_digest"`
	IssuedAt       time.Time `json:"issued_at" cbor:"issued_at"`
	ExpiresAt      time.Time `json:"expires_at" cbor:"expires_at"`
}

var serviceSecretApprovalFields = []string{"schema_version", "intent", "approval_id", "challenge_id", "signer_key_id", "session_id", "connection_id", "deployment_id", "task_id", "execution_id", "manifest_digest", "recipe_digest", "artifact_digest", "slot_id", "secret_ref", "purpose", "delivery", "context_digest", "issued_at", "expires_at", "signature"}

func ParseServiceSecretApprovalProof(raw []byte) (ServiceSecretApprovalProof, error) {
	var proof ServiceSecretApprovalProof
	if len(raw) == 0 || len(raw) > MaxServiceSecretApprovalBytes {
		return proof, errCode("invalid_approval_proof")
	}
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, serviceSecretApprovalFields) || decodeSingle(raw, &proof) != nil || proof.validate() != nil {
		return ServiceSecretApprovalProof{}, errCode("invalid_approval_proof")
	}
	return proof, nil
}

func (proof ServiceSecretApprovalProof) Context() ServiceSecretContextV1 {
	return ServiceSecretContextV1{SchemaVersion: ServiceSecretContextSchema, SessionID: proof.SessionID, ConnectionID: proof.ConnectionID, DeploymentID: proof.DeploymentID, TaskID: proof.TaskID, ExecutionID: proof.ExecutionID, ManifestDigest: proof.ManifestDigest, RecipeDigest: proof.RecipeDigest, ArtifactDigest: proof.ArtifactDigest, SlotID: proof.SlotID, SecretRef: proof.SecretRef, Purpose: proof.Purpose, Delivery: proof.Delivery, ExpiresAt: CanonicalInstant(proof.ExpiresAt)}
}

func (proof ServiceSecretApprovalProof) validate() error {
	if proof.SchemaVersion != approvalSchemaVersion || proof.Intent != ServiceSecretApprovalIntent || !approvalIdentifierPattern.MatchString(proof.ApprovalID) || !approvalIdentifierPattern.MatchString(proof.ChallengeID) || !approvalIdentifierPattern.MatchString(proof.SignerKeyID) {
		return errCode("invalid_approval_proof")
	}
	if proof.IssuedAt.IsZero() || proof.ExpiresAt.IsZero() || proof.IssuedAt.Location() != time.UTC || proof.ExpiresAt.Location() != time.UTC || !proof.ExpiresAt.After(proof.IssuedAt) || proof.ExpiresAt.Sub(proof.IssuedAt) > 10*time.Minute {
		return errCode("invalid_approval_proof")
	}
	context := proof.Context()
	digest, err := context.Digest()
	if err != nil || digest != proof.ContextDigest {
		return errCode("invalid_approval_proof")
	}
	signature, err := base64.RawURLEncoding.DecodeString(proof.Signature)
	if err != nil || base64.RawURLEncoding.EncodeToString(signature) != proof.Signature || len(signature) != ed25519.SignatureSize {
		return errCode("invalid_approval_proof")
	}
	return nil
}

func (proof ServiceSecretApprovalProof) SigningPayload() ([]byte, error) {
	if err := proof.validate(); err != nil {
		return nil, err
	}
	payload := serviceSecretApprovalSigningPayload{proof.SchemaVersion, serviceSecretApprovalPayloadVersion, approvalHashAlgorithm, proof.Intent, proof.ApprovalID, proof.ChallengeID, proof.SignerKeyID, proof.SessionID, proof.ConnectionID, proof.DeploymentID, proof.TaskID, proof.ExecutionID, proof.ManifestDigest, proof.RecipeDigest, proof.ArtifactDigest, proof.SlotID, proof.SecretRef, proof.Purpose, proof.Delivery, proof.ContextDigest, proof.IssuedAt.UTC(), proof.ExpiresAt.UTC()}
	return deterministicCBOR(payload)
}

func (proof ServiceSecretApprovalProof) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	payload, err := proof.SigningPayload()
	if err != nil {
		return err
	}
	if !proof.ExpiresAt.After(now.UTC()) {
		return errCode("approval_expired")
	}
	signature, _ := base64.RawURLEncoding.DecodeString(proof.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errCode("invalid_approval_signature")
	}
	return nil
}

func MarshalServiceSecretApprovalProof(proof ServiceSecretApprovalProof) ([]byte, error) {
	if err := proof.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(proof)
}
