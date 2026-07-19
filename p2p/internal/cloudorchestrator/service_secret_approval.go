package cloudorchestrator

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"
)

const (
	ServiceSecretApprovalIntent      = "service_secret"
	ServiceSecretContextSchema       = "dirextalk.service-secret-context/v1"
	serviceSecretPayloadVersion      = "service-secret-approval-signing-payload/v1"
	maxServiceSecretApprovalLifetime = 10 * time.Minute
	serviceSecretInstantLayout       = "2006-01-02T15:04:05.000Z"
)

type ServiceSecretContextV1 struct {
	SchemaVersion  string `json:"schema_version" cbor:"schema_version"`
	SessionID      string `json:"session_id" cbor:"session_id"`
	ConnectionID   string `json:"connection_id" cbor:"connection_id"`
	DeploymentID   string `json:"deployment_id" cbor:"deployment_id"`
	TaskID         string `json:"task_id" cbor:"task_id"`
	ExecutionID    string `json:"execution_id" cbor:"execution_id"`
	ManifestDigest string `json:"manifest_digest" cbor:"manifest_digest"`
	RecipeDigest   string `json:"recipe_digest" cbor:"recipe_digest"`
	ArtifactDigest string `json:"artifact_digest" cbor:"artifact_digest"`
	SlotID         string `json:"slot_id" cbor:"slot_id"`
	SecretRef      string `json:"secret_ref" cbor:"secret_ref"`
	Purpose        string `json:"purpose" cbor:"purpose"`
	Delivery       string `json:"delivery" cbor:"delivery"`
	ExpiresAt      string `json:"expires_at" cbor:"expires_at"`
}

func (c ServiceSecretContextV1) Validate() error {
	if c.SchemaVersion != ServiceSecretContextSchema {
		return errors.New("service secret context schema is invalid")
	}
	for label, value := range map[string]string{"session_id": c.SessionID, "connection_id": c.ConnectionID, "deployment_id": c.DeploymentID, "task_id": c.TaskID, "execution_id": c.ExecutionID, "slot_id": c.SlotID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("service secret context binding is invalid")
		}
	}
	for label, value := range map[string]string{"manifest_digest": c.ManifestDigest, "recipe_digest": c.RecipeDigest, "artifact_digest": c.ArtifactDigest} {
		if validateDigest(label, value) != nil {
			return errors.New("service secret context digest is invalid")
		}
	}
	if validateOpaqueReference("secret_ref", c.SecretRef, secretRefPattern) != nil || validateCompiledRecipePurpose(c.Purpose) != nil || (c.Delivery != string(SecretDeliveryFile) && c.Delivery != string(SecretDeliveryEnvironment)) {
		return errors.New("service secret context slot is invalid")
	}
	expires, err := time.Parse(serviceSecretInstantLayout, c.ExpiresAt)
	if err != nil || serviceSecretCanonicalInstant(expires) != c.ExpiresAt {
		return errors.New("service secret context expiry is invalid")
	}
	return nil
}

func (c ServiceSecretContextV1) CanonicalCBOR() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(c)
}

func (c ServiceSecretContextV1) Digest() (string, error) {
	value, err := c.CanonicalCBOR()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

type ServiceSecretApprovalV1 struct {
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
	Signature      string    `json:"signature,omitempty"`
}

type serviceSecretSigningPayloadV1 struct {
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

func NewServiceSecretApprovalV1(value ServiceSecretApprovalV1) (ServiceSecretApprovalV1, error) {
	value.SchemaVersion = SchemaVersionV1
	value.Intent = ServiceSecretApprovalIntent
	value.IssuedAt = value.IssuedAt.UTC().Truncate(time.Second)
	value.ExpiresAt = value.ExpiresAt.UTC().Truncate(time.Second)
	value.Signature = ""
	digest, err := value.Context().Digest()
	if err != nil {
		return ServiceSecretApprovalV1{}, err
	}
	value.ContextDigest = digest
	if err = value.Validate(); err != nil {
		return ServiceSecretApprovalV1{}, err
	}
	return value, nil
}

func (a ServiceSecretApprovalV1) Context() ServiceSecretContextV1 {
	return ServiceSecretContextV1{SchemaVersion: ServiceSecretContextSchema, SessionID: a.SessionID, ConnectionID: a.ConnectionID, DeploymentID: a.DeploymentID, TaskID: a.TaskID, ExecutionID: a.ExecutionID, ManifestDigest: a.ManifestDigest, RecipeDigest: a.RecipeDigest, ArtifactDigest: a.ArtifactDigest, SlotID: a.SlotID, SecretRef: a.SecretRef, Purpose: a.Purpose, Delivery: a.Delivery, ExpiresAt: serviceSecretCanonicalInstant(a.ExpiresAt)}
}

func (a ServiceSecretApprovalV1) Validate() error { return a.validate(false) }

func (a ServiceSecretApprovalV1) validate(requireSignature bool) error {
	if a.SchemaVersion != SchemaVersionV1 || a.Intent != ServiceSecretApprovalIntent {
		return errors.New("service secret approval schema or intent is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("service secret approval identity is invalid")
		}
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || a.IssuedAt.Location() != time.UTC || a.ExpiresAt.Location() != time.UTC || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxServiceSecretApprovalLifetime {
		return errors.New("service secret approval lifetime is invalid")
	}
	digest, err := a.Context().Digest()
	if err != nil || digest != a.ContextDigest {
		return errors.New("service secret approval context is invalid")
	}
	if a.Signature == "" && !requireSignature {
		return nil
	}
	signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
	if err != nil || base64.RawURLEncoding.EncodeToString(signature) != a.Signature || len(signature) != ed25519.SignatureSize {
		return errors.New("service secret approval signature is invalid")
	}
	return nil
}

func (a ServiceSecretApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	p := serviceSecretSigningPayloadV1{a.SchemaVersion, serviceSecretPayloadVersion, HashAlgorithmDeterministicCBORSHA256, a.Intent, a.ApprovalID, a.ChallengeID, a.SignerKeyID, a.SessionID, a.ConnectionID, a.DeploymentID, a.TaskID, a.ExecutionID, a.ManifestDigest, a.RecipeDigest, a.ArtifactDigest, a.SlotID, a.SecretRef, a.Purpose, a.Delivery, a.ContextDigest, a.IssuedAt.UTC(), a.ExpiresAt.UTC()}
	return canonicalCBOR(p)
}

func (a ServiceSecretApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (ServiceSecretApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || !a.ExpiresAt.After(now.UTC()) {
		return ServiceSecretApprovalV1{}, errors.New("service secret approval signing input is invalid")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ServiceSecretApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}

func (a ServiceSecretApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize || !a.ExpiresAt.After(now.UTC()) || a.validate(true) != nil {
		return errors.New("service secret approval signature is invalid")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(a.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("service secret approval signature is invalid")
	}
	return nil
}

func serviceSecretCanonicalInstant(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format(serviceSecretInstantLayout)
}
