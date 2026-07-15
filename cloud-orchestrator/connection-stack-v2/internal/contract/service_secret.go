package contract

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	ServiceSecretContextSchema  = "dirextalk.service-secret-context/v1"
	ServiceSecretEnvelopeSchema = "dirextalk.service-secret-envelope/v1"
	ServiceSecretHKDFInfo       = "dirextalk/service-secret-envelope/v1/aes-256-gcm"
	MaxServiceSecretPlaintext   = 32 * 1024
	MaxServiceSecretEnvelope    = 64 * 1024
)

// ServiceSecretContextV1 is the complete non-secret scope authenticated as
// AES-GCM AAD. Explicit CBOR tags freeze the cross-language map keys.
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

type ServiceSecretEnvelopeV1 struct {
	SchemaVersion         string `json:"schema_version"`
	SessionID             string `json:"session_id"`
	ContextDigest         string `json:"context_digest"`
	EphemeralPublicKeyB64 string `json:"ephemeral_public_key_b64"`
	NonceB64              string `json:"nonce_b64"`
	CiphertextB64         string `json:"ciphertext_b64"`
}

func (context ServiceSecretContextV1) Validate() error {
	if context.SchemaVersion != ServiceSecretContextSchema || !ValidID(context.SessionID) || !ValidConnectionID(context.ConnectionID) || !ValidID(context.DeploymentID) || !recipeTaskIDPattern.MatchString(context.TaskID) || !recipeBindingIDPattern.MatchString(context.ExecutionID) || !namedSHA256Pattern.MatchString(context.ManifestDigest) || !namedSHA256Pattern.MatchString(context.RecipeDigest) || !namedSHA256Pattern.MatchString(context.ArtifactDigest) || !recipeBindingIDPattern.MatchString(context.SlotID) || !approvalSecretRefPattern.MatchString(context.SecretRef) || !safeApprovalText(context.Purpose, 160) || (context.Delivery != "file" && context.Delivery != "environment") {
		return errCode("invalid_service_secret_context")
	}
	expires, err := time.Parse(canonicalInstantLayout, context.ExpiresAt)
	if err != nil || CanonicalInstant(expires) != context.ExpiresAt {
		return errCode("invalid_service_secret_context")
	}
	return nil
}

func (context ServiceSecretContextV1) CanonicalCBOR() ([]byte, error) {
	if err := context.Validate(); err != nil {
		return nil, err
	}
	return deterministicCBOR(context)
}

func (context ServiceSecretContextV1) Digest() (string, error) {
	canonical, err := context.CanonicalCBOR()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func EncryptServiceSecret(context ServiceSecretContextV1, serverPublicKey, ephemeralPrivateKey, nonce, plaintext []byte) (ServiceSecretEnvelopeV1, error) {
	if len(plaintext) == 0 || len(plaintext) > MaxServiceSecretPlaintext || len(nonce) != 12 {
		return ServiceSecretEnvelopeV1{}, errCode("invalid_service_secret_plaintext")
	}
	curve := ecdh.X25519()
	serverPublic, err := curve.NewPublicKey(serverPublicKey)
	if err != nil {
		return ServiceSecretEnvelopeV1{}, errCode("invalid_service_secret_key")
	}
	ephemeralPrivate, err := curve.NewPrivateKey(ephemeralPrivateKey)
	if err != nil {
		return ServiceSecretEnvelopeV1{}, errCode("invalid_service_secret_key")
	}
	shared, err := ephemeralPrivate.ECDH(serverPublic)
	if err != nil {
		return ServiceSecretEnvelopeV1{}, errCode("invalid_service_secret_key")
	}
	return sealServiceSecret(context, ephemeralPrivate.PublicKey().Bytes(), shared, nonce, plaintext)
}

func DecryptServiceSecret(context ServiceSecretContextV1, serverPrivateKey []byte, envelope ServiceSecretEnvelopeV1) ([]byte, error) {
	if err := envelope.ValidateForContext(context); err != nil {
		return nil, err
	}
	curve := ecdh.X25519()
	serverPrivate, err := curve.NewPrivateKey(serverPrivateKey)
	if err != nil {
		return nil, errCode("invalid_service_secret_key")
	}
	ephemeralBytes, _ := decodeServiceSecretBase64(envelope.EphemeralPublicKeyB64, 32)
	ephemeralPublic, err := curve.NewPublicKey(ephemeralBytes)
	if err != nil {
		return nil, errCode("invalid_service_secret_key")
	}
	shared, err := serverPrivate.ECDH(ephemeralPublic)
	if err != nil {
		return nil, errCode("invalid_service_secret_key")
	}
	key, aad, err := serviceSecretKeyAndAAD(context, shared)
	if err != nil {
		return nil, err
	}
	nonce, _ := decodeServiceSecretBase64(envelope.NonceB64, 12)
	ciphertext, err := decodeServiceSecretBase64(envelope.CiphertextB64, -1)
	if err != nil || len(ciphertext) <= 16 || len(ciphertext) > MaxServiceSecretPlaintext+16 {
		return nil, errCode("invalid_service_secret_envelope")
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil || len(plaintext) == 0 || len(plaintext) > MaxServiceSecretPlaintext {
		return nil, errCode("service_secret_decryption_failed")
	}
	return plaintext, nil
}

func sealServiceSecret(context ServiceSecretContextV1, ephemeralPublic, shared, nonce, plaintext []byte) (ServiceSecretEnvelopeV1, error) {
	key, aad, err := serviceSecretKeyAndAAD(context, shared)
	if err != nil {
		return ServiceSecretEnvelopeV1{}, err
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)
	digest, _ := context.Digest()
	envelope := ServiceSecretEnvelopeV1{SchemaVersion: ServiceSecretEnvelopeSchema, SessionID: context.SessionID, ContextDigest: digest, EphemeralPublicKeyB64: base64.RawURLEncoding.EncodeToString(ephemeralPublic), NonceB64: base64.RawURLEncoding.EncodeToString(nonce), CiphertextB64: base64.RawURLEncoding.EncodeToString(ciphertext)}
	return envelope, envelope.ValidateForContext(context)
}

func serviceSecretKeyAndAAD(context ServiceSecretContextV1, shared []byte) ([]byte, []byte, error) {
	aad, err := context.CanonicalCBOR()
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(aad)
	key, err := hkdf.Key(sha256.New, shared, sum[:], ServiceSecretHKDFInfo, 32)
	if err != nil {
		return nil, nil, errCode("service_secret_kdf_failed")
	}
	return key, aad, nil
}

func (envelope ServiceSecretEnvelopeV1) ValidateForContext(context ServiceSecretContextV1) error {
	digest, err := context.Digest()
	if err != nil || envelope.SchemaVersion != ServiceSecretEnvelopeSchema || envelope.SessionID != context.SessionID || envelope.ContextDigest != digest {
		return errCode("invalid_service_secret_envelope")
	}
	if _, err = decodeServiceSecretBase64(envelope.EphemeralPublicKeyB64, 32); err != nil {
		return err
	}
	if _, err = decodeServiceSecretBase64(envelope.NonceB64, 12); err != nil {
		return err
	}
	if ciphertext, decodeErr := decodeServiceSecretBase64(envelope.CiphertextB64, -1); decodeErr != nil || len(ciphertext) <= 16 || len(ciphertext) > MaxServiceSecretPlaintext+16 {
		return errCode("invalid_service_secret_envelope")
	}
	return nil
}

func (envelope ServiceSecretEnvelopeV1) CanonicalJSON() ([]byte, error) {
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > MaxServiceSecretEnvelope {
		return nil, errCode("invalid_service_secret_envelope")
	}
	return raw, nil
}

func (envelope ServiceSecretEnvelopeV1) Digest() (string, error) {
	raw, err := envelope.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func ParseServiceSecretEnvelope(raw []byte) (ServiceSecretEnvelopeV1, error) {
	var envelope ServiceSecretEnvelopeV1
	if len(raw) == 0 || len(raw) > MaxServiceSecretEnvelope {
		return envelope, errCode("invalid_service_secret_envelope")
	}
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, []string{"schema_version", "session_id", "context_digest", "ephemeral_public_key_b64", "nonce_b64", "ciphertext_b64"}) || decodeSingle(raw, &envelope) != nil {
		return envelope, errCode("invalid_service_secret_envelope")
	}
	canonical, err := envelope.CanonicalJSON()
	if err != nil || !bytes.Equal(raw, canonical) {
		return envelope, errCode("noncanonical_payload")
	}
	return envelope, nil
}

func decodeServiceSecretBase64(value string, exactLength int) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value || (exactLength >= 0 && len(decoded) != exactLength) {
		return nil, errCode("invalid_service_secret_envelope")
	}
	return decoded, nil
}
