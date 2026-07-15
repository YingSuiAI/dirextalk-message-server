package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"time"
)

const PairingResumeIntent = "deployment_pairing_resume"

type PairingResumeTargetV1 struct {
	DeploymentID                  string `json:"deployment_id"`
	DeploymentRevision            uint64 `json:"deployment_revision"`
	PlanID                        string `json:"plan_id"`
	CloudConnectionID             string `json:"cloud_connection_id"`
	ExecutionID                   string `json:"execution_id"`
	RecipeExecutionManifestDigest string `json:"recipe_execution_manifest_digest"`
	JobID                         string `json:"job_id"`
	JobRevision                   uint64 `json:"job_revision"`
}

type PairingResumeApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	PairingResumeTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type pairingResumeSigningPayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	PairingResumeTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewPairingResumeApprovalV1(target PairingResumeTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (PairingResumeApprovalV1, error) {
	a := PairingResumeApprovalV1{SchemaVersion: SchemaVersionV1, Intent: PairingResumeIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, PairingResumeTargetV1: target, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	return a, a.Validate()
}

func (t PairingResumeTargetV1) Validate() error {
	for label, value := range map[string]string{"deployment_id": t.DeploymentID, "plan_id": t.PlanID, "cloud_connection_id": t.CloudConnectionID, "execution_id": t.ExecutionID, "job_id": t.JobID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("pairing resume target identity is invalid")
		}
	}
	if t.DeploymentRevision == 0 || t.JobRevision == 0 || validateDigest("recipe_execution_manifest_digest", t.RecipeExecutionManifestDigest) != nil {
		return errors.New("pairing resume target binding is invalid")
	}
	return nil
}

func (a PairingResumeApprovalV1) Validate() error {
	if validateSchema(a.SchemaVersion) != nil || a.Intent != PairingResumeIntent || a.PairingResumeTargetV1.Validate() != nil {
		return errors.New("pairing resume approval is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("pairing resume approval identity is invalid")
		}
	}
	if a.IssuedAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > 5*time.Minute {
		return errors.New("pairing resume approval expiry is invalid")
	}
	if a.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("pairing resume approval signature is invalid")
		}
	}
	return nil
}

func (a PairingResumeApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(pairingResumeSigningPayloadV1{SchemaVersion: a.SchemaVersion, PayloadVersion: "pairing-resume-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: a.Intent, ApprovalID: a.ApprovalID, ChallengeID: a.ChallengeID, SignerKeyID: a.SignerKeyID, PairingResumeTargetV1: a.PairingResumeTargetV1, IssuedAt: a.IssuedAt.UTC(), ExpiresAt: a.ExpiresAt.UTC()})
}

func (a PairingResumeApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (PairingResumeApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || !a.ExpiresAt.After(now.UTC()) {
		return PairingResumeApprovalV1{}, errors.New("pairing resume approval cannot be signed")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return PairingResumeApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}

func (a PairingResumeApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize || a.Signature == "" || a.IssuedAt.After(now.UTC()) || !a.ExpiresAt.After(now.UTC()) {
		return errors.New("pairing resume approval is expired or key is invalid")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(a.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("pairing resume approval signature is invalid")
	}
	return nil
}
