package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"reflect"
	"time"
)

const JobCancelIntent = "cloud_job_cancel"

type JobCancelTargetV1 struct {
	JobID              string `json:"job_id"`
	JobRevision        uint64 `json:"job_revision"`
	JobKind            string `json:"job_kind"`
	PlanID             string `json:"plan_id"`
	DeploymentID       string `json:"deployment_id"`
	DeploymentRevision uint64 `json:"deployment_revision"`
	CloudConnectionID  string `json:"cloud_connection_id"`
	ResourceStatus     string `json:"resource_status"`
}

type JobCancelApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	JobCancelTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type jobCancelSigningPayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	JobCancelTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewJobCancelApprovalV1(target JobCancelTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (JobCancelApprovalV1, error) {
	approval := JobCancelApprovalV1{SchemaVersion: SchemaVersionV1, Intent: JobCancelIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, JobCancelTargetV1: target, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	return approval, approval.Validate()
}

func (target JobCancelTargetV1) Validate() error {
	for label, value := range map[string]string{"job_id": target.JobID, "plan_id": target.PlanID, "deployment_id": target.DeploymentID, "cloud_connection_id": target.CloudConnectionID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("job cancel target identity is invalid")
		}
	}
	if target.JobRevision == 0 || target.DeploymentRevision == 0 || (target.JobKind != "provision" && target.JobKind != "install" && target.JobKind != "verify") ||
		(target.ResourceStatus != "none" && target.ResourceStatus != "active" && target.ResourceStatus != "retained_tracked") {
		return errors.New("job cancel target binding is invalid")
	}
	return nil
}

func (approval JobCancelApprovalV1) Validate() error {
	if validateSchema(approval.SchemaVersion) != nil || approval.Intent != JobCancelIntent || approval.JobCancelTargetV1.Validate() != nil {
		return errors.New("job cancel approval is invalid")
	}
	for label, value := range map[string]string{"approval_id": approval.ApprovalID, "challenge_id": approval.ChallengeID, "signer_key_id": approval.SignerKeyID} {
		if validateIdentifier(label, value) != nil {
			return errors.New("job cancel approval identity is invalid")
		}
	}
	if approval.IssuedAt.IsZero() || !approval.ExpiresAt.After(approval.IssuedAt) || approval.ExpiresAt.Sub(approval.IssuedAt) > 5*time.Minute {
		return errors.New("job cancel approval expiry is invalid")
	}
	if approval.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("job cancel approval signature is invalid")
		}
	}
	return nil
}

func (approval JobCancelApprovalV1) SigningPayload() ([]byte, error) {
	if err := approval.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(jobCancelSigningPayloadV1{SchemaVersion: approval.SchemaVersion, PayloadVersion: "job-cancel-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: approval.Intent, ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID, JobCancelTargetV1: approval.JobCancelTargetV1, IssuedAt: approval.IssuedAt.UTC(), ExpiresAt: approval.ExpiresAt.UTC()})
}

func (approval JobCancelApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (JobCancelApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || !approval.ExpiresAt.After(now.UTC()) {
		return JobCancelApprovalV1{}, errors.New("job cancel approval cannot be signed")
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		return JobCancelApprovalV1{}, err
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return approval, nil
}

func (approval JobCancelApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize || approval.Signature == "" || approval.IssuedAt.After(now.UTC()) || !approval.ExpiresAt.After(now.UTC()) {
		return errors.New("job cancel approval is expired or key is invalid")
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(approval.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("job cancel approval signature is invalid")
	}
	return nil
}

func (approval JobCancelApprovalV1) ValidateAgainst(target JobCancelTargetV1, now time.Time) error {
	if approval.Validate() != nil || !approval.ExpiresAt.After(now.UTC()) || !reflect.DeepEqual(approval.JobCancelTargetV1, target) {
		return errors.New("job cancel approval does not bind current state")
	}
	return nil
}
