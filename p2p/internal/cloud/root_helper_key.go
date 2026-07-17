package cloud

import (
	"context"
	"time"
)

const (
	rootHelperKeySchema         = "dirextalk.agent.root-helper-key/v1"
	rootHelperKeyApprovalSchema = "dirextalk.agent.root-helper-key-approval/v1"
)

type AgentRootHelperKeyClient interface {
	PrepareRootHelperKey(context.Context, AgentRootHelperKeyPrepareRequest) (AgentRootHelperKeyApproval, error)
	ApproveRootHelperKey(context.Context, AgentRootHelperKeyApproveRequest) (AgentRootHelperKeyApproval, error)
	GetRootHelperKey(context.Context, AgentRootHelperKeyGetRequest) (AgentRootHelperKeyApproval, bool, error)
}

type AgentRootHelperKeyPrepareRequest struct {
	IdempotencyKey, DeploymentID, DeviceSignerKeyID string
	ExpectedDeploymentRevision                      int64
}

type AgentRootHelperKeyApproveRequest struct {
	IdempotencyKey, DeploymentID, DeliveryID string
	ExpectedRevision                         int64
	DeviceSignature                          []byte
}

type AgentRootHelperKeyGetRequest struct {
	DeploymentID, DeliveryID string
}

type AgentRootHelperKeySecretPlan struct {
	Partition  string `json:"partition"`
	AccountID  string `json:"account_id"`
	Region     string `json:"region"`
	Name       string `json:"name"`
	VersionID  string `json:"version_id"`
	KMSKeyARN  string `json:"kms_key_arn"`
	TargetPath string `json:"target_path"`
	FileMode   uint32 `json:"file_mode"`
}

type AgentRootHelperKeySecretCoordinate struct {
	ARN       string `json:"arn"`
	Name      string `json:"name"`
	VersionID string `json:"version_id"`
	KMSKeyARN string `json:"kms_key_arn"`
}

type AgentRootHelperKeyBinding struct {
	SchemaVersion     string                             `json:"schema_version"`
	AgentInstanceID   string                             `json:"agent_instance_id"`
	OwnerID           string                             `json:"owner_id"`
	DeliveryID        string                             `json:"delivery_id"`
	DeploymentID      string                             `json:"deployment_id"`
	BindingRevision   int64                              `json:"binding_revision"`
	InstanceID        string                             `json:"instance_id"`
	WorkerRoleARN     string                             `json:"worker_role_arn"`
	WorkerPrincipalID string                             `json:"worker_principal_id"`
	HelperID          string                             `json:"helper_id"`
	SignerKeyID       string                             `json:"signer_key_id"`
	PublicKeyDigest   string                             `json:"public_key_digest"`
	SecretPlan        AgentRootHelperKeySecretPlan       `json:"secret_plan"`
	Secret            AgentRootHelperKeySecretCoordinate `json:"secret,omitempty"`
	NonceDigest       string                             `json:"nonce_digest"`
}

type AgentRootHelperKeyApproval struct {
	SchemaVersion        string
	ChallengeID          string
	DeviceSignerKeyID    string
	Binding              AgentRootHelperKeyBinding
	PublicKey            []byte
	Nonce                []byte
	SigningPayloadCBOR   []byte
	SigningPayloadDigest string
	Status               string
	Revision             int64
	DeviceSignature      []byte
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type rootHelperKeyPublicApproval struct {
	SchemaVersion        string                    `json:"schema_version"`
	ChallengeID          string                    `json:"challenge_id"`
	DeviceSignerKeyID    string                    `json:"device_signer_key_id"`
	Binding              AgentRootHelperKeyBinding `json:"binding"`
	PublicKey            string                    `json:"public_key"`
	Nonce                string                    `json:"nonce"`
	SigningPayloadDigest string                    `json:"signing_payload_digest"`
	Status               string                    `json:"status"`
	Revision             int64                     `json:"revision"`
	DeviceSignature      string                    `json:"device_signature,omitempty"`
	CreatedAt            time.Time                 `json:"created_at"`
	UpdatedAt            time.Time                 `json:"updated_at"`
}
