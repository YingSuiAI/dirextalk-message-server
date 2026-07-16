package cloud

import (
	"context"
	"errors"
	"time"
)

var (
	ErrAgentCloudControlInvalid         = errors.New("agent cloud control request is invalid")
	ErrAgentCloudControlConflict        = errors.New("agent cloud control request conflicts with current state")
	ErrAgentCloudControlRejected        = errors.New("agent cloud control request was rejected")
	ErrAgentCloudControlUnavailable     = errors.New("agent cloud control service is unavailable")
	ErrAgentCloudControlInvalidResponse = errors.New("agent cloud control returned an invalid response")
)

const AgentCloudPlanStatusApproved = "approved"

// AgentCloudControlClient is intentionally narrower than the Agent gRPC
// service. It can neither register approval devices nor retrieve secrets.
type AgentCloudControlClient interface {
	ListAgentCloudPlans(context.Context) ([]AgentCloudPlan, error)
	ListAgentCloudConnections(context.Context) ([]AgentCloudConnection, error)
	GetAgentCloudPlan(context.Context, AgentCloudPlanRequest) (AgentCloudPlan, bool, error)
	CreateAgentCloudApprovalChallenge(context.Context, AgentCloudChallengeRequest) (AgentCloudChallenge, error)
	ApproveAgentCloudPlan(context.Context, AgentCloudApproveRequest) (AgentCloudPlan, error)
	EstablishAgentAWSConnection(context.Context, AgentCloudEstablishRequest) (AgentCloudConnection, error)
	GetAgentCloudConnection(context.Context, AgentCloudConnectionRequest) (AgentCloudConnection, bool, error)
}

type AgentCloudPlanRequest struct{ PlanID string }
type AgentCloudConnectionRequest struct{ ConnectionID string }

type AgentCloudPlan struct {
	PlanID, OwnerID, ConnectionID          string
	Recipe                                 AgentCloudRecipeBinding
	QuoteID, QuoteDigest, QuoteScopeDigest string
	CandidateProfile                       string
	QuoteValidUntil                        time.Time
	Resource                               AgentCloudResourceScope
	Network                                AgentCloudNetworkScope
	SecretScope                            []AgentCloudSecretScope
	IntegrationScope                       []AgentCloudIntegrationScope
	Retention                              AgentCloudRetentionScope
	Status, PlanHash                       string
	Revision                               int64
}

type AgentCloudRecipeBinding struct{ RecipeID, Digest, Maturity string }

type AgentCloudResourceScope struct {
	Region                string
	AvailabilityZones     []string
	InstanceType          string
	InstanceCount         uint32
	Architecture          string
	VCPU                  uint32
	MemoryMiB             uint64
	GPUType               string
	GPUCount              uint32
	GPUMemoryMiB          uint64
	DiskGiB               uint64
	VolumeType            string
	VolumeIOPS            uint32
	VolumeThroughputMiBPS uint32
	VolumeEncrypted       bool
	PurchaseOption        string
	WorkerImageID         string
	WorkerImageDigest     string
}

type AgentCloudNetworkScope struct {
	VPCID, SubnetID, SecurityGroupID    string
	EntryPoint                          string
	PublicExposure                      bool
	IngressPorts                        []uint32
	Hostname                            string
	TLSRequired, AuthenticationRequired bool
}

type AgentCloudSecretScope struct{ SecretRef, Purpose, Delivery string }
type AgentCloudIntegrationScope struct {
	Kind, Name string
	Scopes     []string
}
type AgentCloudRetentionScope struct {
	Class              string
	AutoDestroy        bool
	GracePeriodSeconds uint32
	MaxLifetimeSeconds uint64
}

// ExpectedPlan is the owner-bound plan previously read from Agent. It lets the
// transport reject signed-scope substitution in challenge/approval responses.
type AgentCloudChallengeRequest struct {
	IdempotencyKey, PlanID, SignerKeyID string
	ExpectedRevision                    int64
	ExpectedPlan                        AgentCloudPlan
}

type AgentCloudChallenge struct {
	ApprovalID, ChallengeID, SignerKeyID                     string
	AgentInstanceID, OwnerID, PlanID, PlanHash               string
	ConnectionID, RecipeDigest                               string
	QuoteID, QuoteDigest, QuoteScopeDigest, QuoteCandidateID string
	PlanRevision, Revision                                   int64
	ExpiresAt                                                time.Time
	SigningPayloadCBOR                                       []byte
}

type AgentCloudApprovalSignature struct {
	ApprovalID, ChallengeID, SignerKeyID string
	ExpiresAt                            time.Time
	Signature                            []byte
}

type AgentCloudApproveRequest struct {
	IdempotencyKey, PlanID string
	ExpectedRevision       int64
	ExpectedPlan           AgentCloudPlan
	Approval               AgentCloudApprovalSignature
}

type AgentCloudEstablishRequest struct {
	IdempotencyKey, BootstrapSessionID   string
	ExpectedSessionRevision              int64
	PlanID                               string
	ExpectedPlanRevision                 int64
	Approval                             AgentCloudApprovalSignature
	ExpectedConnectionID, ExpectedRegion string
}

type AgentCloudConnection struct {
	ConnectionID, OwnerID, AccountID, Region  string
	ControlRoleARN, FoundationStackID, Status string
	Revision, CredentialGeneration            int64
	CreatedAt, UpdatedAt                      time.Time
}
