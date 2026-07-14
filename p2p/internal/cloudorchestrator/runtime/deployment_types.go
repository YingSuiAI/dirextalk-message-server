package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

// DeploymentProvisionRequested is private control-plane work created only
// after a device-approved PlanV1 has been persisted. The outbox itself is not
// a public ProductCore or MCP message.
const DeploymentProvisionRequested = cloudmodule.OutboxKindDeploymentProvisionRequested

const deploymentReceiptSchema = "dirextalk.aws.deployment-receipt/v1"

var (
	deploymentDigestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	ec2InstanceIDPattern       = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	ec2AMIIdentifierPattern    = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	ec2VPCIdentifierPattern    = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	ec2SubnetIdentifierPattern = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	ec2VolumeIdentifierPattern = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	ec2ENIIdentifierPattern    = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
	deploymentAvailabilityZone = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9][a-z]$`)
)

// DeploymentProvisionClaim is a private, lease-fenced instruction to create
// exactly one exclusive Worker VM. It contains no AWS credential, user-data,
// SSH key, ingress rule, Worker token, URL, or cloud-control permission.
// QuoteValidUntil is rechecked immediately before signing or network I/O so a
// disconnected runner cannot purchase from an expired user-approved quote.
type DeploymentProvisionClaim struct {
	OutboxID           string
	Kind               string
	AggregateType      string
	AggregateID        string
	DeploymentID       string
	PlanID             string
	ConnectionID       string
	Region             string
	PlanRevision       int64
	QuoteValidUntil    time.Time
	BrokerEndpoint     string
	ExpectedGeneration int64
	NodeKeyID          string
	JobID              string
	LeaseToken         string
	Attempt            int
	Request            DeploymentCreateRequest
	Command            DeploymentCreateCommand
}

// DeploymentCreateRequest is the exact fixed deployment.create payload. The
// Connection Stack validates the requested base AMI and private subnet against
// its trusted manifest and quote receipt. It must reject caller-provided
// user-data, tokens, URLs, key pairs, instance profiles, security groups, or
// arbitrary EC2 parameters.
//
// The Plan ID intentionally stays out of the broker payload: PlanHash plus
// PlanRevision are the approval keys. The durable Store binds that hash to the
// private Plan row before it creates this request.
type DeploymentCreateRequest struct {
	DeploymentID   string                     `json:"deployment_id"`
	PlanHash       string                     `json:"plan_hash"`
	PlanRevision   uint64                     `json:"plan_revision"`
	QuoteID        string                     `json:"quote_id"`
	QuoteDigest    string                     `json:"quote_digest"`
	CandidateID    string                     `json:"candidate_id"`
	ManifestDigest string                     `json:"manifest_digest"`
	WorkerArtifact WorkerArtifactReferenceV1  `json:"worker_artifact"`
	Network        DeploymentNetworkReference `json:"network"`
}

// WorkerArtifactReferenceV1 can name only the fixed base AMI selected through
// the separate trusted Worker artifact registry. It carries no image pull
// secret, bootstrap token, user-data, or arbitrary image URL.
type WorkerArtifactReferenceV1 struct {
	Kind  string `json:"kind"`
	AMIID string `json:"ami_id"`
}

// DeploymentNetworkReference is a Stack-owned private placement reference.
// It has no public IP, ingress, domain, endpoint, or security-group field.
type DeploymentNetworkReference struct {
	VPCID            string `json:"vpc_id"`
	SubnetID         string `json:"subnet_id"`
	AvailabilityZone string `json:"availability_zone"`
}

// Validate rejects any incomplete or expanded deployment-create request. The
// closed shape makes later ingress, secret, cost, or Worker-session changes a
// new reviewed contract rather than an invisible optional field.
func (request DeploymentCreateRequest) Validate() error {
	if !validResearchIdentifier("deployment_id", request.DeploymentID) || request.PlanRevision == 0 ||
		!deploymentDigestPattern.MatchString(request.PlanHash) || !validResearchIdentifier("quote_id", request.QuoteID) ||
		!deploymentDigestPattern.MatchString(request.QuoteDigest) || !validResearchIdentifier("candidate_id", request.CandidateID) ||
		!deploymentDigestPattern.MatchString(request.ManifestDigest) || request.WorkerArtifact.Kind != "ami" ||
		!ec2AMIIdentifierPattern.MatchString(request.WorkerArtifact.AMIID) || !ec2VPCIdentifierPattern.MatchString(request.Network.VPCID) ||
		!ec2SubnetIdentifierPattern.MatchString(request.Network.SubnetID) || !deploymentAvailabilityZone.MatchString(request.Network.AvailabilityZone) {
		return errors.New("deployment create request is invalid")
	}
	return nil
}

// Digest is a stable, length-delimited identity for one requested Worker
// launch. It is separate from the signed-envelope request hash so the runtime
// can prove a persisted command still binds the approved launch references.
func (request DeploymentCreateRequest) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	parts := []string{
		"dirextalk.cloud.deployment-create-request/v1",
		request.DeploymentID,
		request.PlanHash,
		fmt.Sprint(request.PlanRevision),
		request.QuoteID,
		request.QuoteDigest,
		request.CandidateID,
		request.ManifestDigest,
		request.WorkerArtifact.Kind,
		request.WorkerArtifact.AMIID,
		request.Network.VPCID,
		request.Network.SubnetID,
		request.Network.AvailabilityZone,
	}
	for _, value := range parts {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// DeploymentCreateCommand is durable command identity for exactly one Broker
// create request. Once SignedEnvelope is persisted every indeterminate retry
// must replay those byte-for-byte values, preserving the Broker ClientToken
// and preventing a duplicate Worker purchase.
type DeploymentCreateCommand struct {
	CommandID          string
	DeploymentID       string
	ConnectionID       string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	Attempt            int
	IssuedAt           time.Time
	ExpiresAt          time.Time
	RequestDigest      string
	PayloadJSON        string
	PayloadSHA256      string
	RequestSHA256      string
	SignedEnvelope     string
	State              string
}

type SignedDeploymentCreateCommand struct {
	EnvelopeJSON  string
	PayloadJSON   string
	PayloadSHA256 string
	RequestSHA256 string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// BrokerDeployment is a strictly typed, private accepted launch receipt. It
// exposes cloud resource identifiers only to the separate Orchestrator Store;
// ProductCore projections must never include them. It deliberately contains no
// Worker session URL/token, user-data, log, credential, or provider role.
type BrokerDeployment struct {
	Schema              string
	DeploymentID        string
	ConnectionID        string
	CommandID           string
	RequestSHA256       string
	ResourceStatus      string
	InstanceID          string
	VolumeIDs           []string
	NetworkInterfaceIDs []string
	ReceiptJSON         string
}

// DeploymentProvisionStore is the durable, lease-fenced state machine. Its
// implementation belongs to the separate Orchestrator database role; the
// Message Server never invokes it and never gets a signed command or receipt.
type DeploymentProvisionStore interface {
	ClaimDeploymentProvision(context.Context, string, time.Duration) (DeploymentProvisionClaim, bool, error)
	PersistDeploymentCreateCommand(context.Context, DeploymentProvisionClaim, SignedDeploymentCreateCommand) error
	MarkDeploymentProvisionStarted(context.Context, DeploymentProvisionClaim) error
	CommitDeploymentProvision(context.Context, DeploymentProvisionClaim, BrokerDeployment) error
	DeferDeploymentProvision(context.Context, DeploymentProvisionClaim, string, time.Time) error
	ExpireDeploymentCreateCommand(context.Context, DeploymentProvisionClaim) error
	FailDeploymentProvision(context.Context, DeploymentProvisionClaim, string) error
}

// DeploymentProvisionTransport can only build and submit the one fixed typed
// Broker command. It has no AWS SDK, generic action, credential, or Worker
// session API.
type DeploymentProvisionTransport interface {
	BuildDeploymentCreateCommand(DeploymentCreateCommand, DeploymentCreateRequest) (SignedDeploymentCreateCommand, error)
	RequestDeploymentCreate(context.Context, string, DeploymentCreateCommand, SignedDeploymentCreateCommand, DeploymentCreateRequest) (BrokerDeployment, error)
}

type deploymentCreateExpiredError struct{ cause error }

func (e deploymentCreateExpiredError) Error() string {
	if e.cause == nil {
		return "deployment_create_command_expired"
	}
	return "deployment_create_command_expired: " + e.cause.Error()
}

func (e deploymentCreateExpiredError) Unwrap() error { return e.cause }

// DeploymentCreateCommandExpired marks the one explicit Broker result that
// allows a later fenced attempt to allocate a new node counter and envelope.
func DeploymentCreateCommandExpired(cause error) error {
	return deploymentCreateExpiredError{cause: cause}
}

func deploymentCreateCommandExpired(err error) bool {
	var expired deploymentCreateExpiredError
	return errors.As(err, &expired)
}

func DeploymentProvisionRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "deployment_provision_retryable"), cause: cause}
}

func validateDeploymentProvisionClaim(claim DeploymentProvisionClaim) error {
	if claim.Kind != DeploymentProvisionRequested || claim.AggregateType != "deployment" || claim.OutboxID == "" || claim.AggregateID != claim.DeploymentID ||
		!validResearchIdentifier("deployment_id", claim.DeploymentID) || !validResearchIdentifier("plan_id", claim.PlanID) || !validResearchIdentifier("cloud_connection_id", claim.ConnectionID) ||
		!cloudRegion(claim.Region) || claim.PlanRevision <= 0 || claim.QuoteValidUntil.IsZero() || claim.BrokerEndpoint == "" || !cloudKeyIdentifier(claim.NodeKeyID) ||
		claim.ExpectedGeneration <= 0 || !validResearchIdentifier("job_id", claim.JobID) || claim.LeaseToken == "" {
		return errors.New("deployment provision claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return errors.New("deployment provision endpoint is invalid")
	}
	if err := claim.Request.Validate(); err != nil || claim.Request.DeploymentID != claim.DeploymentID || claim.Request.PlanRevision != uint64(claim.PlanRevision) ||
		!strings.HasPrefix(claim.Request.Network.AvailabilityZone, claim.Region) {
		return errors.New("deployment create request does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.CommandID == "" || claim.Command.DeploymentID != claim.DeploymentID || claim.Command.ConnectionID != claim.ConnectionID ||
		claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 ||
		claim.Command.Attempt <= 0 || claim.Command.RequestDigest != digest {
		return errors.New("deployment create command does not bind the claim")
	}
	return nil
}

func validateSignedDeploymentCreateCommand(command DeploymentCreateCommand, signed SignedDeploymentCreateCommand) error {
	if command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || !lowerHexSHA256(signed.PayloadSHA256) ||
		!lowerHexSHA256(signed.RequestSHA256) || len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 16*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed deployment create command is invalid")
	}
	return nil
}

// ValidateBrokerDeployment repeats the Stack receipt checks before a Store can
// transition a Deployment from queued to provisioning. Store implementations
// must call it again under the same lease-fenced transaction that completes
// the outbox, because a process-local validation is not an authorization.
func ValidateBrokerDeployment(claim DeploymentProvisionClaim, signed SignedDeploymentCreateCommand, deployment BrokerDeployment) error {
	if err := validateDeploymentProvisionClaim(claim); err != nil {
		return err
	}
	if err := validateSignedDeploymentCreateCommand(claim.Command, signed); err != nil {
		return err
	}
	if deployment.Schema != deploymentReceiptSchema || deployment.DeploymentID != claim.DeploymentID || deployment.ConnectionID != claim.ConnectionID ||
		deployment.CommandID != claim.Command.CommandID || deployment.RequestSHA256 != signed.RequestSHA256 || deployment.ResourceStatus != "provisioning" ||
		!ec2InstanceIDPattern.MatchString(deployment.InstanceID) || !canonicalCloudResourceIDs(deployment.VolumeIDs, ec2VolumeIdentifierPattern) ||
		!canonicalCloudResourceIDs(deployment.NetworkInterfaceIDs, ec2ENIIdentifierPattern) || strings.TrimSpace(deployment.ReceiptJSON) == "" {
		return errors.New("broker deployment does not bind the signed command")
	}
	return nil
}

func canonicalCloudResourceIDs(values []string, pattern *regexp.Regexp) bool {
	if len(values) == 0 || len(values) > 16 || pattern == nil {
		return false
	}
	for index, value := range values {
		if !pattern.MatchString(value) || (index > 0 && values[index-1] >= value) {
			return false
		}
	}
	return true
}
