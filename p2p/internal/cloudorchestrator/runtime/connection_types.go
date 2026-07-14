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

const ConnectionRegistrationRequested = cloudmodule.OutboxKindConnectionRegistrationRequested

var accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// ConnectionRegistrationClaim is a private, lease-fenced Stack completion.
// Endpoint and Stack ARN are never ProductCore fields; only the independent
// Orchestrator receives them so it can submit the fixed signed attestation.
type ConnectionRegistrationClaim struct {
	OutboxID           string
	Kind               string
	AggregateType      string
	AggregateID        string
	BootstrapID        string
	ConnectionID       string
	RequestedRegion    string
	BrokerEndpoint     string
	StackARN           string
	NodeKeyID          string
	ExpectedGeneration int64
	JobID              string
	LeaseToken         string
	Attempt            int
	Request            ConnectionRegistrationRequest
	Command            ConnectionRegistrationCommand
}

// ConnectionRegistrationRequest is the exact fixed payload sent to Broker V2.
// It contains no account or endpoint claim: the Stack derives those facts from
// its own CloudFormation/Lambda environment and compares the requested Region
// and ARN against its immutable deployment identity.
type ConnectionRegistrationRequest struct {
	BootstrapID     string `json:"bootstrap_id"`
	RequestedRegion string `json:"requested_region"`
	StackARN        string `json:"stack_arn"`
}

func (request ConnectionRegistrationRequest) Validate() error {
	if !validResearchIdentifier("bootstrap_id", request.BootstrapID) || !cloudRegion(request.RequestedRegion) ||
		cloudmodule.ValidateConnectionRegistrationStackARN(request.StackARN, request.RequestedRegion) != nil {
		return errors.New("connection registration request is invalid")
	}
	return nil
}

func (request ConnectionRegistrationRequest) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, value := range []string{request.BootstrapID, request.RequestedRegion, request.StackARN} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ConnectionRegistrationCommand is durable identity for one attestation. It
// is intentionally parallel to QuoteCommand so a network retry can replay the
// byte-for-byte signed envelope rather than burn a second counter.
type ConnectionRegistrationCommand struct {
	CommandID          string
	BootstrapID        string
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

type SignedConnectionRegistrationCommand struct {
	EnvelopeJSON  string
	PayloadJSON   string
	PayloadSHA256 string
	RequestSHA256 string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// BrokerRegistration is the only activation evidence accepted from a
// Connection Stack. It is deliberately narrow: no resource IDs, AWS
// credentials, Lambda logs, public keys, or arbitrary response data are
// accepted into the durable control plane.
type BrokerRegistration struct {
	Schema               string
	BootstrapID          string
	ConnectionID         string
	AccountID            string
	Region               string
	BrokerCommandURL     string
	NodeKeyID            string
	ConnectionGeneration int64
	StackARN             string
	CommandID            string
	RequestSHA256        string
	ReceiptJSON          string
}

type ConnectionRegistrationStore interface {
	ClaimConnectionRegistration(context.Context, string, time.Duration) (ConnectionRegistrationClaim, bool, error)
	PersistConnectionRegistrationCommand(context.Context, ConnectionRegistrationClaim, SignedConnectionRegistrationCommand) error
	MarkConnectionRegistrationStarted(context.Context, ConnectionRegistrationClaim) error
	CommitConnectionRegistration(context.Context, ConnectionRegistrationClaim, BrokerRegistration) error
	DeferConnectionRegistration(context.Context, ConnectionRegistrationClaim, string, time.Time) error
	ExpireConnectionRegistrationCommand(context.Context, ConnectionRegistrationClaim) error
	FailConnectionRegistration(context.Context, ConnectionRegistrationClaim, string) error
}

type ConnectionRegistrationTransport interface {
	BuildConnectionRegistrationCommand(ConnectionRegistrationCommand, ConnectionRegistrationRequest) (SignedConnectionRegistrationCommand, error)
	RequestConnectionRegistration(context.Context, string, ConnectionRegistrationCommand, SignedConnectionRegistrationCommand, ConnectionRegistrationRequest) (BrokerRegistration, error)
}

type connectionRegistrationExpiredError struct{ cause error }

func (e connectionRegistrationExpiredError) Error() string {
	if e.cause == nil {
		return "connection_registration_command_expired"
	}
	return "connection_registration_command_expired: " + e.cause.Error()
}

func (e connectionRegistrationExpiredError) Unwrap() error { return e.cause }

func ConnectionRegistrationCommandExpired(cause error) error {
	return connectionRegistrationExpiredError{cause: cause}
}

func connectionRegistrationCommandExpired(err error) bool {
	var expired connectionRegistrationExpiredError
	return errors.As(err, &expired)
}

func ConnectionRegistrationRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "connection_registration_retryable"), cause: cause}
}

func validateConnectionRegistrationClaim(claim ConnectionRegistrationClaim) error {
	if claim.Kind != ConnectionRegistrationRequested || claim.AggregateType != "connection_bootstrap" || claim.OutboxID == "" || claim.AggregateID != claim.BootstrapID ||
		!validResearchIdentifier("bootstrap_id", claim.BootstrapID) || !validResearchIdentifier("cloud_connection_id", claim.ConnectionID) ||
		!cloudRegion(claim.RequestedRegion) || claim.BrokerEndpoint == "" || claim.StackARN == "" || !cloudKeyIdentifier(claim.NodeKeyID) ||
		claim.ExpectedGeneration <= 0 || claim.JobID == "" || claim.LeaseToken == "" {
		return errors.New("connection registration claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.RequestedRegion); err != nil {
		return errors.New("connection registration endpoint is invalid")
	}
	if err := claim.Request.Validate(); err != nil || claim.Request.BootstrapID != claim.BootstrapID || claim.Request.RequestedRegion != claim.RequestedRegion || claim.Request.StackARN != claim.StackARN {
		return errors.New("connection registration request does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.CommandID == "" || claim.Command.BootstrapID != claim.BootstrapID || claim.Command.ConnectionID != claim.ConnectionID ||
		claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 || claim.Command.RequestDigest != digest {
		return errors.New("connection registration command does not bind the claim")
	}
	return nil
}

func validateSignedConnectionRegistrationCommand(command ConnectionRegistrationCommand, signed SignedConnectionRegistrationCommand) error {
	if command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || !lowerHexSHA256(signed.PayloadSHA256) || !lowerHexSHA256(signed.RequestSHA256) ||
		len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 8*1024 || signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed connection registration command is invalid")
	}
	return nil
}

// ValidateBrokerRegistration repeats all untrusted response checks before a
// Store activates an owner-visible Connection. Store implementations must call
// it again under their fenced transaction.
func ValidateBrokerRegistration(claim ConnectionRegistrationClaim, signed SignedConnectionRegistrationCommand, registration BrokerRegistration) error {
	if err := validateConnectionRegistrationClaim(claim); err != nil {
		return err
	}
	if err := validateSignedConnectionRegistrationCommand(claim.Command, signed); err != nil {
		return err
	}
	if registration.Schema != "dirextalk.aws.connection-registration/v1" || registration.BootstrapID != claim.BootstrapID || registration.ConnectionID != claim.ConnectionID ||
		!accountIDPattern.MatchString(registration.AccountID) || registration.Region != claim.RequestedRegion || registration.NodeKeyID != claim.NodeKeyID ||
		registration.ConnectionGeneration != claim.ExpectedGeneration || registration.StackARN != claim.StackARN || registration.CommandID != claim.Command.CommandID ||
		registration.RequestSHA256 != signed.RequestSHA256 || registration.BrokerCommandURL != claim.BrokerEndpoint || strings.TrimSpace(registration.ReceiptJSON) == "" {
		return errors.New("broker connection registration does not bind the signed command")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(registration.BrokerCommandURL, claim.RequestedRegion); err != nil {
		return fmt.Errorf("broker connection registration endpoint: %w", err)
	}
	return nil
}

func cloudRegion(value string) bool {
	return regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`).MatchString(value)
}

func cloudKeyIdentifier(value string) bool {
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`).MatchString(value)
}
