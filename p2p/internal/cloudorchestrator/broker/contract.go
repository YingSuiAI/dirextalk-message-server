package broker

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

const (
	// CommandSchema is the immutable Connection Stack command envelope schema.
	CommandSchema = "dirextalk.aws.command/v2"
	// CommandSignatureSchema is the domain separator used by Connection Stack
	// V2 when it verifies the Ed25519 node signature.
	CommandSignatureSchema = "dirextalk.aws.command-signature/v2"
	// QuoteSchema is the only quote schema accepted by this package.
	QuoteSchema = "dirextalk.aws.quote/v1"
	// RegistrationSchema is the stack-derived connection attestation schema.
	RegistrationSchema = "dirextalk.aws.connection-registration/v1"
	// ReceiptSchema is the durable receipt schema emitted by Connection Stack V2.
	ReceiptSchema = "dirextalk.aws.command-receipt/v2"
	// QuoteAction is the fixed read-only broker quote action.
	QuoteAction = "quote.request"
	// RegistrationAction is the fixed broker action that proves a Connection
	// Stack belongs to the exact pending bootstrap. It is not an AWS API proxy.
	RegistrationAction = "connection.registration.verify"

	// QuoteValidity is fixed by the Connection Stack quote contract.
	QuoteValidity = 15 * time.Minute

	maxCommandLifetime = 5 * time.Minute
	maxPayloadBytes    = 192 * 1024
	maxRequestBytes    = 256 * 1024
	maxSafeInteger     = int64(9007199254740991) // Number.MAX_SAFE_INTEGER

	canonicalInstantLayout = "2006-01-02T15:04:05.000Z"
)

var (
	idPattern               = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	keyIDPattern            = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	accountIDPattern        = regexp.MustCompile(`^\d{12}$`)
	sha256Pattern           = regexp.MustCompile(`^[0-9a-f]{64}$`)
	namedSHA256Pattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	regionPattern           = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	availabilityZonePattern = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9][a-z]$`)
	instanceTypePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	itemPattern             = regexp.MustCompile(`^[a-z][a-z0-9_.-]{1,63}$`)
	canonicalInstantPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	stackARNPattern         = regexp.MustCompile(`^arn:(?:aws|aws-cn|aws-us-gov):cloudformation:((?:af|ap|ca|cn|eu|il|me|mx|sa|us)(?:-gov)?-[a-z]+-[0-9]):(\d{12}):stack/[A-Za-z][-A-Za-z0-9]{0,127}/[A-Za-z0-9-]{8,128}$`)

	quoteIncludedItems   = []string{"ec2_linux_ondemand"}
	quoteUnincludedItems = []string{
		"cloudwatch_logs",
		"data_transfer",
		"ebs_gp3",
		"public_ipv4",
		"snapshots",
		"taxes",
	}
	commandFields = []string{
		"schema",
		"connection_id",
		"command_id",
		"node_key_id",
		"issued_at",
		"expires_at",
		"expected_generation",
		"node_counter",
		"action",
		"payload_b64",
		"payload_sha256",
		"signature_b64",
	}
	quoteRequestFields = []string{
		"quote_request_id",
		"plan_digest",
		"region",
		"candidates",
	}
	quoteCandidateRequestFields = []string{
		"candidate_id",
		"tier",
		"instance_type",
		"purchase_option",
		"estimated_disk_gib",
	}
	registrationRequestFields = []string{
		"bootstrap_id",
		"requested_region",
		"stack_arn",
	}
	quoteResultFields        = []string{"status", "receipt", "quote"}
	registrationResultFields = []string{"status", "receipt", "registration"}
	receiptFields            = []string{
		"schema",
		"disposition",
		"connection_id",
		"expected_generation",
		"node_counter",
		"command_id",
		"request_sha256",
		"action",
		"quote",
	}
	registrationReceiptFields = []string{
		"schema",
		"disposition",
		"connection_id",
		"expected_generation",
		"node_counter",
		"command_id",
		"request_sha256",
		"action",
	}
	registrationFields = []string{
		"schema",
		"bootstrap_id",
		"connection_id",
		"account_id",
		"region",
		"broker_command_url",
		"node_key_id",
		"connection_generation",
		"worker_artifact",
		"worker_network",
		"worker_resource_manifest_digest",
		"stack_arn",
		"command_id",
		"request_sha256",
	}
	registrationWorkerArtifactFields = []string{"kind", "ami_id"}
	registrationWorkerNetworkFields  = []string{"vpc_id", "subnet_id", "availability_zone"}
	quoteFields                      = []string{
		"schema",
		"quote_id",
		"connection_id",
		"command_id",
		"request_sha256",
		"quote_request_id",
		"plan_digest",
		"region",
		"currency",
		"quoted_at",
		"valid_until",
		"candidates",
		"included_items",
		"unincluded_items",
	}
	quotedCandidateFields = []string{
		"candidate_id",
		"tier",
		"instance_type",
		"purchase_option",
		"estimated_disk_gib",
		"architecture",
		"vcpu",
		"memory_mib",
		"gpu_count",
		"gpu_memory_mib",
		"hourly_minor",
		"thirty_day_minor",
		"startup_upper_minor",
		"availability_zones",
	}
)

// Error is a safe, machine-readable broker boundary error. Its text never
// includes signed commands, payloads, broker response bodies, or secrets.
type Error struct {
	Code       string
	StatusCode int
	cause      error
}

func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "cloud broker error"
	}
	return "cloud broker error: " + e.Code
}

func (e *Error) Unwrap() error { return e.cause }

func newError(code string, cause error) error {
	return &Error{Code: code, cause: cause}
}

func newHTTPError(code string, statusCode int, cause error) error {
	return &Error{Code: code, StatusCode: statusCode, cause: cause}
}

// QuoteCandidate is both the signed quote request candidate and its immutable
// resource binding in a quote result. Cost fields are only populated in Quote.
type QuoteCandidate struct {
	CandidateID      string `json:"candidate_id"`
	Tier             string `json:"tier"`
	InstanceType     string `json:"instance_type"`
	PurchaseOption   string `json:"purchase_option"`
	EstimatedDiskGiB int64  `json:"estimated_disk_gib"`
}

// QuoteRequest is the canonical payload carried by a quote.request command.
// Its field order is intentionally identical to the JavaScript V2 contract.
type QuoteRequest struct {
	QuoteRequestID string           `json:"quote_request_id"`
	PlanDigest     string           `json:"plan_digest"`
	Region         string           `json:"region"`
	Candidates     []QuoteCandidate `json:"candidates"`
}

// RegistrationRequest is the exact, non-secret payload carried by a
// connection.registration.verify command. It binds one pending bootstrap to
// the Region and CloudFormation stack the user created; it cannot express an
// arbitrary AWS operation.
type RegistrationRequest struct {
	BootstrapID     string `json:"bootstrap_id"`
	RequestedRegion string `json:"requested_region"`
	StackARN        string `json:"stack_arn"`
}

// QuoteCommandInput contains only the data required to create a signed
// read-only quote command. PrivateKey must be the orchestrator-mounted node
// key; it is neither retained by QuoteCommand nor sent on the network.
type QuoteCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            QuoteRequest
	PrivateKey         ed25519.PrivateKey
}

// RegistrationCommandInput contains only the data required to create a signed
// Connection Stack registration verification command. PrivateKey must be the
// orchestrator-mounted node key; it is neither retained nor sent on the
// network.
type RegistrationCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            RegistrationRequest
	PrivateKey         ed25519.PrivateKey
}

// QuoteCommand is the exact JSON envelope accepted by Connection Stack V2.
// There are deliberately no generic action, approval, or credential fields.
type QuoteCommand struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	CommandID          string `json:"command_id"`
	NodeKeyID          string `json:"node_key_id"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	Action             string `json:"action"`
	PayloadB64         string `json:"payload_b64"`
	PayloadSHA256      string `json:"payload_sha256"`
	SignatureB64       string `json:"signature_b64"`
}

// RegistrationCommand is the exact JSON envelope accepted by Connection Stack
// V2 for connection.registration.verify. Its action is fixed by Validate and
// no generic action, approval, or credential field exists on this type.
type RegistrationCommand struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	CommandID          string `json:"command_id"`
	NodeKeyID          string `json:"node_key_id"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	Action             string `json:"action"`
	PayloadB64         string `json:"payload_b64"`
	PayloadSHA256      string `json:"payload_sha256"`
	SignatureB64       string `json:"signature_b64"`
}

// QuoteCommandBinding is the immutable logical identity a persisted command
// must retain on every retry. It deliberately excludes a private key: callers
// use it to prove the envelope has not drifted from a previously fenced quote
// request before an HTTP replay or durable receipt decision.
type QuoteCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            QuoteRequest
}

// RegistrationCommandBinding is the immutable identity a persisted
// registration verification command must preserve during retries.
type RegistrationCommandBinding struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Request            RegistrationRequest
}

// Receipt is the durable broker response proof. Quote is required only for
// quote.request and is intentionally the sole result extension accepted here.
type Receipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
	Quote              *Quote `json:"quote"`
}

// RegistrationReceipt is the action-neutral durable receipt returned for a
// registration verification command. Unlike quote receipts it has no result
// extension; the stack attestation is carried only in RegistrationResult.
type RegistrationReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

// Registration is the strict stack-derived connection attestation returned by
// the Broker after it validates a signed registration request.
type Registration struct {
	Schema                       string                  `json:"schema"`
	BootstrapID                  string                  `json:"bootstrap_id"`
	ConnectionID                 string                  `json:"connection_id"`
	AccountID                    string                  `json:"account_id"`
	Region                       string                  `json:"region"`
	BrokerCommandURL             string                  `json:"broker_command_url"`
	NodeKeyID                    string                  `json:"node_key_id"`
	ConnectionGeneration         int64                   `json:"connection_generation"`
	WorkerArtifact               WorkerArtifactReference `json:"worker_artifact"`
	WorkerNetwork                WorkerNetworkReference  `json:"worker_network"`
	WorkerResourceManifestDigest string                  `json:"worker_resource_manifest_digest"`
	StackARN                     string                  `json:"stack_arn"`
	CommandID                    string                  `json:"command_id"`
	RequestSHA256                string                  `json:"request_sha256"`
}

// WorkerArtifactReference and WorkerNetworkReference are private Stack-owned
// launch bindings. They are attested during connection registration so a
// later deployment.create cannot choose an arbitrary AMI or network.
type WorkerArtifactReference struct {
	Kind  string `json:"kind"`
	AMIID string `json:"ami_id"`
}

type WorkerNetworkReference struct {
	VPCID            string `json:"vpc_id"`
	SubnetID         string `json:"subnet_id"`
	AvailabilityZone string `json:"availability_zone"`
}

// QuotedCandidate is a quote candidate bound to the original signed request.
type QuotedCandidate struct {
	CandidateID       string   `json:"candidate_id"`
	Tier              string   `json:"tier"`
	InstanceType      string   `json:"instance_type"`
	PurchaseOption    string   `json:"purchase_option"`
	EstimatedDiskGiB  int64    `json:"estimated_disk_gib"`
	Architecture      string   `json:"architecture"`
	VCPU              int64    `json:"vcpu"`
	MemoryMiB         int64    `json:"memory_mib"`
	GPUCount          int64    `json:"gpu_count"`
	GPUMemoryMiB      int64    `json:"gpu_memory_mib"`
	HourlyMinor       int64    `json:"hourly_minor"`
	ThirtyDayMinor    int64    `json:"thirty_day_minor"`
	StartupUpperMinor int64    `json:"startup_upper_minor"`
	AvailabilityZones []string `json:"availability_zones"`
}

// Quote is the complete immutable quote emitted by the Connection Stack.
// Timestamp strings are retained after canonical validation so projections do
// not accidentally lose the mandatory millisecond precision.
type Quote struct {
	Schema          string            `json:"schema"`
	QuoteID         string            `json:"quote_id"`
	ConnectionID    string            `json:"connection_id"`
	CommandID       string            `json:"command_id"`
	RequestSHA256   string            `json:"request_sha256"`
	QuoteRequestID  string            `json:"quote_request_id"`
	PlanDigest      string            `json:"plan_digest"`
	Region          string            `json:"region"`
	Currency        string            `json:"currency"`
	QuotedAt        string            `json:"quoted_at"`
	ValidUntil      string            `json:"valid_until"`
	Candidates      []QuotedCandidate `json:"candidates"`
	IncludedItems   []string          `json:"included_items"`
	UnincludedItems []string          `json:"unincluded_items"`
}

// QuoteResult is the only successful HTTP response accepted from the broker.
type QuoteResult struct {
	Status  string  `json:"status"`
	Receipt Receipt `json:"receipt"`
	Quote   Quote   `json:"quote"`
}

// RegistrationResult is the only successful HTTP response accepted for a
// connection.registration.verify command.
type RegistrationResult struct {
	Status       string              `json:"status"`
	Receipt      RegistrationReceipt `json:"receipt"`
	Registration Registration        `json:"registration"`
}

// nodeSignatureFields is the fixed cross-language input to Connection Stack
// V2's buildNodeSignatureBase. All typed commands, including read-only ones,
// must retain every approval digest line in the base string: omitted approval
// material is represented by an empty value, never by an omitted line.
type nodeSignatureFields struct {
	Schema                     string
	ConnectionID               string
	CommandID                  string
	NodeKeyID                  string
	IssuedAt                   string
	ExpiresAt                  string
	ExpectedGeneration         int64
	NodeCounter                int64
	Action                     string
	PayloadSHA256              string
	ApprovalBindingSHA256      string
	ApprovalChallengeID        string
	ApprovalSignatureSHA256    string
	ApprovalProofPayloadSHA256 string
}

func nodeSignatureBase(fields nodeSignatureFields) string {
	return fmt.Sprintf(
		"%s\n"+
			"schema=%s\n"+
			"connection_id=%s\n"+
			"command_id=%s\n"+
			"node_key_id=%s\n"+
			"issued_at=%s\n"+
			"expires_at=%s\n"+
			"expected_generation=%d\n"+
			"node_counter=%d\n"+
			"action=%s\n"+
			"payload_sha256=%s\n"+
			"approval_binding_sha256=%s\n"+
			"approval_challenge_id=%s\n"+
			"approval_signature_sha256=%s\n"+
			"approval_proof_payload_sha256=%s\n",
		CommandSignatureSchema,
		fields.Schema,
		fields.ConnectionID,
		fields.CommandID,
		fields.NodeKeyID,
		fields.IssuedAt,
		fields.ExpiresAt,
		fields.ExpectedGeneration,
		fields.NodeCounter,
		fields.Action,
		fields.PayloadSHA256,
		fields.ApprovalBindingSHA256,
		fields.ApprovalChallengeID,
		fields.ApprovalSignatureSHA256,
		fields.ApprovalProofPayloadSHA256,
	)
}

// NewQuoteCommand returns a canonical Connection Stack V2 quote.request
// envelope. It does not call the network and never persists the private key.
func NewQuoteCommand(input QuoteCommandInput) (QuoteCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return QuoteCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateQuoteRequest(input.Request); err != nil {
		return QuoteCommand{}, err
	}
	issuedAt := canonicalInstant(input.IssuedAt)
	expiresAt := canonicalInstant(input.ExpiresAt)
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return QuoteCommand{}, newError("invalid_quote_request", err)
	}
	command := QuoteCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           issuedAt,
		ExpiresAt:          expiresAt,
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             QuoteAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
	}
	if err := validateQuoteCommand(command, false); err != nil {
		return QuoteCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return QuoteCommand{}, err
	}
	return command, nil
}

// NewRegistrationCommand returns a canonical Connection Stack V2
// connection.registration.verify envelope. It never calls the network and
// never persists the private node key.
func NewRegistrationCommand(input RegistrationCommandInput) (RegistrationCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize {
		return RegistrationCommand{}, newError("invalid_node_private_key", nil)
	}
	if err := validateRegistrationRequest(input.Request); err != nil {
		return RegistrationCommand{}, err
	}
	issuedAt := canonicalInstant(input.IssuedAt)
	expiresAt := canonicalInstant(input.ExpiresAt)
	payload, err := json.Marshal(input.Request)
	if err != nil {
		return RegistrationCommand{}, newError("invalid_registration_request", err)
	}
	command := RegistrationCommand{
		Schema:             CommandSchema,
		ConnectionID:       input.ConnectionID,
		CommandID:          input.CommandID,
		NodeKeyID:          input.NodeKeyID,
		IssuedAt:           issuedAt,
		ExpiresAt:          expiresAt,
		ExpectedGeneration: input.ExpectedGeneration,
		NodeCounter:        input.NodeCounter,
		Action:             RegistrationAction,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      sha256Hex(payload),
	}
	if err := validateRegistrationCommand(command, false); err != nil {
		return RegistrationCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	if err := command.Validate(); err != nil {
		return RegistrationCommand{}, err
	}
	return command, nil
}

// Validate validates a persisted/replayed envelope without requiring a private
// key. Expired envelopes are valid here because the Connection Stack may return
// their durable idempotent receipt; it alone decides whether a new receipt can
// be issued.
func (command QuoteCommand) Validate() error {
	return validateQuoteCommand(command, true)
}

// Validate validates a persisted/replayed registration verification envelope
// without requiring a private key. Expired envelopes remain valid here because
// the Connection Stack can return its durable idempotent receipt.
func (command RegistrationCommand) Validate() error {
	return validateRegistrationCommand(command, true)
}

// ValidateBinding confirms a strict, persisted command still represents one
// already-authorized logical quote request. It validates the exact envelope,
// timestamps, node identity and canonical request payload, but cannot verify
// the Ed25519 signature because the caller intentionally need not hold the
// node public-key registration material.
func (command QuoteCommand) ValidateBinding(binding QuoteCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if binding.IssuedAt.IsZero() || binding.ExpiresAt.IsZero() || !binding.ExpiresAt.After(binding.IssuedAt) ||
		command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID ||
		command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter ||
		command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateQuoteRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.QuoteRequest()
	if err != nil {
		return err
	}
	if !sameQuoteRequest(request, binding.Request) {
		return newError("invalid_quote_request", nil)
	}
	return nil
}

// ValidateBinding confirms a strict, persisted registration command still
// represents the one pending bootstrap it was authorized to verify.
func (command RegistrationCommand) ValidateBinding(binding RegistrationCommandBinding) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if binding.IssuedAt.IsZero() || binding.ExpiresAt.IsZero() || !binding.ExpiresAt.After(binding.IssuedAt) ||
		command.ConnectionID != binding.ConnectionID || command.CommandID != binding.CommandID || command.NodeKeyID != binding.NodeKeyID ||
		command.ExpectedGeneration != binding.ExpectedGeneration || command.NodeCounter != binding.NodeCounter ||
		command.IssuedAt != canonicalInstant(binding.IssuedAt) || command.ExpiresAt != canonicalInstant(binding.ExpiresAt) {
		return newError("invalid_command", nil)
	}
	if err := validateRegistrationRequest(binding.Request); err != nil {
		return err
	}
	request, err := command.RegistrationRequest()
	if err != nil {
		return err
	}
	if request != binding.Request {
		return newError("invalid_registration_request", nil)
	}
	return nil
}

// VerifySignature verifies the command against an explicit node public key.
// The HTTP client does not need this for replay, but callers can use it when
// loading an envelope from an untrusted boundary.
func (command QuoteCommand) VerifySignature(publicKey ed25519.PublicKey) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return newError("invalid_node_public_key", nil)
	}
	signature, _ := base64.StdEncoding.DecodeString(command.SignatureB64)
	if !ed25519.Verify(publicKey, []byte(command.SignatureBase()), signature) {
		return newError("invalid_node_signature", nil)
	}
	return nil
}

// VerifySignature verifies a registration command against the registered node
// public key. The HTTP client does not need it for a durable replay.
func (command RegistrationCommand) VerifySignature(publicKey ed25519.PublicKey) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return newError("invalid_node_public_key", nil)
	}
	signature, _ := base64.StdEncoding.DecodeString(command.SignatureB64)
	if !ed25519.Verify(publicKey, []byte(command.SignatureBase()), signature) {
		return newError("invalid_node_signature", nil)
	}
	return nil
}

// SignatureBase exactly mirrors buildNodeSignatureBase in the Connection Stack
// V2 JavaScript contract. quote.request carries no approval material, but its
// four approval-digest lines are still present and empty.
func (command QuoteCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{
		Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID,
		NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		Action: command.Action, PayloadSHA256: command.PayloadSHA256,
	})
}

// SignatureBase exactly mirrors buildNodeSignatureBase in the Connection Stack
// V2 JavaScript contract. Registration verification carries no approval
// material, so all four approval-digest lines remain empty.
func (command RegistrationCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{
		Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID,
		NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		Action: command.Action, PayloadSHA256: command.PayloadSHA256,
	})
}

// RequestSHA256 is the durable request identity calculated by the Connection
// Stack from the signature base, not from the HTTP body.
func (command QuoteCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// RequestSHA256 is the durable identity used by the Connection Stack for a
// registration verification command.
func (command RegistrationCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

// QuoteRequest returns the strictly decoded request bound into a command.
func (command QuoteCommand) QuoteRequest() (QuoteRequest, error) {
	if err := command.Validate(); err != nil {
		return QuoteRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeQuoteRequestJSON(payload)
	if err != nil {
		return QuoteRequest{}, newError("invalid_payload", err)
	}
	if err := validateQuoteRequest(request); err != nil {
		return QuoteRequest{}, err
	}
	return request, nil
}

// RegistrationRequest returns the strictly decoded, exact registration
// verification payload bound into this command.
func (command RegistrationCommand) RegistrationRequest() (RegistrationRequest, error) {
	if err := command.Validate(); err != nil {
		return RegistrationRequest{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(command.PayloadB64)
	request, err := decodeRegistrationRequestJSON(payload)
	if err != nil {
		return RegistrationRequest{}, newError("invalid_payload", err)
	}
	if err := validateRegistrationRequest(request); err != nil {
		return RegistrationRequest{}, err
	}
	return request, nil
}

// ParseQuoteCommand strictly parses an exact persisted V2 quote envelope. It
// rejects unknown, duplicate, differently cased, and action-incompatible
// fields before callers replay the durable command bytes.
func ParseQuoteCommand(raw []byte) (QuoteCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return QuoteCommand{}, newError("invalid_command", err)
	}
	var command QuoteCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return QuoteCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return QuoteCommand{}, err
	}
	return command, nil
}

// ParseRegistrationCommand strictly parses an exact persisted V2
// connection.registration.verify envelope. It rejects unknown, duplicate,
// differently cased, and action-incompatible fields before a durable replay.
func ParseRegistrationCommand(raw []byte) (RegistrationCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return RegistrationCommand{}, newError("invalid_command", err)
	}
	var command RegistrationCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return RegistrationCommand{}, newError("invalid_command", err)
	}
	if err := command.Validate(); err != nil {
		return RegistrationCommand{}, err
	}
	return command, nil
}

// ValidateQuoteResult validates the complete public receipt/quote response
// before it can be stored or projected by the orchestrator.
func ValidateQuoteResult(command QuoteCommand, result QuoteResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "quote_issued" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	request, err := command.QuoteRequest()
	if err != nil {
		return err
	}
	if err := validateReceipt(command, result.Receipt); err != nil {
		return err
	}
	if result.Receipt.Quote == nil {
		return newError("missing_quote_receipt", nil)
	}
	if err := validateQuote(command, request, result.Quote); err != nil {
		return err
	}
	if err := validateQuote(command, request, *result.Receipt.Quote); err != nil {
		return err
	}
	if !quotesEqual(result.Quote, *result.Receipt.Quote) {
		return newError("quote_receipt_mismatch", nil)
	}
	return nil
}

// ValidateRegistrationResult validates the complete public registration
// response before a stack-derived broker endpoint can be persisted. Every
// attested field is bound to the exact signed command and its payload.
func ValidateRegistrationResult(command RegistrationCommand, result RegistrationResult) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if result.Status != "connection_registered" && result.Status != "idempotent" {
		return newError("invalid_broker_status", nil)
	}
	if err := validateRegistrationReceipt(command, result.Receipt); err != nil {
		return err
	}
	if result.Status == "connection_registered" && result.Receipt.Disposition != "committed" {
		return newError("invalid_registration_receipt", nil)
	}
	if result.Status == "idempotent" && result.Receipt.Disposition != "idempotent" {
		return newError("invalid_registration_receipt", nil)
	}
	request, err := command.RegistrationRequest()
	if err != nil {
		return err
	}
	if err := validateRegistration(command, request, result.Registration); err != nil {
		return err
	}
	return nil
}

func validateQuoteCommand(command QuoteCommand, requireSignature bool) error {
	if command.Schema != CommandSchema || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) || !keyIDPattern.MatchString(command.NodeKeyID) || command.Action != QuoteAction {
		return newError("invalid_command", nil)
	}
	if !safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, err := parseCanonicalInstant(command.IssuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expiresAt, err := parseCanonicalInstant(command.ExpiresAt)
	if err != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime {
		return newError("invalid_command", err)
	}
	if !sha256Pattern.MatchString(command.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeQuoteRequestJSON(payload)
	if err != nil {
		return newError("invalid_payload", err)
	}
	if err := validateQuoteRequest(request); err != nil {
		return err
	}
	canonicalPayload, err := json.Marshal(request)
	if err != nil || !bytes.Equal(payload, canonicalPayload) {
		return newError("noncanonical_payload", err)
	}
	if requireSignature {
		signature, err := decodeCanonicalBase64(command.SignatureB64)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func validateRegistrationCommand(command RegistrationCommand, requireSignature bool) error {
	if command.Schema != CommandSchema || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) || !keyIDPattern.MatchString(command.NodeKeyID) || command.Action != RegistrationAction {
		return newError("invalid_command", nil)
	}
	if !safePositive(command.ExpectedGeneration) || !safeNonnegative(command.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, err := parseCanonicalInstant(command.IssuedAt)
	if err != nil {
		return newError("invalid_command", err)
	}
	expiresAt, err := parseCanonicalInstant(command.ExpiresAt)
	if err != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime {
		return newError("invalid_command", err)
	}
	if !sha256Pattern.MatchString(command.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) > maxPayloadBytes || sha256Hex(payload) != command.PayloadSHA256 {
		return newError("invalid_payload", err)
	}
	request, err := decodeRegistrationRequestJSON(payload)
	if err != nil {
		return newError("invalid_payload", err)
	}
	if err := validateRegistrationRequest(request); err != nil {
		return err
	}
	canonicalPayload, err := json.Marshal(request)
	if err != nil || !bytes.Equal(payload, canonicalPayload) {
		return newError("noncanonical_payload", err)
	}
	if requireSignature {
		signature, err := decodeCanonicalBase64(command.SignatureB64)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", err)
		}
	}
	return nil
}

func validateQuoteRequest(request QuoteRequest) error {
	if !idPattern.MatchString(request.QuoteRequestID) || !namedSHA256Pattern.MatchString(request.PlanDigest) || !regionPattern.MatchString(request.Region) {
		return newError("invalid_quote_request", nil)
	}
	if len(request.Candidates) < 1 || len(request.Candidates) > 3 {
		return newError("invalid_quote_request", nil)
	}
	seenIDs := make(map[string]struct{}, len(request.Candidates))
	seenTiers := make(map[string]struct{}, len(request.Candidates))
	for _, candidate := range request.Candidates {
		if !idPattern.MatchString(candidate.CandidateID) || !validTier(candidate.Tier) || !instanceTypePattern.MatchString(candidate.InstanceType) || candidate.PurchaseOption != "on_demand" || candidate.EstimatedDiskGiB < 8 || candidate.EstimatedDiskGiB > 16384 {
			return newError("invalid_quote_request", nil)
		}
		if _, exists := seenIDs[candidate.CandidateID]; exists {
			return newError("invalid_quote_request", nil)
		}
		if _, exists := seenTiers[candidate.Tier]; exists {
			return newError("invalid_quote_request", nil)
		}
		seenIDs[candidate.CandidateID] = struct{}{}
		seenTiers[candidate.Tier] = struct{}{}
	}
	return nil
}

func validateRegistrationRequest(request RegistrationRequest) error {
	if !idPattern.MatchString(request.BootstrapID) || !regionPattern.MatchString(request.RequestedRegion) {
		return newError("invalid_registration_request", nil)
	}
	region, _, ok := parseStackARN(request.StackARN)
	if !ok || region != request.RequestedRegion {
		return newError("invalid_registration_request", nil)
	}
	return nil
}

func sameQuoteRequest(left, right QuoteRequest) bool {
	if left.QuoteRequestID != right.QuoteRequestID || left.PlanDigest != right.PlanDigest || left.Region != right.Region || len(left.Candidates) != len(right.Candidates) {
		return false
	}
	for index, candidate := range left.Candidates {
		other := right.Candidates[index]
		if candidate != other {
			return false
		}
	}
	return true
}

func validateReceipt(command QuoteCommand, receipt Receipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") || receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration || receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID || receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != QuoteAction {
		return newError("invalid_quote_receipt", nil)
	}
	if receipt.Quote == nil {
		return newError("missing_quote_receipt", nil)
	}
	return nil
}

func validateRegistrationReceipt(command RegistrationCommand, receipt RegistrationReceipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") || receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration || receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID || receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != RegistrationAction {
		return newError("invalid_registration_receipt", nil)
	}
	return nil
}

func validateRegistration(command RegistrationCommand, request RegistrationRequest, registration Registration) error {
	if registration.Schema != RegistrationSchema || !idPattern.MatchString(registration.BootstrapID) || !idPattern.MatchString(registration.ConnectionID) || !accountIDPattern.MatchString(registration.AccountID) || !regionPattern.MatchString(registration.Region) || !keyIDPattern.MatchString(registration.NodeKeyID) || !safePositive(registration.ConnectionGeneration) || !idPattern.MatchString(registration.CommandID) || !sha256Pattern.MatchString(registration.RequestSHA256) {
		return newError("invalid_registration", nil)
	}
	if registration.WorkerArtifact.Kind != "fixed_ami" || !amiIDPattern.MatchString(registration.WorkerArtifact.AMIID) ||
		!vpcIDPattern.MatchString(registration.WorkerNetwork.VPCID) || !subnetIDPattern.MatchString(registration.WorkerNetwork.SubnetID) ||
		!availabilityZonePattern.MatchString(registration.WorkerNetwork.AvailabilityZone) ||
		registration.WorkerNetwork.AvailabilityZone[:len(registration.WorkerNetwork.AvailabilityZone)-1] != registration.Region ||
		!namedSHA256Pattern.MatchString(registration.WorkerResourceManifestDigest) {
		return newError("invalid_registration", nil)
	}
	endpoint, err := parseBrokerEndpoint(registration.BrokerCommandURL)
	if err != nil || endpoint.String() != registration.BrokerCommandURL {
		return newError("invalid_registration", err)
	}
	stackRegion, stackAccountID, stackARNValid := parseStackARN(registration.StackARN)
	if !stackARNValid || stackRegion != registration.Region || stackAccountID != registration.AccountID || registration.BootstrapID != request.BootstrapID || registration.ConnectionID != command.ConnectionID || registration.Region != request.RequestedRegion || registration.NodeKeyID != command.NodeKeyID || registration.ConnectionGeneration != command.ExpectedGeneration || registration.StackARN != request.StackARN || registration.CommandID != command.CommandID || registration.RequestSHA256 != command.RequestSHA256() {
		return newError("invalid_registration", nil)
	}
	return nil
}

func validateQuote(command QuoteCommand, request QuoteRequest, quote Quote) error {
	if quote.Schema != QuoteSchema || !idPattern.MatchString(quote.QuoteID) || quote.QuoteID != "quote-"+command.RequestSHA256()[:32] || quote.ConnectionID != command.ConnectionID || quote.CommandID != command.CommandID || quote.RequestSHA256 != command.RequestSHA256() || quote.QuoteRequestID != request.QuoteRequestID || quote.PlanDigest != request.PlanDigest || quote.Region != request.Region || quote.Currency != "USD" {
		return newError("invalid_quote", nil)
	}
	quotedAt, err := parseCanonicalInstant(quote.QuotedAt)
	if err != nil {
		return newError("invalid_quote", err)
	}
	validUntil, err := parseCanonicalInstant(quote.ValidUntil)
	if err != nil || validUntil.Sub(quotedAt) != QuoteValidity {
		return newError("invalid_quote", err)
	}
	issuedAt, _ := parseCanonicalInstant(command.IssuedAt)
	expiresAt, _ := parseCanonicalInstant(command.ExpiresAt)
	if quotedAt.Before(issuedAt) || quotedAt.After(expiresAt) {
		return newError("invalid_quote", nil)
	}
	if len(quote.Candidates) != len(request.Candidates) {
		return newError("invalid_quote", nil)
	}
	for index, candidate := range quote.Candidates {
		requested := request.Candidates[index]
		if candidate.CandidateID != requested.CandidateID || candidate.Tier != requested.Tier || candidate.InstanceType != requested.InstanceType || candidate.PurchaseOption != requested.PurchaseOption || candidate.EstimatedDiskGiB != requested.EstimatedDiskGiB ||
			(candidate.Architecture != "amd64" && candidate.Architecture != "arm64") || candidate.VCPU < 1 || candidate.VCPU > 65535 || candidate.MemoryMiB < 1 || candidate.MemoryMiB > 4294967295 ||
			candidate.GPUCount < 0 || candidate.GPUCount > 65535 || candidate.GPUMemoryMiB < 0 || candidate.GPUMemoryMiB > 4294967295 ||
			(candidate.GPUCount == 0) != (candidate.GPUMemoryMiB == 0) || !safeNonnegative(candidate.HourlyMinor) || !safeNonnegative(candidate.ThirtyDayMinor) || !safeNonnegative(candidate.StartupUpperMinor) || !canonicalStrings(candidate.AvailabilityZones, availabilityZonePattern, true) {
			return newError("invalid_quote", nil)
		}
	}
	if !sameStrings(quote.IncludedItems, quoteIncludedItems) || !sameStrings(quote.UnincludedItems, quoteUnincludedItems) || !canonicalStrings(quote.IncludedItems, itemPattern, true) || !canonicalStrings(quote.UnincludedItems, itemPattern, true) {
		return newError("invalid_quote", nil)
	}
	return nil
}

func canonicalInstant(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format(canonicalInstantLayout)
}

func parseCanonicalInstant(value string) (time.Time, error) {
	if !canonicalInstantPattern.MatchString(value) {
		return time.Time{}, errors.New("invalid canonical instant")
	}
	parsed, err := time.Parse(canonicalInstantLayout, value)
	if err != nil || canonicalInstant(parsed) != value {
		return time.Time{}, errors.New("invalid canonical instant")
	}
	return parsed, nil
}

func parseStackARN(value string) (region, accountID string, ok bool) {
	matches := stackARNPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	if value == "" || len(value)%4 != 0 {
		return nil, errors.New("invalid canonical base64")
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("invalid canonical base64")
	}
	return decoded, nil
}

func decodeStrictJSON(raw []byte, target any) error {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func decodeQuoteRequestJSON(raw []byte) (QuoteRequest, error) {
	if err := validateQuoteRequestJSONShape(raw); err != nil {
		return QuoteRequest{}, err
	}
	var request QuoteRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return QuoteRequest{}, err
	}
	return request, nil
}

func decodeRegistrationRequestJSON(raw []byte) (RegistrationRequest, error) {
	if err := validateRegistrationRequestJSONShape(raw); err != nil {
		return RegistrationRequest{}, err
	}
	var request RegistrationRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return RegistrationRequest{}, err
	}
	return request, nil
}

func decodeQuoteResultJSON(raw []byte) (QuoteResult, error) {
	if err := validateQuoteResultJSONShape(raw); err != nil {
		return QuoteResult{}, err
	}
	var result QuoteResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return QuoteResult{}, err
	}
	return result, nil
}

func decodeRegistrationResultJSON(raw []byte) (RegistrationResult, error) {
	if err := validateRegistrationResultJSONShape(raw); err != nil {
		return RegistrationResult{}, err
	}
	var result RegistrationResult
	if err := decodeStrictJSON(raw, &result); err != nil {
		return RegistrationResult{}, err
	}
	return result, nil
}

func validateQuoteRequestJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, quoteRequestFields)
	if err != nil {
		return err
	}
	candidates, err := exactJSONArray(object["candidates"])
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if _, err := exactJSONObject(candidate, quoteCandidateRequestFields); err != nil {
			return err
		}
	}
	return nil
}

func validateRegistrationRequestJSONShape(raw []byte) error {
	_, err := exactJSONObject(raw, registrationRequestFields)
	return err
}

func validateQuoteResultJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, quoteResultFields)
	if err != nil {
		return err
	}
	receipt, err := exactJSONObject(object["receipt"], receiptFields)
	if err != nil {
		return err
	}
	if err := validateQuoteJSONShape(receipt["quote"]); err != nil {
		return err
	}
	return validateQuoteJSONShape(object["quote"])
}

func validateRegistrationResultJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, registrationResultFields)
	if err != nil {
		return err
	}
	if _, err := exactJSONObject(object["receipt"], registrationReceiptFields); err != nil {
		return err
	}
	registration, err := exactJSONObject(object["registration"], registrationFields)
	if err != nil {
		return err
	}
	if _, err = exactJSONObject(registration["worker_artifact"], registrationWorkerArtifactFields); err != nil {
		return err
	}
	_, err = exactJSONObject(registration["worker_network"], registrationWorkerNetworkFields)
	return err
}

func validateQuoteJSONShape(raw []byte) error {
	object, err := exactJSONObject(raw, quoteFields)
	if err != nil {
		return err
	}
	candidates, err := exactJSONArray(object["candidates"])
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if _, err := exactJSONObject(candidate, quotedCandidateFields); err != nil {
			return err
		}
	}
	return nil
}

func exactJSONObject(raw []byte, expected []string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := decodeStrictJSON(raw, &object); err != nil {
		return nil, err
	}
	if object == nil || len(object) != len(expected) {
		return nil, errors.New("unexpected JSON object fields")
	}
	for _, key := range expected {
		if _, ok := object[key]; !ok {
			return nil, errors.New("unexpected JSON object fields")
		}
	}
	return object, nil
}

func exactJSONArray(raw []byte) ([]json.RawMessage, error) {
	var values []json.RawMessage
	if err := decodeStrictJSON(raw, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, errors.New("JSON array is required")
	}
	return values, nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, exists := keys[key]; exists {
				return errors.New("duplicate JSON object key")
			}
			keys[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("unterminated JSON array")
		}
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func safePositive(value int64) bool {
	return value > 0 && value <= maxSafeInteger
}

func safeNonnegative(value int64) bool {
	return value >= 0 && value <= maxSafeInteger
}

func validTier(value string) bool {
	return value == "economy" || value == "recommended" || value == "performance"
}

func canonicalStrings(values []string, pattern *regexp.Regexp, nonempty bool) bool {
	if (nonempty && len(values) == 0) || (!nonempty && values == nil) {
		return false
	}
	previous := ""
	for index, value := range values {
		if !pattern.MatchString(value) || (index > 0 && previous >= value) {
			return false
		}
		previous = value
	}
	return true
}

func sameStrings(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range actual {
		if actual[index] != expected[index] {
			return false
		}
	}
	return true
}

func quotesEqual(left, right Quote) bool {
	if left.Schema != right.Schema || left.QuoteID != right.QuoteID || left.ConnectionID != right.ConnectionID || left.CommandID != right.CommandID || left.RequestSHA256 != right.RequestSHA256 || left.QuoteRequestID != right.QuoteRequestID || left.PlanDigest != right.PlanDigest || left.Region != right.Region || left.Currency != right.Currency || left.QuotedAt != right.QuotedAt || left.ValidUntil != right.ValidUntil || !sameStrings(left.IncludedItems, right.IncludedItems) || !sameStrings(left.UnincludedItems, right.UnincludedItems) || len(left.Candidates) != len(right.Candidates) {
		return false
	}
	for index, candidate := range left.Candidates {
		other := right.Candidates[index]
		if candidate.CandidateID != other.CandidateID || candidate.Tier != other.Tier || candidate.InstanceType != other.InstanceType || candidate.PurchaseOption != other.PurchaseOption || candidate.EstimatedDiskGiB != other.EstimatedDiskGiB || candidate.Architecture != other.Architecture || candidate.VCPU != other.VCPU || candidate.MemoryMiB != other.MemoryMiB || candidate.GPUCount != other.GPUCount || candidate.GPUMemoryMiB != other.GPUMemoryMiB || candidate.HourlyMinor != other.HourlyMinor || candidate.ThirtyDayMinor != other.ThirtyDayMinor || candidate.StartupUpperMinor != other.StartupUpperMinor || !sameStrings(candidate.AvailabilityZones, other.AvailabilityZones) {
			return false
		}
	}
	return true
}
