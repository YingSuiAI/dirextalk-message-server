// Package brokertransport adapts the closed Connection Stack V2 quote,
// registration and approval-bound deployment.create contracts to the Cloud
// Orchestrator runtime. It is intentionally unable to issue arbitrary provider
// actions or hold AWS credentials.
package brokertransport

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const commandLifetime = 4 * time.Minute

var _ runtime.QuoteTransport = (*Transport)(nil)
var _ runtime.ConnectionRegistrationTransport = (*Transport)(nil)
var _ runtime.DeploymentProvisionTransport = (*Transport)(nil)
var _ runtime.ServiceDestroyTransport = (*Transport)(nil)
var _ runtime.ServiceBackupTransport = (*Transport)(nil)
var _ runtime.ServiceRestorePlanTransport = (*Transport)(nil)

// Transport keeps the mounted node key in process memory only. The key is
// never serialized, returned, or written to PostgreSQL.
type Transport struct {
	privateKey ed25519.PrivateKey
	now        func() time.Time
	rootCAs    *x509.CertPool
}

func New(privateKey ed25519.PrivateKey, now func() time.Time) (*Transport, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("cloud broker node signing key is invalid")
	}
	if now == nil {
		now = time.Now
	}
	return &Transport{privateKey: append(ed25519.PrivateKey(nil), privateKey...), now: now}, nil
}

// BuildQuoteCommand signs one exact V2 envelope. The runtime persists the
// returned bytes before calling RequestQuote, so any indeterminate retry uses
// the same counter, timestamps, payload and signature.
func (t *Transport) BuildQuoteCommand(command runtime.QuoteCommand, request cloudcontracts.QuoteRequestV1) (runtime.SignedQuoteCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedQuoteCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedQuoteCommand{}, fmt.Errorf("invalid quote request: %w", err)
	}
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.ConnectionID != request.CloudConnectionID || command.NodeKeyID == "" || command.ExpectedGeneration <= 0 || command.NodeCounter <= 0 {
		return runtime.SignedQuoteCommand{}, errors.New("quote command does not bind the request")
	}
	issuedAt := t.now().UTC().Truncate(time.Millisecond)
	expiresAt := issuedAt.Add(commandLifetime)
	brokerCommand, err := broker.NewQuoteCommand(broker.QuoteCommandInput{
		ConnectionID:       command.ConnectionID,
		CommandID:          command.CommandID,
		NodeKeyID:          command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration,
		NodeCounter:        command.NodeCounter,
		IssuedAt:           issuedAt,
		ExpiresAt:          expiresAt,
		Request:            brokerRequest(request, digest),
		PrivateKey:         t.privateKey,
	})
	if err != nil {
		return runtime.SignedQuoteCommand{}, fmt.Errorf("sign quote command: %w", err)
	}
	payload, err := base64.StdEncoding.DecodeString(brokerCommand.PayloadB64)
	if err != nil {
		return runtime.SignedQuoteCommand{}, errors.New("signed quote payload is invalid")
	}
	envelope, err := json.Marshal(brokerCommand)
	if err != nil {
		return runtime.SignedQuoteCommand{}, errors.New("signed quote envelope is invalid")
	}
	return runtime.SignedQuoteCommand{
		EnvelopeJSON:  string(envelope),
		PayloadJSON:   string(payload),
		PayloadSHA256: brokerCommand.PayloadSHA256,
		RequestSHA256: brokerCommand.RequestSHA256(),
		IssuedAt:      issuedAt,
		ExpiresAt:     expiresAt,
	}, nil
}

// RequestQuote re-parses the durable envelope rather than reconstructing it.
// This prevents a caller from accidentally changing a retry's signature or
// payload after it was persisted.
func (t *Transport) RequestQuote(ctx context.Context, endpoint string, command runtime.QuoteCommand, signed runtime.SignedQuoteCommand, request cloudcontracts.QuoteRequestV1) (runtime.BrokerQuote, error) {
	brokerCommand, err := broker.ParseQuoteCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.BrokerQuote{}, errors.New("persisted quote envelope is invalid")
	}
	if err := commandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.BrokerQuote{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.BrokerQuote{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitQuote(ctx, brokerCommand)
	if err != nil {
		return runtime.BrokerQuote{}, classifyBrokerError(err)
	}
	return runtimeQuote(result)
}

// BuildConnectionRegistrationCommand signs the one fixed attestation command
// that can activate a pending Connection Stack. It has no provider mutation,
// approval, credential, or arbitrary action surface.
func (t *Transport) BuildConnectionRegistrationCommand(command runtime.ConnectionRegistrationCommand, request runtime.ConnectionRegistrationRequest) (runtime.SignedConnectionRegistrationCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedConnectionRegistrationCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedConnectionRegistrationCommand{}, errors.New("invalid connection registration request")
	}
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.ConnectionID == "" || command.BootstrapID != request.BootstrapID ||
		command.NodeKeyID == "" || command.ExpectedGeneration <= 0 || command.NodeCounter <= 0 {
		return runtime.SignedConnectionRegistrationCommand{}, errors.New("connection registration command does not bind the request")
	}
	issuedAt := t.now().UTC().Truncate(time.Millisecond)
	expiresAt := issuedAt.Add(commandLifetime)
	brokerCommand, err := broker.NewRegistrationCommand(broker.RegistrationCommandInput{
		ConnectionID:       command.ConnectionID,
		CommandID:          command.CommandID,
		NodeKeyID:          command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration,
		NodeCounter:        command.NodeCounter,
		IssuedAt:           issuedAt,
		ExpiresAt:          expiresAt,
		Request:            registrationRequest(request),
		PrivateKey:         t.privateKey,
	})
	if err != nil {
		return runtime.SignedConnectionRegistrationCommand{}, fmt.Errorf("sign connection registration command: %w", err)
	}
	payload, err := base64.StdEncoding.DecodeString(brokerCommand.PayloadB64)
	if err != nil {
		return runtime.SignedConnectionRegistrationCommand{}, errors.New("signed connection registration payload is invalid")
	}
	envelope, err := json.Marshal(brokerCommand)
	if err != nil {
		return runtime.SignedConnectionRegistrationCommand{}, errors.New("signed connection registration envelope is invalid")
	}
	return runtime.SignedConnectionRegistrationCommand{
		EnvelopeJSON:  string(envelope),
		PayloadJSON:   string(payload),
		PayloadSHA256: brokerCommand.PayloadSHA256,
		RequestSHA256: brokerCommand.RequestSHA256(),
		IssuedAt:      issuedAt,
		ExpiresAt:     expiresAt,
	}, nil
}

// RequestConnectionRegistration re-parses the exact persisted registration
// envelope before any network use. This preserves one signed counter and
// payload across indeterminate retries.
func (t *Transport) RequestConnectionRegistration(ctx context.Context, endpoint string, command runtime.ConnectionRegistrationCommand, signed runtime.SignedConnectionRegistrationCommand, request runtime.ConnectionRegistrationRequest) (runtime.BrokerRegistration, error) {
	brokerCommand, err := broker.ParseRegistrationCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.BrokerRegistration{}, errors.New("persisted connection registration envelope is invalid")
	}
	if err := registrationCommandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.BrokerRegistration{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.BrokerRegistration{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitRegistration(ctx, brokerCommand)
	if err != nil {
		return runtime.BrokerRegistration{}, classifyRegistrationBrokerError(err)
	}
	return runtimeRegistration(result)
}

// BuildDeploymentCreateCommand signs one exact approval-bound create command.
// ApprovalProofJSON is read only from the private approved-plan record and is
// serialized only inside the returned durable Broker envelope; it never enters
// ProductCore, event payloads or logs. quoteValidUntil is the independently
// durable quote boundary; approval expiry is always derived directly from the
// parsed signed ApprovalV1 so a caller cannot lengthen a billable envelope by
// passing a separate, drifted timestamp.
func (t *Transport) BuildDeploymentCreateCommand(command runtime.DeploymentCreateCommand, request runtime.DeploymentCreateRequest, approvalProofJSON string, quoteValidUntil time.Time) (runtime.SignedDeploymentCreateCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("invalid deployment create request")
	}
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.DeploymentID != request.DeploymentID || command.ConnectionID == "" || command.NodeKeyID == "" ||
		command.ExpectedGeneration <= 0 || command.ExpectedGeneration != request.ConnectionGeneration || command.NodeCounter <= 0 {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("deployment create command does not bind the request")
	}
	approvalProof, err := broker.ParseApprovalProof([]byte(approvalProofJSON))
	if err != nil {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("deployment approval proof is invalid")
	}
	issuedAt := t.now().UTC().Truncate(time.Millisecond)
	if !approvalProof.ExpiresAt.After(issuedAt) {
		return runtime.SignedDeploymentCreateCommand{}, runtime.DeploymentProvisionPlanExpired(runtime.DeploymentProvisionApprovalExpired, errors.New("deployment approval expired before envelope issuance"))
	}
	quoteValidUntil = quoteValidUntil.UTC().Truncate(time.Millisecond)
	if !quoteValidUntil.After(issuedAt) {
		return runtime.SignedDeploymentCreateCommand{}, runtime.DeploymentProvisionPlanExpired(runtime.DeploymentProvisionQuoteExpired, errors.New("deployment quote expired before envelope issuance"))
	}
	expiresAt := issuedAt.Add(commandLifetime)
	if quoteValidUntil.Before(expiresAt) {
		expiresAt = quoteValidUntil
	}
	approvalExpiresAt := approvalProof.ExpiresAt.UTC().Truncate(time.Millisecond)
	if approvalExpiresAt.Before(expiresAt) {
		expiresAt = approvalExpiresAt
	}
	if !expiresAt.After(issuedAt) {
		return runtime.SignedDeploymentCreateCommand{}, runtime.DeploymentProvisionPlanExpired(runtime.DeploymentProvisionApprovalExpired, errors.New("deployment approval leaves no envelope lifetime"))
	}
	brokerCommand, err := broker.NewDeploymentCommand(broker.DeploymentCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issuedAt, ExpiresAt: expiresAt,
		Request: deploymentRequest(request), ApprovalProof: approvalProof, PrivateKey: t.privateKey,
	})
	if err != nil {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("sign deployment create command failed")
	}
	payload, err := base64.StdEncoding.DecodeString(brokerCommand.PayloadB64)
	if err != nil {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("signed deployment payload is invalid")
	}
	envelope, err := json.Marshal(brokerCommand)
	if err != nil {
		return runtime.SignedDeploymentCreateCommand{}, errors.New("signed deployment envelope is invalid")
	}
	return runtime.SignedDeploymentCreateCommand{
		EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: brokerCommand.PayloadSHA256,
		RequestSHA256: brokerCommand.RequestSHA256(), IssuedAt: issuedAt, ExpiresAt: expiresAt,
	}, nil
}

// RequestDeploymentCreate re-parses the exact persisted private envelope and
// rejects any binding drift before the single Broker HTTP request. Its retry
// identity is the preserved command ID/node counter/signature, never a new
// cloud purchase attempt.
func (t *Transport) RequestDeploymentCreate(ctx context.Context, endpoint string, command runtime.DeploymentCreateCommand, signed runtime.SignedDeploymentCreateCommand, request runtime.DeploymentCreateRequest) (runtime.BrokerDeployment, error) {
	brokerCommand, err := broker.ParseDeploymentCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.BrokerDeployment{}, errors.New("persisted deployment envelope is invalid")
	}
	if err := deploymentCommandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.BrokerDeployment{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.BrokerDeployment{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitDeployment(ctx, brokerCommand)
	if err != nil {
		return runtime.BrokerDeployment{}, classifyDeploymentBrokerError(err)
	}
	return runtimeDeployment(result)
}

func brokerRequest(request cloudcontracts.QuoteRequestV1, digest string) broker.QuoteRequest {
	candidates := make([]broker.QuoteCandidate, len(request.Candidates))
	for index, candidate := range request.Candidates {
		candidates[index] = broker.QuoteCandidate{
			CandidateID: candidate.CandidateID, Tier: string(candidate.Tier), InstanceType: candidate.InstanceType,
			PurchaseOption: string(candidate.PurchaseOption), EstimatedDiskGiB: int64(candidate.EstimatedDiskGiB),
		}
	}
	return broker.QuoteRequest{
		QuoteRequestID: request.QuoteRequestID,
		PlanDigest:     digest,
		Region:         request.Region,
		Candidates:     candidates,
	}
}

func registrationRequest(request runtime.ConnectionRegistrationRequest) broker.RegistrationRequest {
	return broker.RegistrationRequest{
		BootstrapID:     request.BootstrapID,
		RequestedRegion: request.RequestedRegion,
		StackARN:        request.StackARN,
	}
}

func deploymentRequest(request runtime.DeploymentCreateRequest) broker.DeploymentRequest {
	return broker.DeploymentRequest{
		Schema:                 request.Schema,
		DeploymentID:           request.DeploymentID,
		ConnectionGeneration:   request.ConnectionGeneration,
		PlanHash:               request.PlanHash,
		PlanRevision:           request.PlanRevision,
		QuoteID:                request.QuoteID,
		QuoteDigest:            request.QuoteDigest,
		CandidateID:            request.CandidateID,
		ResourceManifestDigest: request.ResourceManifestDigest,
		WorkerArtifact: broker.DeploymentWorkerArtifact{
			Kind: request.WorkerArtifact.Kind, AMIID: request.WorkerArtifact.AMIID,
		},
		Network: broker.DeploymentNetwork{
			VPCID: request.Network.VPCID, SubnetID: request.Network.SubnetID, AvailabilityZone: request.Network.AvailabilityZone,
		},
	}
}

func commandMatches(command runtime.QuoteCommand, signed runtime.SignedQuoteCommand, actual broker.QuoteCommand, request cloudcontracts.QuoteRequestV1) error {
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest {
		return errors.New("persisted quote envelope does not bind the quote request")
	}
	if err := actual.ValidateBinding(broker.QuoteCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: brokerRequest(request, digest),
	}); err != nil {
		return errors.New("persisted quote envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted quote envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted quote envelope payload is invalid")
	}
	return nil
}

func registrationCommandMatches(command runtime.ConnectionRegistrationCommand, signed runtime.SignedConnectionRegistrationCommand, actual broker.RegistrationCommand, request runtime.ConnectionRegistrationRequest) error {
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.BootstrapID != request.BootstrapID {
		return errors.New("persisted connection registration envelope does not bind the request")
	}
	if err := actual.ValidateBinding(broker.RegistrationCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: registrationRequest(request),
	}); err != nil {
		return errors.New("persisted connection registration envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted connection registration envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted connection registration envelope payload is invalid")
	}
	return nil
}

func deploymentCommandMatches(command runtime.DeploymentCreateCommand, signed runtime.SignedDeploymentCreateCommand, actual broker.DeploymentCommand, request runtime.DeploymentCreateRequest) error {
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.DeploymentID != request.DeploymentID || command.ExpectedGeneration != request.ConnectionGeneration {
		return errors.New("persisted deployment envelope does not bind the deployment request")
	}
	if err := actual.ValidateBinding(broker.DeploymentCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: deploymentRequest(request), ApprovalProof: actual.ApprovalProof,
	}); err != nil {
		return errors.New("persisted deployment envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted deployment envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted deployment envelope payload is invalid")
	}
	return nil
}

func runtimeQuote(result broker.QuoteResult) (runtime.BrokerQuote, error) {
	quotedAt, err := time.Parse("2006-01-02T15:04:05.000Z", result.Quote.QuotedAt)
	if err != nil {
		return runtime.BrokerQuote{}, errors.New("broker quote timestamp is invalid")
	}
	validUntil, err := time.Parse("2006-01-02T15:04:05.000Z", result.Quote.ValidUntil)
	if err != nil {
		return runtime.BrokerQuote{}, errors.New("broker quote expiry is invalid")
	}
	candidates := make([]cloudcontracts.QuoteCandidateV1, len(result.Quote.Candidates))
	for index, candidate := range result.Quote.Candidates {
		candidates[index] = cloudcontracts.QuoteCandidateV1{
			CandidateID: candidate.CandidateID, Tier: cloudcontracts.QuoteTier(candidate.Tier), InstanceType: candidate.InstanceType,
			PurchaseOption: cloudcontracts.PurchaseOption(candidate.PurchaseOption), Architecture: cloudcontracts.Architecture(candidate.Architecture),
			VCPU: uint16(candidate.VCPU), MemoryMiB: uint32(candidate.MemoryMiB), GPUCount: uint16(candidate.GPUCount), GPUMemoryMiB: uint32(candidate.GPUMemoryMiB), HourlyMinor: candidate.HourlyMinor,
			ThirtyDayMinor: candidate.ThirtyDayMinor, StartupUpperMinor: candidate.StartupUpperMinor,
			EstimatedDiskGiB: uint32(candidate.EstimatedDiskGiB), AvailabilityZones: append([]string(nil), candidate.AvailabilityZones...),
		}
	}
	receipt, err := json.Marshal(result.Receipt)
	if err != nil {
		return runtime.BrokerQuote{}, errors.New("broker receipt cannot be encoded")
	}
	return runtime.BrokerQuote{
		Schema: result.Quote.Schema, QuoteID: result.Quote.QuoteID, ConnectionID: result.Quote.ConnectionID,
		CommandID: result.Quote.CommandID, RequestSHA256: result.Quote.RequestSHA256, QuoteRequestID: result.Quote.QuoteRequestID,
		PlanDigest: result.Quote.PlanDigest, Region: result.Quote.Region, Currency: result.Quote.Currency,
		QuotedAt: quotedAt.UTC(), ValidUntil: validUntil.UTC(), Candidates: candidates,
		IncludedItems: append([]string(nil), result.Quote.IncludedItems...), UnincludedItems: append([]string(nil), result.Quote.UnincludedItems...),
		ReceiptJSON: string(receipt),
	}, nil
}

func runtimeRegistration(result broker.RegistrationResult) (runtime.BrokerRegistration, error) {
	receipt, err := json.Marshal(result.Receipt)
	if err != nil {
		return runtime.BrokerRegistration{}, errors.New("broker connection registration receipt cannot be encoded")
	}
	return runtime.BrokerRegistration{
		Schema:               result.Registration.Schema,
		BootstrapID:          result.Registration.BootstrapID,
		ConnectionID:         result.Registration.ConnectionID,
		AccountID:            result.Registration.AccountID,
		Region:               result.Registration.Region,
		BrokerCommandURL:     result.Registration.BrokerCommandURL,
		NodeKeyID:            result.Registration.NodeKeyID,
		ConnectionGeneration: result.Registration.ConnectionGeneration,
		WorkerArtifact: runtime.WorkerArtifactReferenceV1{
			Kind: result.Registration.WorkerArtifact.Kind, AMIID: result.Registration.WorkerArtifact.AMIID,
		},
		WorkerNetwork: runtime.DeploymentNetworkReference{
			VPCID: result.Registration.WorkerNetwork.VPCID, SubnetID: result.Registration.WorkerNetwork.SubnetID,
			AvailabilityZone: result.Registration.WorkerNetwork.AvailabilityZone,
		},
		WorkerResourceManifestDigest: result.Registration.WorkerResourceManifestDigest,
		StackARN:                     result.Registration.StackARN,
		CommandID:                    result.Registration.CommandID,
		RequestSHA256:                result.Registration.RequestSHA256,
		ReceiptJSON:                  string(receipt),
	}, nil
}

func runtimeDeployment(result broker.DeploymentResult) (runtime.BrokerDeployment, error) {
	receipt, err := json.Marshal(result.Receipt)
	if err != nil {
		return runtime.BrokerDeployment{}, errors.New("broker deployment receipt cannot be encoded")
	}
	return runtime.BrokerDeployment{
		Schema: result.Deployment.Schema, DeploymentID: result.Deployment.DeploymentID, ConnectionID: result.Deployment.ConnectionID,
		CommandID: result.Receipt.CommandID, RequestSHA256: result.Deployment.RequestSHA256, ResourceStatus: result.Deployment.ResourceStatus,
		InstanceID: result.Deployment.InstanceID, VolumeIDs: append([]string(nil), result.Deployment.VolumeIDs...),
		NetworkInterfaceIDs: append([]string(nil), result.Deployment.NetworkInterfaceIDs...), ReceiptJSON: string(receipt),
	}, nil
}

func classifyBrokerError(err error) error {
	var brokerError *broker.Error
	if !errors.As(err, &brokerError) {
		return runtime.QuoteRetryable("broker_unavailable", err)
	}
	if brokerError.Code == "expired_command" {
		return runtime.QuoteCommandExpired(err)
	}
	if brokerError.StatusCode == 429 || brokerError.StatusCode >= 500 {
		return runtime.QuoteRetryable("broker_unavailable", err)
	}
	switch brokerError.Code {
	case "broker_timeout":
		return runtime.QuoteRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.QuoteRetryable("broker_unavailable", err)
	default:
		return err
	}
}

func classifyRegistrationBrokerError(err error) error {
	var brokerError *broker.Error
	if !errors.As(err, &brokerError) {
		return runtime.ConnectionRegistrationRetryable("broker_unavailable", err)
	}
	if brokerError.Code == "expired_command" {
		return runtime.ConnectionRegistrationCommandExpired(err)
	}
	if brokerError.StatusCode == 429 || brokerError.StatusCode >= 500 {
		return runtime.ConnectionRegistrationRetryable("broker_unavailable", err)
	}
	switch brokerError.Code {
	case "broker_timeout":
		return runtime.ConnectionRegistrationRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.ConnectionRegistrationRetryable("broker_unavailable", err)
	default:
		return err
	}
}

func classifyDeploymentBrokerError(err error) error {
	var brokerError *broker.Error
	if !errors.As(err, &brokerError) {
		return runtime.DeploymentProvisionRetryable("broker_unavailable", err)
	}
	if brokerError.Code == "expired_command" {
		return runtime.DeploymentCreateCommandExpired(err)
	}
	if brokerError.Code == "quote_expired" {
		// The closed Stack contract returns quote_expired before it invokes the
		// dedicated EC2 provisioner, so this is known-no-create rather than an
		// indeterminate HTTP failure.
		return runtime.DeploymentProvisionPlanExpired(runtime.DeploymentProvisionQuoteExpired, err)
	}
	if brokerError.Code == "approval_expired" {
		// Like quote_expired, the Stack consumes/verifies the device proof before
		// EC2 provisioning and never returns this after a cloud mutation.
		return runtime.DeploymentProvisionPlanExpired(runtime.DeploymentProvisionApprovalExpired, err)
	}
	if brokerError.StatusCode == 429 || brokerError.StatusCode >= 500 {
		return runtime.DeploymentProvisionRetryable("broker_unavailable", err)
	}
	switch brokerError.Code {
	case "broker_timeout":
		return runtime.DeploymentProvisionRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.DeploymentProvisionRetryable("broker_unavailable", err)
	default:
		return err
	}
}
