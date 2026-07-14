package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	invalidQuoteClaimCode    = "invalid_quote_claim"
	invalidQuoteResultCode   = "invalid_quote_result"
	quoteTransportFailedCode = "quote_transport_failed"
)

var (
	requiredQuoteIncludedItems   = []string{"ec2_linux_ondemand"}
	requiredQuoteUnincludedItems = []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"}
)

// QuoteRunner independently obtains a signed, read-only quote after the
// researcher has persisted an experimental draft. It cannot create a Worker or
// issue any action other than the Connection Stack's quote.request command.
type QuoteRunner struct {
	store     QuoteStore
	transport QuoteTransport
	cfg       Config
}

func NewQuoteRunner(store QuoteStore, transport QuoteTransport, cfg Config) *QuoteRunner {
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = cfg.Lease / 2
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &QuoteRunner{store: store, transport: transport, cfg: cfg}
}

// RunOnce claims at most one price request. Network ambiguity deliberately
// retains the exact signed command for replay. A Broker's expired_command is
// the sole result that retires that envelope and allows a new counter.
func (r *QuoteRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud quote store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud quote transport is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute {
		return false, errors.New("cloud quote lease must be between 1ns and 5m")
	}
	if r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud quote attempt timeout must be shorter than its lease")
	}
	claim, found, err := r.store.ClaimQuoteRequest(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err := validateQuoteClaim(claim); err != nil {
		return true, r.store.FailQuote(ctx, claim, invalidQuoteClaimCode)
	}
	if err := r.store.MarkQuoteStarted(ctx, claim); err != nil {
		// Do not sign or invoke a Broker until the user-visible job state is
		// durably fenced. The source outbox remains leased for a safe retry.
		return true, fmt.Errorf("mark cloud quote started: %w", err)
	}
	signed := SignedQuoteCommand{
		EnvelopeJSON:  claim.Command.SignedEnvelope,
		PayloadJSON:   claim.Command.PayloadJSON,
		PayloadSHA256: claim.Command.PayloadSHA256,
		RequestSHA256: claim.Command.RequestSHA256,
		IssuedAt:      claim.Command.IssuedAt,
		ExpiresAt:     claim.Command.ExpiresAt,
	}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildQuoteCommand(claim.Command, claim.Request)
		if err != nil {
			return true, r.store.FailQuote(ctx, claim, invalidQuoteClaimCode)
		}
		if err := r.store.PersistQuoteCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist cloud quote command: %w", err)
		}
		claim.Command.PayloadJSON = signed.PayloadJSON
		claim.Command.PayloadSHA256 = signed.PayloadSHA256
		claim.Command.RequestSHA256 = signed.RequestSHA256
		claim.Command.SignedEnvelope = signed.EnvelopeJSON
		claim.Command.IssuedAt = signed.IssuedAt
		claim.Command.ExpiresAt = signed.ExpiresAt
	}
	if err := validateSignedQuoteCommand(claim.Command, signed); err != nil {
		return true, r.store.FailQuote(ctx, claim, invalidQuoteClaimCode)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, err := r.transport.RequestQuote(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		// Shutdown never changes a Broker command's durable state. A later
		// process will reuse its exact envelope after the lease expires.
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferQuote(ctx, claim, "quote_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if quoteCommandExpired(err) {
			return true, r.store.ExpireQuoteCommand(ctx, claim)
		}
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferQuote(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailQuote(ctx, claim, quoteTransportFailedCode)
	}
	if err := ValidateBrokerQuote(claim, signed, result); err != nil {
		return true, r.store.FailQuote(ctx, claim, invalidQuoteResultCode)
	}
	if err := r.store.CommitQuote(ctx, claim, result); err != nil {
		return true, fmt.Errorf("commit cloud quote: %w", err)
	}
	return true, nil
}

func (r *QuoteRunner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func validateQuoteClaim(claim QuoteClaim) error {
	if claim.Kind != QuotePlanRequested || claim.AggregateType != "plan" || claim.OutboxID == "" || claim.AggregateID != claim.PlanID ||
		claim.PlanID == "" || claim.ConnectionID == "" || claim.PlanRevision <= 0 || claim.LeaseToken == "" ||
		claim.BrokerEndpoint == "" || claim.ExpectedGeneration <= 0 || claim.NodeKeyID == "" {
		return errors.New("quote claim envelope is invalid")
	}
	if err := claim.Request.Validate(); err != nil {
		return fmt.Errorf("quote request is invalid: %w", err)
	}
	if claim.Request.PlanID != claim.PlanID || claim.Request.PlanRevision != uint64(claim.PlanRevision) || claim.Request.CloudConnectionID != claim.ConnectionID {
		return errors.New("quote request does not bind the claimed plan")
	}
	if claim.Command.CommandID == "" || claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID ||
		claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.RequestDigest == "" {
		return errors.New("quote command does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil {
		return fmt.Errorf("quote request digest: %w", err)
	}
	if claim.Command.RequestDigest != digest {
		return errors.New("quote command digest does not bind the request")
	}
	return nil
}

func validateSignedQuoteCommand(command QuoteCommand, signed SignedQuoteCommand) error {
	if command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" ||
		!lowerHexSHA256(signed.PayloadSHA256) || !lowerHexSHA256(signed.RequestSHA256) || len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 192*1024 || signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) ||
		signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed quote command is invalid")
	}
	return nil
}

// ValidateBrokerQuote repeats the untrusted Broker receipt binding before it
// reaches PostgreSQL. Store implementations call it again inside their fenced
// transaction so a prior process-local validation cannot be confused with a
// durable authorization decision.
func ValidateBrokerQuote(claim QuoteClaim, signed SignedQuoteCommand, quote BrokerQuote) error {
	if err := validateQuoteClaim(claim); err != nil {
		return err
	}
	if err := validateSignedQuoteCommand(claim.Command, signed); err != nil {
		return err
	}
	if quote.Schema != "dirextalk.aws.quote/v1" || quote.QuoteID != "quote-"+signed.RequestSHA256[:32] || quote.ConnectionID != claim.ConnectionID || quote.CommandID != claim.Command.CommandID ||
		quote.RequestSHA256 != signed.RequestSHA256 || quote.QuoteRequestID != claim.Request.QuoteRequestID || quote.Region != claim.Request.Region ||
		quote.Currency != "USD" || quote.QuotedAt.IsZero() || quote.ValidUntil.IsZero() || quote.ReceiptJSON == "" {
		return errors.New("broker quote does not bind the signed command")
	}
	digest, err := claim.Request.Digest()
	if err != nil {
		return err
	}
	if quote.PlanDigest != digest {
		return errors.New("broker quote does not bind the quote request digest")
	}
	if quote.QuotedAt.Before(signed.IssuedAt) || quote.QuotedAt.After(signed.ExpiresAt) || quote.ValidUntil.Sub(quote.QuotedAt) != 15*time.Minute {
		return errors.New("broker quote was issued after its command expired")
	}
	if len(quote.Candidates) != len(claim.Request.Candidates) {
		return errors.New("broker quote candidates do not match the request")
	}
	for index, candidate := range quote.Candidates {
		expected := claim.Request.Candidates[index]
		if candidate.CandidateID != expected.CandidateID || candidate.Tier != expected.Tier || candidate.InstanceType != expected.InstanceType ||
			candidate.PurchaseOption != expected.PurchaseOption || candidate.EstimatedDiskGiB != expected.EstimatedDiskGiB {
			return errors.New("broker quote candidate does not match the request")
		}
	}
	if !sameStringSlice(quote.IncludedItems, requiredQuoteIncludedItems) || !sameStringSlice(quote.UnincludedItems, requiredQuoteUnincludedItems) {
		return errors.New("broker quote cost coverage is invalid")
	}
	projected := cloudcontracts.QuoteV1{
		SchemaVersion:     cloudcontracts.SchemaVersionV1,
		QuoteID:           quote.QuoteID,
		CloudConnectionID: quote.ConnectionID,
		Region:            quote.Region,
		Currency:          quote.Currency,
		QuotedAt:          quote.QuotedAt,
		ValidUntil:        quote.ValidUntil,
		Candidates:        quote.Candidates,
		IncludedItems:     quote.IncludedItems,
		UnincludedItems:   quote.UnincludedItems,
	}
	if err := projected.Validate(); err != nil {
		return fmt.Errorf("broker quote is invalid: %w", err)
	}
	return nil
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index, value := range left {
		if value != right[index] {
			return false
		}
	}
	return true
}

func lowerHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
