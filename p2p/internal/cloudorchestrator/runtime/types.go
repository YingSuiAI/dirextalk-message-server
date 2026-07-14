// Package runtime owns the independent Cloud Orchestrator's private
// research-outbox execution loop. It has no AWS SDK or credential API.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const ResearchGoalRequested = "cloud.goal.research.requested"

// Claim is a lease-fenced private outbox record. PayloadJSON remains private
// to the Orchestrator and must never become a ProductCore realtime payload.
type Claim struct {
	OutboxID      string
	Kind          string
	AggregateType string
	AggregateID   string
	GoalID        string
	PlanID        string
	ConnectionID  string
	PlanRevision  int64
	PayloadJSON   string
	LeaseToken    string
	Attempt       int
}

// ResearchInput is the minimum private hand-off to a source researcher. The
// researcher may return an immutable recipe/quote/plan proposal, but cannot
// receive credentials or call a provider control plane through this contract.
type ResearchInput struct {
	GoalID       string
	PlanID       string
	ConnectionID string
	PlanRevision int64
	Prompt       string
}

// ResearchOutput is a validated candidate that can transition a plan from
// researching to ready_for_confirmation. It contains only de-secretsed
// contracts and a short owner-visible summary.
type ResearchOutput struct {
	Plan    cloudcontracts.PlanV1
	Recipe  cloudcontracts.RecipeV1
	Quote   cloudcontracts.QuoteV1
	Title   string
	Summary string
}

// Store isolates the process from PostgreSQL implementation details. Every
// mutation that settles a claim must fence on Claim.LeaseToken.
type Store interface {
	ClaimResearchGoal(context.Context, string, time.Duration) (Claim, bool, error)
	MarkResearchStarted(context.Context, Claim) error
	CommitResearch(context.Context, Claim, ResearchOutput) error
	DeferResearch(context.Context, Claim, string, time.Time) error
	FailResearch(context.Context, Claim, string) error
}

// Planner performs official-source research and produces a candidate. It is
// deliberately not an AWS client and cannot receive a device approval.
type Planner interface {
	Research(context.Context, ResearchInput) (ResearchOutput, error)
}

// Retryable marks a source/planner error that can safely be re-attempted
// without changing the user-approved goal. Error text is intentionally not
// persisted by the runtime; only Code reaches the durable retry record.
func Retryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "research_retryable"), cause: cause}
}

type retryableError struct {
	code  string
	cause error
}

func (e retryableError) Error() string {
	if e.cause == nil {
		return e.code
	}
	return e.code + ": " + e.cause.Error()
}

func (e retryableError) Unwrap() error { return e.cause }

func retryCode(err error) (string, bool) {
	var retry retryableError
	if errors.As(err, &retry) {
		return retry.code, true
	}
	return "", false
}

// ValidateFor confirms that the output advances exactly the leased Plan. It
// is exported so the PostgreSQL store can repeat validation immediately before
// its fencing transaction; callers must never treat prior Agent validation as
// a substitute for this check.
func (o ResearchOutput) ValidateFor(input ResearchInput) error {
	if input.PlanID == "" || input.ConnectionID == "" || input.PlanRevision <= 0 {
		return errors.New("research input is incomplete")
	}
	if o.Plan.PlanID != input.PlanID || o.Plan.CloudConnectionID != input.ConnectionID {
		return errors.New("research output does not bind the claimed plan and connection")
	}
	if o.Plan.Revision != uint64(input.PlanRevision+1) || o.Plan.Status != cloudcontracts.PlanReadyForConfirmation {
		return errors.New("research output must advance the claimed plan one revision to ready_for_confirmation")
	}
	if err := o.Recipe.Validate(); err != nil {
		return fmt.Errorf("invalid recipe: %w", err)
	}
	if err := o.Quote.Validate(); err != nil {
		return fmt.Errorf("invalid quote: %w", err)
	}
	if o.Quote.CloudConnectionID != input.ConnectionID {
		return errors.New("quote does not bind the claimed cloud connection")
	}
	recipeDigest, err := o.Recipe.Digest()
	if err != nil {
		return fmt.Errorf("digest recipe: %w", err)
	}
	quoteDigest, err := o.Quote.Digest()
	if err != nil {
		return fmt.Errorf("digest quote: %w", err)
	}
	if o.Plan.Recipe.RecipeID != o.Recipe.RecipeID || o.Plan.Recipe.Digest != recipeDigest || o.Plan.Recipe.Maturity != o.Recipe.Maturity {
		return errors.New("plan recipe binding does not match the researched recipe")
	}
	if o.Plan.Quote.QuoteID != o.Quote.QuoteID || o.Plan.Quote.Digest != quoteDigest || !o.Plan.Quote.ValidUntil.Equal(o.Quote.ValidUntil) {
		return errors.New("plan quote binding does not match the researched quote")
	}
	if err := o.Plan.Validate(); err != nil {
		return fmt.Errorf("invalid plan: %w", err)
	}
	if _, err := o.Plan.Hash(); err != nil {
		return fmt.Errorf("hash plan: %w", err)
	}
	if err := validateDisplayText("title", o.Title, 160); err != nil {
		return err
	}
	if err := validateDisplayText("summary", o.Summary, 2000); err != nil {
		return err
	}
	return nil
}

func validateDisplayText(label, value string, maximum int) error {
	if value == "" || strings.TrimSpace(value) != value || utf8.RuneCountInString(value) > maximum {
		return fmt.Errorf("%s must contain 1 to %d trimmed characters", label, maximum)
	}
	if cloudmodule.ContainsSensitiveGoalMaterial(value) {
		return fmt.Errorf("%s must not contain secret material", label)
	}
	return nil
}

func normalizedErrorCode(value, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 96 {
		return fallback
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return fallback
		}
	}
	return value
}
