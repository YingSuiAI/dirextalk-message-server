// Package runtime owns the independent Cloud Orchestrator's private outbox
// execution loops. It has no AWS SDK or credential API.
package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	ResearchGoalRequested = "cloud.goal.research.requested"
	QuotePlanRequested    = "cloud.plan.quote.requested"
)

// Claim is a lease-fenced private outbox record. PayloadJSON remains private
// to the Orchestrator and must never become a ProductCore realtime payload.
type Claim struct {
	OutboxID       string
	Kind           string
	AggregateType  string
	AggregateID    string
	GoalID         string
	PlanID         string
	ConnectionID   string
	PlanRevision   int64
	PayloadJSON    string
	LeaseToken     string
	Attempt        int
	SelectedRecipe *SelectedRecipeInput
}

type SelectedRecipeInput struct {
	RecipeID string                  `json:"recipe_id"`
	Revision int64                   `json:"recipe_revision"`
	Digest   string                  `json:"recipe_digest"`
	Recipe   cloudcontracts.RecipeV1 `json:"recipe"`
}

// ResearchInput is the minimum private hand-off to a source researcher. The
// researcher may return only an experimental recipe draft and candidate
// instance requests. It cannot receive credentials or call a provider control
// plane through this contract.
type ResearchInput struct {
	GoalID         string               `json:"goal_id"`
	PlanID         string               `json:"plan_id"`
	ConnectionID   string               `json:"cloud_connection_id"`
	PlanRevision   int64                `json:"plan_revision"`
	Prompt         string               `json:"goal"`
	SelectedRecipe *SelectedRecipeInput `json:"selected_recipe,omitempty"`
}

// ResearchOutput is a validated, experimental research draft. It deliberately
// has no PlanV1, price, quote identifier, approval binding, or plan hash: the
// Store derives the later quote request and an independent Broker obtains the
// only price that can be displayed to the owner.
type ResearchOutput struct {
	Recipe  cloudcontracts.RecipeV1        `json:"recipe"`
	Draft   cloudcontracts.ResearchDraftV1 `json:"draft"`
	Title   string                         `json:"title"`
	Summary string                         `json:"summary"`
}

// Validate rejects malformed and secret-bearing research input before a
// private researcher can send an owner goal to a model provider.
func (i ResearchInput) Validate() error {
	if !validResearchIdentifier("goal_id", i.GoalID) || !validResearchIdentifier("plan_id", i.PlanID) || !validResearchIdentifier("cloud_connection_id", i.ConnectionID) || i.PlanRevision <= 0 {
		return errors.New("research input is incomplete")
	}
	if strings.TrimSpace(i.Prompt) != i.Prompt || utf8.RuneCountInString(i.Prompt) == 0 || utf8.RuneCountInString(i.Prompt) > 12000 || cloudmodule.ContainsSensitiveGoalMaterial(i.Prompt) {
		return errors.New("research input goal is invalid")
	}
	if i.SelectedRecipe != nil {
		selected := i.SelectedRecipe
		if selected.Revision <= 0 || selected.RecipeID == "" || selected.Digest == "" || selected.Recipe.Validate() != nil || selected.Recipe.RecipeID != selected.RecipeID {
			return errors.New("selected research recipe is invalid")
		}
		digest, digestErr := selected.Recipe.Digest()
		if digestErr != nil || digest != selected.Digest {
			return errors.New("selected research recipe is invalid")
		}
	}
	return nil
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

// QuoteClaim is the independently leased, private quote request. Endpoint
// metadata is deliberately available only to the Orchestrator; ProductCore
// projections never contain it. The Node signing key itself is not present in
// this value and must remain a mounted file owned by the process.
type QuoteClaim struct {
	OutboxID      string
	Kind          string
	AggregateType string
	AggregateID   string
	PlanID        string
	ConnectionID  string
	PlanRevision  int64
	LeaseToken    string
	Attempt       int

	BrokerEndpoint     string
	ExpectedGeneration int64
	NodeKeyID          string
	Request            cloudcontracts.QuoteRequestV1
	Command            QuoteCommand
}

// QuoteCommand is a durable command identity. A retry must replay its exact
// SignedEnvelope instead of signing the same logical request again.
type QuoteCommand struct {
	CommandID          string
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

// SignedQuoteCommand is the byte-for-byte command delivered to a Connection
// Stack. It is persisted before the first network attempt and reused for every
// indeterminate retry.
type SignedQuoteCommand struct {
	EnvelopeJSON  string
	PayloadJSON   string
	PayloadSHA256 string
	RequestSHA256 string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// BrokerQuote is a strictly typed, de-secretsed quote receipt returned by the
// Connection Stack. ReceiptJSON is private Orchestrator audit data and never
// becomes a ProductCore event or public quote projection.
type BrokerQuote struct {
	Schema          string
	QuoteID         string
	ConnectionID    string
	CommandID       string
	RequestSHA256   string
	QuoteRequestID  string
	PlanDigest      string
	Region          string
	Currency        string
	QuotedAt        time.Time
	ValidUntil      time.Time
	Candidates      []cloudcontracts.QuoteCandidateV1
	IncludedItems   []string
	UnincludedItems []string
	ReceiptJSON     string
}

// QuoteStore is the durable quote-execution boundary. It owns counters,
// command receipts, and quote materialization. The Message Server does not
// implement or invoke this interface.
type QuoteStore interface {
	ClaimQuoteRequest(context.Context, string, time.Duration) (QuoteClaim, bool, error)
	PersistQuoteCommand(context.Context, QuoteClaim, SignedQuoteCommand) error
	MarkQuoteStarted(context.Context, QuoteClaim) error
	CommitQuote(context.Context, QuoteClaim, BrokerQuote) error
	DeferQuote(context.Context, QuoteClaim, string, time.Time) error
	ExpireQuoteCommand(context.Context, QuoteClaim) error
	FailQuote(context.Context, QuoteClaim, string) error
}

// QuoteTransport knows how to form and send one signed, typed Broker command.
// It has no provider SDK capability: it can only speak the fixed Connection
// Stack command endpoint.
type QuoteTransport interface {
	BuildQuoteCommand(QuoteCommand, cloudcontracts.QuoteRequestV1) (SignedQuoteCommand, error)
	RequestQuote(context.Context, string, QuoteCommand, SignedQuoteCommand, cloudcontracts.QuoteRequestV1) (BrokerQuote, error)
}

// Planner performs official-source research and produces an experimental
// draft. It is deliberately not an AWS client and cannot receive a device
// approval.
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

// ValidateFor confirms that the output can be bound to the leased Plan. It is
// exported so the PostgreSQL store can repeat validation immediately before
// its fencing transaction; callers must never treat prior Agent validation as
// a substitute for this check.
func (o ResearchOutput) ValidateFor(input ResearchInput) error {
	if input.PlanID == "" || input.ConnectionID == "" || input.PlanRevision <= 0 {
		return errors.New("research input is incomplete")
	}
	if err := o.Recipe.Validate(); err != nil {
		return fmt.Errorf("invalid recipe: %w", err)
	}
	if input.SelectedRecipe != nil {
		expected, actual := input.SelectedRecipe.Recipe, o.Recipe
		expectedCBOR, expectedErr := expected.CanonicalRecipeCBOR()
		actualCBOR, actualErr := actual.CanonicalRecipeCBOR()
		if expectedErr != nil || actualErr != nil || !bytes.Equal(expectedCBOR, actualCBOR) {
			return errors.New("research output replaced the selected recipe")
		}
	}
	if err := o.Draft.Validate(); err != nil {
		return fmt.Errorf("invalid research draft: %w", err)
	}
	if input.SelectedRecipe != nil && len(o.Draft.Candidates) != 3 {
		return errors.New("selected recipe research requires exactly three candidates")
	}
	if err := validateDisplayText("title", o.Title, 160); err != nil {
		return err
	}
	if err := validateDisplayText("summary", o.Summary, 2000); err != nil {
		return err
	}
	return nil
}

// QuoteRetryable marks a Broker outcome that can be safely replayed with the
// exact persisted envelope. The error text itself is never persisted.
func QuoteRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "quote_retryable"), cause: cause}
}

// QuoteCommandExpired marks an exact Broker rejection before a receipt was
// created. The Store retires that envelope and allocates a new counter only on
// the next fenced attempt.
func QuoteCommandExpired(cause error) error {
	return quoteExpiredError{cause: cause}
}

type quoteExpiredError struct{ cause error }

func (e quoteExpiredError) Error() string {
	if e.cause == nil {
		return "quote_command_expired"
	}
	return "quote_command_expired: " + e.cause.Error()
}

func (e quoteExpiredError) Unwrap() error { return e.cause }

func quoteCommandExpired(err error) bool {
	var expired quoteExpiredError
	return errors.As(err, &expired)
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

func validResearchIdentifier(_ string, value string) bool {
	if value == "" || strings.TrimSpace(value) != value || utf8.RuneCountInString(value) > 200 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return !cloudmodule.ContainsSensitiveGoalMaterial(value)
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
