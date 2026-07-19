// Package agentevents consumes the independent Agent's durable EventV1 stream
// and projects only owner-bound, de-secreted cloud summaries into ProductCore.
package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

const maximumSummaryBytes = 1 << 20

var (
	ErrInvalidEvent      = errors.New("Agent event is invalid")
	ErrUnsafeEvent       = errors.New("Agent event summary is unsafe")
	ErrProjectionStopped = errors.New("Agent event projection is stopped")
	eventName            = regexp.MustCompile(`^[a-z][a-z0-9_.]{2,95}$`)
	planHash             = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	awsRegion            = regexp.MustCompile(`^[a-z]{2}(?:-[a-z0-9]+){1,2}-[0-9]$`)
	ec2InstanceType      = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}\.[a-z0-9][a-z0-9-]{0,31}$`)

	projectableEvents = map[string]projectableEvent{
		// Add another event only together with its exact de-secreted schema.
		"cloud.plan.changed": {aggregateType: "cloud_plan", decode: decodeSafePlanSummary},
		"cloud.task.changed": {aggregateType: "cloud_task", decode: decodeSafeCloudTaskSummary},
		"cloud.step.changed": {aggregateType: "cloud_step", decode: decodeSafeCloudStepSummary},
	}
)

type Source struct {
	AgentInstanceID string
	CallerID        string
}

type Event struct {
	Seq           int64
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Revision      int64
	SummaryJSON   []byte
	OccurredAt    time.Time
}

type Projection struct {
	SourceEventSeq int64
	Type           string
	EventID        string
	DedupeKey      string
	Payload        map[string]any
	CreatedAt      time.Time
}

type CommitRequest struct {
	Source     Source
	Event      Event
	Projection *Projection
}

type CommitResult struct {
	Cursor   int64
	Inserted bool
}

type Store interface {
	Cursor(context.Context, Source) (int64, error)
	Commit(context.Context, CommitRequest) (CommitResult, error)
}

type EventStream interface {
	Recv() (Event, error)
}

type Client interface {
	WatchEvents(context.Context, int64) (EventStream, error)
}

type Config struct {
	RetryDelay time.Duration
	Notify     func()
}

type Relay struct {
	client Client
	store  Store
	source Source
	cfg    Config
}

func New(client Client, store Store, source Source, cfg Config) *Relay {
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Second
	}
	return &Relay{client: client, store: store, source: source, cfg: cfg}
}

// Run reconnects from the last committed Agent cursor. Validation failures are
// terminal and deliberately leave the cursor before the unsafe source event.
func (relay *Relay) Run(ctx context.Context) error {
	if err := relay.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		err := relay.consumeConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrProjectionStopped) {
			return nil
		}
		if errors.Is(err, ErrInvalidEvent) || errors.Is(err, ErrUnsafeEvent) {
			return err
		}
		timer := time.NewTimer(relay.cfg.RetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (relay *Relay) consumeConnection(ctx context.Context) error {
	if err := relay.validate(); err != nil {
		return err
	}
	cursor, err := relay.store.Cursor(ctx, relay.source)
	if errors.Is(err, ErrProjectionStopped) {
		return err
	}
	if err != nil || cursor < 0 {
		return errors.New("load Agent event cursor failed")
	}
	streamContext, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := relay.client.WatchEvents(streamContext, cursor)
	if err != nil {
		return errors.New("open Agent event stream failed")
	}
	for {
		event, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		projection, prepareErr := prepareProjection(relay.source, event)
		if prepareErr != nil {
			return prepareErr
		}
		result, commitErr := relay.store.Commit(ctx, CommitRequest{Source: relay.source, Event: event, Projection: projection})
		if errors.Is(commitErr, ErrProjectionStopped) {
			return commitErr
		}
		if commitErr != nil {
			return errors.New("commit Agent event projection failed")
		}
		if result.Cursor < cursor || result.Cursor < event.Seq {
			return errors.New("Agent event cursor did not advance")
		}
		cursor = result.Cursor
		if result.Inserted && relay.cfg.Notify != nil {
			relay.cfg.Notify()
		}
	}
}

func (relay *Relay) validate() error {
	if relay == nil || relay.client == nil || relay.store == nil || !validSource(relay.source) {
		return errors.New("Agent event relay is unavailable")
	}
	return nil
}

type projectableEvent struct {
	aggregateType string
	decode        summaryDecoder
}

type summaryDecoder func(Source, Event) (map[string]any, string, error)

func prepareProjection(source Source, event Event) (*Projection, error) {
	if !validEventMetadata(event) {
		return nil, ErrInvalidEvent
	}
	spec, projectable := projectableEvents[event.EventType]
	if !projectable {
		return nil, nil
	}
	if event.AggregateType != spec.aggregateType {
		return nil, ErrInvalidEvent
	}
	payload, owner, err := spec.decode(source, event)
	if err != nil {
		return nil, err
	}
	if owner != source.CallerID {
		return nil, nil
	}
	return &Projection{
		SourceEventSeq: event.Seq,
		Type:           event.EventType,
		EventID:        event.EventID,
		DedupeKey:      "agent-event:" + source.AgentInstanceID + ":" + event.EventID,
		Payload:        payload,
		CreatedAt:      event.OccurredAt.UTC(),
	}, nil
}

func validSource(source Source) bool {
	parsed, err := uuid.Parse(strings.TrimSpace(source.AgentInstanceID))
	caller := strings.TrimSpace(source.CallerID)
	return err == nil && parsed != uuid.Nil && parsed.String() == source.AgentInstanceID && caller == source.CallerID &&
		len(caller) >= 3 && len(caller) <= 256 && !strings.ContainsAny(caller, "\x00\r\n\t") &&
		!cloudmodule.ContainsSensitiveGoalMaterial(caller)
}

func validEventMetadata(event Event) bool {
	if event.Seq <= 0 || event.Revision <= 0 || !eventName.MatchString(event.EventType) || !eventName.MatchString(event.AggregateType) ||
		!canonicalUUID(event.EventID) || !canonicalUUID(event.AggregateID) || event.OccurredAt.IsZero() || event.OccurredAt.Location() != time.UTC {
		return false
	}
	return event.OccurredAt.Unix() > 0 && !event.OccurredAt.After(time.Now().UTC().Add(10*time.Minute))
}

type planEventSummary struct {
	PlanID               string         `json:"plan_id"`
	OwnerID              string         `json:"owner_id"`
	Status               string         `json:"status"`
	Revision             int64          `json:"revision"`
	PlanHash             string         `json:"plan_hash"`
	QuoteID              string         `json:"quote_id"`
	QuoteValidUntil      string         `json:"quote_valid_until"`
	Region               string         `json:"region"`
	InstanceType         string         `json:"instance_type"`
	PublicExposure       bool           `json:"public_exposure"`
	SecretReferenceCount int            `json:"secret_reference_count"`
	Actor                planEventActor `json:"actor"`
}

type planEventActor struct {
	ClientID     string `json:"client_id"`
	CredentialID string `json:"credential_id"`
}

var validPlanStatuses = map[string]struct{}{
	"researching": {}, "quoting": {}, "ready_for_confirmation": {},
	"approved": {}, "expired": {}, "superseded": {},
}

func decodeSafePlanSummary(source Source, event Event) (map[string]any, string, error) {
	if len(event.SummaryJSON) == 0 || len(event.SummaryJSON) > maximumSummaryBytes {
		return nil, "", ErrInvalidEvent
	}
	decoder := json.NewDecoder(bytes.NewReader(event.SummaryJSON))
	decoder.DisallowUnknownFields()
	var summary planEventSummary
	if err := decoder.Decode(&summary); err != nil {
		// Unknown fields are unsafe because they could become an unreviewed secret
		// channel. Malformed JSON is invalid; neither reaches persistence.
		if strings.Contains(err.Error(), "unknown field") {
			return nil, "", ErrUnsafeEvent
		}
		return nil, "", ErrInvalidEvent
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, "", ErrInvalidEvent
	}
	quoteValidUntil, quoteErr := time.Parse(time.RFC3339Nano, summary.QuoteValidUntil)
	_, statusValid := validPlanStatuses[summary.Status]
	if summary.PlanID != event.AggregateID || !canonicalUUID(summary.PlanID) ||
		summary.Revision != event.Revision || !statusValid || !planHash.MatchString(summary.PlanHash) ||
		!canonicalUUID(summary.QuoteID) || quoteErr != nil || quoteValidUntil.Unix() <= 0 ||
		!strings.HasSuffix(summary.QuoteValidUntil, "Z") || !awsRegion.MatchString(summary.Region) ||
		!ec2InstanceType.MatchString(summary.InstanceType) || summary.SecretReferenceCount < 0 ||
		summary.SecretReferenceCount > 1024 {
		return nil, "", ErrInvalidEvent
	}
	if !validSummaryIdentifier(summary.OwnerID, 3, 256) ||
		!validSummaryIdentifier(summary.Actor.ClientID, 1, 256) || !canonicalUUID(summary.Actor.CredentialID) {
		return nil, "", ErrInvalidEvent
	}
	if cloudmodule.ContainsSensitiveGoalMaterial(summary.Actor.ClientID) {
		return nil, "", ErrUnsafeEvent
	}
	return map[string]any{
		"plan_id":                  summary.PlanID,
		"owner_id":                 summary.OwnerID,
		"status":                   summary.Status,
		"revision":                 summary.Revision,
		"plan_hash":                summary.PlanHash,
		"quote_id":                 summary.QuoteID,
		"quote_valid_until":        quoteValidUntil.UTC().Format(time.RFC3339Nano),
		"region":                   summary.Region,
		"instance_type":            summary.InstanceType,
		"public_exposure":          summary.PublicExposure,
		"secret_reference_count":   summary.SecretReferenceCount,
		"source_agent_instance_id": source.AgentInstanceID,
	}, summary.OwnerID, nil
}

const cloudTaskEventSummarySchemaV1 = 1

type cloudTaskEventSummary struct {
	SchemaVersion   int    `json:"schema_version"`
	TaskID          string `json:"task_id"`
	OwnerID         string `json:"owner_id"`
	ExecutionStatus string `json:"execution_status"`
	OutcomeStatus   string `json:"outcome_status"`
	CurrentStage    string `json:"current_stage"`
	RelatedPlanID   string `json:"related_plan_id,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	Revision        int64  `json:"revision"`
	UpdatedAt       string `json:"updated_at"`
}

type cloudStepEventSummary struct {
	SchemaVersion   int    `json:"schema_version"`
	TaskID          string `json:"task_id"`
	StepID          string `json:"step_id"`
	OwnerID         string `json:"owner_id"`
	ExecutionStatus string `json:"execution_status"`
	OutcomeStatus   string `json:"outcome_status"`
	CurrentStage    string `json:"current_stage"`
	RelatedPlanID   string `json:"related_plan_id,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	Revision        int64  `json:"revision"`
	UpdatedAt       string `json:"updated_at"`
}

var (
	validCloudTaskExecutionStatuses = map[string]struct{}{
		"draft": {}, "planning": {}, "awaiting_approval": {}, "queued": {},
		"running": {}, "waiting_user": {}, "verifying": {}, "finished": {},
	}
	validCloudTaskOutcomeStatuses = map[string]struct{}{
		"pending": {}, "succeeded": {}, "failed": {}, "canceled": {},
		"timed_out": {}, "interrupted": {},
	}
	validCloudTaskStages = map[string]struct{}{
		"research": {}, "recipe": {}, "quote": {}, "waiting_user": {},
		"ready_for_confirmation": {}, "finished": {},
	}
	validCloudTaskErrorCodes = map[string]struct{}{
		"": {}, "task_failed": {}, "task_canceled": {}, "task_timed_out": {}, "task_interrupted": {},
	}
)

func decodeSafeCloudTaskSummary(source Source, event Event) (map[string]any, string, error) {
	var summary cloudTaskEventSummary
	if err := decodeExactSummaryJSON(event.SummaryJSON, &summary); err != nil {
		return nil, "", err
	}
	updatedAt, err := validateCloudTaskSummary(
		summary.SchemaVersion, summary.TaskID, summary.OwnerID, summary.ExecutionStatus, summary.OutcomeStatus,
		summary.CurrentStage, summary.RelatedPlanID, summary.ErrorCode, summary.Revision, summary.UpdatedAt, event,
	)
	if err != nil {
		return nil, "", err
	}
	if summary.TaskID != event.AggregateID {
		return nil, "", ErrInvalidEvent
	}
	payload := map[string]any{
		"schema_version":           cloudTaskEventSummarySchemaV1,
		"task_id":                  summary.TaskID,
		"owner_id":                 summary.OwnerID,
		"execution_status":         summary.ExecutionStatus,
		"outcome_status":           summary.OutcomeStatus,
		"current_stage":            summary.CurrentStage,
		"revision":                 summary.Revision,
		"updated_at":               updatedAt.UTC().Format(time.RFC3339Nano),
		"source_agent_instance_id": source.AgentInstanceID,
	}
	if summary.RelatedPlanID != "" {
		payload["related_plan_id"] = summary.RelatedPlanID
	}
	if summary.ErrorCode != "" {
		payload["error_code"] = summary.ErrorCode
	}
	return payload, summary.OwnerID, nil
}

func decodeSafeCloudStepSummary(source Source, event Event) (map[string]any, string, error) {
	var summary cloudStepEventSummary
	if err := decodeExactSummaryJSON(event.SummaryJSON, &summary); err != nil {
		return nil, "", err
	}
	updatedAt, err := validateCloudTaskSummary(
		summary.SchemaVersion, summary.TaskID, summary.OwnerID, summary.ExecutionStatus, summary.OutcomeStatus,
		summary.CurrentStage, summary.RelatedPlanID, summary.ErrorCode, summary.Revision, summary.UpdatedAt, event,
	)
	if err != nil {
		return nil, "", err
	}
	if !canonicalUUID(summary.StepID) || summary.StepID != event.AggregateID {
		return nil, "", ErrInvalidEvent
	}
	payload := map[string]any{
		"schema_version":           cloudTaskEventSummarySchemaV1,
		"task_id":                  summary.TaskID,
		"step_id":                  summary.StepID,
		"owner_id":                 summary.OwnerID,
		"execution_status":         summary.ExecutionStatus,
		"outcome_status":           summary.OutcomeStatus,
		"current_stage":            summary.CurrentStage,
		"revision":                 summary.Revision,
		"updated_at":               updatedAt.UTC().Format(time.RFC3339Nano),
		"source_agent_instance_id": source.AgentInstanceID,
	}
	if summary.RelatedPlanID != "" {
		payload["related_plan_id"] = summary.RelatedPlanID
	}
	if summary.ErrorCode != "" {
		payload["error_code"] = summary.ErrorCode
	}
	return payload, summary.OwnerID, nil
}

func decodeExactSummaryJSON(encoded []byte, destination any) error {
	if len(encoded) == 0 || len(encoded) > maximumSummaryBytes {
		return ErrInvalidEvent
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return ErrUnsafeEvent
		}
		return ErrInvalidEvent
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidEvent
	}
	return nil
}

func validateCloudTaskSummary(schemaVersion int, taskID, ownerID, executionStatus, outcomeStatus, currentStage, relatedPlanID, errorCode string, revision int64, updatedAtRaw string, event Event) (time.Time, error) {
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	_, executionValid := validCloudTaskExecutionStatuses[executionStatus]
	_, outcomeValid := validCloudTaskOutcomeStatuses[outcomeStatus]
	_, stageValid := validCloudTaskStages[currentStage]
	_, errorCodeValid := validCloudTaskErrorCodes[errorCode]
	if schemaVersion != cloudTaskEventSummarySchemaV1 || !canonicalUUID(taskID) ||
		!validSummaryIdentifier(ownerID, 3, 256) || !executionValid || !outcomeValid || !stageValid || !errorCodeValid ||
		revision != event.Revision || revision < 1 || err != nil || updatedAt.Unix() <= 0 ||
		!strings.HasSuffix(updatedAtRaw, "Z") || updatedAt.Location() != time.UTC || updatedAt.After(time.Now().UTC().Add(10*time.Minute)) {
		return time.Time{}, ErrInvalidEvent
	}
	if relatedPlanID != "" && !canonicalUUID(relatedPlanID) {
		return time.Time{}, ErrInvalidEvent
	}
	return updatedAt.UTC(), nil
}

func validSummaryIdentifier(value string, minimum, maximum int) bool {
	return strings.TrimSpace(value) == value && len(value) >= minimum && len(value) <= maximum &&
		!strings.ContainsAny(value, "\x00\r\n\t")
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}
