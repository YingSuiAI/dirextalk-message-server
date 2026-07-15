package legacygateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/google/uuid"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
)

type Identity struct {
	AgentRoomID string
	OwnerMXID   string
}

type SenderResolver func(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error)

type Config struct {
	TenantID          string
	ConversationID    string
	Identity          func() Identity
	ResolveSender     SenderResolver
	Now               func() time.Time
	NewRequestEventID func() (string, error)
}

// Module turns one authenticated Matrix invocation into one durable Agent Run
// reservation. It does not read prompt bodies or project fabricated Run
// results; those arrive through the Agent Control result boundary.
type Module struct {
	store             Store
	ingress           Ingress
	tenantID          string
	conversationID    string
	identity          func() Identity
	resolveSender     SenderResolver
	now               func() time.Time
	newRequestEventID func() (string, error)
	processMu         sync.Mutex
}

func New(store Store, ingress Ingress, cfg Config) (*Module, error) {
	if store == nil {
		return nil, errors.New("legacy Agent Gateway store is required")
	}
	if ingress == nil {
		return nil, errors.New("legacy Agent Gateway ingress is required")
	}
	tenantID, err := canonicalUUIDv7(cfg.TenantID, "tenant_id")
	if err != nil {
		return nil, err
	}
	conversationID, err := canonicalUUIDv7(cfg.ConversationID, "conversation_id")
	if err != nil {
		return nil, err
	}
	if cfg.Identity == nil {
		return nil, errors.New("legacy Agent Gateway identity is required")
	}
	if cfg.ResolveSender == nil {
		return nil, errors.New("legacy Agent Gateway sender resolver is required")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	newRequestEventID := cfg.NewRequestEventID
	if newRequestEventID == nil {
		newRequestEventID = func() (string, error) {
			id, err := uuid.NewV7()
			return id.String(), err
		}
	}
	return &Module{
		store:             store,
		ingress:           ingress,
		tenantID:          tenantID,
		conversationID:    conversationID,
		identity:          cfg.Identity,
		resolveSender:     cfg.ResolveSender,
		now:               now,
		newRequestEventID: newRequestEventID,
	}, nil
}

func (m *Module) ProcessOutputEvent(ctx context.Context, output roomserverAPI.OutputEvent) error {
	if output.Type != roomserverAPI.OutputTypeNewRoomEvent || output.NewRoomEvent == nil || output.NewRoomEvent.Event == nil {
		return nil
	}
	event := output.NewRoomEvent.Event
	if event.Type() != InvocationEventType || event.StateKey() != nil {
		return nil
	}
	identity := m.identity()
	roomID := event.RoomID().String()
	if roomID == "" || roomID != strings.TrimSpace(identity.AgentRoomID) {
		return nil
	}
	// The JetStream durable is configured with one worker, but callers and
	// redelivery can still overlap during tests, shutdown, or future consumer
	// changes. Serialize the reserve -> ingress -> commit window so one source
	// invocation cannot make two concurrent ingress calls before acceptance is
	// durable.
	m.processMu.Lock()
	defer m.processMu.Unlock()
	senderUserID, err := m.resolveSender(ctx, event.RoomID(), event.SenderID())
	if err != nil {
		return fmt.Errorf("resolve Matrix invocation sender: %w", err)
	}
	if senderUserID == nil {
		return errors.New("resolve Matrix invocation sender: roomserver returned no user ID")
	}
	if senderUserID.String() != strings.TrimSpace(identity.OwnerMXID) {
		logrus.Warn("Legacy Agent Gateway ignored an invocation not sent by the local owner")
		return nil
	}

	invocation, err := ParseInvocationContent(m.tenantID, roomID, event.Content())
	if err != nil {
		logrus.WithError(err).Warn("Legacy Agent Gateway ignored invalid invocation content")
		return nil
	}
	requestEventID, err := m.newRequestEventID()
	if err != nil {
		return fmt.Errorf("create opaque Agent request event id: %w", err)
	}
	candidate, err := BuildCandidate(
		m.tenantID,
		roomID,
		event.EventID(),
		m.conversationID,
		requestEventID,
		invocation,
		m.now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("build Agent invocation reservation: %w", err)
	}
	reservation, err := m.store.ReserveInvocation(ctx, candidate)
	if err != nil {
		return fmt.Errorf("reserve Agent invocation: %w", err)
	}
	if reservation.Status == ReservationConflict {
		logrus.Warn("Legacy Agent Gateway rejected a conflicting invocation replay")
		return nil
	}
	if reservation.Status != ReservationInserted && reservation.Status != ReservationReplay {
		return fmt.Errorf("reserve Agent invocation: unknown reservation status %q", reservation.Status)
	}
	record := reservation.Record
	switch record.State {
	case InvocationAccepted, InvocationRejected:
		return nil
	case InvocationPending:
	default:
		return fmt.Errorf("reserve Agent invocation: unknown invocation state %q", record.State)
	}

	receipt, err := m.ingress.CreateRun(ctx, createRunRequest(record))
	if err != nil {
		if !IsPermanentError(err) {
			return fmt.Errorf("create Agent Run: %w", err)
		}
		code := string(IngressErrorCodeOf(err))
		if code == "" {
			code = "rejected"
		}
		if _, markErr := m.store.MarkRejected(
			ctx,
			record.MatrixRoomID,
			record.RequestID,
			record.SourceDigest,
			code,
			m.now().UTC(),
		); markErr != nil {
			return fmt.Errorf("record Agent Run rejection: %w", markErr)
		}
		return nil
	}
	if receipt.RequestID != record.RequestID {
		return errors.New("create Agent Run returned a mismatched request_id")
	}
	if _, err := m.store.MarkAccepted(
		ctx,
		record.MatrixRoomID,
		record.RequestID,
		record.SourceDigest,
		receipt,
		m.now().UTC(),
	); err != nil {
		return fmt.Errorf("record accepted Agent Run: %w", err)
	}
	return nil
}

func createRunRequest(record InvocationRecord) CreateRunRequest {
	return CreateRunRequest{
		RequestID:            record.RequestID,
		IdempotencyDigest:    record.IdempotencyDigest,
		InstallationID:       record.InstallationID,
		ConversationID:       record.ConversationID,
		RequestEventID:       record.RequestEventID,
		PreferredConnectorID: record.PreferredConnectorID,
		RequiredCapabilities: append([]string(nil), record.RequiredCapabilities...),
		DispatchMode:         record.DispatchMode,
		GrantVersion:         record.GrantVersion,
	}
}

func canonicalUUIDv7(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	parsed, err := uuid.Parse(value)
	if err != nil || parsed.Version() != 7 || parsed.String() != value {
		return "", fmt.Errorf("legacy Agent Gateway %s must be a canonical UUIDv7", field)
	}
	return value, nil
}
