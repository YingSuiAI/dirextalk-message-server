// Package calls owns ProductCore call-session actions and lifecycle projection.
package calls

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionCreate   = "calls.create"
	actionIncoming = "calls.incoming"
	actionGet      = "calls.get"
	actionEvent    = "calls.event"
	actionActive   = "calls.active"
	actionList     = "calls.list"
)

// Store is the sole call-state repository used by Module.
type Store interface {
	UpsertCall(ctx context.Context, call dirextalkdomain.CallRecord) error
	ListCalls(ctx context.Context, roomID string, activeOnly bool) ([]dirextalkdomain.CallRecord, error)
}

// Config supplies owner identity, deterministic test seams, and the event boundary.
type Config struct {
	ServerName   string
	OwnerMXID    string
	Now          func() time.Time
	NewCallID    func() string
	PublishEvent func(context.Context, dirextalkdomain.Event) error
}

// Module implements all call actions over one Store path.
type Module struct {
	store        Store
	serverName   string
	ownerMXID    string
	now          func() time.Time
	newCallID    func() string
	publishEvent func(context.Context, dirextalkdomain.Event) error

	mutationMu sync.Mutex
}

func New(store Store, cfg Config) *Module {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	newCallID := cfg.NewCallID
	if newCallID == nil {
		newCallID = defaultCallID
	}
	return &Module{
		store:        store,
		serverName:   cfg.ServerName,
		ownerMXID:    cfg.OwnerMXID,
		now:          now,
		newCallID:    newCallID,
		publishEvent: cfg.PublishEvent,
	}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionCreate:   m.handleSession,
		actionIncoming: m.handleSession,
		actionGet:      m.handleGet,
		actionEvent:    m.handleEvent,
		actionActive:   m.listHandler(true),
		actionList:     m.listHandler(false),
	}
}

func (m *Module) handleSession(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()

	values := actionbase.Params(params)
	now := m.now().UTC()
	callID := values.String("call_id")
	if callID == "" {
		callID = m.newCallID()
	}
	roomID := values.String("room_id")
	if roomID == "" {
		roomID = "!call:" + m.serverName
	}
	state := fallback(values.String("event"), "ringing")
	createdAt := callTimeParam(values.Raw("created_at"), values.Raw("created_at_ms"))
	if createdAt == "" {
		createdAt = now.Format(time.RFC3339Nano)
	}
	call := dirextalkdomain.CallRecord{
		CallID:        callID,
		RoomID:        roomID,
		RoomType:      "direct",
		MediaType:     fallback(values.String("media_type"), "voice"),
		CreatedByMXID: fallback(values.String("created_by_mxid"), m.ownerMXID),
		State:         state,
		CreatedAt:     createdAt,
	}
	existing, ok, err := m.callByID(ctx, callID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if ok && dirextalkdomain.TerminalCallState(existing.State) {
		return existing, nil
	}
	applyCallLifecycle(&call, state, values, now, m.ownerMXID)
	if err := m.store.UpsertCall(ctx, call); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.publishChanged(ctx, call); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return call, nil
}

func (m *Module) callByID(ctx context.Context, callID string) (dirextalkdomain.CallRecord, bool, error) {
	calls, err := m.store.ListCalls(ctx, "", false)
	if err != nil {
		return dirextalkdomain.CallRecord{}, false, err
	}
	for _, call := range calls {
		if call.CallID == callID {
			return call, true, nil
		}
	}
	return dirextalkdomain.CallRecord{}, false, nil
}

func (m *Module) handleGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	callID := actionbase.Params(params).String("call_id")
	if callID == "" {
		return nil, actionbase.BadRequest("call_id is required")
	}
	call, ok, err := m.callByID(ctx, callID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(404, "call not found")
	}
	return call, nil
}

func (m *Module) handleEvent(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	values := actionbase.Params(params)
	callID := values.String("call_id")
	if callID == "" {
		return nil, actionbase.BadRequest("call_id is required")
	}
	event := values.String("event")
	switch event {
	case "connected", "ended", "rejected", "missed", "failed":
	default:
		return nil, actionbase.BadRequest("event must be connected, ended, rejected, missed, or failed")
	}

	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()
	call, ok, err := m.callByID(ctx, callID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(404, "call not found")
	}
	if dirextalkdomain.TerminalCallState(call.State) && call.State != event {
		return call, nil
	}
	call.State = event
	if mediaType := values.String("media_type"); mediaType != "" {
		call.MediaType = mediaType
	}
	applyCallLifecycle(&call, event, values, m.now().UTC(), m.ownerMXID)
	if err := m.store.UpsertCall(ctx, call); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.publishChanged(ctx, call); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return call, nil
}

func (m *Module) listHandler(activeOnly bool) actionbase.Handler {
	return func(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
		roomID := actionbase.Params(params).String("room_id")
		calls, err := m.store.ListCalls(ctx, roomID, activeOnly)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		return map[string]any{"calls": calls}, nil
	}
}

func (m *Module) publishChanged(ctx context.Context, call dirextalkdomain.CallRecord) error {
	if m.publishEvent == nil {
		return nil
	}
	return m.publishEvent(ctx, dirextalkdomain.Event{
		Type:    "call.changed",
		RoomID:  call.RoomID,
		Payload: map[string]any{"call": call},
	})
}

func fallback(value, fallbackValue string) string {
	if value != "" {
		return value
	}
	return fallbackValue
}

func defaultCallID() string {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Sprintf("call_p2p_%d", time.Now().UnixNano())
	}
	return "call_p2p_" + hex.EncodeToString(random[:])
}
