// Package conversation owns ProductCore conversation actions and read-model presentation.
package conversation

import (
	"context"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionList = "conversations.list"
	actionGet  = "conversations.get"
)

// Store is the durable conversation repository used by Module.
type Store interface {
	UpsertConversation(ctx context.Context, record dirextalkdomain.ConversationRecord) error
	SetConversationCreator(ctx context.Context, matrixRoomID, creatorMXID string) error
	GetConversationByID(ctx context.Context, conversationID string) (dirextalkdomain.ConversationRecord, bool, error)
	GetConversationByRoomID(ctx context.Context, matrixRoomID string) (dirextalkdomain.ConversationRecord, bool, error)
	ListConversations(ctx context.Context) ([]dirextalkdomain.ConversationRecord, error)
	DeleteConversationByRoomID(ctx context.Context, matrixRoomID string) error
}

// Hydrator supplies the related product facts needed to present a conversation.
type Hydrator interface {
	ContactByRoom(ctx context.Context, roomID string) (dirextalkdomain.ContactRecord, bool, error)
	GroupByRoom(ctx context.Context, roomID string) (dirextalkdomain.GroupRecord, bool, error)
	ChannelByRoom(ctx context.Context, roomID string) (dirextalkdomain.Channel, bool, error)
	CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error)
	Member(ctx context.Context, roomID, userID string) (dirextalkdomain.MemberRecord, bool, error)
	OwnerMXID() string
}

// Module implements conversation actions over one Store path.
type Module struct {
	store    Store
	hydrator Hydrator
}

func New(store Store, hydrator Hydrator) *Module {
	return &Module{store: store, hydrator: hydrator}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionList: m.handleList,
		actionGet:  m.handleGet,
	}
}

func (m *Module) Save(ctx context.Context, record dirextalkdomain.ConversationRecord) error {
	return m.store.UpsertConversation(ctx, dirextalkdomain.NormalizeConversationRecord(record))
}

// SetCreator reconciles the creator extracted from authoritative
// m.room.create state. An empty creator clears stale legacy projections;
// ordinary conversation projection saves cannot mutate this identity.
func (m *Module) SetCreator(ctx context.Context, roomID, creatorMXID string) error {
	roomID = strings.TrimSpace(roomID)
	creatorMXID = strings.TrimSpace(creatorMXID)
	if roomID == "" {
		return nil
	}
	return m.store.SetConversationCreator(ctx, roomID, creatorMXID)
}

func (m *Module) DeleteKindByRoom(ctx context.Context, roomID string, kind dirextalkdomain.ConversationKind) error {
	if roomID == "" {
		return nil
	}
	record, ok, err := m.store.GetConversationByRoomID(ctx, roomID)
	if err != nil || !ok || record.Kind != kind {
		return err
	}
	return m.store.DeleteConversationByRoomID(ctx, roomID)
}

func (m *Module) ListRecords(ctx context.Context) ([]dirextalkdomain.ConversationRecord, error) {
	return m.store.ListConversations(ctx)
}

func (m *Module) GetRecord(ctx context.Context, conversationID, roomID string) (dirextalkdomain.ConversationRecord, bool, error) {
	if conversationID != "" {
		return m.store.GetConversationByID(ctx, conversationID)
	}
	return m.store.GetConversationByRoomID(ctx, roomID)
}

func (m *Module) handleList(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	records, err := m.ListRecords(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	views := make([]dirextalkdomain.ConversationView, 0, len(records))
	for _, record := range records {
		view, err := m.View(ctx, record)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		views = append(views, view)
	}
	return map[string]any{"conversations": views}, nil
}

func (m *Module) handleGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	values := actionbase.Params(params)
	conversationID := values.String("conversation_id")
	roomID := values.String("room_id")
	if conversationID == "" && roomID == "" {
		return nil, actionbase.BadRequest("conversation_id or room_id is required")
	}
	record, ok, err := m.GetRecord(ctx, conversationID, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "conversation not found")
	}
	view, err := m.View(ctx, record)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return view, nil
}

func (m *Module) Operation(ctx context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error) {
	roomID = strings.TrimSpace(roomID)
	operation := map[string]any{
		"action":  action,
		"status":  status,
		"room_id": roomID,
	}
	if roomID == "" {
		return operation, nil, nil
	}
	record, ok, err := m.GetRecord(ctx, "", roomID)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return operation, nil, nil
	}
	view, err := m.View(ctx, record)
	if err != nil {
		return nil, nil, err
	}
	operation["conversation_id"] = view.ConversationID
	return operation, &view, nil
}

func (m *Module) AttachOperation(ctx context.Context, result map[string]any, action, status, roomID string) error {
	operation, view, err := m.Operation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	result["operation"] = operation
	if view != nil {
		result["conversation"] = *view
	}
	return nil
}
