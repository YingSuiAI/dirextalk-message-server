package conversation

import (
	"context"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type moduleStore struct {
	records       []dirextalkdomain.ConversationRecord
	byID          map[string]dirextalkdomain.ConversationRecord
	byRoom        map[string]dirextalkdomain.ConversationRecord
	listErr       error
	getErr        error
	upserted      dirextalkdomain.ConversationRecord
	creatorRoomID string
	creatorMXID   string
	deletedRoomID string
	getIDs        []string
	getRoomIDs    []string
}

func (s *moduleStore) UpsertConversation(_ context.Context, record dirextalkdomain.ConversationRecord) error {
	s.upserted = record
	return nil
}

func (s *moduleStore) SetConversationCreator(_ context.Context, roomID, creatorMXID string) error {
	s.creatorRoomID = roomID
	s.creatorMXID = creatorMXID
	return nil
}

func (s *moduleStore) GetConversationByID(_ context.Context, id string) (dirextalkdomain.ConversationRecord, bool, error) {
	s.getIDs = append(s.getIDs, id)
	if s.getErr != nil {
		return dirextalkdomain.ConversationRecord{}, false, s.getErr
	}
	record, ok := s.byID[id]
	return record, ok, nil
}

func (s *moduleStore) GetConversationByRoomID(_ context.Context, roomID string) (dirextalkdomain.ConversationRecord, bool, error) {
	s.getRoomIDs = append(s.getRoomIDs, roomID)
	if s.getErr != nil {
		return dirextalkdomain.ConversationRecord{}, false, s.getErr
	}
	record, ok := s.byRoom[roomID]
	return record, ok, nil
}

func (s *moduleStore) ListConversations(context.Context) ([]dirextalkdomain.ConversationRecord, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]dirextalkdomain.ConversationRecord(nil), s.records...), nil
}

func (s *moduleStore) DeleteConversationByRoomID(_ context.Context, roomID string) error {
	s.deletedRoomID = roomID
	return nil
}

type moduleHydrator struct {
	ownerMXID string
	viewErr   error
	contacts  map[string]dirextalkdomain.ContactRecord
	groups    map[string]dirextalkdomain.GroupRecord
	channels  map[string]dirextalkdomain.Channel
	joined    map[string]int64
	members   map[string]dirextalkdomain.MemberRecord
}

func (h *moduleHydrator) ContactByRoom(_ context.Context, roomID string) (dirextalkdomain.ContactRecord, bool, error) {
	if h.viewErr != nil {
		return dirextalkdomain.ContactRecord{}, false, h.viewErr
	}
	record, ok := h.contacts[roomID]
	return record, ok, nil
}

func (h *moduleHydrator) GroupByRoom(_ context.Context, roomID string) (dirextalkdomain.GroupRecord, bool, error) {
	if h.viewErr != nil {
		return dirextalkdomain.GroupRecord{}, false, h.viewErr
	}
	record, ok := h.groups[roomID]
	return record, ok, nil
}

func (h *moduleHydrator) ChannelByRoom(_ context.Context, roomID string) (dirextalkdomain.Channel, bool, error) {
	if h.viewErr != nil {
		return dirextalkdomain.Channel{}, false, h.viewErr
	}
	record, ok := h.channels[roomID]
	return record, ok, nil
}

func (h *moduleHydrator) CountJoinedMembers(_ context.Context, roomID, channelID string) (int64, error) {
	if h.viewErr != nil {
		return 0, h.viewErr
	}
	return h.joined[roomID+"|"+channelID], nil
}

func (h *moduleHydrator) Member(_ context.Context, roomID, userID string) (dirextalkdomain.MemberRecord, bool, error) {
	if h.viewErr != nil {
		return dirextalkdomain.MemberRecord{}, false, h.viewErr
	}
	record, ok := h.members[roomID+"|"+userID]
	return record, ok, nil
}

func (h *moduleHydrator) OwnerMXID() string { return h.ownerMXID }

func TestHandlersListAndGetPreserveActionContract(t *testing.T) {
	ctx := context.Background()
	first := dirextalkdomain.ConversationRecord{
		ConversationID:  "conv_first",
		MatrixRoomID:    "!first:example.com",
		Kind:            dirextalkdomain.ConversationKindSystem,
		Lifecycle:       dirextalkdomain.ConversationLifecycleActive,
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
		Title:           "First",
	}
	second := first
	second.ConversationID = "conv_second"
	second.MatrixRoomID = "!second:example.com"
	second.Title = "Second"
	store := &moduleStore{
		records: []dirextalkdomain.ConversationRecord{first, second},
		byID:    map[string]dirextalkdomain.ConversationRecord{first.ConversationID: first},
		byRoom:  map[string]dirextalkdomain.ConversationRecord{second.MatrixRoomID: second},
	}
	module := New(store, &moduleHydrator{})
	handlers := module.Handlers()
	if len(handlers) != 2 || handlers["conversations.list"] == nil || handlers["conversations.get"] == nil {
		t.Fatalf("Handlers() = %#v, want exact list/get coverage", handlers)
	}

	response, apiErr := handlers["conversations.list"](ctx, nil)
	if apiErr != nil {
		t.Fatalf("conversations.list error = %#v", apiErr)
	}
	views, ok := response.(map[string]any)["conversations"].([]dirextalkdomain.ConversationView)
	if !ok || len(views) != 2 || views[0].ConversationID != first.ConversationID || views[1].ConversationID != second.ConversationID {
		t.Fatalf("conversations.list response = %#v", response)
	}

	response, apiErr = handlers["conversations.get"](ctx, map[string]any{
		"conversation_id": "  " + first.ConversationID + "  ",
		"room_id":         second.MatrixRoomID,
	})
	if apiErr != nil {
		t.Fatalf("conversations.get error = %#v", apiErr)
	}
	view, ok := response.(dirextalkdomain.ConversationView)
	if !ok || view.ConversationID != first.ConversationID {
		t.Fatalf("conversations.get response = %#v, want id lookup result", response)
	}
	if !reflect.DeepEqual(store.getIDs, []string{first.ConversationID}) || len(store.getRoomIDs) != 0 {
		t.Fatalf("get calls ids=%v rooms=%v, want conversation_id precedence", store.getIDs, store.getRoomIDs)
	}

	response, apiErr = handlers["conversations.get"](ctx, map[string]any{"room_id": second.MatrixRoomID})
	if apiErr != nil {
		t.Fatalf("conversations.get by room error = %#v", apiErr)
	}
	if view = response.(dirextalkdomain.ConversationView); view.ConversationID != second.ConversationID {
		t.Fatalf("conversations.get by room = %#v", view)
	}
}

func TestSaveDeleteAndOperationUseConversationStore(t *testing.T) {
	ctx := context.Background()
	roomID := "!group:example.com"
	record := dirextalkdomain.ConversationRecord{
		MatrixRoomID: roomID,
		Kind:         dirextalkdomain.ConversationKindGroup,
		Title:        "Group",
	}
	store := &moduleStore{byRoom: map[string]dirextalkdomain.ConversationRecord{}}
	module := New(store, &moduleHydrator{})
	if err := module.Save(ctx, record); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if store.upserted.ConversationID == "" || store.upserted.Lifecycle != dirextalkdomain.ConversationLifecycleActive || store.upserted.ProjectionState != dirextalkdomain.ConversationProjectionReady {
		t.Fatalf("Save() record = %#v, want normalized record", store.upserted)
	}
	if err := module.SetCreator(ctx, "  "+roomID+"  ", "  @creator:example.com  "); err != nil {
		t.Fatalf("SetCreator() error = %v", err)
	}
	if store.creatorRoomID != roomID || store.creatorMXID != "@creator:example.com" {
		t.Fatalf("SetCreator() = (%q, %q), want normalized room and creator", store.creatorRoomID, store.creatorMXID)
	}
	if err := module.SetCreator(ctx, roomID, "  "); err != nil {
		t.Fatalf("SetCreator(clear) error = %v", err)
	}
	if store.creatorRoomID != roomID || store.creatorMXID != "" {
		t.Fatalf("SetCreator(clear) = (%q, %q), want room and empty creator", store.creatorRoomID, store.creatorMXID)
	}

	stored := store.upserted
	store.byRoom[roomID] = stored
	operation, view, err := module.Operation(ctx, "groups.join", "ok", "  "+roomID+"  ")
	if err != nil {
		t.Fatalf("Operation() error = %v", err)
	}
	if operation["action"] != "groups.join" || operation["status"] != "ok" || operation["room_id"] != roomID || operation["conversation_id"] != stored.ConversationID {
		t.Fatalf("Operation() = %#v", operation)
	}
	if view == nil || view.ConversationID != stored.ConversationID {
		t.Fatalf("Operation() view = %#v", view)
	}
	result := map[string]any{"status": "ok"}
	if err := module.AttachOperation(ctx, result, "groups.join", "ok", roomID); err != nil {
		t.Fatalf("AttachOperation() error = %v", err)
	}
	if !reflect.DeepEqual(result["operation"], operation) {
		t.Fatalf("attached operation = %#v, want %#v", result["operation"], operation)
	}
	attached, ok := result["conversation"].(dirextalkdomain.ConversationView)
	if !ok || attached.ConversationID != stored.ConversationID {
		t.Fatalf("attached conversation = %#v", result["conversation"])
	}

	if err := module.DeleteKindByRoom(ctx, roomID, dirextalkdomain.ConversationKindChannel); err != nil {
		t.Fatalf("DeleteKindByRoom(wrong kind) error = %v", err)
	}
	if store.deletedRoomID != "" {
		t.Fatalf("wrong kind deleted room %q", store.deletedRoomID)
	}
	if err := module.DeleteKindByRoom(ctx, roomID, dirextalkdomain.ConversationKindGroup); err != nil {
		t.Fatalf("DeleteKindByRoom() error = %v", err)
	}
	if store.deletedRoomID != roomID {
		t.Fatalf("deleted room = %q, want %q", store.deletedRoomID, roomID)
	}
}
