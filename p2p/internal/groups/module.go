// Package groups owns ProductCore group CRUD, listing, lifecycle, and policy actions.
package groups

import (
	"context"
	"errors"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionCreate             = "groups.create"
	actionUpdate             = "groups.update"
	actionList               = "groups.list"
	actionDissolve           = "groups.dissolve"
	actionMute               = "groups.mute"
	actionUnmute             = "groups.unmute"
	actionInvitePolicyUpdate = "groups.invite_policy.update"
)

// View is the public ProductCore group response. Operation and Conversation
// are response-only presentation fields and are never written to Store.
type View struct {
	RoomID       string                            `json:"room_id"`
	Name         string                            `json:"name"`
	Topic        string                            `json:"topic"`
	AvatarURL    string                            `json:"avatar_url"`
	MemberCount  int64                             `json:"member_count"`
	InvitePolicy string                            `json:"invite_policy"`
	Muted        bool                              `json:"muted"`
	Operation    map[string]any                    `json:"operation,omitempty"`
	Conversation *dirextalkdomain.ConversationView `json:"conversation,omitempty"`
}

// Store is the durable group repository used by Module.
type Store interface {
	UpsertGroup(ctx context.Context, group dirextalkdomain.GroupRecord) error
	DeleteGroup(ctx context.Context, roomID string) error
	ListGroups(ctx context.Context) ([]dirextalkdomain.GroupRecord, error)
	GetGroupByRoom(ctx context.Context, roomID string) (dirextalkdomain.GroupRecord, bool, error)
	ListJoinedGroupsForUser(ctx context.Context, userID string) ([]dirextalkdomain.GroupRecord, error)
}

// ConversationPort owns the related durable conversation projection.
type ConversationPort interface {
	Save(ctx context.Context, record dirextalkdomain.ConversationRecord) error
	DeleteKindByRoom(ctx context.Context, roomID string, kind dirextalkdomain.ConversationKind) error
	Operation(ctx context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error)
}

// Config contains the narrow Matrix, membership, and identity boundaries used
// by group workflows. Durable group and conversation state remain module-owned.
type Config struct {
	CreateRoom      func(context.Context, View) (string, *actionbase.Error)
	SaveOwnerMember func(context.Context, string) error
	PublishState    func(context.Context, View, bool) error
	SetMemberMute   func(context.Context, string, bool) *actionbase.Error
	RequireOwner    func(context.Context, string) *actionbase.Error
	OwnerMXID       func() string
}

type Module struct {
	store        Store
	conversation ConversationPort
	config       Config
}

func New(store Store, conversation ConversationPort, cfg Config) *Module {
	return &Module{store: store, conversation: conversation, config: cfg}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionCreate:             m.Create,
		actionUpdate:             m.Update,
		actionList:               m.handleList,
		actionDissolve:           m.Dissolve,
		actionMute:               m.policyHandler(actionMute),
		actionUnmute:             m.policyHandler(actionUnmute),
		actionInvitePolicyUpdate: m.policyHandler(actionInvitePolicyUpdate),
	}
}

func ViewFromRecord(record dirextalkdomain.GroupRecord) View {
	return View{
		RoomID:       record.RoomID,
		Name:         record.Name,
		Topic:        record.Topic,
		AvatarURL:    record.AvatarURL,
		MemberCount:  record.MemberCount,
		InvitePolicy: record.InvitePolicy,
		Muted:        record.Muted,
	}
}

func RecordFromView(view View) dirextalkdomain.GroupRecord {
	return dirextalkdomain.GroupRecord{
		RoomID:       view.RoomID,
		Name:         view.Name,
		Topic:        view.Topic,
		AvatarURL:    view.AvatarURL,
		MemberCount:  view.MemberCount,
		InvitePolicy: view.InvitePolicy,
		Muted:        view.Muted,
	}
}

func ViewsFromRecords(records []dirextalkdomain.GroupRecord) []View {
	views := make([]View, 0, len(records))
	for _, record := range records {
		views = append(views, ViewFromRecord(record))
	}
	return views
}

func RecordsFromViews(views []View) []dirextalkdomain.GroupRecord {
	records := make([]dirextalkdomain.GroupRecord, 0, len(views))
	for _, view := range views {
		records = append(records, RecordFromView(view))
	}
	return records
}

// Save persists the durable group before refreshing its conversation record.
func (m *Module) Save(ctx context.Context, group View) error {
	return m.saveWithCreator(ctx, group, "")
}

func (m *Module) saveWithCreator(ctx context.Context, group View, creatorMXID string) error {
	if m.store == nil {
		return errors.New("group store is not configured")
	}
	record := RecordFromView(group)
	if err := m.store.UpsertGroup(ctx, record); err != nil {
		return err
	}
	if m.conversation == nil {
		return errors.New("group conversation port is not configured")
	}
	conversation := dirextalkdomain.ConversationFromGroup(record)
	conversation.CreatedByMXID = creatorMXID
	return m.conversation.Save(ctx, conversation)
}

// Delete removes the durable group before removing its group conversation.
func (m *Module) Delete(ctx context.Context, roomID string) error {
	if m.store == nil {
		return errors.New("group store is not configured")
	}
	if err := m.store.DeleteGroup(ctx, roomID); err != nil {
		return err
	}
	if m.conversation == nil {
		return errors.New("group conversation port is not configured")
	}
	return m.conversation.DeleteKindByRoom(ctx, roomID, dirextalkdomain.ConversationKindGroup)
}

func (m *Module) List(ctx context.Context) ([]View, error) {
	if m.store == nil {
		return nil, errors.New("group store is not configured")
	}
	records, err := m.store.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	return ViewsFromRecords(records), nil
}

func (m *Module) ListJoined(ctx context.Context, userID string) ([]View, error) {
	if m.store == nil {
		return nil, errors.New("group store is not configured")
	}
	records, err := m.store.ListJoinedGroupsForUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	return ViewsFromRecords(records), nil
}

func (m *Module) ByRoom(ctx context.Context, roomID string) (View, bool, error) {
	if m.store == nil {
		return View{}, false, errors.New("group store is not configured")
	}
	record, ok, err := m.store.GetGroupByRoom(ctx, roomID)
	if err != nil || !ok {
		return View{}, ok, err
	}
	return ViewFromRecord(record), true, nil
}

// WithOperation attaches the response-only conversation operation fields.
func (m *Module) WithOperation(ctx context.Context, group View, action, status string) (View, error) {
	if m.conversation == nil {
		return View{}, errors.New("group conversation port is not configured")
	}
	operation, conversation, err := m.conversation.Operation(ctx, action, status, group.RoomID)
	if err != nil {
		return View{}, err
	}
	group.Operation = operation
	group.Conversation = conversation
	return group, nil
}
