// Package channels owns ProductCore channel CRUD, listing, lifecycle, and
// member-count persistence orchestration.
package channels

import (
	"context"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionCreate       = "channels.create"
	actionUpdate       = "channels.update"
	actionList         = "channels.list"
	actionDissolve     = "channels.dissolve"
	actionMute         = "channels.mute"
	actionUnmute       = "channels.unmute"
	actionPublicGet    = "channels.public.get"
	actionPublicSearch = "channels.public.search"
	actionUserPublic   = "users.public_channels"
)

// Channel is the durable and public ProductCore channel shape.
type Channel = dirextalkdomain.Channel

// Store is the durable channel repository used by Module.
type Store interface {
	UpsertChannel(ctx context.Context, channel dirextalkdomain.Channel) error
	DeleteChannel(ctx context.Context, channelID string) error
	ListChannels(ctx context.Context) ([]dirextalkdomain.Channel, error)
	GetChannelByIDOrRoom(ctx context.Context, channelID, roomID string) (dirextalkdomain.Channel, bool, error)
	ListJoinedChannelsForUser(ctx context.Context, userID string) ([]dirextalkdomain.Channel, error)
	SearchPublicChannels(ctx context.Context, query string, limit int) ([]dirextalkdomain.Channel, error)
	ListPublicChannelsForOwner(ctx context.Context, userID string) ([]dirextalkdomain.Channel, error)
}

// ConversationPort owns the durable channel conversation projection.
type ConversationPort interface {
	Save(ctx context.Context, record dirextalkdomain.ConversationRecord) error
}

// MemberCounter supplies current joined and pending product-member counts.
type MemberCounter interface {
	CountProductMembers(ctx context.Context, roomID, channelID string) (joined, pending int64, err error)
}

// Config contains the narrow Matrix, membership, and identity boundaries used
// by channel workflows.
type Config struct {
	NewChannelID       func() string
	CreateRoom         func(context.Context, Channel) (string, *actionbase.Error)
	SaveOwnerMember    func(context.Context, string, string) error
	PublishState       func(context.Context, Channel, bool) error
	PublishHistory     func(context.Context, Channel) error
	SetMemberMute      func(context.Context, string, string, bool) *actionbase.Error
	RequireOwner       func(context.Context, string) *actionbase.Error
	OwnerMXID          func() string
	RemotePublicGet    func(context.Context, string, string, map[string]any) (Channel, bool, *actionbase.Error)
	FetchRoomChannel   func(context.Context, string) (Channel, bool, *actionbase.Error)
	RemoteUserChannels func(context.Context, string, map[string]any) (RemoteUserChannelsResult, *actionbase.Error)
	IsMatrixRoomID     func(string) bool
}

type Module struct {
	store        Store
	conversation ConversationPort
	members      MemberCounter
	config       Config
}

func New(store Store, conversation ConversationPort, members MemberCounter, cfg Config) *Module {
	return &Module{store: store, conversation: conversation, members: members, config: cfg}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionCreate:       m.Create,
		actionUpdate:       m.Update,
		actionList:         m.handleList,
		actionDissolve:     m.Dissolve,
		actionMute:         m.policyHandler(true),
		actionUnmute:       m.policyHandler(false),
		actionPublicGet:    m.PublicGet,
		actionPublicSearch: m.PublicSearch,
		actionUserPublic:   m.UserPublicChannels,
	}
}

// Save persists the channel before refreshing its conversation projection.
func (m *Module) Save(ctx context.Context, channel Channel) error {
	return m.saveWithCreator(ctx, channel, "")
}

func (m *Module) saveWithCreator(ctx context.Context, channel Channel, creatorMXID string) error {
	if m.store == nil {
		return errors.New("channel store is not configured")
	}
	if err := m.store.UpsertChannel(ctx, channel); err != nil {
		return err
	}
	if m.conversation == nil {
		return errors.New("channel conversation port is not configured")
	}
	conversation := dirextalkdomain.ConversationFromChannel(channel)
	conversation.CreatedByMXID = creatorMXID
	return m.conversation.Save(ctx, conversation)
}

// Delete removes the durable channel. Channel dissolution historically leaves
// the conversation projection untouched, so this method deliberately does not
// delete a conversation record.
func (m *Module) Delete(ctx context.Context, channelID string) error {
	if m.store == nil {
		return errors.New("channel store is not configured")
	}
	return m.store.DeleteChannel(ctx, strings.TrimSpace(channelID))
}

func (m *Module) List(ctx context.Context) ([]Channel, error) {
	if m.store == nil {
		return nil, errors.New("channel store is not configured")
	}
	return m.store.ListChannels(ctx)
}

func (m *Module) ListJoined(ctx context.Context, userID string) ([]Channel, error) {
	if m.store == nil {
		return nil, errors.New("channel store is not configured")
	}
	return m.store.ListJoinedChannelsForUser(ctx, strings.TrimSpace(userID))
}

func (m *Module) SearchPublic(ctx context.Context, query string, limit int) ([]Channel, error) {
	if m.store == nil {
		return nil, errors.New("channel store is not configured")
	}
	return m.store.SearchPublicChannels(ctx, query, limit)
}

func (m *Module) ListPublic(ctx context.Context, userID string) ([]Channel, error) {
	if m.store == nil {
		return nil, errors.New("channel store is not configured")
	}
	return m.store.ListPublicChannelsForOwner(ctx, strings.TrimSpace(userID))
}

func (m *Module) ByIDOrRoom(ctx context.Context, channelID, roomID string) (Channel, bool, error) {
	if m.store == nil {
		return Channel{}, false, errors.New("channel store is not configured")
	}
	return m.store.GetChannelByIDOrRoom(ctx, strings.TrimSpace(channelID), strings.TrimSpace(roomID))
}

// Snapshot preserves the legacy best-effort channel lookup used in response
// enrichment: repository errors and missing rows both produce the zero value.
func (m *Module) Snapshot(ctx context.Context, channelID string) Channel {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return Channel{}
	}
	channel, ok, err := m.ByIDOrRoom(ctx, channelID, "")
	if err != nil || !ok {
		return Channel{}
	}
	return channel
}

// WithCurrentCounts refreshes stale counts and persists them. The historic
// behavior keeps stored counts when both current counts are zero.
func (m *Module) WithCurrentCounts(ctx context.Context, channel Channel) (Channel, error) {
	if strings.TrimSpace(channel.ChannelID) == "" || m.members == nil {
		return channel, nil
	}
	memberCount, pendingJoinCount, err := m.members.CountProductMembers(ctx, channel.RoomID, channel.ChannelID)
	if err != nil {
		return Channel{}, err
	}
	if memberCount == 0 && pendingJoinCount == 0 {
		return channel, nil
	}
	if channel.MemberCount == memberCount && channel.PendingJoinCount == pendingJoinCount {
		return channel, nil
	}
	channel.MemberCount = memberCount
	channel.PendingJoinCount = pendingJoinCount
	if err := m.Save(ctx, channel); err != nil {
		return Channel{}, err
	}
	return channel, nil
}

// RefreshCounts refreshes a stored channel even when both current counts are
// zero. This is used after authoritative membership mutations.
func (m *Module) RefreshCounts(ctx context.Context, channelID string) error {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" || m.members == nil {
		return nil
	}
	channel, ok, err := m.ByIDOrRoom(ctx, channelID, "")
	if err != nil || !ok {
		return err
	}
	memberCount, pendingJoinCount, err := m.members.CountProductMembers(ctx, channel.RoomID, channelID)
	if err != nil {
		return err
	}
	channel.MemberCount = memberCount
	channel.PendingJoinCount = pendingJoinCount
	return m.Save(ctx, channel)
}
