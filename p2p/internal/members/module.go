// Package members owns shared ProductCore group and channel member reads and
// lifecycle and policy mutations.
package members

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionChannelInvite             = "channels.invite"
	actionChannelInviteGrantCreate  = "channels.invite_grant.create"
	actionChannelJoin               = "channels.join"
	actionChannelPublicJoinRequest  = "channels.public.join_request"
	actionChannelPublicJoinResult   = "channels.public.join_result"
	actionChannelMembers            = "channels.members"
	actionChannelLeave              = "channels.leave"
	actionChannelRemove             = "channels.member.remove"
	actionChannelJoinRequestApprove = "channels.join_request.approve"
	actionChannelJoinRequestReject  = "channels.join_request.reject"
	actionChannelMute               = "channels.member.mute"
	actionChannelUnmute             = "channels.member.unmute"
	actionGroupMembers              = "groups.members"
	actionGroupLeave                = "groups.leave"
	actionGroupRemove               = "groups.member.remove"
	actionGroupInviteReject         = "groups.invite.reject"
	actionGroupInvite               = "groups.invite"
	actionGroupJoin                 = "groups.join"
	actionGroupMute                 = "groups.member.mute"
	actionGroupUnmute               = "groups.member.unmute"
	actionRoomReactivate            = "rooms.reactivate"
)

// Store owns the durable member and channel invite-grant records used by Module.
type Store interface {
	ListMembers(ctx context.Context, roomID, channelID string) ([]dirextalkdomain.MemberRecord, error)
	UpsertChannelInviteGrant(ctx context.Context, grant dirextalkdomain.ChannelInviteGrant) error
	ListChannelInviteGrants(ctx context.Context) ([]dirextalkdomain.ChannelInviteGrant, error)
}

type ConversationPort interface {
	Operation(ctx context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error)
}

// InviteSender performs one prepared Matrix invite. isDirect remains explicit
// because grant-created channel invites always use the non-direct form.
type InviteSender func(context.Context, dirextalkdomain.MemberRecord, map[string]any, bool) *actionbase.Error

type Config struct {
	ResolveTarget            func(context.Context, map[string]any) (roomID, channelID string, err error)
	NewMember                func(roomID, channelID, userID string) dirextalkdomain.MemberRecord
	LookupMember             func(context.Context, string, string) (dirextalkdomain.MemberRecord, bool, error)
	SaveMember               func(context.Context, dirextalkdomain.MemberRecord) error
	PublishPolicy            func(context.Context, dirextalkdomain.MemberRecord) *actionbase.Error
	Conversation             ConversationPort
	OwnerMXID                func() string
	KickMember               func(context.Context, string, string, string, string) *actionbase.Error
	LeaveMember              func(context.Context, string, string) *actionbase.Error
	PublishJoinRequest       func(context.Context, string, string, string, string) *actionbase.Error
	CompleteJoinRequest      func(context.Context, bool, dirextalkdomain.MemberRecord, map[string]any) (map[string]any, *actionbase.Error)
	LookupChannel            func(context.Context, string, string) (dirextalkdomain.Channel, bool, error)
	RequireOwner             func(context.Context, string) *actionbase.Error
	RejectBlocked            func(context.Context, string, ...string) *actionbase.Error
	PrepareInvite            func(context.Context, string, string, string) (InviteSender, *actionbase.Error)
	ShareRoomMembers         func(context.Context, string) ([]dirextalkdomain.MemberRecord, error)
	ChannelSnapshot          func(context.Context, string) dirextalkdomain.Channel
	ApplyLocalProfile        func(*dirextalkdomain.MemberRecord)
	JoinRetained             func(context.Context, string, *dirextalkdomain.MemberRecord, map[string]any) *actionbase.Error
	SaveRetainedMetadata     func(context.Context, string, dirextalkdomain.MemberRecord, map[string]any) *actionbase.Error
	ForwardPublicJoinRequest func(context.Context, map[string]any) (map[string]any, bool, *actionbase.Error)
	EmitJoinRequestChanged   func(context.Context, dirextalkdomain.MemberRecord, string)
	NewGrantID               func() string
	Now                      func() time.Time
}

type Module struct {
	store  Store
	config Config
	now    func() time.Time
}

func New(store Store, cfg Config) *Module {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Module{store: store, config: cfg, now: now}
}

// Handlers returns the group and channel actions sharing the same member view.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionChannelInvite:             m.inviteHandler("channel"),
		actionChannelInviteGrantCreate:  m.ChannelInviteGrantCreate,
		actionChannelJoin:               m.joinHandler("channel"),
		actionChannelPublicJoinRequest:  m.ChannelPublicJoinRequest,
		actionChannelPublicJoinResult:   m.ChannelPublicJoinResult,
		actionChannelMembers:            m.List,
		actionChannelLeave:              m.lifecycleHandler(actionChannelLeave),
		actionChannelRemove:             m.lifecycleHandler(actionChannelRemove),
		actionChannelJoinRequestApprove: m.joinRequestHandler(actionChannelJoinRequestApprove),
		actionChannelJoinRequestReject:  m.joinRequestHandler(actionChannelJoinRequestReject),
		actionChannelMute:               m.mutationHandler("channel", actionChannelMute, true),
		actionChannelUnmute:             m.mutationHandler("channel", actionChannelUnmute, false),
		actionGroupMembers:              m.List,
		actionGroupLeave:                m.lifecycleHandler(actionGroupLeave),
		actionGroupRemove:               m.lifecycleHandler(actionGroupRemove),
		actionGroupInviteReject:         m.lifecycleHandler(actionGroupInviteReject),
		actionGroupInvite:               m.inviteHandler("group"),
		actionGroupJoin:                 m.joinHandler("group"),
		actionGroupMute:                 m.mutationHandler("group", actionGroupMute, true),
		actionGroupUnmute:               m.mutationHandler("group", actionGroupUnmute, false),
		actionRoomReactivate:            m.RoomReactivate,
	}
}

func (m *Module) List(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	if m.store == nil {
		return nil, actionbase.InternalError(errors.New("member store is not configured"))
	}
	members, err := m.store.ListMembers(ctx, params.String("room_id"), params.String("channel_id"))
	if err != nil {
		return nil, actionbase.InternalError(err)
	}

	visible := make([]dirextalkdomain.MemberRecord, 0, len(members))
	for _, member := range members {
		if !dirextalkdomain.MemberHidden(member.Membership) {
			visible = append(visible, member)
		}
	}
	status := params.FirstString("status", "membership")
	return map[string]any{"members": filter(visible, status, params.String("role"))}, nil
}

func filter(members []dirextalkdomain.MemberRecord, status, role string) []dirextalkdomain.MemberRecord {
	normalized := make([]dirextalkdomain.MemberRecord, 0, len(members))
	for _, member := range members {
		member.Role = dirextalkdomain.NormalizeProductMemberRole(member.Role)
		if status != "" && !strings.EqualFold(member.Membership, status) {
			continue
		}
		if role != "" && !strings.EqualFold(member.Role, role) {
			continue
		}
		normalized = append(normalized, member)
	}
	SortByJoinOrder(normalized)
	return normalized
}

// SortByJoinOrder applies the stable public member ordering in place.
func SortByJoinOrder(members []dirextalkdomain.MemberRecord) {
	sort.SliceStable(members, func(i, j int) bool {
		left, right := members[i], members[j]
		leftOwner := strings.EqualFold(left.Role, "owner")
		rightOwner := strings.EqualFold(right.Role, "owner")
		if leftOwner != rightOwner {
			return leftOwner
		}
		if left.JoinedAt != right.JoinedAt {
			if left.JoinedAt == 0 {
				return false
			}
			if right.JoinedAt == 0 {
				return true
			}
			return left.JoinedAt < right.JoinedAt
		}
		return left.UserID < right.UserID
	})
}
