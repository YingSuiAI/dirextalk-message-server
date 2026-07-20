package conversation

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

// View builds the ProductCore response record from the durable conversation.
func (m *Module) View(ctx context.Context, record dirextalkdomain.ConversationRecord) (dirextalkdomain.ConversationView, error) {
	view := conversationViewFromRecord(record)
	var err error
	switch record.Kind {
	case dirextalkdomain.ConversationKindDirect:
		view, err = m.directView(ctx, view)
	case dirextalkdomain.ConversationKindGroup:
		view, err = m.groupView(ctx, view)
	case dirextalkdomain.ConversationKindChannel:
		view, err = m.channelView(ctx, view)
	}
	if err != nil {
		return view, err
	}
	return finalizeView(view), nil
}

func conversationViewFromRecord(record dirextalkdomain.ConversationRecord) dirextalkdomain.ConversationView {
	return dirextalkdomain.ConversationView{
		ConversationID:   record.ConversationID,
		MatrixRoomID:     record.MatrixRoomID,
		Kind:             record.Kind,
		Lifecycle:        record.Lifecycle,
		PeerMXID:         record.PeerMXID,
		Title:            record.Title,
		AvatarURL:        record.AvatarURL,
		LastEventID:      record.LastEventID,
		LastMessage:      record.LastMessage,
		LastActivityAt:   record.LastActivityAt,
		ProjectionState:  record.ProjectionState,
		ProjectionReason: record.ProjectionReason,
	}
}

func (m *Module) directView(ctx context.Context, view dirextalkdomain.ConversationView) (dirextalkdomain.ConversationView, error) {
	var (
		contact dirextalkdomain.ContactRecord
		ok      bool
		err     error
	)
	if strings.TrimSpace(view.MatrixRoomID) != "" {
		contact, ok, err = m.hydrator.ContactByRoom(ctx, view.MatrixRoomID)
	}
	if err != nil {
		return view, err
	}
	if ok {
		view.PeerMXID = trimmedFallback(contact.PeerMXID, view.PeerMXID)
		view.Title = trimmedFallback(contact.DisplayName, view.Title)
		view.AvatarURL = trimmedFallback(contact.AvatarURL, view.AvatarURL)
		view.RelationshipStatus = contact.Status
		view.Membership = directMembership(contact.Status)
	} else {
		view.Membership = "join"
	}
	view.MemberCount = 2
	view.Role = "member"
	return view, nil
}

func (m *Module) groupView(ctx context.Context, view dirextalkdomain.ConversationView) (dirextalkdomain.ConversationView, error) {
	group, ok, err := m.hydrator.GroupByRoom(ctx, view.MatrixRoomID)
	if err != nil {
		return view, err
	}
	if ok && group.MemberCount > 0 {
		view.MemberCount = group.MemberCount
	}
	return m.hydrateMembership(ctx, view, "")
}

func (m *Module) channelView(ctx context.Context, view dirextalkdomain.ConversationView) (dirextalkdomain.ConversationView, error) {
	channel, ok, err := m.hydrator.ChannelByRoom(ctx, view.MatrixRoomID)
	if err != nil {
		return view, err
	}
	channelID := ""
	if ok {
		channelID = channel.ChannelID
		view.Title = trimmedFallback(channel.Name, view.Title)
		view.AvatarURL = trimmedFallback(channel.AvatarURL, view.AvatarURL)
		view.ChannelType = trimmedFallback(channel.ChannelType, "chat")
		view.CommentsEnabled = channel.CommentsEnabled
		if channel.MemberCount > 0 {
			view.MemberCount = channel.MemberCount
		}
	}
	return m.hydrateMembership(ctx, view, channelID)
}

func (m *Module) hydrateMembership(ctx context.Context, view dirextalkdomain.ConversationView, channelID string) (dirextalkdomain.ConversationView, error) {
	joined, err := m.hydrator.CountJoinedMembers(ctx, view.MatrixRoomID, channelID)
	if err != nil {
		return view, err
	}
	if joined > 0 {
		view.MemberCount = joined
	}
	ownerMXID := m.hydrator.OwnerMXID()
	if view.MatrixRoomID == "" || ownerMXID == "" {
		return view, nil
	}
	member, ok, err := m.hydrator.Member(ctx, view.MatrixRoomID, ownerMXID)
	if err != nil {
		return view, err
	}
	if ok && !dirextalkdomain.MemberHidden(member.Membership) {
		view.Membership = member.Membership
		view.Role = dirextalkdomain.NormalizeProductMemberRole(member.Role)
	}
	return view, nil
}

func directMembership(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted":
		return "join"
	case "pending_inbound", "pending_outbound", "pending":
		return "pending"
	case "rejected", "reject":
		return "reject"
	default:
		return strings.TrimSpace(status)
	}
}

func finalizeView(view dirextalkdomain.ConversationView) dirextalkdomain.ConversationView {
	view.HydrationState, view.HydrationReason = hydration(view)
	view.Capabilities = capabilities(view)
	return view
}

func hydration(view dirextalkdomain.ConversationView) (string, string) {
	if view.ProjectionState != "" && view.ProjectionState != dirextalkdomain.ConversationProjectionReady {
		return string(view.ProjectionState), view.ProjectionReason
	}
	switch view.Kind {
	case dirextalkdomain.ConversationKindDirect:
		if strings.TrimSpace(view.PeerMXID) == "" && strings.TrimSpace(view.RelationshipStatus) == "" {
			return string(dirextalkdomain.ConversationProjectionPending), "direct_relationship_missing"
		}
	case dirextalkdomain.ConversationKindGroup, dirextalkdomain.ConversationKindChannel:
		if strings.TrimSpace(view.Membership) == "" {
			return string(dirextalkdomain.ConversationProjectionPending), "owner_membership_missing"
		}
	}
	return string(dirextalkdomain.ConversationProjectionReady), ""
}

func capabilities(view dirextalkdomain.ConversationView) dirextalkdomain.ConversationCapabilities {
	active := view.Lifecycle == dirextalkdomain.ConversationLifecycleActive
	ready := view.HydrationState == string(dirextalkdomain.ConversationProjectionReady)
	if view.Kind == dirextalkdomain.ConversationKindSystem {
		return dirextalkdomain.ConversationCapabilities{Open: ready && active}
	}
	joined := dirextalkdomain.MemberMembershipJoined(view.Membership)
	owner := dirextalkdomain.ProductOwnerRole(view.Role)
	open := ready && active && joined
	if view.Kind == dirextalkdomain.ConversationKindDirect {
		open = ready && active && strings.EqualFold(view.RelationshipStatus, "accepted")
	}
	memberConversation := view.Kind == dirextalkdomain.ConversationKindGroup || view.Kind == dirextalkdomain.ConversationKindChannel
	canCall := open && view.Kind != dirextalkdomain.ConversationKindChannel
	manageMembers := open && owner && memberConversation
	leave := open && memberConversation
	isChannel := view.Kind == dirextalkdomain.ConversationKindChannel
	deleteConversation := false
	if view.Kind == dirextalkdomain.ConversationKindDirect {
		deleteConversation = open
	} else {
		deleteConversation = manageMembers
	}
	return dirextalkdomain.ConversationCapabilities{
		Open:            open,
		Send:            open,
		SendMedia:       open,
		Call:            canCall,
		Invite:          manageMembers,
		ManageMembers:   manageMembers,
		Rename:          manageMembers,
		RemoveMembers:   manageMembers,
		Leave:           leave,
		Delete:          deleteConversation,
		PostCreate:      open && owner && isChannel,
		CommentCreate:   open && isChannel && view.CommentsEnabled,
		ReactionToggle:  open && isChannel,
		PostRecall:      open && owner && isChannel,
		CommentRecall:   open && owner && isChannel,
		CommentsEnabled: isChannel && view.CommentsEnabled,
	}
}

func trimmedFallback(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
