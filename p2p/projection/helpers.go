package projection

import (
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkprojection"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/domain"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func EventTime(event *types.HeaderedEvent) time.Time {
	ts := int64(event.OriginServerTS())
	if ts <= 0 {
		return time.Now().UTC()
	}
	return time.UnixMilli(ts).UTC()
}

func ConversationKindFromContent(content map[string]any) (domain.ConversationKind, string) {
	if kind := ConversationKindFromRoomType(trimString(content["room_type"])); kind != "" {
		return kind, ""
	}
	return "", "missing explicit room_type"
}

func ConversationKindFromRoomType(roomType string) domain.ConversationKind {
	switch dirextalkprojection.ConversationKindFromRoomType(roomType) {
	case dirextalkstate.RoomKindDirect:
		return domain.ConversationKindDirect
	case dirextalkstate.RoomKindGroup:
		return domain.ConversationKindGroup
	case dirextalkstate.RoomKindChannel:
		return domain.ConversationKindChannel
	default:
		return ""
	}
}

func ChannelProfile(roomID, channelID string, existing domain.Channel, content map[string]any) domain.Channel {
	return dirextalkprojection.ChannelProfile(roomID, channelID, existing, content)
}

func GroupProfile(roomID string, existing domain.GroupRecord, content map[string]any) domain.GroupRecord {
	return groupRecordFromInternal(dirextalkprojection.GroupProfile(roomID, internalGroupRecord(existing), content))
}

func MemberPolicy(roomID, userID string, existing domain.MemberRecord, ok bool, content map[string]any, now time.Time) domain.MemberRecord {
	return dirextalkprojection.MemberPolicy(roomID, userID, existing, ok, content, now)
}

func JoinRequestMember(roomID, channelID, userID string, existing domain.MemberRecord, ok bool, content map[string]any, now time.Time) (domain.MemberRecord, bool) {
	return dirextalkprojection.JoinRequestMember(roomID, channelID, userID, existing, ok, content, now)
}

func internalGroupRecord(group domain.GroupRecord) dirextalkdomain.GroupRecord {
	return dirextalkdomain.GroupRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}
}

func groupRecordFromInternal(group dirextalkdomain.GroupRecord) domain.GroupRecord {
	return domain.GroupRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}
}

func trimString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}
