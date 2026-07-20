package dirextalkprojection

import (
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
)

func ChannelProfile(roomID, channelID string, existing dirextalkdomain.Channel, content map[string]any) dirextalkdomain.Channel {
	channelType := fallbackString(trimString(content["channel_type"]), fallbackString(existing.ChannelType, "post"))
	commentsEnabled := existing.CommentsEnabled
	if _, ok := content["comments_enabled"]; ok {
		commentsEnabled = boolParam(content["comments_enabled"])
	}
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	description := existing.Description
	if _, ok := content["description"]; ok {
		description = trimString(content["description"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	return dirextalkdomain.Channel{
		ChannelID:        channelID,
		RoomID:           roomID,
		Name:             fallbackString(trimString(content["name"]), fallbackString(existing.Name, channelID)),
		Description:      description,
		AvatarURL:        avatarURL,
		Visibility:       fallbackString(trimString(content["visibility"]), fallbackString(existing.Visibility, "private")),
		JoinPolicy:       fallbackString(trimString(content["join_policy"]), fallbackString(existing.JoinPolicy, "invite")),
		ChannelType:      channelType,
		CommentsEnabled:  commentsEnabled,
		Muted:            muted,
		MemberCount:      memberCount,
		PendingJoinCount: existing.PendingJoinCount,
	}
}

func GroupProfile(roomID string, existing dirextalkdomain.GroupRecord, content map[string]any) dirextalkdomain.GroupRecord {
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	topic := existing.Topic
	if _, ok := content["topic"]; ok {
		topic = trimString(content["topic"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	return dirextalkdomain.GroupRecord{
		RoomID:       roomID,
		Name:         fallbackString(trimString(content["name"]), fallbackString(existing.Name, roomID)),
		Topic:        topic,
		AvatarURL:    avatarURL,
		MemberCount:  memberCount,
		InvitePolicy: fallbackString(trimString(content["invite_policy"]), fallbackString(existing.InvitePolicy, "member")),
		Muted:        muted,
	}
}

func MemberPolicy(roomID, userID string, existing dirextalkdomain.MemberRecord, ok bool, content map[string]any, now time.Time) dirextalkdomain.MemberRecord {
	member := existing
	if !ok {
		member = dirextalkdomain.MemberRecord{
			RoomID:     roomID,
			UserID:     userID,
			Domain:     domainFromMXID(userID),
			Membership: "join",
			Role:       "member",
			JoinedAt:   now.UnixMilli(),
		}
	}
	if role := trimString(content["role"]); role != "" {
		member.Role = role
	}
	if _, ok := content["muted"]; ok {
		member.Muted = boolParam(content["muted"])
	}
	return member
}

func JoinRequestMember(
	roomID, channelID, userID string,
	existing dirextalkdomain.MemberRecord,
	ok bool,
	status, requestID string,
	now time.Time,
) (dirextalkdomain.MemberRecord, bool) {
	membership := ""
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "pending":
		membership = "pending"
	case "approved":
		membership = "invite"
	case "rejected":
		membership = "reject"
	default:
		return dirextalkdomain.MemberRecord{}, false
	}
	requestID = strings.TrimSpace(requestID)
	if ok && status == "approved" && preserveApprovedJoinRequestWorkflow(existing) {
		return existing, true
	}
	if ok && preserveJoinRequestGeneration(existing, requestID) {
		return existing, true
	}
	member := existing
	if !ok {
		member = dirextalkdomain.MemberRecord{RoomID: roomID, ChannelID: channelID, UserID: userID}
	}
	member.RoomID = roomID
	member.UserID = userID
	member.Membership = membership
	member.Role = fallbackString(member.Role, "member")
	member.Domain = fallbackString(member.Domain, domainFromMXID(userID))
	if member.JoinedAt == 0 {
		member.JoinedAt = now.UnixMilli()
	}
	if requestID != "" {
		member.RequestID = requestID
	}
	return member, true
}

func preserveApprovedJoinRequestWorkflow(existing dirextalkdomain.MemberRecord) bool {
	switch strings.ToLower(strings.TrimSpace(existing.Membership)) {
	case "approved", "joining", "join_failed", "join":
		return true
	default:
		return false
	}
}

func preserveJoinRequestGeneration(existing dirextalkdomain.MemberRecord, requestID string) bool {
	requestID = strings.TrimSpace(requestID)
	return strings.TrimSpace(existing.RequestID) != "" && strings.TrimSpace(existing.RequestID) != requestID
}

func ConversationKindFromRoomType(roomType string) dirextalkstate.RoomKind {
	return dirextalkstate.RoomKindFromRoomType(roomType)
}

func trimString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func boolParam(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}

func domainFromMXID(mxid string) string {
	trimmed := strings.TrimPrefix(mxid, "@")
	if idx := strings.Index(trimmed, ":"); idx >= 0 && idx+1 < len(trimmed) {
		return trimmed[idx+1:]
	}
	return ""
}
