package serviceapi

import (
	"sort"

	"github.com/YingSuiAI/direxio-message-server/p2p/domain"
)

func PublicAction(action string) bool {
	switch action {
	case "portal.bootstrap", "portal.auth", "portal.status", "contacts.reactivate", "channels.public.search", "channels.public.get", "channels.public.join_request", "channels.public.join_result", "users.public_channels":
		return true
	default:
		return false
	}
}

func PermissionItems(perms map[string]domain.APIPermission) []domain.APIPermission {
	items := make([]domain.APIPermission, 0, len(perms))
	for _, item := range perms {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Action < items[j].Action
	})
	return items
}

func DefaultAPIPermissions() map[string]domain.APIPermission {
	items := []domain.APIPermission{
		{Action: "agent.password", Description: "Agent reads current portal password", Enabled: true},
		{Action: "agent.matrix_session.create", Description: "Create an internal Matrix session for Agent tooling", Enabled: true},
		{Action: "agent.config.get", Description: "Read Agent config", Enabled: true},
		{Action: "agent.config.update", Description: "Update Agent config", Enabled: true},
		{Action: "agent.status", Description: "Read Agent status", Enabled: true},
		{Action: "apis.list", Description: "List Agent-controllable API permissions", Enabled: true},
		{Action: "profile.get", Description: "Read owner profile", Enabled: true},
		{Action: "profile.update", Description: "Update owner profile", Enabled: true},
		{Action: "sync.bootstrap", Description: "Read first-screen metadata", Enabled: true},
		{Action: "conversations.list", Description: "List ProductCore conversations", Enabled: true},
		{Action: "conversations.get", Description: "Read ProductCore conversation", Enabled: true},
		{Action: "mcp.rooms.search", Description: "Search MCP room summaries", Enabled: true},
		{Action: "mcp.messages.send", Description: "Send MCP plain text message", Enabled: true},
		{Action: "mcp.messages.list", Description: "List MCP ordinary message summaries", Enabled: true},
		{Action: "mcp.channel_posts.list", Description: "List MCP channel post summaries", Enabled: true},
		{Action: "mcp.channel_comments.list", Description: "List MCP channel comment summaries", Enabled: true},
		{Action: "mcp.channel_comments.create", Description: "Create MCP channel post comment", Enabled: true},
		{Action: "events.stream", Description: "Stream projected P2P events with SSE", Enabled: true},
		{Action: "sync.read_marker", Description: "Update read marker", Enabled: true},
		{Action: "contacts.list", Description: "List contacts", Enabled: true},
		{Action: "contacts.request", Description: "Create contact request", Enabled: true},
		{Action: "contacts.reactivate", Description: "Reinvite a retained peer to an existing direct room", Enabled: true},
		{Action: "contacts.requests.accept", Description: "Accept contact request", Enabled: true},
		{Action: "contacts.requests.reject", Description: "Reject contact request", Enabled: true},
		{Action: "contacts.requests.delete", Description: "Delete contact request", Enabled: true},
		{Action: "contacts.update", Description: "Update contact remark", Enabled: true},
		{Action: "contacts.delete", Description: "Delete contact", Enabled: true},
		{Action: "favorites.list", Description: "List favorite messages", Enabled: true},
		{Action: "favorites.add", Description: "Add favorite message", Enabled: true},
		{Action: "favorites.delete", Description: "Delete favorite message", Enabled: true},
		{Action: "favorites.delete_batch", Description: "Batch delete favorites", Enabled: true},
		{Action: "reports.submit", Description: "Submit user or channel report", Enabled: true},
		{Action: "calls.get", Description: "Read call session detail", Enabled: true},
		{Action: "calls.incoming", Description: "Register incoming call session", Enabled: true},
		{Action: "calls.event", Description: "Update call session state", Enabled: true},
		{Action: "channels.create", Description: "Create channel", Enabled: true},
		{Action: "channels.list", Description: "List channels", Enabled: true},
		{Action: "channels.join", Description: "Join channel by room id", Enabled: true},
		{Action: "channels.update", Description: "Update channel", Enabled: true},
		{Action: "channels.invite", Description: "Invite channel members", Enabled: true},
		{Action: "channels.invite_grant.create", Description: "Create a room-scoped channel invite grant", Enabled: true},
		{Action: "channels.leave", Description: "Leave channel", Enabled: true},
		{Action: "channels.dissolve", Description: "Dissolve owned channel", Enabled: true},
		{Action: "channels.members", Description: "List channel members", Enabled: true},
		{Action: "channels.member.remove", Description: "Remove channel member", Enabled: true},
		{Action: "channels.member.mute", Description: "Mute channel member", Enabled: true},
		{Action: "channels.member.unmute", Description: "Unmute channel member", Enabled: true},
		{Action: "channels.mute", Description: "Mute channel", Enabled: true},
		{Action: "channels.unmute", Description: "Unmute channel", Enabled: true},
		{Action: "channels.posts.list", Description: "List channel posts", Enabled: true},
		{Action: "channels.posts.create", Description: "Create channel post", Enabled: true},
		{Action: "channels.posts.recall", Description: "Recall channel post", Enabled: true},
		{Action: "channels.comments.list", Description: "List channel post comments", Enabled: true},
		{Action: "channels.comments.create", Description: "Create channel post comment", Enabled: true},
		{Action: "channels.comments.recall", Description: "Recall channel comment", Enabled: true},
		{Action: "channels.post_reaction.toggle", Description: "Toggle channel post reaction", Enabled: true},
		{Action: "channels.comment_reaction.toggle", Description: "Toggle channel comment reaction", Enabled: true},
		{Action: "channels.my_comments", Description: "List owner channel comments", Enabled: true},
		{Action: "channels.my_reactions", Description: "List owner channel reactions", Enabled: true},
		{Action: "channels.read_marker", Description: "Update channel read marker", Enabled: true},
		{Action: "channels.join_request.approve", Description: "Approve channel join request", Enabled: true},
		{Action: "channels.join_request.reject", Description: "Reject channel join request", Enabled: true},
		{Action: "channels.public.get", Description: "Read public channel detail", Enabled: true},
		{Action: "channels.public.join_request", Description: "Create public channel join request", Enabled: true},
		{Action: "users.public_channels", Description: "List public channels owned by a user", Enabled: true},
		{Action: "groups.create", Description: "Create group", Enabled: true},
		{Action: "groups.list", Description: "List groups", Enabled: true},
		{Action: "groups.update", Description: "Update group profile", Enabled: true},
		{Action: "groups.invite", Description: "Invite group members", Enabled: true},
		{Action: "groups.invite.reject", Description: "Reject current user's group invite", Enabled: true},
		{Action: "groups.join", Description: "Join group", Enabled: true},
		{Action: "groups.members", Description: "List group members", Enabled: true},
		{Action: "groups.leave", Description: "Leave group", Enabled: true},
		{Action: "groups.dissolve", Description: "Dissolve owned group", Enabled: true},
		{Action: "groups.mute", Description: "Mute group", Enabled: true},
		{Action: "groups.unmute", Description: "Unmute group", Enabled: true},
		{Action: "groups.invite_policy.update", Description: "Update group invite policy", Enabled: true},
		{Action: "groups.member.remove", Description: "Remove group member", Enabled: true},
		{Action: "groups.member.mute", Description: "Mute group member", Enabled: true},
		{Action: "groups.member.unmute", Description: "Unmute group member", Enabled: true},
		{Action: "calls.create", Description: "Create call session", Enabled: true},
		{Action: "calls.list", Description: "List call sessions", Enabled: true},
		{Action: "calls.active", Description: "List active calls", Enabled: true},
		{Action: "follows.list", Description: "List followed domains", Enabled: true},
		{Action: "follows.add", Description: "Add followed domain", Enabled: true},
		{Action: "follows.remove", Description: "Remove followed domain", Enabled: true},
	}
	perms := make(map[string]domain.APIPermission, len(items))
	for _, item := range items {
		perms[item.Action] = item
	}
	return perms
}
