package serviceapi

var publicActions = []string{
	"portal.bootstrap",
	"portal.auth",
	"portal.status",
	"contacts.reactivate",
	"channels.public.search",
	"channels.public.get",
	"channels.public.join_request",
	"channels.public.join_result",
	"users.public_channels",
}

var agentActions = []string{
	"mcp.rooms.search",
	"mcp.messages.send",
	"mcp.messages.list",
	"mcp.room_members.list",
	"mcp.channel_posts.list",
	"mcp.channel_comments.list",
	"mcp.channel_comments.create",
}

func PublicActions() []string {
	actions := make([]string, len(publicActions))
	copy(actions, publicActions)
	return actions
}

func AgentActions() []string {
	actions := make([]string, len(agentActions))
	copy(actions, agentActions)
	return actions
}

func PublicAction(action string) bool {
	for _, publicAction := range publicActions {
		if action == publicAction {
			return true
		}
	}
	return false
}

func AgentAction(action string) bool {
	for _, agentAction := range agentActions {
		if action == agentAction {
			return true
		}
	}
	return false
}
