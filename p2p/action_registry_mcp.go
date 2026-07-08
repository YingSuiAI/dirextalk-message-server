package p2p

func (s *Service) registerMCPActions(actions map[string]actionHandler) {
	actions["mcp.rooms.search"] = s.mcpRoomsSearch
	actions["mcp.contacts.list"] = s.mcpContactsList
	actions["mcp.contacts.search"] = s.mcpContactsSearch
	actions["mcp.messages.send"] = s.mcpMessagesSend
	actions["mcp.messages.list"] = s.mcpMessagesList
	actions["mcp.room_members.list"] = s.mcpRoomMembersList
	actions["mcp.channel_posts.list"] = s.mcpChannelPostsList
	actions["mcp.channel_comments.list"] = s.mcpChannelCommentsList
	actions["mcp.channel_comments.create"] = s.mcpChannelCommentCreate
}
