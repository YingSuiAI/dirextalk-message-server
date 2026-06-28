package p2p

import "context"

type actionHandler func(context.Context, map[string]any) (any, *apiError)

func (s *Service) actionHandlers() map[string]actionHandler {
	return map[string]actionHandler{
		"portal.bootstrap":            s.bootstrap,
		"portal.auth":                 s.auth,
		"portal.status":               s.portalStatusAction,
		"portal.password":             s.changePortalPassword,
		"agent.password":              s.agentPasswordAction,
		"agent.matrix_session.create": s.agentMatrixSession,
		"profile.get":                 s.getProfileAction,
		"profile.update":              s.updateProfile,
		"sync.bootstrap":              s.syncBootstrapAction,
		"conversations.list":          s.conversationListAction,
		"conversations.get":           s.conversationGet,

		"mcp.rooms.search":             s.mcpRoomsSearch,
		"mcp.messages.send":            s.mcpMessagesSend,
		"mcp.messages.list":            s.mcpMessagesList,
		"mcp.room_members.list":        s.mcpRoomMembersList,
		"mcp.channel_posts.list":       s.mcpChannelPostsList,
		"mcp.channel_comments.list":    s.mcpChannelCommentsList,
		"mcp.channel_comments.create":  s.mcpChannelCommentCreate,
		"sync.read_marker":             s.updateReadMarker,
		"agent.config.get":             s.getAgentConfigAction,
		"agent.config.update":          s.updateAgentConfigAction,
		"follows.list":                 s.followListAction,
		"follows.add":                  s.followAdd,
		"follows.remove":               s.followRemove,
		"favorites.list":               s.favoriteListAction,
		"favorites.add":                s.favoriteMessage,
		"favorites.delete":             s.favoriteDelete,
		"favorites.delete_batch":       s.favoriteDeleteBatch,
		"reports.submit":               s.reportSubmit,
		"contacts.list":                s.contactListAction,
		"contacts.request":             s.contactRequest,
		"contacts.reactivate":          s.contactReactivate,
		"contacts.requests.accept":     s.contactMutationAction("contacts.requests.accept"),
		"contacts.requests.reject":     s.contactMutationAction("contacts.requests.reject"),
		"contacts.requests.delete":     s.contactMutationAction("contacts.requests.delete"),
		"contacts.delete":              s.contactMutationAction("contacts.delete"),
		"contacts.update":              s.contactUpdate,
		"calls.create":                 s.callSession,
		"calls.incoming":               s.callSession,
		"calls.get":                    s.callGet,
		"calls.event":                  s.callEvent,
		"calls.active":                 s.callListAction(true),
		"calls.list":                   s.callListAction(false),
		"groups.create":                s.groupResult,
		"groups.update":                s.groupUpdate,
		"groups.invite":                s.inviteMembersAction("group"),
		"groups.join":                  s.joinMemberAction("group"),
		"groups.list":                  s.groupListAction,
		"groups.members":               s.memberListAction,
		"groups.dissolve":              s.dissolveGroup,
		"groups.leave":                 s.memberMutationAction("group", "groups.leave"),
		"groups.invite.reject":         s.memberMutationAction("group", "groups.invite.reject"),
		"groups.member.remove":         s.memberMutationAction("group", "groups.member.remove"),
		"groups.member.mute":           s.memberMutationAction("group", "groups.member.mute"),
		"groups.member.unmute":         s.memberMutationAction("group", "groups.member.unmute"),
		"groups.mute":                  s.groupPolicyMutationAction("groups.mute"),
		"groups.unmute":                s.groupPolicyMutationAction("groups.unmute"),
		"groups.invite_policy.update":  s.groupPolicyMutationAction("groups.invite_policy.update"),
		"channels.create":              s.channelResult,
		"channels.update":              s.channelUpdate,
		"channels.join":                s.joinMemberAction("channel"),
		"channels.invite_grant.create": s.channelInviteGrantCreate,
		"channels.invite":              s.inviteMembersAction("channel"),
		"channels.dissolve":            s.dissolveChannel,
		"channels.leave":               s.memberMutationAction("channel", "channels.leave"),
		"channels.member.remove":       s.memberMutationAction("channel", "channels.member.remove"),
		"channels.member.mute":         s.memberMutationAction("channel", "channels.member.mute"),
		"channels.member.unmute":       s.memberMutationAction("channel", "channels.member.unmute"),
		"channels.join_request.approve": s.memberMutationAction(
			"channel",
			"channels.join_request.approve",
		),
		"channels.join_request.reject": s.memberMutationAction(
			"channel",
			"channels.join_request.reject",
		),
		"channels.mute":                s.channelPolicyMutationAction("channels.mute"),
		"channels.unmute":              s.channelPolicyMutationAction("channels.unmute"),
		"channels.read_marker":         s.updateReadMarker,
		"channels.list":                s.channelListAction,
		"channels.members":             s.memberListAction,
		"channels.public.search":       s.channelPublicSearch,
		"channels.public.get":          s.channelPublicGet,
		"channels.public.join_request": s.channelJoinRequest,
		"channels.public.join_result":  s.channelJoinResult,
		"users.public_channels":        s.userPublicChannels,
		"channels.posts.list":          s.channelPostsAction,
		"channels.posts.create":        s.channelPost,
		"channels.posts.recall":        s.recallChannelContentAction("channels.posts.recall"),
		"channels.comments.recall":     s.recallChannelContentAction("channels.comments.recall"),
		"channels.comments.list":       s.channelCommentsAction,
		"channels.comments.create":     s.channelComment,
		"channels.post_reaction.toggle": s.channelReactionAction(
			"channels.post_reaction.toggle",
		),
		"channels.comment_reaction.toggle": s.channelReactionAction(
			"channels.comment_reaction.toggle",
		),
		"channels.my_comments":  s.myChannelCommentsAction,
		"channels.my_reactions": s.myReactionsAction,
	}
}

func (s *Service) portalStatusAction(context.Context, map[string]any) (any, *apiError) {
	return s.portalStatus(), nil
}

func (s *Service) agentPasswordAction(context.Context, map[string]any) (any, *apiError) {
	return s.agentPassword(), nil
}

func (s *Service) getProfileAction(context.Context, map[string]any) (any, *apiError) {
	return s.getProfile(), nil
}

func (s *Service) syncBootstrapAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.syncBootstrap(ctx)
}

func (s *Service) conversationListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.conversationList(ctx)
}

func (s *Service) getAgentConfigAction(context.Context, map[string]any) (any, *apiError) {
	return s.getAgentConfig(), nil
}

func (s *Service) updateAgentConfigAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.updateAgentConfig(ctx, params)
}

func (s *Service) followListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.followList(ctx), nil
}

func (s *Service) favoriteListAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.favoriteList(ctx, params), nil
}

func (s *Service) contactListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.contactList(ctx)
}

func (s *Service) contactMutationAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.contactMutation(ctx, action, params)
	}
}

func (s *Service) callListAction(activeOnly bool) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.callList(ctx, params, activeOnly), nil
	}
}

func (s *Service) inviteMembersAction(roomKind string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.inviteMembers(ctx, roomKind, params)
	}
}

func (s *Service) joinMemberAction(roomKind string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.joinMember(ctx, roomKind, params)
	}
}

func (s *Service) groupListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.groupList(ctx), nil
}

func (s *Service) memberListAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.memberList(ctx, params), nil
}

func (s *Service) memberMutationAction(roomKind, action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.memberMutation(ctx, roomKind, action, params)
	}
}

func (s *Service) groupPolicyMutationAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.groupPolicyMutation(ctx, action, params)
	}
}

func (s *Service) channelPolicyMutationAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.channelPolicyMutation(ctx, action, params)
	}
}

func (s *Service) channelListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.channelList(ctx), nil
}

func (s *Service) channelPostsAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.channelPosts(ctx, params), nil
}

func (s *Service) recallChannelContentAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.recallChannelContent(ctx, action, params)
	}
}

func (s *Service) channelCommentsAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.channelComments(ctx, params), nil
}

func (s *Service) channelReactionAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.channelReaction(ctx, action, params)
	}
}

func (s *Service) myChannelCommentsAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.myChannelComments(ctx, params), nil
}

func (s *Service) myReactionsAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	return s.myReactions(ctx), nil
}
