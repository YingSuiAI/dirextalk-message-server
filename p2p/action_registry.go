package p2p

import "context"

type actionHandler func(context.Context, map[string]any) (any, *apiError)

func (s *Service) actionHandlers() map[string]actionHandler {
	actions := map[string]actionHandler{}
	s.registerPortalActions(actions)
	s.registerProfileAndSyncActions(actions)
	s.registerAgentActions(actions)
	s.registerPluginActions(actions)
	s.registerContactActions(actions)
	s.registerBlockActions(actions)
	s.registerSocialActions(actions)
	s.registerCallActions(actions)
	s.registerGroupActions(actions)
	s.registerChannelActions(actions)
	return actions
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
