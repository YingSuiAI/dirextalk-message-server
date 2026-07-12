package p2p

import (
	"context"
	"fmt"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
)

type apiError = actionbase.Error
type actionHandler = actionbase.Handler

type actionHandlerModule struct {
	name     string
	handlers map[string]actionHandler
}

func (s *Service) actionHandlers() map[string]actionHandler {
	modules := []actionHandlerModule{
		s.collectActionHandlerModule("portal", s.registerPortalActions),
		s.collectActionHandlerModule("release", s.registerReleaseActions),
		s.collectActionHandlerModule("profile-and-sync", s.registerProfileAndSyncActions),
		{name: "conversations", handlers: s.conversationModule.Handlers()},
		s.collectActionHandlerModule("agent", s.registerAgentActions),
		s.collectActionHandlerModule("plugins", s.registerPluginActions),
		s.collectActionHandlerModule("contacts", s.registerContactActions),
		s.collectActionHandlerModule("blocks", s.registerBlockActions),
		s.collectActionHandlerModule("social", s.registerSocialActions),
		s.collectActionHandlerModule("calls", s.registerCallActions),
		s.collectActionHandlerModule("groups", s.registerGroupActions),
		s.collectActionHandlerModule("channels", s.registerChannelActions),
	}
	return mustBuildActionHandlers(
		serviceapi.ActionSpecs(),
		[]string{serviceapi.RealtimeWSTicketAction},
		modules,
	)
}

func (s *Service) collectActionHandlerModule(name string, register func(map[string]actionHandler)) actionHandlerModule {
	handlers := make(map[string]actionHandler)
	register(handlers)
	return actionHandlerModule{name: name, handlers: handlers}
}

func mustBuildActionHandlers(specs []serviceapi.ActionSpec, routeSpecial []string, modules []actionHandlerModule) map[string]actionHandler {
	registry, err := actionbase.NewRegistry(specs, routeSpecial...)
	if err != nil {
		panic(fmt.Sprintf("build ProductCore action registry: %v", err))
	}
	for _, module := range modules {
		if err := registry.Merge(module.name, module.handlers); err != nil {
			panic(fmt.Sprintf("build ProductCore action registry: %v", err))
		}
	}
	if err := registry.Validate(); err != nil {
		panic(fmt.Sprintf("build ProductCore action registry: %v", err))
	}
	return registry.Handlers()
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
