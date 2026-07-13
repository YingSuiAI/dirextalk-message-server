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
		{name: "plugins", handlers: s.pluginsModule.Handlers()},
		{name: "contacts", handlers: s.contactsModule.Handlers()},
		{name: "members", handlers: s.membersModule.Handlers()},
		{name: "blocks", handlers: s.blocksModule.Handlers()},
		{name: "social", handlers: s.socialModule.Handlers()},
		{name: "calls", handlers: s.callsModule.Handlers()},
		{name: "groups", handlers: s.groupsModule.Handlers()},
		{name: "channels", handlers: s.channelsModule.Handlers()},
		{name: "channel-content", handlers: s.channelContentModule.Handlers()},
		s.collectActionHandlerModule("channel-adapters", s.registerChannelActions),
		{name: "reports", handlers: s.reportsModule.Handlers()},
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
