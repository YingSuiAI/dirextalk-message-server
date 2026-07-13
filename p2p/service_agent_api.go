package p2p

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

// NativeAgentRunner remains the public runtime injection boundary while the
// default implementation and action ownership live in internal/agent.
type NativeAgentRunner interface {
	Apply(context.Context, string) error
	Invoke(context.Context, string, map[string]any) (map[string]any, error)
	Stream(context.Context, string, map[string]any, func(nativeagent.Event) error) error
}

func (s *Service) agentPassword() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"password": s.password}
}

func (s *Service) agentMatrixSession(ctx context.Context, params map[string]any) (any, *apiError) {
	session, apiErr := s.createAgentMatrixSession(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"access_token": session["access_token"],
		"device_id":    session["device_id"],
		"user_id":      session["user_id"],
		"homeserver":   session["homeserver"],
	}, nil
}

func (s *Service) createAgentMatrixSession(ctx context.Context, params map[string]any) (map[string]any, *apiError) {
	s.matrixSessionMu.Lock()
	defer s.matrixSessionMu.Unlock()

	requestedDeviceID := requestedMatrixDeviceID(params)
	s.mu.Lock()
	issuer := s.sessions
	userID := s.agentMXIDLocked()
	displayName := s.agentDisplayNameLocked()
	homeserver := s.homeserver
	s.mu.Unlock()
	session := map[string]any{
		"device_id":  requestedDeviceID,
		"user_id":    userID,
		"homeserver": homeserver,
	}
	if issuer == nil {
		return session, nil
	}
	token, err := issuer.EnsureMatrixSession(ctx, userID, displayName, "", requestedDeviceID, false)
	if err != nil {
		return nil, internalError(err)
	}
	session["access_token"] = token
	return session, nil
}

func (s *Service) getAgentConfig() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return agentConfigToMap(s.agentConfig)
}

func (s *Service) updateAgentConfig(ctx context.Context, params map[string]any) (any, *apiError) {
	disableAgent := false
	s.mu.Lock()
	if displayName := trimString(params["display_name"]); displayName != "" {
		s.agentConfig.DisplayName = displayName
	}
	if _, ok := params["avatar_url"]; ok {
		s.agentConfig.AvatarURL = trimString(params["avatar_url"])
	}
	if contextWindow := int64Param(params["context_window"]); contextWindow > 0 {
		s.agentConfig.ContextWindow = contextWindow
	}
	if _, ok := params["enabled"]; ok {
		s.agentConfig.Enabled = boolParam(params["enabled"])
		disableAgent = !s.agentConfig.Enabled
	}
	if model := trimString(params["model"]); model != "" {
		s.agentConfig.Model = model
	}
	if systemPrompt := trimString(params["system_prompt"]); systemPrompt != "" {
		s.agentConfig.SystemPrompt = systemPrompt
	}
	if _, ok := params["mcp_blocked_room_ids"]; ok {
		s.agentConfig.MCPBlockedRoomIDs = stringSliceParam(params["mcp_blocked_room_ids"])
	}
	s.agentConfig = normalizeAgentConfig(s.agentConfig)
	result := agentConfigToMap(s.agentConfig)
	state := s.portalStateLocked()
	s.mu.Unlock()
	if store := s.portalStore(); store != nil {
		if err := store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if disableAgent {
		if err := s.publishCurrentAgentStatusState(ctx); err != nil {
			return nil, transportWriteError(err)
		}
	}
	return result, nil
}

func agentConfigToMap(cfg agentConfig) map[string]any {
	blockedRoomIDs := append([]string(nil), cfg.MCPBlockedRoomIDs...)
	return map[string]any{
		"display_name":         cfg.DisplayName,
		"avatar_url":           cfg.AvatarURL,
		"context_window":       cfg.ContextWindow,
		"enabled":              cfg.Enabled,
		"model":                cfg.Model,
		"system_prompt":        cfg.SystemPrompt,
		"mcp_blocked_room_ids": blockedRoomIDs,
	}
}

// nativeAgentConfigStore adapts the account-scoped durable portal record to
// the runtime's narrow configuration store.
type nativeAgentConfigStore struct {
	service *Service
}

func (s nativeAgentConfigStore) Load(context.Context) (map[string]any, bool, error) {
	if s.service == nil {
		return map[string]any{}, false, nil
	}
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	return agentConfigToNativeMap(s.service.agentConfig), true, nil
}

func (s nativeAgentConfigStore) Save(ctx context.Context, config map[string]any) error {
	if s.service == nil {
		return fmt.Errorf("native agent config store is unavailable")
	}
	ctx, finishOperation := s.service.beginAccountOperation(ctx)
	defer finishOperation()
	if s.service.accountIsDeprovisioned() {
		return fmt.Errorf("account is deprovisioned")
	}
	s.service.mu.Lock()
	s.service.agentConfig = agentConfigFromNativeMap(s.service.agentConfig, config)
	state := s.service.portalStateLocked()
	s.service.mu.Unlock()
	if store := s.service.portalStore(); store != nil {
		return store.SavePortal(ctx, state)
	}
	return nil
}
