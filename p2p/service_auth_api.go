package p2p

import "context"

func (s *Service) bootstrap(ctx context.Context, params map[string]any) (any, *apiError) {
	password := trimString(params["password"])
	if password == "" {
		password = trimString(params["token"])
	}
	if password == "" {
		return nil, badRequest("password is required")
	}
	s.mu.Lock()
	if password != s.password {
		s.mu.Unlock()
		return nil, statusError(401, "password invalid")
	}
	session := s.sessionLocked()
	state := s.portalStateLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return s.refreshMatrixSession(ctx, session, params, true)
}

func (s *Service) auth(ctx context.Context, params map[string]any) (any, *apiError) {
	password := trimString(params["password"])
	s.mu.Lock()
	if password == "" || password != s.password {
		s.mu.Unlock()
		return nil, statusError(401, "password invalid")
	}
	session := s.sessionLocked()
	s.mu.Unlock()
	return s.refreshMatrixSession(ctx, session, params, true)
}

func (s *Service) changePortalPassword(ctx context.Context, params map[string]any) (any, *apiError) {
	oldPassword := trimString(params["old_password"])
	newPassword := trimString(params["new_password"])
	if newPassword == "" {
		return nil, badRequest("new_password is required")
	}
	s.mu.Lock()
	if oldPassword == "" || oldPassword != s.password {
		s.mu.Unlock()
		return nil, statusError(401, "password invalid")
	}
	s.password = newPassword
	s.accessToken = randomToken("p2p_access")
	s.initialized = true
	session := s.sessionLocked()
	state := s.portalStateLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return s.refreshMatrixSession(ctx, session, params, true)
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

func (s *Service) refreshMatrixSession(ctx context.Context, session map[string]any, params map[string]any, revokeExistingDevices bool) (map[string]any, *apiError) {
	s.matrixSessionMu.Lock()
	defer s.matrixSessionMu.Unlock()

	requestedDeviceID := requestedMatrixDeviceID(params)
	s.mu.Lock()
	issuer := s.sessions
	userID := s.profile.UserID
	displayName := s.profile.DisplayName
	avatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	if issuer == nil {
		session["device_id"] = requestedDeviceID
		return session, nil
	}
	token, err := issuer.EnsureMatrixSession(ctx, userID, displayName, avatarURL, requestedDeviceID, revokeExistingDevices)
	if err != nil {
		return nil, internalError(err)
	}
	s.mu.Lock()
	s.accessToken = token
	s.matrixDeviceID = requestedDeviceID
	state := s.portalStateLocked()
	session = s.sessionLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return session, nil
}

func (s *Service) portalStatus() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	policyIndexMode := "unavailable"
	policyIndexReady := false
	if s.transport != nil {
		policyIndexMode = "matrix_state"
		policyIndexReady = true
	}
	return map[string]any{
		"initialized":        s.initialized,
		"user_id":            s.ownerMXID,
		"homeserver":         s.homeserver,
		"store_mode":         s.storeMode,
		"projector_started":  s.projectorStarted,
		"policy_index_mode":  policyIndexMode,
		"policy_index_ready": policyIndexReady,
		"event_stream_ready": true,
	}
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
	result := agentConfigToMap(s.agentConfig)
	s.mu.Unlock()
	if disableAgent {
		if err := s.publishCurrentAgentStatusState(ctx); err != nil {
			return nil, transportWriteError(err)
		}
	}
	return result, nil
}

func agentConfigToMap(cfg agentConfig) map[string]any {
	return map[string]any{
		"display_name":   cfg.DisplayName,
		"context_window": cfg.ContextWindow,
		"enabled":        cfg.Enabled,
		"model":          cfg.Model,
		"system_prompt":  cfg.SystemPrompt,
	}
}
