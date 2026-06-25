package p2p

import (
	"context"
	"sort"
	"strings"
)

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
	session, apiErr := s.refreshMatrixSession(ctx, map[string]any{}, params, false)
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

func (s *Service) updateAgentConfig(params map[string]any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if displayName := trimString(params["display_name"]); displayName != "" {
		s.agentConfig.DisplayName = displayName
	}
	if contextWindow := int64Param(params["context_window"]); contextWindow > 0 {
		s.agentConfig.ContextWindow = contextWindow
	}
	if _, ok := params["enabled"]; ok {
		s.agentConfig.Enabled = boolParam(params["enabled"])
	}
	if model := trimString(params["model"]); model != "" {
		s.agentConfig.Model = model
	}
	if systemPrompt := trimString(params["system_prompt"]); systemPrompt != "" {
		s.agentConfig.SystemPrompt = systemPrompt
	}
	if actions := stringSliceParam(params["allowed_actions"]); actions != nil {
		s.agentConfig.AllowedActions = actions
	}
	return agentConfigToMap(s.agentConfig)
}

func (s *Service) agentStatus() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	configured := strings.TrimSpace(s.agentRoomID) != "" &&
		strings.TrimSpace(s.ownerMXID) != "" &&
		strings.TrimSpace(s.agentConfig.DisplayName) != ""
	return map[string]any{
		"online":        s.agentConfig.Enabled,
		"connected":     s.agentConfig.Enabled,
		"configured":    configured,
		"display_name":  s.agentConfig.DisplayName,
		"agent_room_id": s.agentRoomID,
	}
}

func agentConfigToMap(cfg agentConfig) map[string]any {
	return map[string]any{
		"display_name":    cfg.DisplayName,
		"context_window":  cfg.ContextWindow,
		"enabled":         cfg.Enabled,
		"model":           cfg.Model,
		"system_prompt":   cfg.SystemPrompt,
		"allowed_actions": append([]string{}, cfg.AllowedActions...),
	}
}

func (s *Service) apiPermissionList() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"items": apiPermissionItemsLocked(s.apiPerms)}
}

func (s *Service) apiPermissionStatus(params map[string]any) (any, *apiError) {
	rawItems, _ := params["items"].([]any)
	if len(rawItems) == 0 {
		return nil, badRequest("items cannot be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range rawItems {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, badRequest("invalid permission item")
		}
		action := trimString(item["action"])
		if action == "" {
			return nil, badRequest("action is required")
		}
		perm, ok := s.apiPerms[action]
		if !ok {
			return nil, badRequest("route is not Agent-permission controlled")
		}
		perm.Enabled = boolParam(item["enabled"])
		s.apiPerms[action] = perm
	}
	return map[string]any{"items": apiPermissionItemsLocked(s.apiPerms)}, nil
}

func apiPermissionItemsLocked(perms map[string]apiPermission) []apiPermission {
	items := make([]apiPermission, 0, len(perms))
	for _, item := range perms {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Action < items[j].Action
	})
	return items
}

func defaultAPIPermissions() map[string]apiPermission {
	items := []apiPermission{
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
	perms := make(map[string]apiPermission, len(items))
	for _, item := range items {
		perms[item.Action] = item
	}
	return perms
}
