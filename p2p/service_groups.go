package p2p

import (
	"context"
	"strings"
	"time"
)

type groupStore interface {
	UpsertGroup(ctx context.Context, group groupStorageRecord) error
	DeleteGroup(ctx context.Context, roomID string) error
	ListGroups(ctx context.Context) ([]groupStorageRecord, error)
	GetGroupByRoom(ctx context.Context, roomID string) (groupStorageRecord, bool, error)
	ListJoinedGroupsForUser(ctx context.Context, userID string) ([]groupStorageRecord, error)
}

func (s *Service) groupStore() groupStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) ensureProductRoom(ctx context.Context, kind string, req CreateRoomRequest) (string, *apiError) {
	if s.transport != nil {
		s.mu.Lock()
		req.CreatorMXID = s.ownerMXID
		if req.CreatorDisplayName == "" {
			req.CreatorDisplayName = s.profile.DisplayName
		}
		if req.CreatorAvatarURL == "" {
			req.CreatorAvatarURL = s.profile.AvatarURL
		}
		s.mu.Unlock()
		if req.RoomType == "" {
			req.RoomType = dirextalkRoomType(kind)
		}
		res, err := s.transport.CreateRoom(ctx, req)
		if err != nil {
			return "", internalError(err)
		}
		return res.RoomID, nil
	}
	return "!" + kind + "-" + randomToken("room") + ":" + s.serverName, nil
}

func (s *Service) saveOwnerMember(ctx context.Context, roomID, channelID string) error {
	s.mu.Lock()
	member := memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      s.ownerMXID,
		DisplayName: s.profile.DisplayName,
		AvatarURL:   s.profile.AvatarURL,
		Domain:      s.serverName,
		Membership:  "join",
		Role:        "owner",
		JoinedAt:    time.Now().UTC().UnixMilli(),
	}
	s.mu.Unlock()
	return s.saveMember(ctx, member)
}

func (s *Service) groupResult(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	needsStatePublish := roomID != ""
	name := fallbackString(trimString(params["name"]), trimString(params["group_name"]))
	group := groupRecord{
		RoomID:       roomID,
		Name:         fallbackString(name, "Group"),
		Topic:        trimString(params["topic"]),
		AvatarURL:    trimString(params["avatar_url"]),
		MemberCount:  1,
		InvitePolicy: fallbackString(trimString(params["invite_policy"]), "member"),
	}
	if roomID == "" {
		var apiErr *apiError
		roomID, apiErr = s.ensureProductRoom(ctx, "group", CreateRoomRequest{
			Name:       fallbackString(name, "Group"),
			Topic:      trimString(params["topic"]),
			Visibility: "private",
			RoomType:   DirextalkRoomTypeGroup,
			IsDirect:   false,
			InitialState: []RoomStateEvent{
				joinedHistoryVisibilityStateEvent(),
				groupStateEvent(group, false),
			},
		})
		if apiErr != nil {
			return nil, apiErr
		}
	}
	group.RoomID = roomID
	if group.Name == "" || group.Name == "Group" && name == "" {
		group.Name = fallbackString(name, roomID)
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if err := s.saveOwnerMember(ctx, group.RoomID, ""); err != nil {
		return nil, internalError(err)
	}
	if needsStatePublish {
		if err := s.publishGroupState(ctx, group, false); err != nil {
			return nil, internalError(err)
		}
	}
	result, err := s.groupRecordWithConversationOperation(ctx, group, "groups.create", "ok")
	if err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) groupRecordWithConversationOperation(ctx context.Context, group groupRecord, action, status string) (groupRecord, error) {
	roomID := strings.TrimSpace(group.RoomID)
	operation := map[string]any{
		"action":  action,
		"status":  status,
		"room_id": roomID,
	}
	if roomID != "" {
		record, ok, err := s.getConversation(ctx, "", roomID)
		if err != nil {
			return groupRecord{}, err
		}
		if ok {
			view, err := s.conversationView(ctx, record)
			if err != nil {
				return groupRecord{}, err
			}
			group.Conversation = &view
			operation["conversation_id"] = view.ConversationID
		}
	}
	group.Operation = operation
	return group, nil
}

func (s *Service) groupUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	if name := fallbackString(trimString(params["name"]), trimString(params["group_name"])); name != "" {
		group.Name = name
	}
	if _, ok := params["topic"]; ok {
		group.Topic = trimString(params["topic"])
	}
	if _, ok := params["avatar_url"]; ok {
		group.AvatarURL = trimString(params["avatar_url"])
	}
	if policy := trimString(params["invite_policy"]); policy != "" {
		group.InvitePolicy = policy
	}
	if _, ok := params["muted"]; ok {
		group.Muted = boolParam(params["muted"])
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if err := s.publishGroupState(ctx, group, false); err != nil {
		return nil, internalError(err)
	}
	return group, nil
}

func (s *Service) groupList(ctx context.Context) any {
	if store := s.groupStore(); store != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		s.mu.Unlock()
		storedGroups, err := store.ListJoinedGroupsForUser(ctx, ownerMXID)
		if err != nil {
			return map[string]any{"groups": []groupRecord{}}
		}
		return map[string]any{"groups": groupRecordsFromStorage(storedGroups)}
	}
	groups, err := s.listGroups(ctx)
	if err != nil {
		return map[string]any{"groups": []groupRecord{}}
	}
	groups, err = s.joinedGroupsForOwner(ctx, groups)
	if err != nil {
		return map[string]any{"groups": []groupRecord{}}
	}
	return map[string]any{"groups": groups}
}

func (s *Service) groupPolicyMutation(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	switch action {
	case "groups.mute":
		group.Muted = true
	case "groups.unmute":
		group.Muted = false
	case "groups.invite_policy.update":
		if policy := trimString(params["invite_policy"]); policy != "" {
			group.InvitePolicy = policy
		}
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if action == "groups.invite_policy.update" {
		if err := s.publishGroupState(ctx, group, false); err != nil {
			return nil, internalError(err)
		}
	}
	if action == "groups.mute" || action == "groups.unmute" {
		if err := s.setProductMemberMute(ctx, roomID, "", group.Muted); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"status": "ok", "room_id": group.RoomID, "muted": group.Muted, "group": group}, nil
	}
	return group, nil
}

func (s *Service) saveGroup(ctx context.Context, group groupRecord) error {
	s.mu.Lock()
	s.groups[group.RoomID] = group
	s.mu.Unlock()
	if store := s.groupStore(); store != nil {
		if err := store.UpsertGroup(ctx, groupStorageRecordFromGroup(group)); err != nil {
			return err
		}
	}
	return s.saveConversation(ctx, conversationFromGroup(group))
}

func groupStateEvent(group groupRecord, dissolved bool) RoomStateEvent {
	return roomProfileForGroup(group, dissolved)
}

func (s *Service) publishGroupState(ctx context.Context, group groupRecord, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(group.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     group.RoomID,
		SenderMXID: senderMXID,
		Event:      groupStateEvent(group, dissolved),
	})
}

func (s *Service) deleteGroup(ctx context.Context, roomID string) error {
	s.mu.Lock()
	delete(s.groups, roomID)
	deleteConversationKindByRoomLocked(s.conversations, roomID, conversationKindGroup)
	s.mu.Unlock()
	if store := s.groupStore(); store != nil {
		if err := store.DeleteGroup(ctx, roomID); err != nil {
			return err
		}
		return s.deleteStoredConversationKind(ctx, roomID, conversationKindGroup)
	}
	return nil
}

func (s *Service) dissolveGroup(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	if apiErr := s.requireOwnerMember(ctx, group.RoomID); apiErr != nil {
		return nil, apiErr
	}
	if err := s.publishGroupState(ctx, group, true); err != nil {
		return nil, internalError(err)
	}
	if err := s.deleteGroup(ctx, group.RoomID); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "group": group}, nil
}

func (s *Service) groupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error) {
	if store := s.groupStore(); store != nil {
		group, ok, err := store.GetGroupByRoom(ctx, roomID)
		if err != nil || !ok {
			return groupRecord{}, ok, err
		}
		return groupRecordFromStorage(group), true, nil
	}
	groups, err := s.listGroups(ctx)
	if err != nil {
		return groupRecord{}, false, err
	}
	for _, group := range groups {
		if group.RoomID == roomID {
			return group, true, nil
		}
	}
	return groupRecord{}, false, nil
}
