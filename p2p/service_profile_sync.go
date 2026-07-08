package p2p

import (
	"context"
	"strings"
	"time"
)

func (s *Service) getProfile() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profile
}

func (s *Service) portalOwnerWellKnown() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"matrix_user_id": s.profile.UserID,
		"mxid":           s.profile.UserID,
		"user_id":        s.profile.UserID,
		"display_name":   s.profile.DisplayName,
		"avatar_url":     s.profile.AvatarURL,
	}
}

func (s *Service) updateProfile(ctx context.Context, params map[string]any) (any, *apiError) {
	s.mu.Lock()
	if v := trimString(params["display_name"]); v != "" {
		s.profile.DisplayName = v
	}
	s.profile.AvatarURL = trimString(params["avatar_url"])
	s.profile.Gender = trimString(params["gender"])
	s.profile.Birthday = trimString(params["birthday"])
	s.profile.Phone = trimString(params["phone"])
	s.profile.Email = trimString(params["email"])
	profile := s.profile
	state := s.portalStateLocked()
	s.mu.Unlock()
	if store := s.portalStore(); store != nil {
		if err := store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.updateMatrixProfile(ctx, profile); err != nil {
		return nil, internalError(err)
	}
	if err := s.updateOwnerMemberProfiles(ctx, profile); err != nil {
		return nil, internalError(err)
	}
	return profile, nil
}

func (s *Service) updateMatrixProfile(ctx context.Context, profile ownerProfile) error {
	s.mu.Lock()
	issuer := s.sessions
	s.mu.Unlock()
	updater, ok := issuer.(MatrixProfileUpdater)
	if !ok || updater == nil {
		return nil
	}
	return updater.UpdateMatrixProfile(ctx, profile.UserID, profile.DisplayName, profile.AvatarURL)
}

func (s *Service) updateOwnerMemberProfiles(ctx context.Context, profile ownerProfile) error {
	members, err := s.membersForUser(ctx, profile.UserID)
	if err != nil {
		return err
	}
	for _, member := range members {
		member.DisplayName = profile.DisplayName
		member.AvatarURL = profile.AvatarURL
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
		if s.transport != nil {
			if err := s.transport.UpdateMemberProfile(ctx, UpdateMemberProfileRequest{
				RoomID:      member.RoomID,
				UserMXID:    profile.UserID,
				DisplayName: profile.DisplayName,
				AvatarURL:   profile.AvatarURL,
				Timestamp:   time.Now().UTC(),
			}); err != nil {
				continue
			}
		}
	}
	return nil
}

func (s *Service) membersForUser(ctx context.Context, userID string) ([]memberRecord, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, nil
	}
	if store := s.memberStore(); store != nil {
		return store.ListMembersForUser(ctx, userID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.UserID == userID && !memberHidden(member.Membership) {
			filtered = append(filtered, member)
		}
	}
	return filtered, nil
}

func (s *Service) syncBootstrap(ctx context.Context) (any, *apiError) {
	contacts, err := s.listContacts(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	s.mu.Lock()
	userID := s.ownerMXID
	agentRoomID := s.agentRoomID
	systemRoomID := s.systemRoomID
	s.mu.Unlock()
	var groups []groupRecord
	var channels []channel
	var visibleGroups []groupRecord
	var visibleChannels []channel
	if s.store != nil {
		visibleGroups, err = s.groupStore().ListJoinedGroupsForUser(ctx, userID)
		if err != nil {
			return nil, internalError(err)
		}
		visibleChannels, err = s.channelStore().ListJoinedChannelsForUser(ctx, userID)
		if err != nil {
			return nil, internalError(err)
		}
		groups = visibleGroups
		channels = visibleChannels
	} else {
		groups, err = s.listGroups(ctx)
		if err != nil {
			return nil, internalError(err)
		}
		channels, err = s.listChannels(ctx)
		if err != nil {
			return nil, internalError(err)
		}
		visibleGroups, err = s.joinedGroupsForOwner(ctx, groups)
		if err != nil {
			return nil, internalError(err)
		}
		visibleChannels, err = s.joinedChannelsForOwner(ctx, channels)
		if err != nil {
			return nil, internalError(err)
		}
	}
	members, err := s.membersForUser(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if s.store != nil && hasPendingGroupInvite(members) {
		groups, err = s.listGroups(ctx)
		if err != nil {
			return nil, internalError(err)
		}
	}
	if s.store != nil && hasPendingChannelInvite(members) {
		channels, err = s.listChannels(ctx)
		if err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{
		"synced_at":      time.Now().UTC().Format(time.RFC3339Nano),
		"user":           map[string]any{"user_id": userID},
		"agent_room_id":  agentRoomID,
		"system_room_id": systemRoomID,
		"contacts":       contacts,
		"groups":         visibleGroups,
		"channels":       visibleChannels,
		"pending": map[string]any{
			"friend_requests": pendingFriendRequestsFromContacts(contacts),
			"group_invites":   pendingGroupInvitesFromMembers(members, groups),
			"channel_notices": pendingChannelInvitesFromMembers(members, channels),
		},
	}, nil
}

func hasPendingGroupInvite(members []memberRecord) bool {
	for _, member := range members {
		if member.ChannelID == "" && strings.EqualFold(strings.TrimSpace(member.Membership), "invite") {
			return true
		}
	}
	return false
}

func hasPendingChannelInvite(members []memberRecord) bool {
	for _, member := range members {
		if member.ChannelID != "" && strings.EqualFold(strings.TrimSpace(member.Membership), "invite") {
			return true
		}
	}
	return false
}

func pendingFriendRequestsFromContacts(contacts []contactRecord) []map[string]any {
	pending := make([]map[string]any, 0)
	for _, contact := range contacts {
		if !strings.EqualFold(strings.TrimSpace(contact.Status), "pending_inbound") {
			continue
		}
		id := fallbackString(contact.RoomID, contact.PeerMXID)
		title := fallbackString(contact.DisplayName, contact.PeerMXID)
		pending = append(pending, map[string]any{
			"id":     id,
			"title":  title,
			"remark": contact.Remark,
		})
	}
	return pending
}

func pendingGroupInvitesFromMembers(members []memberRecord, groups []groupRecord) []map[string]any {
	groupByRoom := make(map[string]groupRecord, len(groups))
	for _, group := range groups {
		if strings.TrimSpace(group.RoomID) != "" {
			groupByRoom[group.RoomID] = group
		}
	}
	pending := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "invite") || member.ChannelID != "" {
			continue
		}
		roomID := strings.TrimSpace(member.RoomID)
		if roomID == "" || seen[roomID] {
			continue
		}
		seen[roomID] = true
		group := groupByRoom[roomID]
		title := fallbackString(group.Name, roomID)
		pending = append(pending, pendingItem(roomID, title, member.JoinedAt))
	}
	return pending
}

func pendingChannelInvitesFromMembers(members []memberRecord, channels []channel) []map[string]any {
	channelByID := make(map[string]channel, len(channels))
	channelByRoom := make(map[string]channel, len(channels))
	for _, ch := range channels {
		if strings.TrimSpace(ch.ChannelID) != "" {
			channelByID[ch.ChannelID] = ch
		}
		if strings.TrimSpace(ch.RoomID) != "" {
			channelByRoom[ch.RoomID] = ch
		}
	}
	pending := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "invite") || member.ChannelID == "" {
			continue
		}
		ch, ok := channelByID[member.ChannelID]
		if !ok {
			ch = channelByRoom[member.RoomID]
		}
		id := fallbackString(ch.RoomID, member.RoomID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		title := fallbackString(ch.Name, fallbackString(member.ChannelID, id))
		pending = append(pending, pendingItem(id, title, member.JoinedAt))
	}
	return pending
}

func pendingItem(id, title string, ts int64) map[string]any {
	item := map[string]any{
		"id":    id,
		"title": title,
	}
	if ts > 0 {
		item["created_at"] = time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
	}
	return item
}

type readMarkerStore interface {
	SaveReadMarker(ctx context.Context, marker readMarker) error
}

func (s *Service) readMarkerStore() readMarkerStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) updateReadMarker(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	eventID := trimString(params["event_id"])
	if roomID == "" || eventID == "" {
		return nil, badRequest("room_id and event_id are required")
	}
	marker := readMarker{
		RoomID:         roomID,
		EventID:        eventID,
		OriginServerTS: int64Param(params["origin_server_ts"]),
	}
	s.mu.Lock()
	s.readMarkers[roomID] = marker
	s.mu.Unlock()
	if store := s.readMarkerStore(); store != nil {
		if err := store.SaveReadMarker(ctx, marker); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}
