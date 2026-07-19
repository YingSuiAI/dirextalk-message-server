package p2p

import (
	"context"
	"errors"
	"strings"
	"time"
)

type serviceProfilePort struct{ service *Service }

func (p serviceProfilePort) Current() ownerProfile {
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	return p.service.profile
}

func (p serviceProfilePort) Commit(ctx context.Context, mutate func(*ownerProfile)) (ownerProfile, error) {
	p.service.mu.Lock()
	mutate(&p.service.profile)
	profile := p.service.profile
	state := p.service.portalStateLocked()
	p.service.mu.Unlock()
	if store := p.service.portalStore(); store != nil {
		if err := store.SavePortal(ctx, state); err != nil {
			return profile, err
		}
	}
	return profile, nil
}

func (p serviceProfilePort) UpdateMatrix(ctx context.Context, profile ownerProfile) error {
	p.service.mu.Lock()
	issuer := p.service.sessions
	p.service.mu.Unlock()
	updater, ok := issuer.(MatrixProfileUpdater)
	if !ok || updater == nil {
		return nil
	}
	return updater.UpdateMatrixProfile(ctx, profile.UserID, profile.DisplayName, profile.AvatarURL)
}

func (p serviceProfilePort) UpdateMembers(ctx context.Context, profile ownerProfile) error {
	members, err := p.service.membersForUser(ctx, profile.UserID)
	if err != nil {
		return err
	}
	for _, member := range members {
		member.DisplayName = profile.DisplayName
		member.AvatarURL = profile.AvatarURL
		if err := p.service.saveMember(ctx, member); err != nil {
			return err
		}
		if p.service.transport != nil {
			if err := p.service.transport.UpdateMemberProfile(ctx, UpdateMemberProfileRequest{
				RoomID: member.RoomID, UserMXID: profile.UserID,
				DisplayName: profile.DisplayName, AvatarURL: profile.AvatarURL,
				Timestamp: time.Now().UTC(),
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
	store := s.memberStore()
	if store == nil {
		return nil, errors.New("member store is not configured")
	}
	return store.ListMembersForUser(ctx, userID)
}

func (s *Service) syncBootstrap(ctx context.Context) (any, *apiError) {
	contacts, err := s.listContacts(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	readMarkerStore := s.readMarkerStore()
	if readMarkerStore == nil {
		return nil, internalError(errors.New("read marker store is not configured"))
	}
	readMarkers, err := readMarkerStore.ListReadMarkers(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	s.mu.Lock()
	userID := s.ownerMXID
	agentRoomID := s.agentRoomID
	systemRoomID := s.systemRoomID
	s.mu.Unlock()
	visibleGroups, err := s.groupsModule.ListJoined(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	groups := visibleGroups
	visibleChannels, err := s.channelsModule.ListJoined(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	channels := visibleChannels
	members, err := s.membersForUser(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if hasPendingGroupInvite(members) {
		groups, err = s.listGroups(ctx)
		if err != nil {
			return nil, internalError(err)
		}
	}
	if hasPendingChannelInvite(members) {
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
		"read_markers":   readMarkers,
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
		status := strings.ToLower(strings.TrimSpace(contact.Status))
		if status != "pending_inbound" && status != "joining" {
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
	GetReadMarker(ctx context.Context, roomID string) (readMarker, bool, error)
	ListReadMarkers(ctx context.Context) ([]readMarker, error)
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
	store := s.readMarkerStore()
	if store == nil {
		return nil, internalError(errors.New("read marker store is not configured"))
	}
	if err := store.SaveReadMarker(ctx, marker); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok"}, nil
}
