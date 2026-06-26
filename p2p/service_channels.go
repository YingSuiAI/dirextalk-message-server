package p2p

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
)

func (s *Service) channelResult(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	if channelID == "" {
		channelID = "ch_" + randomToken("channel")
	}
	roomID := trimString(params["room_id"])
	existingRoomID := roomID != ""
	channelType := fallbackString(trimString(params["channel_type"]), "chat")
	ch := channel{
		ChannelID:        channelID,
		RoomID:           roomID,
		Name:             fallbackString(trimString(params["name"]), channelID),
		Description:      trimString(params["description"]),
		AvatarURL:        trimString(params["avatar_url"]),
		Visibility:       fallbackString(trimString(params["visibility"]), "public"),
		JoinPolicy:       fallbackString(trimString(params["join_policy"]), "open"),
		ChannelType:      channelType,
		CommentsEnabled:  true,
		MemberCount:      1,
		PendingJoinCount: 0,
		IsOwned:          true,
		Role:             "owner",
		MemberStatus:     "join",
	}
	if _, ok := params["comments_enabled"]; ok {
		ch.CommentsEnabled = boolParam(params["comments_enabled"])
	}
	if roomID == "" {
		var apiErr *apiError
		initialState := []RoomStateEvent{
			channelStateEvent(ch, false),
		}
		if historyVisibilityState, ok := channelHistoryVisibilityStateEvent(channelType); ok {
			initialState = append([]RoomStateEvent{historyVisibilityState}, initialState...)
		}
		roomID, apiErr = s.ensureProductRoom(ctx, "channel", CreateRoomRequest{
			Name:         fallbackString(trimString(params["name"]), channelID),
			Topic:        trimString(params["description"]),
			Visibility:   fallbackString(trimString(params["visibility"]), "public"),
			RoomType:     DirexioRoomTypeChannel,
			IsDirect:     false,
			InitialState: initialState,
		})
		if apiErr != nil {
			return nil, apiErr
		}
	}
	ch.RoomID = roomID
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.saveOwnerMember(ctx, ch.RoomID, ch.ChannelID); err != nil {
		return nil, internalError(err)
	}
	if existingRoomID {
		if err := s.publishChannelHistoryVisibilityState(ctx, ch); err != nil {
			return nil, internalError(err)
		}
	}
	return ch, nil
}

func (s *Service) channelUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if name := trimString(params["name"]); name != "" {
		ch.Name = name
	}
	if _, ok := params["description"]; ok {
		ch.Description = trimString(params["description"])
	}
	if _, ok := params["avatar_url"]; ok {
		ch.AvatarURL = trimString(params["avatar_url"])
	}
	if visibility := trimString(params["visibility"]); visibility != "" {
		ch.Visibility = visibility
	}
	if joinPolicy := trimString(params["join_policy"]); joinPolicy != "" {
		ch.JoinPolicy = joinPolicy
	}
	if _, ok := params["comments_enabled"]; ok {
		ch.CommentsEnabled = boolParam(params["comments_enabled"])
	}
	if _, ok := params["muted"]; ok {
		ch.Muted = boolParam(params["muted"])
	}
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.publishChannelState(ctx, ch, false); err != nil {
		return nil, internalError(err)
	}
	return ch, nil
}

func (s *Service) channelList(ctx context.Context) any {
	channels, err := s.listChannels(ctx)
	if err != nil {
		return map[string]any{"channels": []channel{}}
	}
	enriched, err := s.joinedChannelsForOwner(ctx, channels)
	if err != nil {
		return map[string]any{"channels": []channel{}}
	}
	return map[string]any{"channels": enriched}
}

func (s *Service) channelPublicGet(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		if remote, fetched, apiErr := s.remotePublicChannelGet(ctx, channelID, roomID, params); apiErr != nil {
			return nil, apiErr
		} else if fetched {
			ch = remote
			ok = true
		}
	}
	if !ok {
		if roomID != "" {
			s.mu.Lock()
			transport := s.transport
			s.mu.Unlock()
			if transport != nil {
				fetched, found, fetchErr := transport.GetRoomChannel(ctx, roomID)
				if fetchErr != nil {
					if roomServer := domainFromMatrixID(roomID, "!"); roomServer != "" && roomServer != s.serverName {
						return nil, statusError(404, "channel not found")
					}
					return nil, internalError(fetchErr)
				}
				if found {
					ch = fetched
					ok = true
					if err := s.saveChannel(ctx, ch); err != nil {
						return nil, internalError(err)
					}
				}
			}
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
	}
	if !strings.EqualFold(ch.Visibility, "public") {
		return nil, statusError(404, "channel not found")
	}
	ch, err = s.channelWithCurrentCounts(ctx, ch)
	if err != nil {
		return nil, internalError(err)
	}
	return ch, nil
}

func (s *Service) channelPublicSearch(ctx context.Context, params map[string]any) (any, *apiError) {
	rawQuery := trimString(params["q"])
	query := strings.ToLower(rawQuery)
	limit := int(int64Param(params["limit"]))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if matrixRoomIDQuery(rawQuery) {
		ch, apiErr := s.channelPublicGet(ctx, map[string]any{
			"room_id":              rawQuery,
			"remote_node_base_url": remoteNodeBaseURLParam(params),
		})
		if apiErr != nil {
			if apiErr.Status == 404 {
				return map[string]any{"channels": []channel{}, "results": []channel{}}, nil
			}
			return nil, apiErr
		}
		channelResult, ok := ch.(channel)
		if !ok {
			return nil, internalError(fmt.Errorf("public get returned %T", ch))
		}
		return map[string]any{"channels": []channel{channelResult}, "results": []channel{channelResult}}, nil
	}
	channels, err := s.listChannels(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	results := make([]channel, 0, len(channels))
	for _, ch := range channels {
		if !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(ch.ChannelID+" "+ch.RoomID+" "+ch.Name+" "+ch.Description), query) {
			continue
		}
		ch, err = s.channelWithCurrentCounts(ctx, ch)
		if err != nil {
			return nil, internalError(err)
		}
		results = append(results, ch)
		if len(results) >= limit {
			break
		}
	}
	return map[string]any{"channels": results, "results": results}, nil
}

func (s *Service) userPublicChannels(ctx context.Context, params map[string]any) (any, *apiError) {
	userID := fallbackString(trimString(params["user_id"]), trimString(params["user_mxid"]))
	userID = fallbackString(userID, trimString(params["mxid"]))
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	if remoteNodeBaseURLParam(params) != "" {
		ownerNode := domainFromMXID(userID)
		if ownerNode == "" {
			return nil, badRequest("valid user_id is required")
		}
		var remote struct {
			UserID   string    `json:"user_id"`
			Channels []channel `json:"channels"`
			Results  []channel `json:"results"`
		}
		status, err := s.remotePublicAction(ctx, ownerNode, "users.public_channels", params, &remote)
		if err != nil {
			if status != 0 && status != http.StatusBadGateway {
				return nil, statusError(status, err.Error())
			}
			return nil, statusError(http.StatusBadGateway, err.Error())
		}
		if status != http.StatusOK {
			return nil, statusError(status, "target node public channels lookup failed")
		}
		channels := remote.Channels
		if channels == nil {
			channels = remote.Results
		}
		return map[string]any{"user_id": fallbackString(remote.UserID, userID), "channels": channels, "results": channels}, nil
	}
	channels, err := s.listChannels(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	members, err := s.membersForUser(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	ownedChannelIDs := map[string]bool{}
	ownedRoomIDs := map[string]bool{}
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		if !strings.EqualFold(member.Role, "owner") {
			continue
		}
		if member.ChannelID != "" {
			ownedChannelIDs[member.ChannelID] = true
		}
		if member.RoomID != "" {
			ownedRoomIDs[member.RoomID] = true
		}
	}
	publicChannels := make([]channel, 0, len(channels))
	for _, ch := range channels {
		if !ownedChannelIDs[ch.ChannelID] && !ownedRoomIDs[ch.RoomID] {
			continue
		}
		if !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		ch, err = s.channelWithCurrentCounts(ctx, ch)
		if err != nil {
			return nil, internalError(err)
		}
		publicChannels = append(publicChannels, ch)
	}
	sort.SliceStable(publicChannels, func(i, j int) bool {
		if publicChannels[i].Name == publicChannels[j].Name {
			return publicChannels[i].ChannelID < publicChannels[j].ChannelID
		}
		return publicChannels[i].Name < publicChannels[j].Name
	})
	return map[string]any{"user_id": userID, "channels": publicChannels}, nil
}

func (s *Service) channelPolicyMutation(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	ch.Muted = action == "channels.mute"
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.setProductMemberMute(ctx, ch.RoomID, ch.ChannelID, ch.Muted); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "channel_id": ch.ChannelID, "room_id": ch.RoomID, "muted": ch.Muted, "channel": ch}, nil
}

func (s *Service) saveChannel(ctx context.Context, ch channel) error {
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	return s.saveConversation(ctx, conversationFromChannel(ch))
}

func channelStateEvent(ch channel, dissolved bool) RoomStateEvent {
	return roomProfileForChannel(ch, dissolved)
}

func (s *Service) publishChannelState(ctx context.Context, ch channel, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(ch.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     ch.RoomID,
		SenderMXID: senderMXID,
		Event:      channelStateEvent(ch, dissolved),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) publishChannelHistoryVisibilityState(ctx context.Context, ch channel) error {
	if s.transport == nil || strings.TrimSpace(ch.RoomID) == "" {
		return nil
	}
	if historyVisibilityState, ok := channelHistoryVisibilityStateEvent(ch.ChannelType); ok {
		s.mu.Lock()
		senderMXID := s.ownerMXID
		s.mu.Unlock()
		return s.transport.SendStateEvent(ctx, SendStateEventRequest{
			RoomID:     ch.RoomID,
			SenderMXID: senderMXID,
			Event:      historyVisibilityState,
		})
	}
	return nil
}

func (s *Service) publishJoinRequestState(ctx context.Context, roomID, userID, status, reason string) *apiError {
	if s.transport == nil || strings.TrimSpace(roomID) == "" || strings.TrimSpace(userID) == "" {
		return nil
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "pending", "approved", "rejected":
	default:
		return badRequest("invalid join request status")
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	content := map[string]any{
		"status":     status,
		"room_id":    roomID,
		"user_id":    userID,
		"created_at": now,
		"updated_at": now,
	}
	if strings.TrimSpace(reason) != "" {
		content["reason"] = strings.TrimSpace(reason)
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     roomID,
		SenderMXID: senderMXID,
		Event: RoomStateEvent{
			Type:     DirexioJoinRequestEventType,
			StateKey: productpolicy.UserStateKey(userID),
			Content:  content,
		},
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) publishMemberPolicyState(ctx context.Context, member memberRecord) *apiError {
	if s.transport == nil || strings.TrimSpace(member.RoomID) == "" || strings.TrimSpace(member.UserID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     member.RoomID,
		SenderMXID: senderMXID,
		Event: RoomStateEvent{
			Type:     DirexioMemberPolicyEventType,
			StateKey: productpolicy.UserStateKey(member.UserID),
			Content: map[string]any{
				"role":    fallbackString(member.Role, "member"),
				"muted":   member.Muted,
				"user_id": member.UserID,
				"room_id": member.RoomID,
			},
		},
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) deleteChannel(ctx context.Context, channelID string) error {
	s.mu.Lock()
	delete(s.channels, channelID)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.DeleteChannel(ctx, channelID)
	}
	return nil
}

func (s *Service) dissolveChannel(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if apiErr := s.requireOwnerMember(ctx, ch.RoomID); apiErr != nil {
		return nil, apiErr
	}
	if err := s.publishChannelState(ctx, ch, true); err != nil {
		return nil, internalError(err)
	}
	if err := s.deleteChannel(ctx, ch.ChannelID); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "channel": ch}, nil
}

func (s *Service) channelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error) {
	channels, err := s.listChannels(ctx)
	if err != nil {
		return channel{}, false, err
	}
	for _, ch := range channels {
		if channelID != "" && ch.ChannelID == channelID {
			return ch, true, nil
		}
		if roomID != "" && ch.RoomID == roomID {
			return ch, true, nil
		}
	}
	return channel{}, false, nil
}

func (s *Service) channelSnapshot(ctx context.Context, channelID string) channel {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return channel{}
	}
	if s.store != nil {
		channels, err := s.store.ListChannels(ctx)
		if err == nil {
			for _, ch := range channels {
				if ch.ChannelID == channelID {
					return ch
				}
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.channels[channelID]
}

func (s *Service) channelWithCurrentCounts(ctx context.Context, ch channel) (channel, error) {
	if strings.TrimSpace(ch.ChannelID) == "" {
		return ch, nil
	}
	var members []memberRecord
	var err error
	if s.store != nil {
		members, err = s.store.ListMembers(ctx, "", ch.ChannelID)
		if err != nil {
			return channel{}, err
		}
	} else {
		s.mu.Lock()
		members = make([]memberRecord, 0, len(s.members))
		for _, member := range s.members {
			if member.ChannelID == ch.ChannelID {
				members = append(members, member)
			}
		}
		s.mu.Unlock()
	}
	if len(members) == 0 {
		return ch, nil
	}
	memberCount, pendingJoinCount := memberCounts(members)
	if ch.MemberCount == memberCount && ch.PendingJoinCount == pendingJoinCount {
		return ch, nil
	}
	ch.MemberCount = memberCount
	ch.PendingJoinCount = pendingJoinCount
	if err := s.saveChannel(ctx, ch); err != nil {
		return channel{}, err
	}
	return ch, nil
}

func (s *Service) refreshStoredChannelCounts(ctx context.Context, channelID string) error {
	channelID = strings.TrimSpace(channelID)
	if s.store == nil || channelID == "" {
		return nil
	}
	channels, err := s.store.ListChannels(ctx)
	if err != nil {
		return err
	}
	var target channel
	for _, ch := range channels {
		if ch.ChannelID == channelID {
			target = ch
			break
		}
	}
	if target.ChannelID == "" {
		return nil
	}
	members, err := s.store.ListMembers(ctx, "", channelID)
	if err != nil {
		return err
	}
	target.MemberCount, target.PendingJoinCount = memberCounts(members)
	return s.store.UpsertChannel(ctx, target)
}

func (s *Service) refreshStoredGroupCounts(ctx context.Context, roomID string) error {
	roomID = strings.TrimSpace(roomID)
	if s.store == nil || roomID == "" {
		return nil
	}
	groups, err := s.store.ListGroups(ctx)
	if err != nil {
		return err
	}
	var target groupRecord
	for _, group := range groups {
		if group.RoomID == roomID {
			target = group
			break
		}
	}
	if target.RoomID == "" {
		return nil
	}
	members, err := s.store.ListMembers(ctx, roomID, "")
	if err != nil {
		return err
	}
	target.MemberCount, _ = memberCounts(members)
	return s.store.UpsertGroup(ctx, target)
}

func (s *Service) refreshChannelCountsLocked(channelID string) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return
	}
	ch, ok := s.channels[channelID]
	if !ok {
		return
	}
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.ChannelID == channelID {
			members = append(members, member)
		}
	}
	ch.MemberCount, ch.PendingJoinCount = memberCounts(members)
	s.channels[channelID] = ch
}

func (s *Service) refreshGroupCountsLocked(roomID string) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return
	}
	group, ok := s.groups[roomID]
	if !ok {
		return
	}
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.ChannelID == "" && member.RoomID == roomID {
			members = append(members, member)
		}
	}
	group.MemberCount, _ = memberCounts(members)
	s.groups[roomID] = group
}

func memberCounts(members []memberRecord) (int64, int64) {
	var joined, pending int64
	for _, member := range members {
		switch strings.ToLower(strings.TrimSpace(member.Membership)) {
		case "join", "joined":
			joined++
		case "pending":
			pending++
		}
	}
	return joined, pending
}
