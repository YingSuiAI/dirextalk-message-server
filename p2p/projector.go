package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
)

func (s *Service) ProjectOutputEvent(ctx context.Context, output roomserverAPI.OutputEvent) error {
	if output.Type == roomserverAPI.OutputTypeRedactedEvent && output.RedactedEvent != nil {
		return s.removeProjectedEvent(ctx, output.RedactedEvent.RedactedEventID)
	}
	if output.Type == roomserverAPI.OutputTypeNewInviteEvent && output.NewInviteEvent != nil {
		return s.ProjectRoomEvent(ctx, output.NewInviteEvent.Event)
	}
	if output.Type != roomserverAPI.OutputTypeNewRoomEvent || output.NewRoomEvent == nil {
		return nil
	}
	return s.ProjectRoomEvent(ctx, output.NewRoomEvent.Event)
}

func (s *Service) ProjectRoomEvent(ctx context.Context, event *types.HeaderedEvent) error {
	if event == nil {
		return nil
	}
	switch event.Type() {
	case "m.room.message":
		return s.projectMessage(ctx, event)
	case "m.reaction":
		return s.projectReaction(ctx, event)
	case "m.room.member":
		if event.StateKey() != nil {
			return s.projectMember(ctx, event)
		}
	case DirexioRoomProfileEventType:
		if event.StateKey() != nil {
			return s.projectRoomProfileState(ctx, event)
		}
	case DirexioMemberPolicyEventType:
		if event.StateKey() != nil {
			return s.projectMemberPolicyState(ctx, event)
		}
	case DirexioJoinRequestEventType:
		if event.StateKey() != nil {
			return s.projectJoinRequestState(ctx, event)
		}
	}
	return nil
}

func (s *Service) projectReaction(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	relatesTo, _ := content["m.relates_to"].(map[string]any)
	reactionName := trimString(relatesTo["key"])
	if reactionName == "" {
		reactionName = fallbackString(trimString(content["reaction"]), "like")
	}
	postID := trimString(content["post_id"])
	commentID := trimString(content["comment_id"])
	targetType := "post"
	targetID := postID
	if commentID != "" {
		targetType = "comment"
		targetID = commentID
	}
	if targetID == "" {
		targetID = trimString(relatesTo["event_id"])
	}
	if targetID == "" {
		return nil
	}
	record := reactionRecord{
		TargetType: targetType,
		TargetID:   targetID,
		ChannelID:  trimString(content["channel_id"]),
		PostID:     postID,
		CommentID:  commentID,
		Reaction:   reactionName,
		UserID:     string(event.SenderID()),
		Active:     true,
		CreatedAt:  eventTime(event).Format(time.RFC3339Nano),
	}
	return s.saveReaction(ctx, record)
}

//nolint:gocyclo // Member projection accepts Matrix, direct-contact, group, and channel membership state.
func (s *Service) projectMember(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = productpolicy.UserIDFromStateKey(*event.StateKey())
	}
	if override := trimString(content["user_id"]); override != "" {
		userID = override
	}
	if userID == "" {
		userID = string(event.SenderID())
	}
	roomID := event.RoomID().String()
	channelID := trimString(content["channel_id"])
	if channelID == "" {
		channelID = s.channelIDForRoom(ctx, roomID)
	}
	existing, hasExisting, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return err
	}
	if channelID == "" && hasExisting {
		channelID = existing.ChannelID
	}
	displayName := existing.DisplayName
	if _, ok := content["displayname"]; ok {
		displayName = trimString(content["displayname"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	domain := fallbackString(existing.Domain, domainFromMXID(userID))
	membership := fallbackString(trimString(content["membership"]), fallbackString(existing.Membership, "join"))
	role := existing.Role
	if nextRole := trimString(content["role"]); nextRole != "" {
		role = nextRole
	}
	role = fallbackString(role, "member")
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	joinedAt := existing.JoinedAt
	if joinedAt == 0 {
		joinedAt = eventTime(event).UnixMilli()
	}
	member := memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      userID,
		DisplayName: displayName,
		AvatarURL:   avatarURL,
		Domain:      domain,
		Membership:  membership,
		Role:        role,
		Muted:       muted,
		JoinedAt:    joinedAt,
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "invite") &&
		userID == s.ownerMXID {
		if contact, ok := s.contactRequestFromInvite(event); ok {
			return s.savePendingInboundContact(ctx, contact)
		}
		if boolParam(content["is_direct"]) {
			if contact, ok := s.directContactFromInvite(event); ok {
				return s.savePendingInboundContact(ctx, contact)
			}
			return s.savePendingInboundContact(ctx, contactRecord{
				PeerMXID:    string(event.SenderID()),
				DisplayName: displayNameFromMXID(string(event.SenderID())),
				Domain:      domainFromMXID(string(event.SenderID())),
				RoomID:      event.RoomID().String(),
				Status:      "pending_inbound",
			})
		}
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	_ = s.appendP2PEvent(ctx, p2pEvent{
		Type:    "room.member.projected",
		RoomID:  member.RoomID,
		Payload: map[string]any{"user_id": member.UserID, "membership": member.Membership, "channel_id": member.ChannelID},
	})
	if strings.EqualFold(member.Membership, "invite") &&
		!boolParam(content["is_direct"]) &&
		userID == s.ownerMXID {
		if updated, ok, err := s.projectProductInvite(ctx, event, member); err != nil {
			return err
		} else if ok {
			return s.saveMember(ctx, updated)
		}
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "join") &&
		userID != "" &&
		userID != s.ownerMXID {
		if err := s.projectDirectContactMember(ctx, member, content); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) projectDirectContactMember(ctx context.Context, member memberRecord, content map[string]any) error {
	contact, ok, err := s.lookupContactByRoom(ctx, member.RoomID)
	if err != nil || !ok {
		return err
	}
	if contact.PeerMXID != "" && contact.PeerMXID != member.UserID {
		return nil
	}
	changed := false
	if contact.PeerMXID == "" {
		contact.PeerMXID = member.UserID
		changed = true
	}
	if member.DisplayName != "" && contact.DisplayName != member.DisplayName {
		contact.DisplayName = member.DisplayName
		changed = true
	}
	if _, hasAvatar := content["avatar_url"]; hasAvatar && contact.AvatarURL != member.AvatarURL {
		contact.AvatarURL = member.AvatarURL
		changed = true
	}
	if contact.Domain == "" {
		contact.Domain = domainFromMXID(member.UserID)
		changed = true
	}
	if strings.EqualFold(contact.Status, "pending_outbound") {
		contact.Status = "accepted"
		contact.Remark = ""
		changed = true
	}
	if !changed {
		return nil
	}
	return s.saveContact(ctx, contact)
}

func (s *Service) projectProductInvite(ctx context.Context, event *types.HeaderedEvent, member memberRecord) (memberRecord, bool, error) {
	content, ok := s.productInviteFromInvite(event)
	if !ok {
		return member, false, nil
	}
	switch trimString(content["room_type"]) {
	case DirexioRoomTypeGroup:
		group := groupRecord{
			RoomID:       fallbackString(trimString(content["room_id"]), event.RoomID().String()),
			Name:         fallbackString(trimString(content["name"]), event.RoomID().String()),
			Topic:        trimString(content["topic"]),
			AvatarURL:    trimString(content["avatar_url"]),
			InvitePolicy: fallbackString(trimString(content["invite_policy"]), "member"),
		}
		if err := s.saveGroup(ctx, group); err != nil {
			return member, false, err
		}
		member.RoomID = group.RoomID
		member.ChannelID = ""
		return member, true, nil
	case DirexioRoomTypeChannel:
		ch := channel{
			ChannelID:       fallbackString(trimString(content["channel_id"]), event.RoomID().String()),
			RoomID:          fallbackString(trimString(content["room_id"]), event.RoomID().String()),
			Name:            fallbackString(trimString(content["name"]), event.RoomID().String()),
			Description:     trimString(content["description"]),
			AvatarURL:       trimString(content["avatar_url"]),
			Visibility:      fallbackString(trimString(content["visibility"]), "public"),
			JoinPolicy:      fallbackString(trimString(content["join_policy"]), "invite"),
			ChannelType:     fallbackString(trimString(content["channel_type"]), "chat"),
			CommentsEnabled: boolParam(content["comments_enabled"]),
			MemberStatus:    "invite",
			Role:            fallbackString(member.Role, "member"),
		}
		if err := s.saveChannel(ctx, ch); err != nil {
			return member, false, err
		}
		member.RoomID = ch.RoomID
		member.ChannelID = ch.ChannelID
		return member, true, nil
	default:
		return member, false, nil
	}
}

func (s *Service) productInviteFromInvite(event *types.HeaderedEvent) (map[string]any, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type    string         `json:"type"`
			Content map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return nil, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return nil, false
	}
	for _, state := range unsigned.InviteRoomState {
		if state.Type == DirexioRoomProfileEventType {
			switch trimString(state.Content["room_type"]) {
			case DirexioRoomTypeGroup, DirexioRoomTypeChannel:
				return state.Content, true
			}
		}
	}
	return nil, false
}

func (s *Service) contactRequestFromInvite(event *types.HeaderedEvent) (contactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type    string         `json:"type"`
			Sender  string         `json:"sender"`
			Content map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return contactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return contactRecord{}, false
	}
	for _, state := range unsigned.InviteRoomState {
		if state.Type == DirexioRoomProfileEventType && trimString(state.Content["room_type"]) == DirexioRoomTypeDirect {
			if contact, ok := s.contactRequestFromContent(event.RoomID().String(), state.Sender, state.Content); ok {
				return contact, true
			}
		}
	}
	return contactRecord{}, false
}

func (s *Service) directContactFromInvite(event *types.HeaderedEvent) (contactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type     string         `json:"type"`
			Sender   string         `json:"sender"`
			StateKey string         `json:"state_key"`
			Content  map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return contactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return contactRecord{}, false
	}
	requester := string(event.SenderID())
	if requester == "" || requester == s.ownerMXID {
		return contactRecord{}, false
	}
	displayName := ""
	avatarURL := ""
	for _, state := range unsigned.InviteRoomState {
		if state.Type != "m.room.member" {
			continue
		}
		if state.StateKey != requester && state.Sender != requester {
			continue
		}
		if membership := trimString(state.Content["membership"]); membership != "" && !strings.EqualFold(membership, "join") {
			continue
		}
		displayName = trimString(state.Content["displayname"])
		avatarURL = trimString(state.Content["avatar_url"])
		break
	}
	return contactRecord{
		PeerMXID:    requester,
		DisplayName: fallbackString(displayName, displayNameFromMXID(requester)),
		AvatarURL:   avatarURL,
		Domain:      domainFromMXID(requester),
		RoomID:      event.RoomID().String(),
		Status:      "pending_inbound",
	}, true
}

func (s *Service) contactRequestFromContent(roomID, sender string, content map[string]any) (contactRecord, bool) {
	requester := trimString(content["requester_mxid"])
	if requester == "" {
		requester = strings.TrimSpace(sender)
	}
	if requester == "" || requester == s.ownerMXID {
		return contactRecord{}, false
	}
	target := trimString(content["target_mxid"])
	if target != "" && target != s.ownerMXID {
		return contactRecord{}, false
	}
	return contactRecord{
		PeerMXID:    requester,
		DisplayName: fallbackString(trimString(content["display_name"]), displayNameFromMXID(requester)),
		AvatarURL:   trimString(content["avatar_url"]),
		Domain:      fallbackString(trimString(content["domain"]), domainFromMXID(requester)),
		RoomID:      roomID,
		Status:      "pending_inbound",
		Remark:      contactRequestRemark(content),
	}, true
}

func (s *Service) savePendingInboundContact(ctx context.Context, contact contactRecord) error {
	contact.Status = "pending_inbound"
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return err
	}
	for _, existing := range contacts {
		if existing.PeerMXID != contact.PeerMXID {
			continue
		}
		if contactAccepted(existing.Status) {
			return nil
		}
		if !contactDeleted(existing.Status) && !strings.EqualFold(strings.TrimSpace(existing.Status), "rejected") && !strings.EqualFold(strings.TrimSpace(existing.Status), "reject") {
			return nil
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:   "contact.requested",
		RoomID: contact.RoomID,
		Payload: map[string]any{
			"room_id":      contact.RoomID,
			"peer_mxid":    contact.PeerMXID,
			"display_name": contact.DisplayName,
			"avatar_url":   contact.AvatarURL,
			"domain":       contact.Domain,
			"status":       contact.Status,
			"remark":       contact.Remark,
		},
	})
}

func (s *Service) channelIDForRoom(ctx context.Context, roomID string) string {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return ""
	}
	s.mu.Lock()
	for _, ch := range s.channels {
		if ch.RoomID == roomID {
			s.mu.Unlock()
			return ch.ChannelID
		}
	}
	s.mu.Unlock()
	if s.store != nil {
		channels, err := s.store.ListChannels(ctx)
		if err == nil {
			for _, ch := range channels {
				if ch.RoomID == roomID {
					return ch.ChannelID
				}
			}
		}
	}
	return ""
}

func (s *Service) projectMessage(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	if handled, err := s.projectAgentRoomMessage(ctx, event, content); handled || err != nil {
		return err
	}
	if !s.shouldProjectRoomMessage(ctx, event.RoomID().String(), content) {
		return nil
	}
	body := trimString(content["body"])
	msgType := fallbackString(trimString(content["client_type"]), trimString(content["msgtype"]))
	if msgType == "" {
		msgType = "text"
	}
	if err := s.projectConversationActivity(ctx, event, body, msgType); err != nil {
		return err
	}
	switch trimString(content["p2p_kind"]) {
	case "channel_post":
		return s.projectChannelPost(ctx, event, content, body, msgType)
	case "channel_comment":
		return s.projectChannelComment(ctx, event, content, body, msgType)
	default:
		return nil
	}
}

func (s *Service) projectAgentRoomMessage(ctx context.Context, event *types.HeaderedEvent, content map[string]any) (bool, error) {
	roomID := event.RoomID().String()
	if !s.isAgentRoom(roomID) {
		return false, nil
	}
	if contentHasAgentGatewayMarker(content) {
		return true, nil
	}
	body := trimString(content["body"])
	msgType := fallbackString(trimString(content["client_type"]), trimString(content["msgtype"]))
	if msgType == "" {
		msgType = "m.text"
	}
	return true, s.appendP2PEvent(ctx, p2pEvent{
		Type:    AgentRoomMessageEventType,
		RoomID:  roomID,
		EventID: event.EventID(),
		Payload: map[string]any{
			"room_id":          roomID,
			"event_id":         event.EventID(),
			"sender_mxid":      string(event.SenderID()),
			"body":             body,
			"msgtype":          msgType,
			"origin_server_ts": int64(event.OriginServerTS()),
		},
	})
}

func (s *Service) isAgentRoom(roomID string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return roomID == strings.TrimSpace(s.agentRoomID)
}

func contentHasAgentGatewayMarker(content map[string]any) bool {
	return boolParam(content[AgentGatewayContentKey]) || trimString(content[AgentGatewaySourceContentKey]) != ""
}

func (s *Service) projectConversationActivity(ctx context.Context, event *types.HeaderedEvent, body, msgType string) error {
	record, ok, err := s.getConversation(ctx, "", event.RoomID().String())
	if err != nil || !ok {
		return err
	}
	record.LastEventID = event.EventID()
	record.LastActivityAt = int64(event.OriginServerTS())
	record.LastMessage = conversationActivityPreview(body, msgType)
	record.UpdatedAt = time.Now().UTC().UnixMilli()
	return s.saveConversation(ctx, record)
}

func conversationActivityPreview(body, msgType string) string {
	body = strings.TrimSpace(body)
	if body != "" {
		return body
	}
	switch strings.ToLower(strings.TrimSpace(msgType)) {
	case "m.image", "image":
		return "图片"
	case "m.video", "video":
		return "视频"
	case "m.audio", "audio":
		return "语音"
	case "m.file", "file":
		return "文件"
	default:
		return ""
	}
}

func (s *Service) projectChannelPost(ctx context.Context, event *types.HeaderedEvent, content map[string]any, body, msgType string) error {
	postID := trimString(content["post_id"])
	if postID == "" {
		postID = "post_" + strings.TrimPrefix(event.EventID(), "$")
	}
	post := channelPostRecord{
		PostID:         postID,
		ChannelID:      trimString(content["channel_id"]),
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		AuthorMXID:     string(event.SenderID()),
		AuthorName:     trimString(content["sender_name"]),
		Body:           body,
		MessageType:    msgType,
		MediaJSON:      trimString(content["media_json"]),
		OriginServerTS: int64(event.OriginServerTS()),
		CommentCount:   0,
	}
	s.mu.Lock()
	s.posts = append(s.posts, post)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.InsertChannelPost(ctx, post)
	}
	return nil
}

func (s *Service) projectChannelComment(ctx context.Context, event *types.HeaderedEvent, content map[string]any, body, msgType string) error {
	commentID := trimString(content["comment_id"])
	if commentID == "" {
		commentID = "comment_" + strings.TrimPrefix(event.EventID(), "$")
	}
	mentionsJSON := "[]"
	if rawMentionsJSON, ok := content["mentions_json"]; ok {
		var err error
		mentionsJSON, err = jsonArrayStringParam(rawMentionsJSON)
		if err != nil {
			mentionsJSON = "[]"
		}
	} else if rawMentions, ok := content["mentions"]; ok {
		var err error
		mentionsJSON, err = jsonArrayStringParam(rawMentions)
		if err != nil {
			mentionsJSON = "[]"
		}
	}
	comment := channelCommentRecord{
		CommentID:         commentID,
		PostID:            trimString(content["post_id"]),
		ChannelID:         trimString(content["channel_id"]),
		EventID:           event.EventID(),
		AuthorMXID:        string(event.SenderID()),
		AuthorName:        trimString(content["sender_name"]),
		Body:              body,
		MessageType:       msgType,
		MediaJSON:         trimString(content["media_json"]),
		ReplyToCommentID:  trimString(content["reply_to_comment_id"]),
		ReplyToAuthorMXID: trimString(content["reply_to_author_mxid"]),
		MentionsJSON:      mentionsJSON,
		OriginServerTS:    int64(event.OriginServerTS()),
		ReactionCount:     0,
		ReactedByMe:       false,
	}
	s.mu.Lock()
	s.comments = append(s.comments, comment)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.InsertChannelComment(ctx, comment)
	}
	return nil
}

func (s *Service) projectRoomProfileState(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	kind, _ := conversationKindFromContent(content)
	if kind == "" {
		return nil
	}
	if kind == conversationKindDirect {
		if err := s.deleteGroup(ctx, event.RoomID().String()); err != nil {
			return err
		}
	}
	if err := s.projectConversationProfile(ctx, event, kind, content); err != nil {
		return err
	}
	switch kind {
	case conversationKindChannel:
		return s.projectChannelProfileContent(ctx, event, content)
	case conversationKindGroup:
		return s.projectGroupProfileContent(ctx, event, content)
	case conversationKindDirect:
		return s.projectDirectProfileContent(ctx, event, content)
	default:
		return nil
	}
}

func (s *Service) projectDirectProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	roomID := event.RoomID().String()
	if err := s.deleteGroup(ctx, roomID); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "profile.changed",
		RoomID:  roomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeDirect, "dissolved": boolParam(content["dissolved"])},
	})
}

func (s *Service) projectChannelProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	channelID := trimString(content["channel_id"])
	if channelID == "" {
		if existing, ok, _ := s.channelByIDOrRoom(ctx, "", event.RoomID().String()); ok {
			channelID = existing.ChannelID
		}
	}
	if channelID == "" {
		channelID = event.RoomID().String()
	}
	existing, _, _ := s.channelByIDOrRoom(ctx, channelID, event.RoomID().String())
	if boolParam(content["dissolved"]) {
		return s.deleteChannel(ctx, channelID)
	}
	channelType := fallbackString(trimString(content["channel_type"]), fallbackString(existing.ChannelType, "chat"))
	commentsEnabled := existing.CommentsEnabled
	if _, ok := content["comments_enabled"]; ok {
		commentsEnabled = boolParam(content["comments_enabled"])
	}
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	description := existing.Description
	if _, ok := content["description"]; ok {
		description = trimString(content["description"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	ch := channel{
		ChannelID:        channelID,
		RoomID:           event.RoomID().String(),
		Name:             fallbackString(trimString(content["name"]), fallbackString(existing.Name, channelID)),
		Description:      description,
		AvatarURL:        avatarURL,
		Visibility:       fallbackString(trimString(content["visibility"]), fallbackString(existing.Visibility, "private")),
		JoinPolicy:       fallbackString(trimString(content["join_policy"]), fallbackString(existing.JoinPolicy, "invite")),
		ChannelType:      channelType,
		CommentsEnabled:  commentsEnabled,
		Muted:            muted,
		MemberCount:      memberCount,
		PendingJoinCount: existing.PendingJoinCount,
	}
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "profile.changed",
		RoomID:  ch.RoomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeChannel, "channel_id": ch.ChannelID, "dissolved": false},
	})
}

func (s *Service) projectGroupProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	roomID := event.RoomID().String()
	if boolParam(content["dissolved"]) {
		return s.deleteGroup(ctx, roomID)
	}
	existing, _, _ := s.groupByRoom(ctx, roomID)
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	topic := existing.Topic
	if _, ok := content["topic"]; ok {
		topic = trimString(content["topic"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	group := groupRecord{
		RoomID:       roomID,
		Name:         fallbackString(trimString(content["name"]), fallbackString(existing.Name, roomID)),
		Topic:        topic,
		AvatarURL:    avatarURL,
		MemberCount:  memberCount,
		InvitePolicy: fallbackString(trimString(content["invite_policy"]), fallbackString(existing.InvitePolicy, "member")),
		Muted:        muted,
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "profile.changed",
		RoomID:  group.RoomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeGroup, "dissolved": false},
	})
}

func (s *Service) projectMemberPolicyState(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = productpolicy.UserIDFromStateKey(*event.StateKey())
	}
	if userID == "" {
		return nil
	}
	member, ok, err := s.lookupMember(ctx, event.RoomID().String(), userID)
	if err != nil {
		return err
	}
	if !ok {
		member = memberRecord{
			RoomID:     event.RoomID().String(),
			UserID:     userID,
			Domain:     domainFromMXID(userID),
			Membership: "join",
			Role:       "member",
			JoinedAt:   eventTime(event).UnixMilli(),
		}
	}
	if role := trimString(content["role"]); role != "" {
		member.Role = role
	}
	if _, ok := content["muted"]; ok {
		member.Muted = boolParam(content["muted"])
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "room.member_policy.projected",
		RoomID:  member.RoomID,
		Payload: map[string]any{"user_id": member.UserID, "role": member.Role, "muted": member.Muted},
	})
}

func (s *Service) projectJoinRequestState(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = *event.StateKey()
	}
	if override := trimString(content["user_id"]); override != "" {
		userID = override
	}
	if userID == "" {
		return nil
	}
	membership := ""
	switch strings.ToLower(strings.TrimSpace(trimString(content["status"]))) {
	case "pending":
		membership = "pending"
	case "approved":
		membership = "invite"
	case "rejected":
		membership = "reject"
	default:
		return nil
	}
	roomID := event.RoomID().String()
	member, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return err
	}
	if !ok {
		member = s.memberRecordFor(roomID, trimString(content["channel_id"]), userID)
	}
	member.RoomID = roomID
	member.UserID = userID
	member.Membership = membership
	member.Role = fallbackString(member.Role, "member")
	member.Domain = fallbackString(member.Domain, domainFromMXID(userID))
	if member.JoinedAt == 0 {
		member.JoinedAt = eventTime(event).UnixMilli()
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "channel.join_request.changed",
		RoomID:  roomID,
		EventID: event.EventID(),
		Payload: map[string]any{"user_id": userID, "status": trimString(content["status"])},
	})
}

func (s *Service) shouldProjectRoomMessage(ctx context.Context, roomID string, content map[string]any) bool {
	if trimString(content["p2p_kind"]) != "" || trimString(content["channel_id"]) != "" {
		return true
	}
	if s.channelIDForRoom(ctx, roomID) != "" {
		return true
	}
	if _, ok, err := s.groupByRoom(ctx, roomID); err == nil && ok {
		return true
	}
	if _, ok, err := s.lookupContactByRoom(ctx, roomID); err == nil && ok {
		return true
	}
	return false
}

func (s *Service) removeProjectedEvent(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	s.mu.Lock()
	removed := false
	posts := s.posts[:0]
	for _, post := range s.posts {
		if post.EventID != eventID {
			posts = append(posts, post)
		} else {
			removed = true
		}
	}
	s.posts = posts
	comments := s.comments[:0]
	for _, comment := range s.comments {
		if comment.EventID != eventID {
			comments = append(comments, comment)
		} else {
			removed = true
		}
	}
	s.comments = comments
	s.mu.Unlock()
	if s.store != nil {
		storeRemoved, err := s.storeHasChannelContentEvent(ctx, eventID)
		if err != nil {
			return err
		}
		removed = removed || storeRemoved
		if err := s.store.DeleteChannelPost(ctx, eventID); err != nil {
			return err
		}
		if err := s.store.DeleteChannelComment(ctx, eventID); err != nil {
			return err
		}
	}
	if !removed {
		return nil
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "room.redaction.projected",
		EventID: eventID,
		Payload: map[string]any{"redacted_event_id": eventID},
	})
}

func (s *Service) storeHasChannelContentEvent(ctx context.Context, eventID string) (bool, error) {
	posts, err := s.store.ListChannelPosts(ctx, "")
	if err != nil {
		return false, err
	}
	for _, post := range posts {
		if post.EventID == eventID {
			return true, nil
		}
	}
	comments, err := s.store.ListChannelComments(ctx, "")
	if err != nil {
		return false, err
	}
	for _, comment := range comments {
		if comment.EventID == eventID {
			return true, nil
		}
	}
	return false, nil
}

func eventTime(event *types.HeaderedEvent) time.Time {
	ts := int64(event.OriginServerTS())
	if ts <= 0 {
		return time.Now().UTC()
	}
	return time.UnixMilli(ts).UTC()
}
