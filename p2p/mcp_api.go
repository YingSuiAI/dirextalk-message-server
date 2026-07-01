package p2p

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/p2p/mcp"
)

const defaultMCPLimit = mcp.DefaultLimit
const maxMCPLimit = mcp.MaxLimit

type mcpRoomSummary = mcp.RoomSummary
type mcpMessageSummary = mcp.MessageSummary
type mcpMemberSummary = mcp.MemberSummary
type mcpPostSummary = mcp.PostSummary
type mcpCommentSummary = mcp.CommentSummary
type matrixMessageReader = mcp.MessageReader

func mcpLimit(params map[string]any) int {
	limit := int(int64Param(params["limit"]))
	if limit <= 0 {
		return defaultMCPLimit
	}
	if limit > maxMCPLimit {
		return maxMCPLimit
	}
	return limit
}

func inMCPTimeRange(ts, fromTS, toTS int64) bool {
	return mcp.InTimeRange(ts, fromTS, toTS)
}

func (s *Service) mcpRoomsSearch(ctx context.Context, params map[string]any) (any, *apiError) {
	query := strings.ToLower(trimString(params["query"]))
	kind := strings.ToLower(trimString(params["type"]))
	if kind == "" {
		kind = "all"
	}
	if kind != "all" && kind != "contact" && kind != "group" && kind != "channel" {
		return nil, badRequest("type must be contact, group, channel, or all")
	}
	records, err := s.listConversations(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	rooms := make([]mcpRoomSummary, 0, len(records))
	for _, record := range records {
		view, err := s.conversationView(ctx, record)
		if err != nil {
			return nil, internalError(err)
		}
		summary := mcpRoomSummaryFromConversation(view)
		summary = s.enrichMCPRoomSummaryWithMatrixMemberCount(ctx, summary)
		if summary.RoomID == "" {
			continue
		}
		if s.mcpRoomBlocked(summary.RoomID) {
			continue
		}
		if kind != "all" && summary.Type != kind {
			continue
		}
		if query != "" && !mcpRoomMatches(summary, query) {
			continue
		}
		rooms = append(rooms, summary)
	}
	sort.SliceStable(rooms, func(i, j int) bool {
		if rooms[i].LastTS == rooms[j].LastTS {
			return rooms[i].Name < rooms[j].Name
		}
		return rooms[i].LastTS > rooms[j].LastTS
	})
	limit := mcpLimit(params)
	if len(rooms) > limit {
		rooms = rooms[:limit]
	}
	return map[string]any{"rooms": rooms}, nil
}

func (s *Service) enrichMCPRoomSummaryWithMatrixMemberCount(ctx context.Context, summary mcpRoomSummary) mcpRoomSummary {
	if summary.RoomID == "" || (summary.Type != "group" && summary.Type != "channel") {
		return summary
	}
	members, err := s.mcpMatrixRoomMembers(ctx, summary.RoomID)
	if err != nil || len(members) == 0 {
		return summary
	}
	var count int64
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		count++
	}
	if count > 0 {
		summary.Subtitle = mcpFormatMemberCount(count)
	}
	return summary
}

func (s *Service) mcpMessagesSend(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	msg := fallbackString(trimString(params["msg"]), trimString(params["text"]))
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	if apiErr := s.requireMCPRoomAllowed(roomID); apiErr != nil {
		return nil, apiErr
	}
	if msg == "" {
		return nil, badRequest("msg is required")
	}
	gatewayMarked := mcpGatewayMarked(params)
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	senderMXID := s.ownerMXID
	if gatewayMarked {
		senderMXID = s.agentMXIDLocked()
	}
	s.mu.Unlock()
	if !gatewayMarked && agentRoomID != "" && roomID == agentRoomID {
		return nil, badRequest("mcp.messages.send cannot send owner messages to the agent room; use the Matrix agent gateway")
	}
	if s.transport == nil {
		return nil, internalError(errors.New("matrix transport is unavailable"))
	}
	content := map[string]any{
		"msgtype": "m.text",
		"body":    msg,
	}
	if gatewayMarked {
		content[AgentGatewayContentKey] = true
		if source := trimString(params["gateway_source"]); source != "" {
			content[AgentGatewaySourceContentKey] = source
		} else {
			content[AgentGatewaySourceContentKey] = "agent_gateway"
		}
	}
	res, err := s.transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  senderMXID,
		RoomID:      roomID,
		MessageType: "text",
		Timestamp:   time.Now().UTC(),
		Content:     content,
	})
	if err != nil {
		return nil, transportWriteError(err)
	}
	return map[string]any{
		"ok":       true,
		"room_id":  roomID,
		"event_id": res.EventID,
		"ts":       res.OriginServerTS,
	}, nil
}

func mcpGatewayMarked(params map[string]any) bool {
	return boolParam(params["agent_gateway"]) ||
		boolParam(params["gateway"]) ||
		boolParam(params[AgentGatewayContentKey]) ||
		trimString(params["gateway_source"]) != ""
}

func (s *Service) mcpMessagesList(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	if apiErr := s.requireMCPRoomAllowed(roomID); apiErr != nil {
		return nil, apiErr
	}
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	s.mu.Lock()
	reader := s.matrixMessages
	s.mu.Unlock()
	if reader == nil {
		return nil, internalError(errors.New("MCP message reader is unavailable"))
	}
	messages, err := reader.ListOrdinaryMessages(ctx, roomID, fromTS, toTS, limit)
	if err != nil {
		return nil, internalError(err)
	}
	name, apiErr := s.mcpMessagesRoomName(ctx, roomID)
	if apiErr != nil {
		return nil, apiErr
	}
	messages = s.enrichMCPMessageSenders(ctx, roomID, messages)
	return map[string]any{"room_id": roomID, "name": name, "messages": messages}, nil
}

func (s *Service) mcpMessagesRoomName(ctx context.Context, roomID string) (string, *apiError) {
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	s.mu.Unlock()
	if roomID != "" && roomID == agentRoomID {
		return agentRoomName, nil
	}
	if record, ok, err := s.getConversation(ctx, "", roomID); err != nil {
		return "", internalError(err)
	} else if ok {
		view, err := s.conversationView(ctx, record)
		if err != nil {
			return "", internalError(err)
		}
		return fallbackString(view.Title, roomID), nil
	}
	return roomID, nil
}

func (s *Service) enrichMCPMessageSenders(ctx context.Context, roomID string, messages []mcpMessageSummary) []mcpMessageSummary {
	if roomID == "" || len(messages) == 0 {
		return messages
	}
	displayNames := s.mcpSenderDisplayNames(ctx, roomID)
	profileResolver := s.currentMatrixProfileResolver()
	profileCache := map[string]matrixUserProfile{}
	for i := range messages {
		mxid := strings.TrimSpace(messages[i].SenderMXID)
		if mxid == "" {
			continue
		}
		displayName := displayNames[mxid]
		if mcpNeedsProfileDisplayName(mxid, displayName) {
			if profile, ok := mcpResolveMatrixProfile(ctx, profileResolver, profileCache, mxid); ok && strings.TrimSpace(profile.DisplayName) != "" {
				displayName = profile.DisplayName
				displayNames[mxid] = displayName
			}
		}
		if displayName == "" {
			continue
		}
		messages[i].SenderDisplayName = displayName
		messages[i].Sender = displayName
	}
	return messages
}

func (s *Service) mcpSenderDisplayNames(ctx context.Context, roomID string) map[string]string {
	names := map[string]string{}
	setName := func(userID, displayName string) {
		userID = strings.TrimSpace(userID)
		displayName = strings.TrimSpace(displayName)
		if userID == "" || displayName == "" {
			return
		}
		current := strings.TrimSpace(names[userID])
		if current == "" || current == displayNameFromMXID(userID) {
			names[userID] = displayName
		}
	}

	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	agentMXID := s.agentMXIDLocked()
	agentDisplayName := s.agentDisplayNameLocked()
	ownerMXID := strings.TrimSpace(s.ownerMXID)
	ownerDisplayName := strings.TrimSpace(s.profile.DisplayName)
	s.mu.Unlock()

	setName(ownerMXID, ownerDisplayName)
	setName(agentMXID, agentDisplayName)

	if record, ok, err := s.getConversation(ctx, "", roomID); err == nil && ok && record.Kind == conversationKindDirect {
		if view, viewErr := s.conversationView(ctx, record); viewErr == nil {
			setName(view.PeerMXID, view.Title)
			setName(ownerMXID, ownerDisplayName)
		}
	}

	if members, err := s.membersForProduct(ctx, roomID, ""); err == nil {
		for _, member := range members {
			setName(member.UserID, member.DisplayName)
		}
	}
	if members, err := s.mcpMatrixRoomMembers(ctx, roomID); err == nil {
		for _, member := range members {
			setName(member.UserID, member.DisplayName)
		}
	}
	if profileResolver := s.currentMatrixProfileResolver(); profileResolver != nil {
		profileCache := map[string]matrixUserProfile{}
		for userID, displayName := range names {
			if !mcpNeedsProfileDisplayName(userID, displayName) {
				continue
			}
			if profile, ok := mcpResolveMatrixProfile(ctx, profileResolver, profileCache, userID); ok {
				setName(userID, profile.DisplayName)
			}
		}
	}

	if roomID == agentRoomID {
		setName(agentMXID, agentDisplayName)
	}
	setName(ownerMXID, ownerDisplayName)
	return names
}

func (s *Service) mcpRoomMembersList(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	status := fallbackString(trimString(params["status"]), trimString(params["membership"]))
	role := trimString(params["role"])
	name := roomID
	knownRoom := false
	if ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID); err != nil {
		return nil, internalError(err)
	} else if ok {
		roomID = fallbackString(roomID, ch.RoomID)
		channelID = fallbackString(channelID, ch.ChannelID)
		name = fallbackString(ch.Name, roomID)
		knownRoom = true
	}
	if name == roomID {
		if group, ok, err := s.groupByRoom(ctx, roomID); err != nil {
			return nil, internalError(err)
		} else if ok {
			name = fallbackString(group.Name, roomID)
			knownRoom = true
		}
	}
	if record, ok, err := s.getConversation(ctx, "", roomID); err != nil {
		return nil, internalError(err)
	} else if ok {
		knownRoom = true
		view, viewErr := s.conversationView(ctx, record)
		if viewErr != nil {
			return nil, internalError(viewErr)
		}
		if strings.TrimSpace(view.Title) != "" {
			name = view.Title
		}
	}
	if !knownRoom {
		return nil, statusError(http.StatusNotFound, "room not found")
	}
	if apiErr := s.requireMCPRoomAllowed(roomID); apiErr != nil {
		return nil, apiErr
	}
	members, err := s.membersForProduct(ctx, roomID, channelID)
	if err != nil {
		return nil, internalError(err)
	}
	summaries := make([]mcpMemberSummary, 0, len(members))
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		summaries = append(summaries, mcpMemberSummaryFromMember(member))
	}
	if matrixMembers, matrixErr := s.mcpMatrixRoomMembers(ctx, roomID); matrixErr != nil && len(summaries) == 0 {
		return nil, internalError(matrixErr)
	} else if matrixErr == nil {
		summaries = mergeMCPMemberSummaries(summaries, matrixMembers)
	}
	if len(summaries) == 0 {
		if directMembers, directName, apiErr := s.mcpDirectRoomMembers(ctx, roomID); apiErr != nil {
			return nil, apiErr
		} else if len(directMembers) > 0 {
			summaries = directMembers
			name = fallbackString(directName, name)
		}
	}
	summaries = filterMCPMemberSummaries(summaries, status, role)
	summaries = s.enrichMCPMemberSummariesWithProfiles(ctx, summaries)
	limit := mcpLimit(params)
	if len(summaries) > limit {
		summaries = summaries[:limit]
	}
	return map[string]any{
		"room_id": roomID,
		"name":    name,
		"members": summaries,
		"count":   len(summaries),
	}, nil
}

func (s *Service) mcpMatrixRoomMembers(ctx context.Context, roomID string) ([]memberRecord, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return nil, nil
	}
	s.mu.Lock()
	transport := s.transport
	s.mu.Unlock()
	if transport == nil {
		return nil, nil
	}
	return transport.ListRoomMembers(ctx, roomID)
}

func (s *Service) currentMatrixProfileResolver() matrixProfileResolver {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.matrixProfiles
}

func (s *Service) enrichMCPMemberSummariesWithProfiles(ctx context.Context, summaries []mcpMemberSummary) []mcpMemberSummary {
	if len(summaries) == 0 {
		return summaries
	}
	profileResolver := s.currentMatrixProfileResolver()
	if profileResolver == nil {
		return summaries
	}
	profileCache := map[string]matrixUserProfile{}
	for i := range summaries {
		userID := strings.TrimSpace(fallbackString(summaries[i].UserMXID, summaries[i].UserID))
		if userID == "" {
			continue
		}
		needsDisplayName := mcpNeedsProfileDisplayName(userID, summaries[i].DisplayName)
		needsAvatar := strings.TrimSpace(summaries[i].AvatarURL) == ""
		if !needsDisplayName && !needsAvatar {
			continue
		}
		profile, ok := mcpResolveMatrixProfile(ctx, profileResolver, profileCache, userID)
		if !ok {
			continue
		}
		if needsDisplayName && strings.TrimSpace(profile.DisplayName) != "" {
			summaries[i].DisplayName = profile.DisplayName
		}
		if needsAvatar && strings.TrimSpace(profile.AvatarURL) != "" {
			summaries[i].AvatarURL = profile.AvatarURL
		}
	}
	return summaries
}

func mcpResolveMatrixProfile(ctx context.Context, resolver matrixProfileResolver, cache map[string]matrixUserProfile, userID string) (matrixUserProfile, bool) {
	if resolver == nil {
		return matrixUserProfile{}, false
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return matrixUserProfile{}, false
	}
	if profile, ok := cache[userID]; ok {
		return profile, strings.TrimSpace(profile.DisplayName) != "" || strings.TrimSpace(profile.AvatarURL) != ""
	}
	profile, err := resolver.ResolveMatrixProfile(ctx, userID)
	if err != nil {
		cache[userID] = matrixUserProfile{}
		return matrixUserProfile{}, false
	}
	cache[userID] = profile
	return profile, strings.TrimSpace(profile.DisplayName) != "" || strings.TrimSpace(profile.AvatarURL) != ""
}

func mcpNeedsProfileDisplayName(userID, displayName string) bool {
	displayName = strings.TrimSpace(displayName)
	return displayName == "" || strings.EqualFold(displayName, displayNameFromMXID(userID))
}

func mergeMCPMemberSummaries(existing []mcpMemberSummary, matrixMembers []memberRecord) []mcpMemberSummary {
	indexByUser := make(map[string]int, len(existing)+len(matrixMembers))
	for i, member := range existing {
		userID := strings.TrimSpace(fallbackString(member.UserMXID, member.UserID))
		if userID != "" {
			indexByUser[userID] = i
		}
	}
	for _, member := range matrixMembers {
		if memberHidden(member.Membership) {
			continue
		}
		summary := mcpMemberSummaryFromMember(member)
		userID := strings.TrimSpace(fallbackString(summary.UserMXID, summary.UserID))
		if userID == "" {
			continue
		}
		if idx, ok := indexByUser[userID]; ok {
			existing[idx] = mergeMCPMemberSummary(existing[idx], summary)
			continue
		}
		indexByUser[userID] = len(existing)
		existing = append(existing, summary)
	}
	return existing
}

func mergeMCPMemberSummary(existing, incoming mcpMemberSummary) mcpMemberSummary {
	existingUserID := fallbackString(existing.UserMXID, existing.UserID)
	if strings.TrimSpace(existing.DisplayName) == "" ||
		strings.TrimSpace(existing.DisplayName) == displayNameFromMXID(existingUserID) {
		existing.DisplayName = incoming.DisplayName
	}
	if strings.TrimSpace(existing.AvatarURL) == "" {
		existing.AvatarURL = incoming.AvatarURL
	}
	if strings.TrimSpace(existing.Membership) == "" {
		existing.Membership = incoming.Membership
	}
	if strings.TrimSpace(existing.Role) == "" || strings.TrimSpace(existing.Role) == "member" {
		existing.Role = incoming.Role
	}
	if existing.JoinedAt == 0 {
		existing.JoinedAt = incoming.JoinedAt
	}
	return existing
}

func filterMCPMemberSummaries(members []mcpMemberSummary, status, role string) []mcpMemberSummary {
	status = strings.ToLower(strings.TrimSpace(status))
	role = strings.ToLower(strings.TrimSpace(role))
	if status == "" && role == "" {
		return members
	}
	filtered := members[:0]
	for _, member := range members {
		if status != "" && strings.ToLower(strings.TrimSpace(member.Membership)) != status {
			continue
		}
		if role != "" && strings.ToLower(strings.TrimSpace(member.Role)) != role {
			continue
		}
		filtered = append(filtered, member)
	}
	return filtered
}

func (s *Service) mcpDirectRoomMembers(ctx context.Context, roomID string) ([]mcpMemberSummary, string, *apiError) {
	record, ok, err := s.getConversation(ctx, "", roomID)
	if err != nil {
		return nil, "", internalError(err)
	}
	if !ok || record.Kind != conversationKindDirect {
		return nil, "", nil
	}
	view, err := s.conversationView(ctx, record)
	if err != nil {
		return nil, "", internalError(err)
	}
	if strings.TrimSpace(view.PeerMXID) == "" {
		return nil, fallbackString(view.Title, roomID), nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	profile := s.profile
	s.mu.Unlock()
	members := make([]mcpMemberSummary, 0, 2)
	if strings.TrimSpace(ownerMXID) != "" {
		members = append(members, mcpMemberSummaryFromIdentity(
			ownerMXID,
			fallbackString(profile.DisplayName, displayNameFromMXID(ownerMXID)),
			profile.AvatarURL,
			"join",
			"owner",
			0,
		))
	}
	members = append(members, mcpMemberSummaryFromIdentity(
		view.PeerMXID,
		fallbackString(view.Title, displayNameFromMXID(view.PeerMXID)),
		view.AvatarURL,
		fallbackString(view.Membership, "join"),
		"member",
		0,
	))
	return members, fallbackString(view.Title, roomID), nil
}

func mcpMemberSummaryFromMember(member memberRecord) mcpMemberSummary {
	return mcpMemberSummaryFromIdentity(
		member.UserID,
		member.DisplayName,
		member.AvatarURL,
		member.Membership,
		member.Role,
		member.JoinedAt,
	)
}

func mcpMemberSummaryFromIdentity(userID, displayName, avatarURL, membership, role string, joinedAt int64) mcpMemberSummary {
	userID = strings.TrimSpace(userID)
	return mcpMemberSummary{
		UserID:      userID,
		UserMXID:    userID,
		Localpart:   localpartFromMXID(userID),
		Domain:      domainFromMXID(userID),
		DisplayName: fallbackString(displayName, displayNameFromMXID(userID)),
		AvatarURL:   strings.TrimSpace(avatarURL),
		Membership:  strings.TrimSpace(membership),
		Role:        normalizeProductMemberRole(role),
		JoinedAt:    joinedAt,
	}
}

func (s *Service) mcpChannelPostsList(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	if apiErr := s.requireMCPRoomAllowed(roomID); apiErr != nil {
		return nil, apiErr
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "channel not found")
	}
	postsAny := s.channelPosts(ctx, map[string]any{"channel_id": ch.ChannelID})
	rawPosts := postsAny.(map[string]any)["posts"].([]channelPostRecord)
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	posts := make([]mcpPostSummary, 0, len(rawPosts))
	for _, post := range rawPosts {
		if !inMCPTimeRange(post.OriginServerTS, fromTS, toTS) {
			continue
		}
		posts = append(posts, mcpPostSummary{
			PostID:       post.PostID,
			TS:           post.OriginServerTS,
			Sender:       fallbackString(post.AuthorName, post.AuthorMXID),
			Msg:          post.Body,
			CommentCount: post.CommentCount,
		})
		if len(posts) >= limit {
			break
		}
	}
	return map[string]any{"room_id": ch.RoomID, "name": ch.Name, "posts": posts}, nil
}

func (s *Service) mcpChannelCommentsList(ctx context.Context, params map[string]any) (any, *apiError) {
	postID := trimString(params["post_id"])
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	post, ok, err := s.channelPostByID(ctx, postID, "")
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	if apiErr := s.requireMCPRoomAllowed(post.RoomID); apiErr != nil {
		return nil, apiErr
	}
	commentsAny := s.channelComments(ctx, map[string]any{"post_id": postID})
	rawComments := commentsAny.(map[string]any)["comments"].([]channelCommentRecord)
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	comments := make([]mcpCommentSummary, 0, len(rawComments))
	for _, comment := range rawComments {
		if !inMCPTimeRange(comment.OriginServerTS, fromTS, toTS) {
			continue
		}
		comments = append(comments, mcpCommentSummary{
			CommentID: comment.CommentID,
			TS:        comment.OriginServerTS,
			Sender:    fallbackString(comment.AuthorName, comment.AuthorMXID),
			Msg:       comment.Body,
		})
		if len(comments) >= limit {
			break
		}
	}
	return map[string]any{"post_id": postID, "comments": comments}, nil
}

func (s *Service) mcpChannelCommentCreate(ctx context.Context, params map[string]any) (any, *apiError) {
	postID := trimString(params["post_id"])
	msg := fallbackString(trimString(params["msg"]), trimString(params["body"]))
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	if msg == "" {
		return nil, badRequest("msg is required")
	}
	post, ok, err := s.channelPostByID(ctx, postID, "")
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	if apiErr := s.requireMCPRoomAllowed(post.RoomID); apiErr != nil {
		return nil, apiErr
	}
	commentAny, apiErr := s.channelComment(ctx, map[string]any{
		"channel_id":   post.ChannelID,
		"room_id":      post.RoomID,
		"post_id":      postID,
		"body":         msg,
		"message_type": "text",
	})
	if apiErr != nil {
		return nil, apiErr
	}
	comment := commentAny.(channelCommentRecord)
	return map[string]any{
		"ok":         true,
		"post_id":    comment.PostID,
		"comment_id": comment.CommentID,
		"ts":         comment.OriginServerTS,
	}, nil
}

func (s *Service) MatrixHistoryAccessToken(ctx context.Context) (string, error) {
	return s.matrixHistoryAccessToken(ctx)
}

func (s *Service) requireMCPRoomAllowed(roomID string) *apiError {
	if s.mcpRoomBlocked(roomID) {
		return statusError(http.StatusForbidden, "room is blocked for MCP")
	}
	return nil
}

func (s *Service) mcpRoomBlocked(roomID string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, blockedRoomID := range s.agentConfig.MCPBlockedRoomIDs {
		if roomID == strings.TrimSpace(blockedRoomID) {
			return true
		}
	}
	return false
}

func (s *Service) matrixHistoryAccessToken(_ context.Context) (string, error) {
	s.mu.Lock()
	token := trimString(s.accessToken)
	s.mu.Unlock()
	if token == "" {
		return "", errors.New("matrix access token is unavailable")
	}
	return token, nil
}

func mcpRoomSummaryFromConversation(view conversationView) mcpRoomSummary {
	roomType := string(view.Kind)
	if roomType == "direct" {
		roomType = "contact"
	}
	subtitle := view.PeerMXID
	if roomType == "group" || roomType == "channel" {
		subtitle = mcpFormatMemberCount(view.MemberCount)
	}
	return mcpRoomSummary{
		Type:     roomType,
		Name:     fallbackString(view.Title, view.MatrixRoomID),
		RoomID:   view.MatrixRoomID,
		Subtitle: subtitle,
		LastMsg:  view.LastMessage,
		LastTS:   view.LastActivityAt,
	}
}

func mcpRoomMatches(room mcpRoomSummary, query string) bool {
	return strings.Contains(strings.ToLower(room.Name), query) ||
		strings.Contains(strings.ToLower(room.Subtitle), query) ||
		strings.Contains(strings.ToLower(room.RoomID), query)
}

func mcpFormatMemberCount(count int64) string {
	if count <= 0 {
		return ""
	}
	return strconv.FormatInt(count, 10) + " members"
}
