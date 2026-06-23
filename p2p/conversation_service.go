package p2p

import (
	"context"
	"fmt"
	"sort"
	"strings"

	rstypes "github.com/YingSuiAI/direxio-message-server/roomserver/types"
)

func (s *Service) saveConversation(ctx context.Context, record conversationRecord) error {
	record = normalizeConversationRecord(record)
	if s.store != nil {
		return s.store.UpsertConversation(ctx, record)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.conversations {
		if existing.MatrixRoomID == record.MatrixRoomID && existing.Kind != record.Kind {
			return fmt.Errorf("conversation kind conflict for room %s: existing %s, incoming %s", record.MatrixRoomID, existing.Kind, record.Kind)
		}
	}
	if existing, ok := s.conversations[record.ConversationID]; ok {
		record = mergeConversationUpdate(existing, record)
	}
	s.conversations[record.ConversationID] = record
	return nil
}

func (s *Service) deleteStoredConversationKind(ctx context.Context, roomID string, kind conversationKind) error {
	if s.store == nil || roomID == "" {
		return nil
	}
	record, ok, err := s.store.GetConversationByRoomID(ctx, roomID)
	if err != nil || !ok || record.Kind != kind {
		return err
	}
	return s.store.DeleteConversationByRoomID(ctx, roomID)
}

func deleteConversationKindByRoomLocked(conversations map[string]conversationRecord, roomID string, kind conversationKind) {
	for id, record := range conversations {
		if record.MatrixRoomID == roomID && record.Kind == kind {
			delete(conversations, id)
		}
	}
}

func mergeConversationUpdate(existing, incoming conversationRecord) conversationRecord {
	if existing.CreatedAt > 0 {
		incoming.CreatedAt = existing.CreatedAt
	}
	if incoming.LastEventID == "" {
		incoming.LastEventID = existing.LastEventID
	}
	if incoming.LastMessage == "" {
		incoming.LastMessage = existing.LastMessage
	}
	if incoming.LastActivityAt <= 0 {
		incoming.LastActivityAt = existing.LastActivityAt
	}
	if incoming.CreatedByMXID == "" {
		incoming.CreatedByMXID = existing.CreatedByMXID
	}
	if incoming.PeerMXID == "" {
		incoming.PeerMXID = existing.PeerMXID
	}
	if incoming.Title == "" {
		incoming.Title = existing.Title
	}
	if incoming.AvatarURL == "" {
		incoming.AvatarURL = existing.AvatarURL
	}
	return incoming
}

func (s *Service) conversationList(ctx context.Context) (any, *apiError) {
	records, err := s.listConversations(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	views := make([]conversationView, 0, len(records))
	for _, record := range records {
		view, err := s.conversationView(ctx, record)
		if err != nil {
			return nil, internalError(err)
		}
		views = append(views, view)
	}
	return map[string]any{"conversations": views}, nil
}

func (s *Service) conversationGet(ctx context.Context, params map[string]any) (any, *apiError) {
	conversationID := trimString(params["conversation_id"])
	roomID := trimString(params["room_id"])
	if conversationID == "" && roomID == "" {
		return nil, badRequest("conversation_id or room_id is required")
	}
	record, ok, err := s.getConversation(ctx, conversationID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "conversation not found")
	}
	view, err := s.conversationView(ctx, record)
	if err != nil {
		return nil, internalError(err)
	}
	return view, nil
}

func (s *Service) listConversations(ctx context.Context) ([]conversationRecord, error) {
	if s.store != nil {
		return s.store.ListConversations(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]conversationRecord, 0, len(s.conversations))
	for _, record := range s.conversations {
		if record.Lifecycle == conversationLifecycleDeleted {
			continue
		}
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].LastActivityAt == records[j].LastActivityAt {
			if records[i].UpdatedAt == records[j].UpdatedAt {
				return records[i].ConversationID < records[j].ConversationID
			}
			return records[i].UpdatedAt > records[j].UpdatedAt
		}
		return records[i].LastActivityAt > records[j].LastActivityAt
	})
	return records, nil
}

func (s *Service) getConversation(ctx context.Context, conversationID, roomID string) (conversationRecord, bool, error) {
	if s.store != nil {
		if conversationID != "" {
			return s.store.GetConversationByID(ctx, conversationID)
		}
		return s.store.GetConversationByRoomID(ctx, roomID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if conversationID != "" {
		record, ok := s.conversations[conversationID]
		return record, ok, nil
	}
	for _, record := range s.conversations {
		if strings.TrimSpace(record.MatrixRoomID) == roomID {
			return record, true, nil
		}
	}
	return conversationRecord{}, false, nil
}

func conversationViewFromRecord(record conversationRecord) conversationView {
	return conversationView{
		ConversationID:   record.ConversationID,
		MatrixRoomID:     record.MatrixRoomID,
		Kind:             record.Kind,
		Lifecycle:        record.Lifecycle,
		PeerMXID:         record.PeerMXID,
		Title:            record.Title,
		AvatarURL:        record.AvatarURL,
		LastEventID:      record.LastEventID,
		LastMessage:      record.LastMessage,
		LastActivityAt:   record.LastActivityAt,
		ProjectionState:  record.ProjectionState,
		ProjectionReason: record.ProjectionReason,
	}
}

func (s *Service) conversationView(ctx context.Context, record conversationRecord) (conversationView, error) {
	view := conversationViewFromRecord(record)
	var err error
	switch record.Kind {
	case conversationKindDirect:
		view, err = s.directConversationView(ctx, view)
	case conversationKindGroup:
		view, err = s.groupConversationView(ctx, view)
	case conversationKindChannel:
		view, err = s.channelConversationView(ctx, view)
	}
	if err != nil {
		return view, err
	}
	return finalizeConversationView(view), nil
}

func (s *Service) directConversationView(ctx context.Context, view conversationView) (conversationView, error) {
	contact, ok, err := s.lookupContactByRoom(ctx, view.MatrixRoomID)
	if err != nil {
		return view, err
	}
	if ok {
		view.PeerMXID = fallbackString(contact.PeerMXID, view.PeerMXID)
		view.Title = fallbackString(contact.DisplayName, view.Title)
		view.AvatarURL = fallbackString(contact.AvatarURL, view.AvatarURL)
		view.RelationshipStatus = contact.Status
		view.Membership = directConversationMembership(contact.Status)
	} else {
		view.Membership = "join"
	}
	view.MemberCount = 2
	view.Role = "member"
	return view, nil
}

func (s *Service) groupConversationView(ctx context.Context, view conversationView) (conversationView, error) {
	if group, ok, err := s.groupByRoom(ctx, view.MatrixRoomID); err != nil {
		return view, err
	} else if ok && group.MemberCount > 0 {
		view.MemberCount = group.MemberCount
	}
	return s.hydrateConversationMembership(ctx, view, "")
}

func (s *Service) channelConversationView(ctx context.Context, view conversationView) (conversationView, error) {
	ch, ok, err := s.channelByIDOrRoom(ctx, "", view.MatrixRoomID)
	if err != nil {
		return view, err
	}
	channelID := ""
	if ok {
		channelID = ch.ChannelID
		view.Title = fallbackString(ch.Name, view.Title)
		view.AvatarURL = fallbackString(ch.AvatarURL, view.AvatarURL)
		view.channelType = fallbackString(ch.ChannelType, "chat")
		view.commentsEnabled = ch.CommentsEnabled
		if ch.MemberCount > 0 {
			view.MemberCount = ch.MemberCount
		}
	}
	return s.hydrateConversationMembership(ctx, view, channelID)
}

func (s *Service) hydrateConversationMembership(ctx context.Context, view conversationView, channelID string) (conversationView, error) {
	members, err := s.membersForProduct(ctx, view.MatrixRoomID, channelID)
	if err != nil {
		return view, err
	}
	if joined, _ := memberCounts(members); joined > 0 {
		view.MemberCount = joined
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	member, ok, err := s.lookupMember(ctx, view.MatrixRoomID, ownerMXID)
	if err != nil {
		return view, err
	}
	if ok && !memberHidden(member.Membership) {
		view.Membership = member.Membership
		view.Role = fallbackString(member.Role, "member")
	}
	return view, nil
}

func directConversationMembership(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted":
		return "join"
	case "pending_inbound", "pending_outbound", "pending":
		return "pending"
	case "rejected", "reject":
		return "reject"
	default:
		return strings.TrimSpace(status)
	}
}

func finalizeConversationView(view conversationView) conversationView {
	view.HydrationState, view.HydrationReason = conversationHydration(view)
	view.Capabilities = conversationCapabilitiesForView(view)
	return view
}

func conversationHydration(view conversationView) (string, string) {
	if view.ProjectionState != "" && view.ProjectionState != conversationProjectionReady {
		return string(view.ProjectionState), view.ProjectionReason
	}
	switch view.Kind {
	case conversationKindDirect:
		if strings.TrimSpace(view.PeerMXID) == "" && strings.TrimSpace(view.RelationshipStatus) == "" {
			return string(conversationProjectionPending), "direct_relationship_missing"
		}
	case conversationKindGroup, conversationKindChannel:
		if strings.TrimSpace(view.Membership) == "" {
			return string(conversationProjectionPending), "owner_membership_missing"
		}
	}
	return string(conversationProjectionReady), ""
}

func conversationCapabilitiesForView(view conversationView) conversationCapabilities {
	active := view.Lifecycle == conversationLifecycleActive
	ready := view.HydrationState == string(conversationProjectionReady)
	joined := strings.EqualFold(view.Membership, "join") || strings.EqualFold(view.Membership, "joined")
	owner := strings.EqualFold(view.Role, "owner") || strings.EqualFold(view.Role, "admin")
	open := ready && active && joined
	if view.Kind == conversationKindDirect {
		open = ready && active && strings.EqualFold(view.RelationshipStatus, "accepted")
	}
	isMemberConversation := view.Kind == conversationKindGroup || view.Kind == conversationKindChannel
	canCall := open && view.Kind != conversationKindChannel
	manageMembers := open && owner && isMemberConversation
	leave := open && isMemberConversation
	isPostChannel := view.Kind == conversationKindChannel && strings.EqualFold(fallbackString(view.channelType, "chat"), "post")
	deleteConversation := false
	if view.Kind == conversationKindDirect {
		deleteConversation = open
	} else {
		deleteConversation = manageMembers
	}
	return conversationCapabilities{
		Open:            open,
		Send:            open,
		SendMedia:       open,
		Call:            canCall,
		Invite:          manageMembers,
		ManageMembers:   manageMembers,
		Rename:          manageMembers,
		RemoveMembers:   manageMembers,
		Leave:           leave,
		Delete:          deleteConversation,
		PostCreate:      open && owner && isPostChannel,
		CommentCreate:   open && isPostChannel && view.commentsEnabled,
		ReactionToggle:  open && isPostChannel,
		PostRecall:      open && owner && isPostChannel,
		CommentRecall:   open && owner && isPostChannel,
		CommentsEnabled: isPostChannel && view.commentsEnabled,
	}
}

func (s *Service) conversationOperation(ctx context.Context, action, status, roomID string) (map[string]any, *conversationView, error) {
	roomID = strings.TrimSpace(roomID)
	operation := map[string]any{
		"action":  action,
		"status":  status,
		"room_id": roomID,
	}
	if roomID != "" {
		record, ok, err := s.getConversation(ctx, "", roomID)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			view, err := s.conversationView(ctx, record)
			if err != nil {
				return nil, nil, err
			}
			operation["conversation_id"] = view.ConversationID
			return operation, &view, nil
		}
	}
	return operation, nil, nil
}

func (s *Service) attachConversationOperation(ctx context.Context, result map[string]any, action, status, roomID string) error {
	operation, conversation, err := s.conversationOperation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	result["operation"] = operation
	if conversation != nil {
		result["conversation"] = *conversation
	}
	return nil
}

func (s *Service) attachContactConversationOperation(ctx context.Context, contact *contactRecord, action, status string) error {
	if contact == nil {
		return nil
	}
	result := map[string]any{}
	if err := s.attachConversationOperation(ctx, result, action, status, contact.RoomID); err != nil {
		return err
	}
	if operation, ok := result["operation"].(map[string]any); ok {
		contact.Operation = operation
	}
	if conversation, ok := result["conversation"].(conversationView); ok {
		contact.Conversation = &conversation
	}
	return nil
}

func conversationFromContact(contact contactRecord) conversationRecord {
	lifecycle := conversationLifecycleActive
	if contactDeleted(contact.Status) {
		lifecycle = conversationLifecycleDeleted
	} else if !strings.EqualFold(contact.Status, "accepted") {
		lifecycle = conversationLifecyclePending
	}
	return conversationRecord{
		MatrixRoomID:    contact.RoomID,
		Kind:            conversationKindDirect,
		Lifecycle:       lifecycle,
		PeerMXID:        contact.PeerMXID,
		Title:           fallbackString(contact.DisplayName, contact.PeerMXID),
		AvatarURL:       contact.AvatarURL,
		ProjectionState: conversationProjectionReady,
	}
}

func conversationFromGroup(group groupRecord) conversationRecord {
	return conversationRecord{
		MatrixRoomID:    group.RoomID,
		Kind:            conversationKindGroup,
		Lifecycle:       conversationLifecycleActive,
		Title:           group.Name,
		AvatarURL:       group.AvatarURL,
		ProjectionState: conversationProjectionReady,
	}
}

func conversationFromChannel(ch channel) conversationRecord {
	return conversationRecord{
		MatrixRoomID:    ch.RoomID,
		Kind:            conversationKindChannel,
		Lifecycle:       conversationLifecycleActive,
		Title:           ch.Name,
		AvatarURL:       ch.AvatarURL,
		ProjectionState: conversationProjectionReady,
	}
}

func (s *Service) projectConversationProfile(ctx context.Context, event *rstypes.HeaderedEvent, kind conversationKind, content map[string]any) error {
	now := eventTime(event).UnixMilli()
	title := fallbackString(trimString(content["name"]), trimString(content["display_name"]))
	lifecycle := conversationLifecycleActive
	if boolParam(content["dissolved"]) {
		lifecycle = conversationLifecycleDissolved
	}
	return s.saveConversation(ctx, conversationRecord{
		MatrixRoomID:    event.RoomID().String(),
		Kind:            kind,
		Lifecycle:       lifecycle,
		CreatedByMXID:   string(event.SenderID()),
		Title:           title,
		AvatarURL:       trimString(content["avatar_url"]),
		ProjectionState: conversationProjectionReady,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
}
