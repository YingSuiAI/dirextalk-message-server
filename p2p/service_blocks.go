package p2p

import (
	"context"
	"sort"
	"strings"
	"time"
)

func (s *Service) blockAdd(ctx context.Context, params map[string]any) (any, *apiError) {
	block, apiErr := s.blockRecordFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	if block.CreatedAt == 0 {
		block.CreatedAt = time.Now().UTC().UnixMilli()
	}
	if err := s.saveBlock(ctx, block); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "blocked", "block": block}, nil
}

func (s *Service) blockRemove(ctx context.Context, params map[string]any) (any, *apiError) {
	block, apiErr := s.blockRecordFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	removed, err := s.deleteBlock(ctx, block.TargetType, block.TargetID)
	if err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "removed": removed, "target_type": block.TargetType, "target_id": block.TargetID}, nil
}

func (s *Service) blockListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	blocks, err := s.listBlocks(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	contacts := make([]blockRecord, 0)
	groups := make([]blockRecord, 0)
	channels := make([]blockRecord, 0)
	for _, block := range blocks {
		switch block.TargetType {
		case "contact":
			contacts = append(contacts, block)
		case "group":
			groups = append(groups, block)
		case "channel":
			channels = append(channels, block)
		}
	}
	return map[string]any{
		"contacts": contacts,
		"groups":   groups,
		"channels": channels,
	}, nil
}

func (s *Service) blockRecordFromParams(ctx context.Context, params map[string]any) (blockRecord, *apiError) {
	targetType := normalizeBlockTargetType(fallbackString(trimString(params["target_type"]), trimString(params["type"])))
	if targetType == "" {
		return blockRecord{}, badRequest("target_type is required")
	}
	block := blockRecord{
		TargetType:  targetType,
		TargetID:    trimString(params["target_id"]),
		RoomID:      trimString(params["room_id"]),
		ChannelID:   trimString(params["channel_id"]),
		PeerMXID:    fallbackString(trimString(params["peer_mxid"]), trimString(params["mxid"])),
		DisplayName: fallbackString(trimString(params["display_name"]), trimString(params["name"])),
		AvatarURL:   trimString(params["avatar_url"]),
	}
	switch targetType {
	case "contact":
		block.PeerMXID = fallbackString(block.PeerMXID, fallbackString(firstMemberID(params), block.TargetID))
		if block.PeerMXID == "" {
			return blockRecord{}, badRequest("peer_mxid is required")
		}
		if contact, ok, err := s.lookupContactByPeer(ctx, block.PeerMXID); err != nil {
			return blockRecord{}, internalError(err)
		} else if ok {
			block.RoomID = fallbackString(block.RoomID, contact.RoomID)
			block.DisplayName = fallbackString(block.DisplayName, contact.DisplayName)
			block.AvatarURL = fallbackString(block.AvatarURL, contact.AvatarURL)
		}
		block.TargetID = block.PeerMXID
		block.DisplayName = fallbackString(block.DisplayName, displayNameFromMXID(block.PeerMXID))
	case "group":
		block.RoomID = fallbackString(block.RoomID, block.TargetID)
		if block.RoomID == "" {
			return blockRecord{}, badRequest("room_id is required")
		}
		if group, ok, err := s.groupByRoom(ctx, block.RoomID); err != nil {
			return blockRecord{}, internalError(err)
		} else if ok {
			block.DisplayName = fallbackString(block.DisplayName, group.Name)
			block.AvatarURL = fallbackString(block.AvatarURL, group.AvatarURL)
		}
		block.TargetID = block.RoomID
		block.DisplayName = fallbackString(block.DisplayName, block.RoomID)
	case "channel":
		if block.RoomID == "" && block.TargetID != "" && strings.HasPrefix(block.TargetID, "!") {
			block.RoomID = block.TargetID
		}
		if block.RoomID == "" {
			return blockRecord{}, badRequest("room_id is required")
		}
		if ch, ok, err := s.channelByIDOrRoom(ctx, "", block.RoomID); err != nil {
			return blockRecord{}, internalError(err)
		} else if ok {
			block.RoomID = fallbackString(block.RoomID, ch.RoomID)
			block.DisplayName = fallbackString(block.DisplayName, ch.Name)
			block.AvatarURL = fallbackString(block.AvatarURL, ch.AvatarURL)
		}
		block.ChannelID = ""
		block.TargetID = block.RoomID
		block.DisplayName = fallbackString(block.DisplayName, block.RoomID)
	default:
		return blockRecord{}, badRequest("target_type must be contact, group, or channel")
	}
	return block, nil
}

func normalizeBlockTargetType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "friend", "user", "member", "contact":
		return "contact"
	case "room", "group":
		return "group"
	case "channel":
		return "channel"
	default:
		return ""
	}
}

func (s *Service) saveBlock(ctx context.Context, block blockRecord) error {
	s.mu.Lock()
	s.blocks[blockKey(block.TargetType, block.TargetID)] = block
	s.mu.Unlock()
	if s.store != nil {
		return s.store.UpsertBlock(ctx, block)
	}
	return nil
}

func (s *Service) deleteBlock(ctx context.Context, targetType, targetID string) (bool, error) {
	key := blockKey(targetType, targetID)
	s.mu.Lock()
	_, removed := s.blocks[key]
	delete(s.blocks, key)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.DeleteBlock(ctx, targetType, targetID)
	}
	return removed, nil
}

func (s *Service) listBlocks(ctx context.Context) ([]blockRecord, error) {
	if s.store != nil {
		return s.store.ListBlocks(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	blocks := make([]blockRecord, 0, len(s.blocks))
	for _, block := range s.blocks {
		blocks = append(blocks, block)
	}
	sortBlocks(blocks)
	return blocks, nil
}

func (s *Service) blockExists(ctx context.Context, targetType string, identifiers ...string) (bool, error) {
	targetType = normalizeBlockTargetType(targetType)
	if targetType == "" {
		return false, nil
	}
	ids := map[string]struct{}{}
	for _, identifier := range identifiers {
		identifier = strings.TrimSpace(identifier)
		if identifier != "" {
			ids[identifier] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	blocks, err := s.listBlocks(ctx)
	if err != nil {
		return false, err
	}
	for _, block := range blocks {
		if block.TargetType != targetType {
			continue
		}
		for _, candidate := range []string{block.TargetID, block.RoomID, block.ChannelID, block.PeerMXID} {
			if _, ok := ids[strings.TrimSpace(candidate)]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Service) rejectIfBlocked(ctx context.Context, targetType string, identifiers ...string) *apiError {
	blocked, err := s.blockExists(ctx, targetType, identifiers...)
	if err != nil {
		return internalError(err)
	}
	if blocked {
		return statusError(403, "already blocked")
	}
	return nil
}

func blockKey(targetType, targetID string) string {
	return normalizeBlockTargetType(targetType) + "|" + strings.TrimSpace(targetID)
}

func sortBlocks(blocks []blockRecord) {
	sort.SliceStable(blocks, func(i, j int) bool {
		if blocks[i].TargetType != blocks[j].TargetType {
			return blocks[i].TargetType < blocks[j].TargetType
		}
		if blocks[i].DisplayName != blocks[j].DisplayName {
			return strings.ToLower(blocks[i].DisplayName) < strings.ToLower(blocks[j].DisplayName)
		}
		return blocks[i].TargetID < blocks[j].TargetID
	})
}
