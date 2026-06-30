package p2p

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	"github.com/YingSuiAI/direxio-message-server/p2p/domain"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const (
	DirexioRoomTypeDirect  = productpolicy.DirexioRoomTypeDirect
	DirexioRoomTypeGroup   = productpolicy.DirexioRoomTypeGroup
	DirexioRoomTypeChannel = productpolicy.DirexioRoomTypeChannel

	DirexioRoomProfileEventType  = productpolicy.DirexioRoomProfileEventType
	DirexioMemberPolicyEventType = productpolicy.DirexioMemberPolicyEventType
	DirexioJoinRequestEventType  = productpolicy.DirexioJoinRequestEventType

	DirexioAgentStatusEventType  = "io.direxio.agent.status"
	AgentGatewayContentKey       = "io.direxio.agent_gateway"
	AgentGatewaySourceContentKey = "io.direxio.gateway_source"
)

type p2pEvent = domain.Event

func direxioRoomType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "direct", "dm", "contact":
		return DirexioRoomTypeDirect
	case "group":
		return DirexioRoomTypeGroup
	case "channel":
		return DirexioRoomTypeChannel
	default:
		return ""
	}
}

func historyVisibilityStateEvent(visibility gomatrixserverlib.HistoryVisibility) RoomStateEvent {
	return RoomStateEvent{
		Type:     spec.MRoomHistoryVisibility,
		StateKey: "",
		Content: map[string]any{
			"history_visibility": string(visibility),
		},
	}
}

func joinedHistoryVisibilityStateEvent() RoomStateEvent {
	return historyVisibilityStateEvent(gomatrixserverlib.HistoryVisibilityJoined)
}

func sharedHistoryVisibilityStateEvent() RoomStateEvent {
	return historyVisibilityStateEvent(gomatrixserverlib.HistoryVisibilityShared)
}

func channelHistoryVisibilityStateEvent(channelType string) (RoomStateEvent, bool) {
	switch strings.ToLower(strings.TrimSpace(channelType)) {
	case "", "chat", "text":
		return joinedHistoryVisibilityStateEvent(), true
	case "post":
		return sharedHistoryVisibilityStateEvent(), true
	default:
		return RoomStateEvent{}, false
	}
}

func agentStatusStateEvent(agentMXID string, online bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioAgentStatusEventType,
		StateKey: strings.TrimSpace(agentMXID),
		Content: map[string]any{
			"online": online,
		},
	}
}

func agentRoomPowerLevelsStateEvent(ownerMXID, agentMXID string) RoomStateEvent {
	return RoomStateEvent{
		Type:     spec.MRoomPowerLevels,
		StateKey: "",
		Content: map[string]any{
			"users": map[string]any{
				strings.TrimSpace(ownerMXID): 100,
				strings.TrimSpace(agentMXID): 50,
			},
			"users_default":  0,
			"events_default": 0,
			"state_default":  50,
			"ban":            50,
			"kick":           50,
			"redact":         50,
			"invite":         0,
			"events": map[string]any{
				spec.MRoomName:              50,
				spec.MRoomTopic:             50,
				spec.MRoomPowerLevels:       100,
				spec.MRoomHistoryVisibility: 100,
				spec.MRoomCanonicalAlias:    50,
				spec.MRoomAvatar:            50,
				spec.MRoomEncryption:        100,
				"m.room.server_acl":         100,
				DirexioAgentStatusEventType: 50,
			},
		},
	}
}

func roomProfileForDirect(name, requesterMXID, targetMXID, requesterDisplayName, requesterAvatarURL, remark string, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":      DirexioRoomTypeDirect,
			"name":           strings.TrimSpace(name),
			"visibility":     "private",
			"join_policy":    "invite",
			"invite_policy":  "owner",
			"requester_mxid": strings.TrimSpace(requesterMXID),
			"target_mxid":    strings.TrimSpace(targetMXID),
			"display_name":   strings.TrimSpace(requesterDisplayName),
			"avatar_url":     strings.TrimSpace(requesterAvatarURL),
			"domain":         domainFromMXID(requesterMXID),
			"remark":         strings.TrimSpace(remark),
			"dissolved":      dissolved,
		},
	}
}

func roomProfileForGroup(group groupRecord, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":     DirexioRoomTypeGroup,
			"room_id":       group.RoomID,
			"name":          group.Name,
			"topic":         group.Topic,
			"avatar_url":    group.AvatarURL,
			"invite_policy": fallbackString(group.InvitePolicy, "member"),
			"muted":         group.Muted,
			"dissolved":     dissolved,
		},
	}
}

func roomProfileForChannel(ch channel, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":        DirexioRoomTypeChannel,
			"channel_id":       ch.ChannelID,
			"room_id":          ch.RoomID,
			"name":             ch.Name,
			"description":      ch.Description,
			"avatar_url":       ch.AvatarURL,
			"visibility":       fallbackString(ch.Visibility, "private"),
			"join_policy":      fallbackString(ch.JoinPolicy, "invite"),
			"channel_type":     fallbackString(ch.ChannelType, "chat"),
			"comments_enabled": ch.CommentsEnabled,
			"muted":            ch.Muted,
			"dissolved":        dissolved,
		},
	}
}

func (s *Service) appendP2PEvent(ctx context.Context, event p2pEvent) error {
	now := time.Now().UTC()
	if event.CreatedAt == "" {
		event.CreatedAt = now.Format(time.RFC3339Nano)
	}
	event.DedupeKey = strings.TrimSpace(event.DedupeKey)
	s.mu.Lock()
	if event.DedupeKey != "" {
		for _, existing := range s.events {
			if existing.DedupeKey == event.DedupeKey {
				s.mu.Unlock()
				return nil
			}
		}
	}
	if event.Seq <= 0 || event.Seq <= s.nextEventSeq {
		event.Seq = now.UnixNano()
		if event.Seq <= s.nextEventSeq {
			event.Seq = s.nextEventSeq + 1
		}
	}
	s.nextEventSeq = event.Seq
	s.mu.Unlock()
	if s.store != nil {
		inserted, err := s.store.InsertEvent(ctx, event)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		s.mu.Lock()
		s.events = append(s.events, event)
		s.mu.Unlock()
		if err := s.pruneP2PEventsAfterAppend(ctx); err != nil {
			return err
		}
	} else {
		s.mu.Lock()
		s.events = append(s.events, event)
		s.mu.Unlock()
		s.pruneMemoryP2PEventsAfterAppend()
	}
	s.notifyP2PEventWaiters()
	return nil
}

func (s *Service) pruneP2PEventsAfterAppend(ctx context.Context) error {
	s.mu.Lock()
	enabled := s.eventRetentionPruneOnWrite
	maxRows := s.eventRetentionMaxRows
	s.mu.Unlock()
	if !enabled || maxRows <= 0 || s.store == nil {
		return nil
	}
	_, err := s.store.PruneEventsToMaxRows(ctx, maxRows)
	return err
}

func (s *Service) pruneMemoryP2PEventsAfterAppend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.eventRetentionPruneOnWrite || s.eventRetentionMaxRows <= 0 {
		return
	}
	maxRows := int(s.eventRetentionMaxRows)
	if len(s.events) <= maxRows {
		return
	}
	events := append([]p2pEvent(nil), s.events...)
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
	s.events = append([]p2pEvent(nil), events[len(events)-maxRows:]...)
}

func (s *Service) listP2PEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if s.store != nil {
		return s.store.ListEvents(ctx, since, limit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]p2pEvent, 0, limit)
	for _, event := range s.events {
		if event.Seq <= since {
			continue
		}
		events = append(events, event)
		if len(events) >= limit {
			break
		}
	}
	return events, nil
}

type p2pEventCursorStatus struct {
	Expired bool
	Since   int64
	Bounds  eventBounds
}

func (s *Service) p2pEventCursorStatus(ctx context.Context, since int64) (p2pEventCursorStatus, error) {
	status := p2pEventCursorStatus{Since: since}
	if since <= 0 {
		return status, nil
	}
	if s.store != nil {
		bounds, err := s.store.EventBounds(ctx)
		if err != nil {
			return p2pEventCursorStatus{}, err
		}
		status.Bounds = bounds
		status.Expired = bounds.Count > 0 && bounds.MinSeq > 0 && since < bounds.MinSeq
		return status, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, event := range s.events {
		if i == 0 || event.Seq < status.Bounds.MinSeq {
			status.Bounds.MinSeq = event.Seq
		}
		if event.Seq > status.Bounds.MaxSeq {
			status.Bounds.MaxSeq = event.Seq
		}
		status.Bounds.Count++
	}
	status.Expired = status.Bounds.Count > 0 && status.Bounds.MinSeq > 0 && since < status.Bounds.MinSeq
	return status, nil
}

func (s *Service) p2pEventWaiter() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventNotify == nil {
		s.eventNotify = make(chan struct{})
	}
	return s.eventNotify
}

func (s *Service) notifyP2PEventWaiters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventNotify == nil {
		s.eventNotify = make(chan struct{})
	}
	close(s.eventNotify)
	s.eventNotify = make(chan struct{})
}
