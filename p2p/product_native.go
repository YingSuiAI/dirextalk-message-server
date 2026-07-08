package p2p

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/domain"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const (
	DirextalkRoomTypeDirect  = productpolicy.DirextalkRoomTypeDirect
	DirextalkRoomTypeGroup   = productpolicy.DirextalkRoomTypeGroup
	DirextalkRoomTypeChannel = productpolicy.DirextalkRoomTypeChannel
	DirextalkRoomTypeSystem  = "io.dirextalk.room.system"

	DirextalkRoomProfileEventType  = productpolicy.DirextalkRoomProfileEventType
	DirextalkMemberPolicyEventType = productpolicy.DirextalkMemberPolicyEventType
	DirextalkJoinRequestEventType  = productpolicy.DirextalkJoinRequestEventType

	DirextalkAgentStatusEventType = "io.dirextalk.agent.status"
	AgentGatewayContentKey        = "io.dirextalk.agent_gateway"
	AgentGatewaySourceContentKey  = "io.dirextalk.gateway_source"
)

type p2pEvent = domain.Event

func roomStateEvent(event dirextalkstate.StateEvent) RoomStateEvent {
	return RoomStateEvent{
		Type:     event.Type,
		StateKey: event.StateKey,
		Content:  event.Content,
	}
}

func dirextalkRoomType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "direct", "dm", "contact":
		return DirextalkRoomTypeDirect
	case "group":
		return DirextalkRoomTypeGroup
	case "channel":
		return DirextalkRoomTypeChannel
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
	return sharedHistoryVisibilityStateEvent(), true
}

func agentStatusStateEvent(agentMXID string, online bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirextalkAgentStatusEventType,
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
				spec.MRoomName:                50,
				spec.MRoomTopic:               50,
				spec.MRoomPowerLevels:         100,
				spec.MRoomHistoryVisibility:   100,
				spec.MRoomCanonicalAlias:      50,
				spec.MRoomAvatar:              50,
				spec.MRoomEncryption:          100,
				"m.room.server_acl":           100,
				DirextalkAgentStatusEventType: 50,
			},
		},
	}
}

func roomProfileForDirect(name, requesterMXID, targetMXID, requesterDisplayName, requesterAvatarURL, remark string, dissolved bool) RoomStateEvent {
	return roomStateEvent(dirextalkstate.DirectRoomProfile(dirextalkstate.DirectRoomProfileInput{
		Name:                 name,
		RequesterMXID:        requesterMXID,
		TargetMXID:           targetMXID,
		RequesterDisplayName: requesterDisplayName,
		RequesterAvatarURL:   requesterAvatarURL,
		Remark:               remark,
		Dissolved:            dissolved,
	}))
}

func accountDeletedDirectProfile(name, ownerMXID, peerMXID, ownerDisplayName, ownerAvatarURL, remark string) RoomStateEvent {
	return roomStateEvent(dirextalkstate.DirectRoomProfile(dirextalkstate.DirectRoomProfileInput{
		Name:                 name,
		RequesterMXID:        ownerMXID,
		TargetMXID:           peerMXID,
		RequesterDisplayName: ownerDisplayName,
		RequesterAvatarURL:   ownerAvatarURL,
		Remark:               remark,
		Dissolved:            true,
		AccountDeleted:       true,
		DeletedMXID:          ownerMXID,
	}))
}

func roomProfileForGroup(group groupRecord, dissolved bool) RoomStateEvent {
	return roomStateEvent(dirextalkstate.GroupRoomProfile(dirextalkstate.GroupProfile{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}, dissolved))
}

func roomProfileForChannel(ch channel, dissolved bool) RoomStateEvent {
	return roomStateEvent(dirextalkstate.ChannelRoomProfile(ch, dissolved))
}

type eventStore interface {
	InsertEvent(ctx context.Context, event p2pEvent) (bool, error)
	ListEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error)
	EventBounds(ctx context.Context) (eventBounds, error)
	PruneEventsToMaxRows(ctx context.Context, maxRows int64) (int64, error)
}

func (s *Service) eventStore() eventStore {
	if s.store == nil {
		return nil
	}
	return s.store
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
	if store := s.eventStore(); store != nil {
		inserted, err := store.InsertEvent(ctx, event)
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
	store := s.eventStore()
	if !enabled || maxRows <= 0 || store == nil {
		return nil
	}
	_, err := store.PruneEventsToMaxRows(ctx, maxRows)
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
	if store := s.eventStore(); store != nil {
		return store.ListEvents(ctx, since, limit)
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
	if store := s.eventStore(); store != nil {
		bounds, err := store.EventBounds(ctx)
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
