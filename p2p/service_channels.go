package p2p

import (
	"context"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
)

func (s *Service) channelStore() channelStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) createChannelRoom(ctx context.Context, ch channel) (string, *apiError) {
	initialState := []RoomStateEvent{channelStateEvent(ch, false)}
	if historyVisibilityState, ok := channelHistoryVisibilityStateEvent(ch.ChannelType); ok {
		initialState = append([]RoomStateEvent{historyVisibilityState}, initialState...)
	}
	return s.ensureProductRoom(ctx, "channel", CreateRoomRequest{
		Name:         ch.Name,
		Topic:        ch.Description,
		Visibility:   ch.Visibility,
		RoomType:     DirextalkRoomTypeChannel,
		IsDirect:     false,
		InitialState: initialState,
	})
}

func (s *Service) fetchRoomChannel(ctx context.Context, roomID string) (channel, bool, *apiError) {
	s.mu.Lock()
	transport := s.transport
	s.mu.Unlock()
	if transport == nil {
		return channel{}, false, nil
	}
	ch, found, err := transport.GetRoomChannel(ctx, roomID)
	if err != nil {
		if roomServer := domainFromMatrixID(roomID, "!"); roomServer != "" && roomServer != s.serverName {
			return channel{}, false, statusError(404, "channel not found")
		}
		return channel{}, false, internalError(err)
	}
	return ch, found, nil
}

func (s *Service) saveChannel(ctx context.Context, ch channel) error {
	return s.channelsModule.Save(ctx, ch)
}

func (s *Service) setChannelMemberMute(ctx context.Context, roomID, channelID string, muted bool) *apiError {
	if err := s.setProductMemberMute(ctx, roomID, channelID, muted); err != nil {
		return internalError(err)
	}
	return nil
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
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     roomID,
		SenderMXID: senderMXID,
		Event:      roomStateEvent(dirextalkstate.JoinRequestState(roomID, userID, status, reason, time.Now().UTC())),
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
		Event:      roomStateEvent(dirextalkstate.MemberPolicyState(member)),
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) deleteChannel(ctx context.Context, channelID string) error {
	return s.channelsModule.Delete(ctx, channelID)
}

func (s *Service) channelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error) {
	return s.channelsModule.ByIDOrRoom(ctx, channelID, roomID)
}

func (s *Service) channelSnapshot(ctx context.Context, channelID string) channel {
	return s.channelsModule.Snapshot(ctx, channelID)
}

func (s *Service) channelWithCurrentCounts(ctx context.Context, ch channel) (channel, error) {
	return s.channelsModule.WithCurrentCounts(ctx, ch)
}

func (s *Service) refreshStoredChannelCounts(ctx context.Context, channelID string) error {
	return s.channelsModule.RefreshCounts(ctx, channelID)
}
