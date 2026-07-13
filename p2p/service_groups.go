package p2p

import (
	"context"
	"strings"
	"time"

	groupsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/groups"
)

// groupStore remains the narrow root adapter used by cross-domain member-count
// refreshes. Group workflows themselves access the Store only through groupsModule.
func (s *Service) groupStore() groupStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) ensureProductRoom(ctx context.Context, kind string, req CreateRoomRequest) (string, *apiError) {
	if s.transport != nil {
		s.mu.Lock()
		req.CreatorMXID = s.ownerMXID
		if req.CreatorDisplayName == "" {
			req.CreatorDisplayName = s.profile.DisplayName
		}
		if req.CreatorAvatarURL == "" {
			req.CreatorAvatarURL = s.profile.AvatarURL
		}
		s.mu.Unlock()
		if req.RoomType == "" {
			req.RoomType = dirextalkRoomType(kind)
		}
		res, err := s.transport.CreateRoom(ctx, req)
		if err != nil {
			return "", internalError(err)
		}
		return res.RoomID, nil
	}
	return "!" + kind + "-" + randomToken("room") + ":" + s.serverName, nil
}

func (s *Service) saveOwnerMember(ctx context.Context, roomID, channelID string) error {
	s.mu.Lock()
	member := memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      s.ownerMXID,
		DisplayName: s.profile.DisplayName,
		AvatarURL:   s.profile.AvatarURL,
		Domain:      s.serverName,
		Membership:  "join",
		Role:        "owner",
		JoinedAt:    time.Now().UTC().UnixMilli(),
	}
	s.mu.Unlock()
	return s.saveMember(ctx, member)
}

func (s *Service) createGroupRoom(ctx context.Context, group groupsmodule.View) (string, *apiError) {
	return s.ensureProductRoom(ctx, "group", CreateRoomRequest{
		Name:       group.Name,
		Topic:      group.Topic,
		Visibility: "private",
		RoomType:   DirextalkRoomTypeGroup,
		IsDirect:   false,
		InitialState: []RoomStateEvent{
			joinedHistoryVisibilityStateEvent(),
			groupStateEvent(group, false),
		},
	})
}

func groupStateEvent(group groupRecord, dissolved bool) RoomStateEvent {
	return roomProfileForGroup(group, dissolved)
}

func (s *Service) publishGroupState(ctx context.Context, group groupsmodule.View, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(group.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     group.RoomID,
		SenderMXID: senderMXID,
		Event:      groupStateEvent(group, dissolved),
	})
}

func (s *Service) setGroupMemberMute(ctx context.Context, roomID string, muted bool) *apiError {
	if err := s.setProductMemberMute(ctx, roomID, "", muted); err != nil {
		return internalError(err)
	}
	return nil
}

// The following facades keep cross-domain callers stable while group storage
// and workflow ownership lives in internal/groups.
func (s *Service) saveGroup(ctx context.Context, group groupRecord) error {
	return s.groupsModule.Save(ctx, group)
}

func (s *Service) groupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error) {
	return s.groupsModule.ByRoom(ctx, roomID)
}

func (s *Service) refreshStoredGroupCounts(ctx context.Context, roomID string) error {
	roomID = strings.TrimSpace(roomID)
	groupStore := s.groupStore()
	if groupStore == nil || roomID == "" {
		return nil
	}
	target, ok, err := groupStore.GetGroupByRoom(ctx, roomID)
	if err != nil || !ok {
		return err
	}
	memberStore := s.memberStore()
	if memberStore == nil {
		return nil
	}
	memberCount, _, err := memberStore.CountProductMembers(ctx, roomID, "")
	if err != nil {
		return err
	}
	target.MemberCount = memberCount
	return groupStore.UpsertGroup(ctx, target)
}
