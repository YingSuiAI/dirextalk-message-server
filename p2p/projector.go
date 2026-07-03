package p2p

import (
	"context"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
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
	case DirextalkRoomProfileEventType:
		if event.StateKey() != nil {
			return s.projectRoomProfileState(ctx, event)
		}
	case DirextalkMemberPolicyEventType:
		if event.StateKey() != nil {
			return s.projectMemberPolicyState(ctx, event)
		}
	case DirextalkJoinRequestEventType:
		if event.StateKey() != nil {
			return s.projectJoinRequestState(ctx, event)
		}
	}
	return nil
}
