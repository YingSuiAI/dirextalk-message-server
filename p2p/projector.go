package p2p

import (
	"context"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (s *Service) ProjectOutputEvent(ctx context.Context, output roomserverAPI.OutputEvent) error {
	ctx, finishOperation := s.beginAccountOperation(ctx)
	defer finishOperation()
	if s.accountIsDeprovisioned() {
		return nil
	}
	if err := s.projectorModule.ProjectOutputEvent(ctx, output); err != nil {
		return err
	}
	if output.Type == roomserverAPI.OutputTypeRedactedEvent && output.RedactedEvent != nil {
		store, err := s.groupAgentStore()
		if err != nil {
			return err
		}
		return store.CancelGroupAgentRequests(ctx, "", "", output.RedactedEvent.RedactedEventID)
	}
	if output.Type == roomserverAPI.OutputTypeNewRoomEvent && output.NewRoomEvent != nil {
		return s.projectGroupAgentEvent(ctx, output.NewRoomEvent.Event)
	}
	return nil
}

func (s *Service) ProjectRoomEvent(ctx context.Context, event *types.HeaderedEvent) error {
	ctx, finishOperation := s.beginAccountOperation(ctx)
	defer finishOperation()
	if s.accountIsDeprovisioned() {
		return nil
	}
	if err := s.projectorModule.ProjectRoomEvent(ctx, event); err != nil {
		return err
	}
	return s.projectGroupAgentEvent(ctx, event)
}
