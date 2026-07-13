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
	return s.projectorModule.ProjectOutputEvent(ctx, output)
}

func (s *Service) ProjectRoomEvent(ctx context.Context, event *types.HeaderedEvent) error {
	ctx, finishOperation := s.beginAccountOperation(ctx)
	defer finishOperation()
	if s.accountIsDeprovisioned() {
		return nil
	}
	return s.projectorModule.ProjectRoomEvent(ctx, event)
}
