package p2p

import (
	"context"
	"net/http"
	"strings"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	realtimewsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/realtimews"
)

type realtimeWSTicket = realtimewsmodule.Ticket

func realtimeWSHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		service.realtimeModule.Handler().ServeHTTP(w, r)
	}
}

type serviceRealtimeActionPort struct{ service *Service }

func (p serviceRealtimeActionPort) Handle(
	ctx context.Context,
	ticket realtimewsmodule.Ticket,
	action string,
	params map[string]any,
) (any, *actionbase.Error) {
	ctx = withPortalActionSession(ctx, portalActionSession{
		DeviceID:   ticket.DeviceID,
		Generation: ticket.Generation,
	})
	return p.service.Handle(ctx, action, params)
}

func (s *Service) createRealtimeWSTicketForToken(token string) (map[string]any, *apiError) {
	token = strings.TrimSpace(token)
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == "" || token != s.accessToken {
		return nil, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN")
	}
	return s.realtimeModule.IssueTicket(realtimewsmodule.Ticket{
		Role:       "owner",
		UserID:     s.profile.UserID,
		DeviceID:   cleanMatrixDeviceID(s.matrixDeviceID),
		Generation: s.portalSessionGeneration,
	}), nil
}

func (s *Service) consumeRealtimeWSTicket(ticket string) error {
	_, err := s.realtimeModule.ConsumeTicket(ticket)
	return err
}

func (s *Service) consumeRealtimeWSTicketRecord(ticket string) (realtimeWSTicket, error) {
	return s.realtimeModule.ConsumeTicket(ticket)
}

func (s *Service) realtimeWSTicketActive(realtimewsmodule.Ticket) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.accountDeprovisioned
}

func (s *Service) handleRealtimeWSRequest(
	ctx context.Context,
	ticket realtimeWSTicket,
	frame map[string]any,
) map[string]any {
	return s.realtimeModule.HandleRequest(ctx, ticket, frame)
}

func (s *Service) shouldSuppressPushForRoom(roomID string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return false
	}
	s.mu.Lock()
	userID := s.profile.UserID
	s.mu.Unlock()
	return s.realtimeModule.ShouldSuppressPush(userID, roomID)
}
