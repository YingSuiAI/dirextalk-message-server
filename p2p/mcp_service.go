package p2p

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type p2pDirextalkMCPInvoker struct {
	service *Service
}

type p2pDirextalkMCPRoomAuthorizer struct {
	service *Service
}

func (i p2pDirextalkMCPInvoker) InvokeCapability(ctx context.Context, action string, params map[string]any) (any, *dirextalkmcp.Error) {
	if i.service == nil {
		return nil, dirextalkmcp.StatusError(http.StatusInternalServerError, "Dirextalk MCP capability service is unavailable")
	}
	ctx, finishOperation := i.service.beginAccountOperation(ctx)
	defer finishOperation()
	if i.service.accountIsDeprovisioned() {
		return nil, dirextalkmcp.StatusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN")
	}
	var (
		value  any
		apiErr *apiError
	)
	switch action {
	case dirextalkmcp.ActionRoomsSearch:
		value, apiErr = i.service.mcpRoomsSearch(ctx, params)
	case dirextalkmcp.ActionContactsList:
		value, apiErr = i.service.mcpContactsList(ctx, params)
	case dirextalkmcp.ActionContactsSearch:
		value, apiErr = i.service.mcpContactsSearch(ctx, params)
	case dirextalkmcp.ActionMessagesSend:
		value, apiErr = i.service.mcpMessagesSend(ctx, params)
	case dirextalkmcp.ActionMessagesList:
		value, apiErr = i.service.mcpMessagesList(ctx, params)
	case dirextalkmcp.ActionRoomMembersList:
		value, apiErr = i.service.mcpRoomMembersList(ctx, params)
	case dirextalkmcp.ActionChannelPostsList:
		value, apiErr = i.service.mcpChannelPostsList(ctx, params)
	case dirextalkmcp.ActionChannelCommentsList:
		value, apiErr = i.service.mcpChannelCommentsList(ctx, params)
	case dirextalkmcp.ActionChannelCommentsCreate:
		value, apiErr = i.service.mcpChannelCommentCreate(ctx, params)
	default:
		return nil, dirextalkmcp.BadRequest("unknown MCP action")
	}
	return value, apiErrorToDirextalkMCP(apiErr)
}

func (a p2pDirextalkMCPRoomAuthorizer) MCPRoomBlocked(roomID string) bool {
	if a.service == nil {
		return false
	}
	return a.service.mcpRoomBlocked(roomID)
}

func (s *Service) dirextalkMCPService() *dirextalkmcp.Service {
	if s == nil {
		return dirextalkmcp.NewService(nil)
	}
	if s.mcpCapabilities == nil {
		s.mcpCapabilities = dirextalkmcp.NewServiceWithConfig(dirextalkmcp.Config{
			Invoker:        p2pDirextalkMCPInvoker{service: s},
			RoomAuthorizer: p2pDirextalkMCPRoomAuthorizer{service: s},
		})
	}
	return s.mcpCapabilities
}

func (s *Service) invokeDirextalkMCPAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		return s.invokeDirextalkMCP(ctx, action, params)
	}
}

func (s *Service) invokeDirextalkMCP(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	value, apiErr := s.dirextalkMCPService().Invoke(ctx, action, cloneAnyMap(params))
	return value, dirextalkMCPErrorToAPI(apiErr)
}

func apiErrorToDirextalkMCP(err *apiError) *dirextalkmcp.Error {
	if err == nil {
		return nil
	}
	return dirextalkmcp.StatusError(err.Status, err.Error)
}

func dirextalkMCPErrorToAPI(err *dirextalkmcp.Error) *apiError {
	if err == nil {
		return nil
	}
	return statusError(err.Status, err.Message)
}
