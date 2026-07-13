package p2p

import (
	"context"
	"errors"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

// Root aliases keep the existing setup and package-local test seams while the
// capability implementation lives in p2p/internal/mcp.
type mcpRoomSummary = dirextalkmcp.RoomSummary
type mcpContactSummary = dirextalkmcp.ContactSummary
type mcpMessageSummary = dirextalkmcp.MessageSummary
type mcpMemberSummary = dirextalkmcp.MemberSummary
type mcpPostSummary = dirextalkmcp.PostSummary
type mcpCommentSummary = dirextalkmcp.CommentSummary
type matrixMessageReader = dirextalkmcp.MessageReader
type mcpMessagePage = dirextalkmcp.Page
type mcpMessagePageResult = dirextalkmcp.MessagePageResult

func (s *Service) dirextalkMCPService() *dirextalkmcp.Service {
	if s == nil {
		return dirextalkmcp.NewService(nil)
	}
	if s.mcpCapabilities != nil {
		return s.mcpCapabilities
	}
	if s.mcpModule != nil {
		return s.mcpModule.Service()
	}
	return dirextalkmcp.NewService(nil)
}

func (s *Service) invokeDirextalkMCP(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	value, mcpErr := s.dirextalkMCPService().Invoke(ctx, action, cloneAnyMap(params))
	return value, dirextalkMCPErrorToAPI(mcpErr)
}

func dirextalkMCPErrorToAPI(err *dirextalkmcp.Error) *apiError {
	if err == nil {
		return nil
	}
	return statusError(err.Status, err.Message)
}

func mcpPageIncludes(ts int64, id string, page mcpMessagePage) bool {
	return dirextalkmcp.InPage(ts, id, page)
}

func (s *Service) mcpFavoriteStateForPost(ctx context.Context, post channelPostRecord) (int64, bool) {
	if s == nil || s.mcpModule == nil {
		return 0, false
	}
	return s.mcpModule.FavoriteStateForPost(ctx, post)
}

// MatrixHistoryAccessToken remains the public setup callback for the existing
// owner Matrix history reader.
func (s *Service) MatrixHistoryAccessToken(ctx context.Context) (string, error) {
	return s.matrixHistoryAccessToken(ctx)
}

func (s *Service) matrixHistoryAccessToken(_ context.Context) (string, error) {
	s.mu.Lock()
	token := trimString(s.accessToken)
	s.mu.Unlock()
	if token == "" {
		return "", errors.New("matrix access token is unavailable")
	}
	return token, nil
}
