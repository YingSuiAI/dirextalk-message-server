package p2p

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmatrix"
)

type matrixUserProfile = dirextalkmatrix.Profile

type matrixProfileResolver interface {
	ResolveMatrixProfile(ctx context.Context, userID string) (matrixUserProfile, error)
}

type HTTPMatrixProfileResolver = dirextalkmatrix.HTTPProfileResolver

func NewHTTPMatrixProfileResolver(baseURL string, client *http.Client) *HTTPMatrixProfileResolver {
	return dirextalkmatrix.NewHTTPProfileResolver(baseURL, client)
}
