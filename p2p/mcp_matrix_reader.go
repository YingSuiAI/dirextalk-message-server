package p2p

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/direxio-message-server/p2p/mcp"
)

type HTTPMCPMessageReader = mcp.HTTPMessageReader

func NewHTTPMCPMessageReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMCPMessageReader {
	return mcp.NewHTTPMessageReader(baseURL, token, client)
}
