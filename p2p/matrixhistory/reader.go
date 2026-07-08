package matrixhistory

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmatrix"
)

func NewHTTPMessageReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMessageReader {
	return dirextalkmatrix.NewHTTPMessageReader(baseURL, token, client)
}
