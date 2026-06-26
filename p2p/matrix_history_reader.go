package p2p

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/direxio-message-server/p2p/matrixhistory"
)

type HTTPMatrixHistoryReader = matrixhistory.HTTPMessageReader

func NewHTTPMatrixHistoryReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMatrixHistoryReader {
	return matrixhistory.NewHTTPMessageReader(baseURL, token, client)
}
