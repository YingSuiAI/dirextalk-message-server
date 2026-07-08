package p2p

import (
	"context"
	"fmt"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
)

type HTTPMatrixHistoryReader = matrixhistory.HTTPMessageReader

type MatrixHistoryReader interface {
	ListOrdinaryMessages(ctx context.Context, roomID string, page matrixhistory.Page) (matrixhistory.MessagePageResult, error)
}

type MatrixChannelContentReader interface {
	ListChannelContent(ctx context.Context, roomID string, limit int) ([]matrixhistory.Event, error)
}

type CompositeMatrixHistoryReader struct {
	messages       MatrixHistoryReader
	channelContent MatrixChannelContentReader
}

func NewHTTPMatrixHistoryReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMatrixHistoryReader {
	return matrixhistory.NewHTTPMessageReader(baseURL, token, client)
}

func NewCompositeMatrixHistoryReader(messages MatrixHistoryReader, channelContent MatrixChannelContentReader) *CompositeMatrixHistoryReader {
	return &CompositeMatrixHistoryReader{
		messages:       messages,
		channelContent: channelContent,
	}
}

func (r *CompositeMatrixHistoryReader) ListOrdinaryMessages(ctx context.Context, roomID string, page matrixhistory.Page) (matrixhistory.MessagePageResult, error) {
	if r == nil || r.messages == nil {
		return matrixhistory.MessagePageResult{}, fmt.Errorf("matrix message reader is unavailable")
	}
	return r.messages.ListOrdinaryMessages(ctx, roomID, page)
}

func (r *CompositeMatrixHistoryReader) ListChannelContent(ctx context.Context, roomID string, limit int) ([]matrixhistory.Event, error) {
	if r == nil {
		return nil, nil
	}
	if r.channelContent != nil {
		return r.channelContent.ListChannelContent(ctx, roomID, limit)
	}
	if reader, ok := r.messages.(MatrixChannelContentReader); ok {
		return reader.ListChannelContent(ctx, roomID, limit)
	}
	return nil, nil
}
