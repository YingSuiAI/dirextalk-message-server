// Package lambdaadapter keeps AWS Lambda event conversion separate from the
// protocol and HTTP boundary. The root Message Server never imports it.
package lambdaadapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"

	"github.com/aws/aws-lambda-go/events"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

// Handler converts API Gateway HTTP API v2 events to the normal net/http
// boundary so public behavior has one implementation in local tests and in
// Lambda. It never forwards Lambda context values or request bodies to logs.
type Handler struct {
	Broker http.Handler
}

func New(broker api.Broker) Handler {
	return Handler{Broker: broker}
}

func (h Handler) Handle(ctx context.Context, event events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	body := []byte(event.Body)
	if event.IsBase64Encoded {
		decoded, err := base64.StdEncoding.DecodeString(event.Body)
		if err != nil {
			return gatewayError(http.StatusBadRequest, "invalid_command"), nil
		}
		body = decoded
	}
	path := event.RawPath
	if path == "" {
		path = event.RequestContext.HTTP.Path
	}
	if path == "" {
		path = "/"
	}
	if event.RawQueryString != "" {
		path += "?" + event.RawQueryString
	}
	method := event.RequestContext.HTTP.Method
	if method == "" {
		return gatewayError(http.StatusBadRequest, "invalid_command"), nil
	}
	requestContext := ctx
	if event.RequestContext.DomainName != "" && event.RequestContext.Stage != "" {
		requestContext = api.WithGatewayRuntime(ctx, api.GatewayRuntime{
			DomainName: event.RequestContext.DomainName,
			Stage:      event.RequestContext.Stage,
		})
	}
	request, err := http.NewRequestWithContext(requestContext, method, "https://connection-stack.invalid"+path, bytes.NewReader(body))
	if err != nil {
		return gatewayError(http.StatusBadRequest, "invalid_command"), nil
	}
	for key, value := range event.Headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	if h.Broker == nil {
		gateway := gatewayError(http.StatusServiceUnavailable, "broker_not_configured")
		return gateway, nil
	}
	h.Broker.ServeHTTP(recorder, request)
	result := recorder.Result()
	defer result.Body.Close()
	return events.APIGatewayV2HTTPResponse{
		StatusCode: result.StatusCode,
		Headers:    flattenHeaders(result.Header),
		Body:       recorder.Body.String(),
	}, nil
}

func gatewayError(status int, code string) events.APIGatewayV2HTTPResponse {
	return events.APIGatewayV2HTTPResponse{
		StatusCode: status,
		Headers: map[string]string{
			"Cache-Control":          "no-store",
			"Content-Type":           "application/json",
			"X-Content-Type-Options": "nosniff",
		},
		Body: `{"error":{"code":"` + code + `"}}` + "\n",
	}
}

func flattenHeaders(headers http.Header) map[string]string {
	result := make(map[string]string, len(headers))
	for key, values := range headers {
		if len(values) > 0 {
			result[key] = values[0]
		}
	}
	return result
}
