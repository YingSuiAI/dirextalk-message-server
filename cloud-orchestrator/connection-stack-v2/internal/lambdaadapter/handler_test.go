package lambdaadapter

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

func TestHandlePreservesFailClosedHTTPBoundary(t *testing.T) {
	handler := New(api.Broker{})
	response, err := handler.Handle(context.Background(), events.APIGatewayV2HTTPRequest{
		RawPath:         "/v2/commands",
		Headers:         map[string]string{"content-type": "application/json"},
		Body:            base64.StdEncoding.EncodeToString([]byte(`{}`)),
		IsBase64Encoded: true,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: http.MethodPost},
		},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	if response.Headers["Cache-Control"] != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", response.Headers["Cache-Control"])
	}
	if response.Body != "{\"error\":{\"code\":\"broker_not_configured\"}}\n" {
		t.Fatalf("body = %q", response.Body)
	}
}

func TestHandleRejectsInvalidGatewayBase64(t *testing.T) {
	response, err := New(api.Broker{}).Handle(context.Background(), events.APIGatewayV2HTTPRequest{
		RawPath:         "/v2/commands",
		Body:            "!not-base64!",
		IsBase64Encoded: true,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestHandleRejectsMissingGatewayMethod(t *testing.T) {
	response, err := New(api.Broker{}).Handle(context.Background(), events.APIGatewayV2HTTPRequest{
		RawPath: "/v2/commands",
		Body:    `{}`,
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestHandlePreservesAndRejectsGatewayQuery(t *testing.T) {
	response, err := New(api.Broker{}).Handle(context.Background(), events.APIGatewayV2HTTPRequest{
		RawPath:        "/v2/commands",
		RawQueryString: "unexpected=true",
		Headers:        map[string]string{"content-type": "application/json"},
		Body:           `{}`,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: http.MethodPost},
		},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}
}

func TestHandlePassesTrustedGatewayRuntimeInContext(t *testing.T) {
	var got api.GatewayRuntime
	handler := Handler{Broker: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		got, _ = api.GatewayRuntimeFromContext(request.Context())
		response.WriteHeader(http.StatusNoContent)
	})}
	response, err := handler.Handle(context.Background(), events.APIGatewayV2HTTPRequest{
		RawPath: "/v2/commands",
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			DomainName: "abcdefghij.execute-api.ap-northeast-1.amazonaws.com",
			Stage:      "prod",
			HTTP:       events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: http.MethodPost},
		},
	})
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("Handle() = (%#v, %v)", response, err)
	}
	if got.DomainName != "abcdefghij.execute-api.ap-northeast-1.amazonaws.com" || got.Stage != "prod" {
		t.Fatalf("gateway runtime = %#v", got)
	}
}
