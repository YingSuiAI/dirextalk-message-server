package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	httpapi "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/httpapi"
	"github.com/gorilla/mux"
)

const PathPrefix = "/_p2p/"

// envelope remains the shared product HTTP request shape used by outbound
// inter-node adapters and package integration tests.
type envelope struct {
	Action string         `json:"action"`
	Params map[string]any `json:"params"`
}

func Register(router *mux.Router, service *Service) {
	product := httpapi.ProductHandler(serviceHTTPProductPort{service: service})
	router.HandleFunc("/query", product).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/command", product).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/ws", realtimeWSHandler(service)).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/agent/voice/webhook", nativeAgentVoiceWebhookHandler(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/agent/voice/volc/custom-llm", nativeAgentVoiceCustomLLMHandler(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/health", httpapi.HealthHandler(nil)).Methods(http.MethodGet, http.MethodOptions)
}

func RegisterMCP(router *mux.Router, service *Service) {
	handler := httpapi.MCPHandler(httpapi.MCPConfig{Port: serviceHTTPMCPPort{service: service}})
	router.HandleFunc("/mcp", handler).Methods(http.MethodPost, http.MethodGet, http.MethodOptions)
}

func RegisterWellKnown(router *mux.Router, service *Service) {
	handler := httpapi.WellKnownHandler(func() any { return service.profileModule.WellKnown() })
	router.HandleFunc("/owner.json", handler).Methods(http.MethodGet, http.MethodOptions)
}

type serviceHTTPProductPort struct{ service *Service }

func (p serviceHTTPProductPort) HasAction(action string) bool {
	if p.service == nil {
		return false
	}
	_, ok := p.service.actions[action]
	return ok
}

func (p serviceHTTPProductPort) Authorize(ctx context.Context, token, action string) (context.Context, bool) {
	if p.service == nil {
		return ctx, false
	}
	identity, authorized := p.service.authorizeProductAction(token, action)
	if !authorized {
		return ctx, false
	}
	if identity.Generation != 0 {
		ctx = withPortalActionSession(ctx, identity)
	}
	return ctx, true
}

func (p serviceHTTPProductPort) Handle(ctx context.Context, action string, params map[string]any) (any, *actionbase.Error) {
	return p.service.Handle(ctx, action, params)
}

func (p serviceHTTPProductPort) CreateWSTicket(token string) (any, *actionbase.Error) {
	return p.service.createRealtimeWSTicketForToken(token)
}

type serviceHTTPMCPPort struct{ service *Service }

func (p serviceHTTPMCPPort) TokenAuthorized(token string) bool {
	return p.service != nil && token != "" && token == p.service.AgentToken()
}

func (p serviceHTTPMCPPort) Tools() []dirextalkmcp.Tool {
	if p.service == nil {
		return nil
	}
	return p.service.dirextalkMCPService().Tools()
}

func (p serviceHTTPMCPPort) Invoke(ctx context.Context, action string, params map[string]any) (any, *dirextalkmcp.Error) {
	return p.service.dirextalkMCPService().Invoke(ctx, action, params)
}

func nativeAgentVoiceWebhookHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpapi.SetCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if service == nil {
			httpapi.WriteError(w, actionbase.StatusError(http.StatusServiceUnavailable, "service is unavailable"))
			return
		}
		var params map[string]any
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024)).Decode(&params); err != nil {
			httpapi.WriteError(w, actionbase.BadRequest("invalid json"))
			return
		}
		token := httpapi.BearerToken(r.Header.Get("Authorization"))
		if token == "" {
			token = r.Header.Get("X-Dirextalk-Voice-Secret")
		}
		response, apiErr := service.HandleNativeAgentVoiceWebhook(r.Context(), token, params)
		if apiErr != nil {
			httpapi.WriteError(w, apiErr)
			return
		}
		httpapi.WriteJSON(w, http.StatusAccepted, response)
	}
}

func nativeAgentVoiceCustomLLMHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpapi.SetCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if service == nil {
			httpapi.WriteError(w, actionbase.StatusError(http.StatusServiceUnavailable, "service is unavailable"))
			return
		}
		var params map[string]any
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024)).Decode(&params); err != nil {
			httpapi.WriteError(w, actionbase.BadRequest("invalid json"))
			return
		}
		token := httpapi.BearerToken(r.Header.Get("Authorization"))
		if token == "" {
			token = r.Header.Get("X-Dirextalk-Voice-Secret")
		}
		if apiErr := service.AuthorizeNativeAgentVoiceWebhook(token); apiErr != nil {
			httpapi.WriteError(w, apiErr)
			return
		}
		sessionID := r.URL.Query().Get("session_id")
		flusher, ok := w.(http.Flusher)
		if !ok {
			httpapi.WriteError(w, actionbase.StatusError(http.StatusInternalServerError, "streaming is unavailable"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		streamID := "dirextalk-voice-" + sessionID
		sentContent := false
		emit := func(text string) error {
			if text == "" {
				return nil
			}
			sentContent = true
			return writeOpenAIChatCompletionChunk(w, flusher, streamID, text, "")
		}
		response, apiErr := service.HandleNativeAgentVoiceCustomLLM(r.Context(), token, sessionID, params, emit)
		if apiErr != nil {
			_ = writeOpenAIChatCompletionChunk(w, flusher, streamID, apiErr.Error, "stop")
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
			return
		}
		if text, _ := response["text"].(string); text != "" && !sentContent {
			// Some Native Agent providers only emit final text. Ensure TTS still receives it.
			_ = writeOpenAIChatCompletionChunk(w, flusher, streamID, text, "")
		}
		_ = writeOpenAIChatCompletionChunk(w, flusher, streamID, "", "stop")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}
}

func writeOpenAIChatCompletionChunk(w http.ResponseWriter, flusher http.Flusher, streamID, text, finishReason string) error {
	choice := map[string]any{
		"index": 0,
		"delta": map[string]any{},
	}
	if text != "" {
		choice["delta"] = map[string]any{"content": text}
	}
	if finishReason != "" {
		choice["finish_reason"] = finishReason
	}
	payload := map[string]any{
		"id":      streamID,
		"object":  "chat.completion.chunk",
		"choices": []any{choice},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// These compatibility helpers remain for root adapters and package tests.
func badRequest(message string) *apiError {
	return actionbase.BadRequest(message)
}

func internalError(err error) *apiError {
	return actionbase.InternalError(err)
}

func statusError(status int, message string) *apiError {
	return actionbase.StatusError(status, message)
}

func codedError(status int, code, message string) *apiError {
	return actionbase.CodedError(status, code, message)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	httpapi.WriteJSON(w, status, value)
}
