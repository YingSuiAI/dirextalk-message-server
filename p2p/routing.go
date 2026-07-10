package p2p

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
	"github.com/gorilla/mux"
)

const PathPrefix = "/_p2p/"

const (
	eventStreamHeartbeat = 25 * time.Second
)

type envelope struct {
	Action string         `json:"action"`
	Params map[string]any `json:"params"`
}

type apiError struct {
	Status int    `json:"-"`
	Error  string `json:"error"`
	Code   string `json:"code,omitempty"`
}

func Register(router *mux.Router, service *Service) {
	router.HandleFunc("/query", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/command", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/ws", realtimeWSHandler(service)).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		buildInfo := internal.CurrentBuildInfo()
		writeJSON(w, http.StatusOK, map[string]any{
			"status":                "ok",
			"version":               buildInfo.Version,
			"commit":                buildInfo.Commit,
			"build_time":            buildInfo.BuildTime,
			"schema_version":        buildInfo.SchemaVersion,
			"schema_compat_version": buildInfo.SchemaCompatVersion,
		})
	}).Methods(http.MethodGet, http.MethodOptions)
}

func RegisterMCP(router *mux.Router, service *Service) {
	router.HandleFunc("/mcp", handleMCP(service)).Methods(http.MethodPost, http.MethodGet, http.MethodOptions)
}

func RegisterWellKnown(router *mux.Router, service *Service) {
	router.HandleFunc("/owner.json", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, service.portalOwnerWellKnown())
	}).Methods(http.MethodGet, http.MethodOptions)
}

func handle(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var req envelope
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
		decoder.UseNumber()
		if err := decoder.Decode(&req); err != nil {
			writeError(w, badRequest("invalid json"))
			return
		}
		if req.Params == nil {
			req.Params = map[string]any{}
		}
		action := strings.TrimSpace(req.Action)
		if action == "" {
			writeError(w, badRequest("action is required"))
			return
		}
		if _, ok := serviceapi.ActionSpecFor(action); !ok {
			writeError(w, badRequest("unknown action"))
			return
		}
		if _, ok := service.actions[action]; !ok && action != realtimeWSTicketAction {
			writeError(w, badRequest("unknown action"))
			return
		}
		req.Action = action
		if !httpProductActionAllowed(action) {
			writeError(w, statusError(http.StatusBadRequest, "action requires websocket"))
			return
		}
		token := bearerToken(r.Header.Get("Authorization"))
		if !publicAction(req.Action) && !service.Authorize(token, req.Action) {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}
		if req.Action == realtimeWSTicketAction {
			response, err := service.createRealtimeWSTicketForToken(token)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, response)
			return
		}
		response, err := service.Handle(r.Context(), req.Action, req.Params)
		if err != nil {
			writeError(w, err)
			return
		}
		response = responseForRequest(r, response)
		writeJSON(w, http.StatusOK, response)
	}
}

func httpProductActionAllowed(action string) bool {
	return serviceapi.HTTPAction(action)
}

func responseForRequest(r *http.Request, response any) any {
	session, ok := response.(map[string]any)
	if !ok {
		return response
	}
	homeserver, ok := session["homeserver"].(string)
	if !ok || !isAutoHomeserver(homeserver) {
		return response
	}
	copy := make(map[string]any, len(session))
	for key, value := range session {
		copy[key] = value
	}
	copy["homeserver"] = requestBaseURL(r)
	return copy
}

func isAutoHomeserver(value string) bool {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "auto") {
		return true
	}
	parsed, err := url.Parse(value)
	return err == nil && strings.EqualFold(parsed.Hostname(), "auto")
}

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwardedProto := firstForwardedValue(r.Header.Get("X-Forwarded-Proto")); forwardedProto != "" {
		scheme = forwardedProto
	}
	host := r.Host
	if forwardedHost := firstForwardedValue(r.Header.Get("X-Forwarded-Host")); forwardedHost != "" {
		host = forwardedHost
	}
	return scheme + "://" + host
}

func firstForwardedValue(value string) string {
	if value == "" {
		return ""
	}
	return strings.TrimSpace(strings.Split(value, ",")[0])
}

func setCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, Last-Event-ID")
	w.Header().Set("Access-Control-Allow-Private-Network", "true")
}

func bearerToken(header string) string {
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(header, "Bearer ")
}

func badRequest(message string) *apiError {
	return statusError(http.StatusBadRequest, message)
}

func statusError(status int, message string) *apiError {
	return &apiError{Status: status, Error: message}
}

func codedError(status int, code, message string) *apiError {
	return &apiError{Status: status, Error: message, Code: code}
}

func writeError(w http.ResponseWriter, err *apiError) {
	value := map[string]string{"error": err.Error}
	if err.Code != "" {
		value["code"] = err.Code
	}
	writeJSON(w, err.Status, value)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
