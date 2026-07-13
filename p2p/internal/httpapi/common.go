// Package httpapi owns the ProductCore and MCP HTTP protocol adapters.
package httpapi

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	rootinternal "github.com/YingSuiAI/dirextalk-message-server/internal"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const maxBodyBytes int64 = 1024 * 1024

// BuildInfoProvider supplies release metadata to public protocol responses.
// A nil provider uses the process build metadata.
type BuildInfoProvider func() rootinternal.BuildInfo

func currentBuildInfo(provider BuildInfoProvider) rootinternal.BuildInfo {
	if provider == nil {
		return rootinternal.CurrentBuildInfo()
	}
	return provider()
}

// SetCORSHeaders preserves the shared ProductCore transport CORS contract.
func SetCORSHeaders(w http.ResponseWriter, r *http.Request) {
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

// BearerToken reads the existing case-sensitive Authorization scheme.
func BearerToken(header string) string {
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(header, "Bearer ")
}

// WriteError writes the ProductCore HTTP error envelope.
func WriteError(w http.ResponseWriter, err *actionbase.Error) {
	value := map[string]string{"error": err.Error}
	if err.Code != "" {
		value["code"] = err.Code
		value["error_code"] = err.Code
	}
	if err.OperationID != "" {
		value["operation_id"] = err.OperationID
	}
	if err.CurrentRoomID != "" {
		value["current_room_id"] = err.CurrentRoomID
	}
	WriteJSON(w, err.Status, value)
}

// WriteJSON writes a JSON response using the existing transport behavior.
func WriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	decoder.UseNumber()
	return decoder.Decode(target)
}

// ResponseForRequest resolves an auto homeserver against the inbound request.
// The original response map is not mutated.
func ResponseForRequest(r *http.Request, response any) any {
	session, ok := response.(map[string]any)
	if !ok {
		return response
	}
	homeserver, ok := session["homeserver"].(string)
	if !ok || !IsAutoHomeserver(homeserver) {
		return response
	}
	copy := make(map[string]any, len(session))
	for key, value := range session {
		copy[key] = value
	}
	copy["homeserver"] = RequestBaseURL(r)
	return copy
}

// IsAutoHomeserver reports whether a configured homeserver uses the auto host.
func IsAutoHomeserver(value string) bool {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "auto") {
		return true
	}
	parsed, err := url.Parse(value)
	return err == nil && strings.EqualFold(parsed.Hostname(), "auto")
}

// RequestBaseURL returns the request-facing origin, including the first
// forwarded values used by the existing deployment contract.
func RequestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwardedProto := FirstForwardedValue(r.Header.Get("X-Forwarded-Proto")); forwardedProto != "" {
		scheme = forwardedProto
	}
	host := r.Host
	if forwardedHost := FirstForwardedValue(r.Header.Get("X-Forwarded-Host")); forwardedHost != "" {
		host = forwardedHost
	}
	return scheme + "://" + host
}

// FirstForwardedValue returns the first comma-separated proxy value.
func FirstForwardedValue(value string) string {
	if value == "" {
		return ""
	}
	return strings.TrimSpace(strings.Split(value, ",")[0])
}
