package p2p

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

const PathPrefix = "/_p2p/"

const (
	eventStreamHeartbeat = 25 * time.Second
	eventStreamRetryMS   = 3000
)

type envelope struct {
	Action string         `json:"action"`
	Params map[string]any `json:"params"`
}

type apiError struct {
	Status int
	Error  string
}

func Register(router *mux.Router, service *Service) {
	router.HandleFunc("/query", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/command", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/events", eventsHandler(service)).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}).Methods(http.MethodGet, http.MethodOptions)
}

//nolint:gocyclo // SSE handler keeps auth, cursor parsing, backlog replay, and live streaming in one HTTP closure.
func eventsHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !service.Authorize(bearerToken(r.Header.Get("Authorization")), "events.stream") {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}
		since := int64(0)
		if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 0 {
				writeError(w, badRequest("since must be a non-negative integer"))
				return
			}
			since = value
		} else if raw := strings.TrimSpace(r.Header.Get("Last-Event-ID")); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 0 {
				writeError(w, badRequest("Last-Event-ID must be a non-negative integer"))
				return
			}
			since = value
		}
		limit := 100
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 || value > 500 {
				writeError(w, badRequest("limit must be between 1 and 500"))
				return
			}
			limit = value
		}
		events, err := service.listP2PEvents(r.Context(), since, limit)
		if err != nil {
			writeError(w, internalError(err))
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, internalError(errors.New("response writer does not support streaming")))
			return
		}
		_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if _, err := w.Write([]byte("retry: " + strconv.Itoa(eventStreamRetryMS) + "\n\n")); err != nil {
			return
		}
		if len(events) == 0 {
			if _, err := w.Write([]byte(": connected\n\n")); err != nil {
				return
			}
		}
		for _, event := range events {
			if err := writeSSEEvent(w, encoder, event); err != nil {
				return
			}
			since = event.Seq
		}
		flusher.Flush()

		heartbeat := time.NewTicker(eventStreamHeartbeat)
		defer heartbeat.Stop()
		for {
			waitForEvent := service.p2pEventWaiter()
			events, err := service.listP2PEvents(r.Context(), since, limit)
			if err != nil {
				return
			}
			if len(events) > 0 {
				for _, event := range events {
					if err := writeSSEEvent(w, encoder, event); err != nil {
						return
					}
					since = event.Seq
				}
				flusher.Flush()
				continue
			}
			select {
			case <-r.Context().Done():
				return
			case <-waitForEvent:
				continue
			case <-heartbeat.C:
				if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, encoder *json.Encoder, event p2pEvent) error {
	if _, err := w.Write([]byte("id: " + strconv.FormatInt(event.Seq, 10) + "\n")); err != nil {
		return err
	}
	if _, err := w.Write([]byte("event: " + event.Type + "\n")); err != nil {
		return err
	}
	if _, err := w.Write([]byte("data: ")); err != nil {
		return err
	}
	if err := encoder.Encode(event); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n"))
	return err
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
		if strings.TrimSpace(req.Action) == "" {
			writeError(w, badRequest("action is required"))
			return
		}
		if !publicAction(req.Action) && !service.Authorize(bearerToken(r.Header.Get("Authorization")), req.Action) {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
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

func writeError(w http.ResponseWriter, err *apiError) {
	writeJSON(w, err.Status, map[string]string{"error": err.Error})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
