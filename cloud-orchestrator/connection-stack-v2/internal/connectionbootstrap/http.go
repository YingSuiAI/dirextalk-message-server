package connectionbootstrap

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

func (service *Service) ControllerHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		secureHeaders(response)
		if request.Method != http.MethodPost || request.URL.Path != "/v1/aws-bootstrap/sessions" {
			writeError(response, http.StatusNotFound)
			return
		}
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 || len(request.TLS.VerifiedChains) == 0 {
			writeError(response, http.StatusUnauthorized)
			return
		}
		raw, err := readRequest(response, request)
		if err != nil {
			writeError(response, http.StatusBadRequest)
			return
		}
		input, err := ParseCreateRequest(raw)
		clear(raw)
		if err != nil {
			writeError(response, http.StatusBadRequest)
			return
		}
		result, err := service.CreateSession(input)
		if err != nil {
			if errors.Is(err, ErrConflict) {
				writeError(response, http.StatusConflict)
				return
			}
			writeError(response, http.StatusBadRequest)
			return
		}
		writeJSON(response, http.StatusCreated, result)
	})
}
func (service *Service) UploadHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		secureHeaders(response)
		const prefix = "/v1/aws-bootstrap/sessions/"
		if request.Method != http.MethodPut || !strings.HasPrefix(request.URL.Path, prefix) || strings.Contains(strings.TrimPrefix(request.URL.Path, prefix), "/") {
			writeError(response, http.StatusNotFound)
			return
		}
		sessionID := strings.TrimPrefix(request.URL.Path, prefix)
		authorization := request.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") || len(authorization) <= 7 {
			writeError(response, http.StatusUnauthorized)
			return
		}
		raw, err := readRequest(response, request)
		if err != nil {
			writeError(response, http.StatusBadRequest)
			return
		}
		envelope, err := ParseUploadEnvelope(raw)
		clear(raw)
		if err != nil {
			writeError(response, http.StatusBadRequest)
			return
		}
		receipt, err := service.Upload(request.Context(), sessionID, strings.TrimPrefix(authorization, "Bearer "), envelope)
		if err != nil {
			switch {
			case errors.Is(err, ErrUnauthorized):
				writeError(response, http.StatusUnauthorized)
			case errors.Is(err, ErrConflict), errors.Is(err, ErrConsumed):
				writeError(response, http.StatusConflict)
			case errors.Is(err, ErrExpired):
				writeError(response, http.StatusGone)
			default:
				writeError(response, http.StatusBadRequest)
			}
			return
		}
		writeJSON(response, http.StatusAccepted, receipt)
	})
}
func readRequest(response http.ResponseWriter, request *http.Request) ([]byte, error) {
	request.Body = http.MaxBytesReader(response, request.Body, maxJSONBytes)
	defer request.Body.Close()
	raw, err := io.ReadAll(request.Body)
	if err != nil || len(raw) == 0 {
		return nil, ErrInvalid
	}
	return raw, nil
}
func secureHeaders(response http.ResponseWriter) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Pragma", "no-cache")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.Header().Set("Content-Type", "application/json")
}
func writeJSON(response http.ResponseWriter, status int, value any) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
func writeError(response http.ResponseWriter, status int) {
	writeJSON(response, status, struct {
		Error string `json:"error"`
	}{http.StatusText(status)})
}
