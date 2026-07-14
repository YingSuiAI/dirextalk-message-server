package researcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const maxResearchRequest = 64 * 1024

// NewResearchHTTPHandler serves the private research protocol after the
// caller has configured mTLS at its listener. The handler is deliberately not
// routable through ProductCore or a public MCP endpoint.
func NewResearchHTTPHandler(planner runtime.Planner) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/healthz" && request.URL.RawQuery == "" {
			writer.Header().Set("Cache-Control", "no-store")
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		if request.Method != http.MethodPost || request.URL.Path != cloudResearchPath || request.URL.RawQuery != "" {
			writeResearchError(writer, http.StatusNotFound, "not_found")
			return
		}
		if planner == nil {
			writeResearchError(writer, http.StatusServiceUnavailable, "researcher_unavailable")
			return
		}
		contentType := strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0]))
		if contentType != "application/json" {
			writeResearchError(writer, http.StatusUnsupportedMediaType, "invalid_request")
			return
		}
		input, err := decodeResearchInput(request.Body)
		if err != nil {
			writeResearchError(writer, http.StatusBadRequest, "invalid_request")
			return
		}
		output, err := planner.Research(request.Context(), input)
		if err != nil || output.ValidateFor(input) != nil {
			writeResearchError(writer, http.StatusBadGateway, "research_failed")
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(writer).Encode(output)
	})
}

func decodeResearchInput(body io.ReadCloser) (runtime.ResearchInput, error) {
	if body == nil {
		return runtime.ResearchInput{}, errors.New("research request body is required")
	}
	defer body.Close()
	content, err := io.ReadAll(io.LimitReader(body, maxResearchRequest+1))
	if err != nil || len(content) > maxResearchRequest {
		return runtime.ResearchInput{}, errors.New("research request is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var input runtime.ResearchInput
	if err := decoder.Decode(&input); err != nil {
		return runtime.ResearchInput{}, errors.New("research request is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return runtime.ResearchInput{}, errors.New("research request is invalid")
	}
	if err := input.Validate(); err != nil {
		return runtime.ResearchInput{}, errors.New("research request is invalid")
	}
	return input, nil
}

func writeResearchError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"code": code})
}
