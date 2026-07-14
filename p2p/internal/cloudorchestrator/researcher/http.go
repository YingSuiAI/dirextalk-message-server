// Package researcher contains private, non-provider research adapters for the
// standalone Cloud Orchestrator. It deliberately has no model credential or
// AWS SDK dependency.
package researcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const (
	cloudResearchPath    = "/v1/cloud-research"
	maxResearchResponse  = 1_000_000
	defaultResearchLimit = 75 * time.Second
)

type HTTPConfig struct {
	Endpoint string
	Client   *http.Client
}

type HTTPPlanner struct {
	endpoint string
	client   *http.Client
}

var _ runtime.Planner = (*HTTPPlanner)(nil)

// NewHTTP accepts only an exact HTTPS research endpoint. Redirects are
// disabled so a private Goal cannot be silently forwarded to another host. The
// configured URL is an endpoint allowlist, not a TLS certificate pin.
func NewHTTP(cfg HTTPConfig) (*HTTPPlanner, error) {
	endpoint, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != cloudResearchPath {
		return nil, errors.New("cloud researcher endpoint is invalid")
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{}
	}
	clientCopy := *client
	if clientCopy.Timeout <= 0 {
		clientCopy.Timeout = defaultResearchLimit
	}
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPPlanner{endpoint: endpoint.String(), client: &clientCopy}, nil
}

func (p *HTTPPlanner) Research(ctx context.Context, input runtime.ResearchInput) (runtime.ResearchOutput, error) {
	if p == nil || p.client == nil || p.endpoint == "" {
		return runtime.ResearchOutput{}, errors.New("cloud researcher is unavailable")
	}
	if err := input.Validate(); err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research input is invalid")
	}
	body, err := json.Marshal(struct {
		GoalID       string `json:"goal_id"`
		PlanID       string `json:"plan_id"`
		ConnectionID string `json:"cloud_connection_id"`
		PlanRevision int64  `json:"plan_revision"`
		Goal         string `json:"goal"`
	}{
		GoalID: input.GoalID, PlanID: input.PlanID, ConnectionID: input.ConnectionID,
		PlanRevision: input.PlanRevision, Goal: input.Prompt,
	})
	if err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research request encoding failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research request is invalid")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return runtime.ResearchOutput{}, ctx.Err()
		}
		return runtime.ResearchOutput{}, runtime.Retryable("researcher_unavailable", errors.New("cloud researcher request failed"))
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		return runtime.ResearchOutput{}, runtime.Retryable("researcher_unavailable", errors.New("cloud researcher is temporarily unavailable"))
	}
	if response.StatusCode != http.StatusOK {
		return runtime.ResearchOutput{}, errors.New("cloud researcher rejected the research request")
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" {
		return runtime.ResearchOutput{}, errors.New("cloud researcher returned an invalid response")
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResearchResponse+1))
	if err != nil || len(responseBody) > maxResearchResponse {
		return runtime.ResearchOutput{}, errors.New("cloud researcher returned an invalid response")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	var output runtime.ResearchOutput
	if err := decoder.Decode(&output); err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud researcher returned an invalid response")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return runtime.ResearchOutput{}, errors.New("cloud researcher returned an invalid response")
	}
	return output, nil
}
