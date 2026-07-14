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
	"unicode"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const cloudResearchSystemPrompt = `You are the internal Dirextalk Cloud Researcher. Return exactly one JSON object matching the ResearchOutput contract supplied by the caller. Do not include markdown, prose, credentials, secret values, pairing material, or raw logs. Produce only an experimental, single-VM proposal with public ingress disabled unless the typed input explicitly requires otherwise. Every recipe source must be an official URL and must include its version, immutable commit or artifact digest, license, and retrieval time. A proposal is never an approval, purchase, deployment, or readiness claim.`

// OpenAICompatibleConfig is intentionally scoped to one process-local model
// credential. The API key is never serialized into ResearchInput, output,
// events, logs, or errors.
type OpenAICompatibleConfig struct {
	Endpoint string
	Model    string
	APIKey   string
	Client   *http.Client
}

// OpenAICompatiblePlanner asks a configured OpenAI-compatible model to
// produce a typed research candidate. It is not an AWS client and can neither
// see an owner approval nor execute a Recipe.
type OpenAICompatiblePlanner struct {
	endpoint string
	model    string
	apiKey   string
	client   *http.Client
}

var _ runtime.Planner = (*OpenAICompatiblePlanner)(nil)

func NewOpenAICompatiblePlanner(cfg OpenAICompatibleConfig) (*OpenAICompatiblePlanner, error) {
	endpoint, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "/v1/chat/completions" {
		return nil, errors.New("cloud researcher model endpoint is invalid")
	}
	model := strings.TrimSpace(cfg.Model)
	apiKey := strings.TrimSpace(cfg.APIKey)
	if !validModelIdentifier(model) || !validModelSecret(apiKey) {
		return nil, errors.New("cloud researcher model configuration is invalid")
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Transport: &http.Transport{Proxy: nil}}
	}
	clientCopy := *client
	if clientCopy.Transport == nil {
		clientCopy.Transport = &http.Transport{Proxy: nil}
	}
	if clientCopy.Timeout <= 0 || clientCopy.Timeout > defaultResearchLimit {
		clientCopy.Timeout = defaultResearchLimit
	}
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &OpenAICompatiblePlanner{endpoint: endpoint.String(), model: model, apiKey: apiKey, client: &clientCopy}, nil
}

func (p *OpenAICompatiblePlanner) Research(ctx context.Context, input runtime.ResearchInput) (runtime.ResearchOutput, error) {
	if p == nil || p.client == nil || p.endpoint == "" || p.model == "" || p.apiKey == "" {
		return runtime.ResearchOutput{}, errors.New("cloud researcher model is unavailable")
	}
	if err := input.Validate(); err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research input is invalid")
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research input encoding failed")
	}
	payload, err := json.Marshal(map[string]any{
		"model": p.model,
		"messages": []map[string]string{
			{"role": "system", "content": cloudResearchSystemPrompt},
			{"role": "user", "content": string(inputJSON)},
		},
		"temperature": 0,
	})
	if err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research request encoding failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(payload))
	if err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud research request is invalid")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+p.apiKey)
	response, err := p.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return runtime.ResearchOutput{}, ctx.Err()
		}
		return runtime.ResearchOutput{}, runtime.Retryable("model_unavailable", errors.New("cloud model request failed"))
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		return runtime.ResearchOutput{}, runtime.Retryable("model_unavailable", errors.New("cloud model is temporarily unavailable"))
	}
	if response.StatusCode != http.StatusOK {
		return runtime.ResearchOutput{}, errors.New("cloud model rejected research request")
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid response")
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResearchResponse+1))
	if err != nil || len(responseBody) > maxResearchResponse {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid response")
	}
	var envelope struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil || len(envelope.Choices) == 0 || strings.TrimSpace(envelope.Choices[0].Message.Content) == "" {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid response")
	}
	decoder := json.NewDecoder(strings.NewReader(envelope.Choices[0].Message.Content))
	decoder.DisallowUnknownFields()
	var output runtime.ResearchOutput
	if err := decoder.Decode(&output); err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid research output")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid research output")
	}
	if err := output.ValidateFor(input); err != nil {
		return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid research output")
	}
	return output, nil
}

func validModelIdentifier(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validModelSecret(value string) bool {
	if value == "" || len(value) > 4096 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
