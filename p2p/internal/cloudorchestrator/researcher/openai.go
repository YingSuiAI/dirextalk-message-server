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

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const cloudResearchSystemPrompt = `You are the internal Dirextalk Cloud Researcher. Return exactly one JSON object matching the ResearchOutput contract supplied by the caller, with exactly recipe, draft, title, and summary. draft must be a non-price ResearchDraftV1: schema_version, region, and one to three candidate requests containing only candidate_id, tier, instance_type, purchase_option (on_demand), and estimated_disk_gib. Recipe volume_slots, data_slots, and secret_slots are optional pre-approval schemas; when needed they may contain only slot_id, purpose, read_only or delivery, never a ref, value, path, environment name, command, or URL. Do not include markdown, prose outside that JSON object, credentials, secret values, pairing material, or raw logs. Never generate PlanV1, QuoteV1, a plan, a quote, a quote ID, any price/cost/currency field, approval, approval binding, hash, digest, provider receipt, purchase, deployment, or readiness claim. Produce only an experimental, single-VM recipe with public ingress disabled unless the typed input explicitly requires otherwise. Every recipe source must be an official URL and must include its version, immutable commit or artifact digest, license, and retrieval time.`

const selectedRecipeResearchSystemPrompt = `You are the internal Dirextalk Cloud Researcher. A trusted private Recipe is already selected in the input. Return exactly one JSON object with exactly draft, title, and summary. Do not return, copy, edit, replace, or propose any recipe field. draft must contain schema_version, region, and exactly three non-price candidate requests containing only candidate_id, tier, instance_type, purchase_option (on_demand), and estimated_disk_gib. Do not include markdown, credentials, secrets, prices, quotes, approvals, hashes, deployments, or readiness claims.`

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
	systemPrompt := cloudResearchSystemPrompt
	if input.SelectedRecipe != nil {
		systemPrompt = selectedRecipeResearchSystemPrompt
	}
	payload, err := json.Marshal(map[string]any{
		"model": p.model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
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
	if input.SelectedRecipe == nil {
		if err := decoder.Decode(&output); err != nil {
			return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid research output")
		}
	} else {
		var selectedOutput struct {
			Draft   cloudcontracts.ResearchDraftV1 `json:"draft"`
			Title   string                         `json:"title"`
			Summary string                         `json:"summary"`
		}
		if err := decoder.Decode(&selectedOutput); err != nil {
			return runtime.ResearchOutput{}, errors.New("cloud model returned an invalid research output")
		}
		output = runtime.ResearchOutput{Recipe: input.SelectedRecipe.Recipe, Draft: selectedOutput.Draft, Title: selectedOutput.Title, Summary: selectedOutput.Summary}
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
