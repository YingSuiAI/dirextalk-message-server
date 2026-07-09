package nativeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func (r *Runtime) modelsList(ctx context.Context, params map[string]any) (map[string]any, error) {
	provider := strings.ToLower(trimString(params["provider"]))
	result := map[string]any{
		"models":    []map[string]any{},
		"providers": modelProviderDefaults(),
	}
	if provider == "" {
		return result, nil
	}
	if !supportsOpenAICompatibleModelList(provider) {
		return nil, fmt.Errorf("model list is not supported for provider %q", provider)
	}
	apiKey := trimString(params["api_key"])
	if apiKey == "" {
		return nil, fmt.Errorf("api_key is required to fetch %s models", provider)
	}
	models, err := r.fetchOpenAICompatibleModels(ctx, provider, trimString(params["base_url"]), apiKey)
	if err != nil {
		return nil, err
	}
	result["models"] = models
	return result, nil
}

func modelProviderDefaults() []map[string]any {
	return []map[string]any{
		{"provider": "openai", "default_base_url": defaultBaseURLForProvider("openai"), "requires_api_key": true, "dynamic_models": true},
		{"provider": "anthropic", "default_base_url": defaultBaseURLForProvider("anthropic"), "requires_api_key": true, "dynamic_models": false},
		{"provider": "deepseek", "default_base_url": defaultBaseURLForProvider("deepseek"), "requires_api_key": true, "dynamic_models": true},
		{"provider": "openai_compatible", "default_base_url": defaultBaseURLForProvider("openai_compatible"), "requires_api_key": true, "dynamic_models": true},
		{"provider": "openrouter", "default_base_url": defaultBaseURLForProvider("openrouter"), "requires_api_key": true, "dynamic_models": true},
		{"provider": "litellm", "default_base_url": defaultBaseURLForProvider("litellm"), "requires_api_key": true, "dynamic_models": true},
	}
}

func supportsOpenAICompatibleModelList(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "openai", "deepseek", "openai_compatible", "openrouter", "litellm":
		return true
	default:
		return false
	}
}

func (r *Runtime) fetchOpenAICompatibleModels(ctx context.Context, provider, baseURL, apiKey string) ([]map[string]any, error) {
	url := strings.TrimRight(openAICompatibleModelsBaseURL(provider, baseURL), "/")
	if url == "" {
		return nil, fmt.Errorf("base_url is required to fetch %s models", provider)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/models", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s models: %w", provider, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch %s models failed: %s", provider, resp.Status)
	}
	var payload struct {
		Data   []map[string]any `json:"data"`
		Models []map[string]any `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode %s models: %w", provider, err)
	}
	rawModels := payload.Data
	if len(rawModels) == 0 {
		rawModels = payload.Models
	}
	models := normalizeModelList(provider, rawModels)
	if len(models) == 0 {
		return nil, fmt.Errorf("fetch %s models returned no models", provider)
	}
	return models, nil
}

func openAICompatibleModelsBaseURL(provider, baseURL string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURLForProvider(provider)
	}
	if baseURL == "" {
		return ""
	}
	if provider == "deepseek" {
		return baseURL
	}
	return normalizedOpenAIBaseURL(nativeModelProfile{Provider: provider, BaseURL: baseURL})
}

func normalizeModelList(provider string, rawModels []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(rawModels))
	models := make([]map[string]any, 0, len(rawModels))
	for _, raw := range rawModels {
		id := fallbackString(trimString(raw["id"]), trimString(raw["name"]))
		if id == "" {
			id = trimString(raw["model"])
		}
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		model := map[string]any{
			"id":       id,
			"name":     fallbackString(trimString(raw["name"]), id),
			"provider": provider,
		}
		for _, key := range []string{"context_length", "context_window", "max_output_tokens", "temperature", "top_p", "top_k", "reasoning_mode", "reasoning_modes", "reasoning_effort_options"} {
			if value, ok := raw[key]; ok {
				model[key] = value
			}
		}
		models = append(models, model)
	}
	return models
}
