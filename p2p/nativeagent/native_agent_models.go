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
		for _, key := range []string{"context_length", "context_window", "max_output_tokens", "temperature", "top_p", "top_k", "reasoning_mode", "reasoning_modes"} {
			if value, ok := raw[key]; ok {
				model[key] = value
			}
		}
		applyKnownModelDefaults(provider, id, model)
		models = append(models, model)
	}
	return models
}

func applyKnownModelDefaults(provider, id string, model map[string]any) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	normalizedID := strings.ToLower(strings.TrimSpace(id))
	if normalizedID == "" {
		return
	}
	setDefault := func(key string, value any) {
		if _, ok := model[key]; !ok {
			model[key] = value
		}
	}
	setDefault("temperature", 1.0)
	setDefault("top_p", 1.0)
	switch provider {
	case "openai":
		switch {
		case strings.Contains(normalizedID, "gpt-4.1"):
			setDefault("context_length", int64(1047576))
			setDefault("max_output_tokens", int64(32768))
		case strings.Contains(normalizedID, "gpt-4o"):
			setDefault("context_length", int64(128000))
			setDefault("max_output_tokens", int64(16384))
		case strings.HasPrefix(normalizedID, "gpt-5"):
			setDefault("context_length", int64(400000))
			setDefault("max_output_tokens", int64(128000))
			setDefault("reasoning_modes", []string{"low", "medium", "high", "xhigh"})
			setDefault("reasoning_mode", "medium")
		case strings.HasPrefix(normalizedID, "o"):
			setDefault("context_length", int64(200000))
			setDefault("max_output_tokens", int64(100000))
			setDefault("reasoning_modes", []string{"low", "medium", "high", "xhigh"})
			setDefault("reasoning_mode", "medium")
		}
	case "deepseek":
		switch {
		case strings.Contains(normalizedID, "reasoner") || strings.Contains(normalizedID, "r1"):
			setDefault("context_length", int64(64000))
			setDefault("max_output_tokens", int64(8192))
			setDefault("reasoning_modes", []string{"auto"})
			setDefault("reasoning_mode", "auto")
		case strings.Contains(normalizedID, "v4"):
			setDefault("context_length", int64(128000))
			setDefault("max_output_tokens", int64(8192))
		}
	}
}
