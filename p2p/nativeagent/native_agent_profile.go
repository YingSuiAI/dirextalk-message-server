package nativeagent

import (
	"encoding/json"
	"strconv"
	"strings"
)

type nativeModelProfile struct {
	Provider        string
	Model           string
	BaseURL         string
	APIKey          string
	Temperature     *float64
	TopP            *float64
	MaxOutputTokens int
	ContextWindow   int
	ReasoningMode   string
}

func (r *Runtime) resolveModelProfile(config map[string]any, params map[string]any) nativeModelProfile {
	raw := map[string]any{}
	for _, key := range []string{"provider", "model", "base_url", "temperature", "top_p", "max_output_tokens", "context_window", "reasoning_mode"} {
		if value, ok := config[key]; ok {
			raw[key] = value
		}
	}
	if profileID := trimString(params["model_profile_id"]); profileID != "" {
		if saved := savedAgentModelProfileByID(config, profileID); saved != nil {
			for key, value := range saved {
				raw[key] = value
			}
		}
	}
	if profile, ok := params["model_profile"].(map[string]any); ok {
		for key, value := range profile {
			raw[key] = value
		}
	}
	provider := strings.ToLower(fallbackString(pluginConfigString(raw, "provider"), "deepseek"))
	model := fallbackString(pluginConfigString(raw, "model"), defaultModelForProvider(provider))
	baseURL := strings.TrimRight(pluginConfigString(raw, "base_url"), "/")
	if baseURL == "" {
		baseURL = defaultBaseURLForProvider(provider)
	}
	return nativeModelProfile{
		Provider:        provider,
		Model:           model,
		BaseURL:         baseURL,
		APIKey:          trimString(raw["api_key"]),
		Temperature:     optionalFloat(raw["temperature"]),
		TopP:            optionalFloat(raw["top_p"]),
		MaxOutputTokens: int(int64Param(raw["max_output_tokens"])),
		ContextWindow:   int(int64Param(raw["context_window"])),
		ReasoningMode:   normalizedReasoningMode(raw["reasoning_mode"]),
	}
}

func normalizedReasoningMode(value any) string {
	mode := strings.ToLower(strings.TrimSpace(trimString(value)))
	switch mode {
	case "", "none", "off":
		return ""
	case "minimal", "low", "medium", "high", "xhigh", "auto", "fast", "deep":
		return mode
	default:
		return mode
	}
}

func defaultModelForProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "openai":
		return "gpt-4.1-mini"
	case "anthropic":
		return "claude-3-5-sonnet-latest"
	case "deepseek":
		return "deepseek-v4-pro"
	case "openrouter":
		return "openai/gpt-4.1-mini"
	case "openai_compatible", "litellm":
		return "gpt-4.1-mini"
	default:
		return "deepseek-v4-pro"
	}
}

func defaultBaseURLForProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "openai":
		return "https://api.openai.com/v1"
	case "anthropic":
		return "https://api.anthropic.com"
	case "deepseek":
		return "https://api.deepseek.com"
	case "openrouter":
		return "https://openrouter.ai/api/v1"
	case "openai_compatible", "litellm":
		return "http://localhost:4000/v1"
	default:
		return ""
	}
}

func optionalFloat(value any) *float64 {
	switch v := value.(type) {
	case float64:
		return &v
	case float32:
		n := float64(v)
		return &n
	case int:
		n := float64(v)
		return &n
	case int64:
		n := float64(v)
		return &n
	case json.Number:
		if n, err := v.Float64(); err == nil {
			return &n
		}
	case string:
		if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return &n
		}
	}
	return nil
}
