package nativeagent

import (
	"context"
	"net/url"
	"strings"

	deepseekmodel "github.com/cloudwego/eino-ext/components/model/deepseek"
	openaimodel "github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
)

func (r *Runtime) newEinoChatModel(ctx context.Context, profile nativeModelProfile) (model.ToolCallingChatModel, error) {
	switch profile.Provider {
	case "deepseek":
		config := &deepseekmodel.ChatModelConfig{
			APIKey:     profile.APIKey,
			Model:      profile.Model,
			BaseURL:    profile.BaseURL,
			HTTPClient: r.client,
			MaxTokens:  profile.MaxOutputTokens,
		}
		if profile.Temperature != nil {
			config.Temperature = float32(*profile.Temperature)
		}
		if profile.TopP != nil {
			config.TopP = float32(*profile.TopP)
		}
		return deepseekmodel.NewChatModel(ctx, config)
	case "anthropic":
		return newAnthropicDirectChatModel(r, profile), nil
	default:
		config := &openaimodel.ChatModelConfig{
			APIKey:     profile.APIKey,
			Model:      profile.Model,
			BaseURL:    normalizedOpenAIBaseURL(profile),
			HTTPClient: r.client,
		}
		if profile.MaxOutputTokens > 0 {
			config.MaxTokens = &profile.MaxOutputTokens
		}
		if profile.Temperature != nil {
			v := float32(*profile.Temperature)
			config.Temperature = &v
		}
		if profile.TopP != nil {
			v := float32(*profile.TopP)
			config.TopP = &v
		}
		return openaimodel.NewChatModel(ctx, config)
	}
}

func normalizedOpenAIBaseURL(profile nativeModelProfile) string {
	base := strings.TrimRight(profile.BaseURL, "/")
	if base == "" {
		base = defaultBaseURLForProvider(profile.Provider)
	}
	if base == "" {
		return base
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Path != "" || profile.Provider == "openai" {
		return base
	}
	return base + "/v1"
}
