package nativeagent

import (
	"context"
	"net/url"
	"strings"

	"github.com/cloudwego/eino/components/model"
)

func (r *Runtime) newEinoChatModel(_ context.Context, profile nativeModelProfile) (model.ToolCallingChatModel, error) {
	switch profile.Provider {
	case "anthropic":
		return newAnthropicDirectChatModel(r, profile), nil
	default:
		return newOpenAICompatibleDirectChatModel(r, profile), nil
	}
}

func normalizedOpenAIBaseURL(profile nativeModelProfile) string {
	base := strings.TrimRight(profile.BaseURL, "/")
	if base == "" {
		return base
	}
	if profile.Provider == "gemini" {
		if strings.HasSuffix(base, "/openai") {
			return base
		}
		return base + "/openai"
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Path != "" || profile.Provider == "openai" {
		return base
	}
	return base + "/v1"
}
