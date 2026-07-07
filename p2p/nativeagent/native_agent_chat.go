package nativeagent

import (
	"context"
	"strings"
)

func (r *Runtime) chat(ctx context.Context, params map[string]any) (map[string]any, error) {
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return nil, err
	}
	profile := r.resolveModelProfile(config, params)
	if profile.APIKey == "" {
		return map[string]any{
			"ok":          false,
			"native":      true,
			"framework":   "eino",
			"provider":    profile.Provider,
			"model":       profile.Model,
			"error":       "model_profile.api_key is required",
			"model_ready": false,
		}, nil
	}
	run, err := r.prepareEinoRun(ctx, config, params, profile)
	if err != nil {
		return nil, err
	}
	tools, cleanup, err := r.enabledEinoTools(ctx, config, params)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	text, toolCalls, produced, err := r.runEinoAgent(ctx, profile, run.inputMessages, run.session, tools)
	if err != nil {
		return nil, err
	}
	r.rememberEinoMessages(ctx, config, params, profile, run, produced)
	return map[string]any{
		"ok":         true,
		"native":     true,
		"framework":  "eino",
		"provider":   profile.Provider,
		"model":      profile.Model,
		"text":       text,
		"tool_calls": toolCalls,
	}, nil
}

func (r *Runtime) agentSystemPrompt(ctx context.Context, config map[string]any, params map[string]any, extra string) string {
	systemPrompt := strings.TrimSpace(pluginConfigString(config, "system_prompt"))
	if requestPrompt := trimString(params["system_prompt"]); requestPrompt != "" {
		systemPrompt = appendPromptBlock(systemPrompt, requestPrompt)
	}
	if skillsPrompt := r.enabledSkillsPrompt(ctx, config); skillsPrompt != "" {
		systemPrompt = appendPromptBlock(systemPrompt, skillsPrompt)
	}
	if strings.TrimSpace(extra) != "" {
		systemPrompt = appendPromptBlock(systemPrompt, strings.TrimSpace(extra))
	}
	return systemPrompt
}

func appendPromptBlock(base, block string) string {
	block = strings.TrimSpace(block)
	if block == "" {
		return strings.TrimSpace(base)
	}
	base = strings.TrimSpace(base)
	if base == "" {
		return block
	}
	return base + "\n\n" + block
}
