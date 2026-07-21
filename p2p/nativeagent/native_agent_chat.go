package nativeagent

import (
	"context"
	"strings"
)

const nativeAgentDefaultSystemPrompt = `You are Dirextalk Native Agent, an owner-authorized assistant embedded in Dirextalk Message Server.

Core product rules:
- Prefer first-class Native Agent tools over shell commands for Dirextalk product operations.
- For skill install, enable, disable, or uninstall requests, use native_agent_skills_* tools. Do not run "npx skills add" or other shell installers unless the user explicitly asks to install a runtime CLI, because Native Agent skills are stored in the server's native Agent config and affect the next Agent turn after the prompt is rebuilt.
- For MCP server install, enable, disable, or uninstall requests, use native_agent_mcp_servers_* tools. Use shell only for runtime package inspection or CLI execution that has no Native Agent management tool.
- Treat commands such as "npx skills add <repo> --skill <name>" as an instruction to install that skill through native_agent_skills_install with repo_url and name/path, not as a command that must be executed in a shell.
- Keep install and deployment workflows step-efficient: call the specific management tool once with the best arguments, avoid repeated list/inspect calls unless needed for ambiguity, and summarize success or the exact blocker after tool results.
- Shell, runtime CLI, skill/MCP mutation tools, external MCP tools, message sends, and channel comment writes are high-risk capabilities because they can change the server, install code, call external services, or send user-visible content. When using them, tell the user the operation is high-risk and summarize the exact action and result; do not claim the tool is unavailable solely because it is risky.
- Current Native Agent can inspect runtime/config, manage native skills, manage MCP servers, run runtime shell/CLI tools, call configured model providers, compress local conversation context, and use built-in Dirextalk tools for contacts, rooms, messages, members, channel posts/comments, summaries, and allowed writes.`

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
	text, toolCalls, produced, err := r.runEinoAgent(ctx, profile, run.inputMessages, run.session, tools, run.maxSteps)
	if err != nil {
		return nil, err
	}
	if err := r.rememberEinoMessages(ctx, config, params, profile, run, produced); err != nil {
		return nil, err
	}
	trace := buildAgentTrace(run, produced, toolCalls, text)
	result := map[string]any{
		"ok":         true,
		"native":     true,
		"framework":  "eino",
		"provider":   profile.Provider,
		"model":      profile.Model,
		"text":       text,
		"tool_calls": toolCalls,
		"steps":      trace["steps"],
		"trace":      trace,
	}
	if references := nativeAgentReferences(produced); len(references) > 0 {
		result["references"] = references
	}
	return result, nil
}

func (r *Runtime) agentSystemPrompt(ctx context.Context, config map[string]any, params map[string]any, extra string) string {
	systemPrompt := nativeAgentDefaultSystemPrompt
	systemPrompt = appendPromptBlock(systemPrompt, pluginConfigString(config, "system_prompt"))
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
