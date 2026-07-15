package nativeagent

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultNativeAgentDataDir = "/var/dirextalk-message-server/agent"
	nativeAgentHTTPTimeout    = 90 * time.Second
	nativeAgentToolCallLimit  = 48
)

type Config struct {
	DataDir           string
	Store             ConfigStore
	Tools             []Tool
	CloudPlanner      CloudPlanner
	CloudStatusReader CloudStatusReader
	CloudRecipeReader CloudRecipeReader
	HTTPClient        *http.Client
}

type ConfigStore interface {
	Load(ctx context.Context) (map[string]any, bool, error)
	Save(ctx context.Context, config map[string]any) error
}

// CloudPlanner is the narrow Eino-Agent-to-control-plane boundary. It can
// persist a research-only goal, but deliberately has no AWS credential,
// pricing, approval, provision, network, or destroy method.
type CloudPlanner interface {
	CreateResearchGoal(context.Context, string, string, string) (map[string]any, error)
}

// CloudStatusReader exposes only a de-secretsed Cloud projection to the
// Eino Agent. It has no approval, secret, provider, or lifecycle mutation
// method, so cloud dialogue can report progress without becoming a control
// plane bypass.
type CloudStatusReader interface {
	ReadCloudStatus(context.Context) (map[string]any, error)
}

// CloudRecipeReader exposes only owner-scoped, de-secreted private Recipe
// summaries. It cannot select a Recipe or mutate a Goal, Plan, or resource.
type CloudRecipeReader interface {
	ReadCloudRecipes(context.Context) ([]CloudRecipeRecommendation, error)
}

type CloudRecipeResourceSummary struct {
	MinVCPU         uint16 `json:"min_vcpu"`
	MinMemoryMiB    uint32 `json:"min_memory_mib"`
	MinGPUMemoryMiB uint32 `json:"min_gpu_memory_mib"`
	MinDiskGiB      uint32 `json:"min_disk_gib"`
	Architecture    string `json:"architecture"`
}

type CloudRecipeRecommendation struct {
	RecipeID  string                     `json:"recipe_id"`
	Name      string                     `json:"name"`
	Version   string                     `json:"version"`
	Maturity  string                     `json:"maturity"`
	Revision  int64                      `json:"revision"`
	Resources CloudRecipeResourceSummary `json:"resources"`
}

type Event struct {
	Event string
	Data  map[string]any
}

type Runtime struct {
	store             ConfigStore
	dataDir           string
	client            *http.Client
	tools             []Tool
	cloudPlanner      CloudPlanner
	cloudStatusReader CloudStatusReader
	cloudRecipeReader CloudRecipeReader
}

func New(config Config) *Runtime {
	dataDir := strings.TrimSpace(config.DataDir)
	if dataDir == "" {
		dataDir = strings.TrimSpace(os.Getenv("P2P_NATIVE_AGENT_DATA_DIR"))
	}
	if dataDir == "" {
		dataDir = defaultNativeAgentDataDir
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: nativeAgentHTTPTimeout}
	}
	return &Runtime{
		store:             config.Store,
		dataDir:           filepath.Clean(dataDir),
		client:            client,
		tools:             append([]Tool{}, config.Tools...),
		cloudPlanner:      config.CloudPlanner,
		cloudStatusReader: config.CloudStatusReader,
		cloudRecipeReader: config.CloudRecipeReader,
	}
}

func (r *Runtime) Apply(ctx context.Context, action string) error {
	switch strings.TrimSpace(action) {
	case "install", "enable":
		return r.ensureDataDirs()
	case "disable", "uninstall":
		return nil
	default:
		return fmt.Errorf("native agent action %q is not supported", action)
	}
}

func (r *Runtime) Invoke(ctx context.Context, action string, params map[string]any) (map[string]any, error) {
	action = strings.TrimSpace(action)
	switch action {
	case "agent.chat":
		return r.chat(ctx, params)
	case "agent.models.list":
		return r.modelsList(ctx, params)
	case "agent.runtime.inspect":
		return r.runtimeInspect(ctx)
	case "agent.runtime.install":
		return r.runtimeInstall(ctx, params)
	case "agent.runtime.which":
		return r.runtimeWhich(ctx, params)
	case "agent.runtime.run":
		return r.runtimeRun(ctx, params)
	case "agent.skills.list":
		return r.skillsList(ctx)
	case "agent.skills.install":
		return r.skillInstall(ctx, params)
	case "agent.skills.enable":
		return r.skillSetEnabled(ctx, params, true)
	case "agent.skills.disable":
		return r.skillSetEnabled(ctx, params, false)
	case "agent.skills.uninstall":
		return r.skillUninstall(ctx, params)
	case "agent.skills.registry.search":
		return map[string]any{"skills": []any{}}, nil
	case "agent.mcp.servers.list":
		return r.mcpServersList(ctx)
	case "agent.mcp.servers.install":
		return r.mcpServerInstall(ctx, params)
	case "agent.mcp.servers.enable":
		return r.mcpServerSetEnabled(ctx, params, true)
	case "agent.mcp.servers.disable":
		return r.mcpServerSetEnabled(ctx, params, false)
	case "agent.mcp.servers.uninstall":
		return r.mcpServerUninstall(ctx, params)
	case "agent.mcp.registry.search":
		return map[string]any{"servers": []any{}}, nil
	case "agent.knowledge.config.get", "agent.knowledge.config.update", "agent.knowledge.sources.list",
		"agent.knowledge.sources.delete", "agent.knowledge.upload.start", "agent.knowledge.upload.chunk",
		"agent.knowledge.upload.finish", "agent.knowledge.memory.create", "agent.knowledge.search",
		"agent.knowledge.status":
		return map[string]any{"supported": false, "status": "unsupported"}, nil
	case "agent.context.compress":
		return r.compressMemory(ctx, params)
	case "agent.config.propose_patch":
		return map[string]any{"ok": true, "patch": map[string]any{}}, nil
	default:
		if strings.HasPrefix(action, "agent.") {
			return r.invokeDirectTool(ctx, action, params)
		}
		return nil, fmt.Errorf("native agent action %q is not implemented", action)
	}
}

func (r *Runtime) Stream(ctx context.Context, action string, params map[string]any, emit func(Event) error) error {
	if strings.TrimSpace(action) != "agent.chat.stream" {
		return fmt.Errorf("native agent stream action %q is not implemented", action)
	}
	ctx, err := prepareCloudDialogueRequest(ctx, params)
	if err != nil {
		return err
	}
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return err
	}
	profile := r.resolveModelProfile(config, params)
	if profile.APIKey == "" {
		if err := emit(Event{Event: "error", Data: map[string]any{"error": "model_profile.api_key is required"}}); err != nil {
			return err
		}
		return emit(Event{Event: "done", Data: map[string]any{"ok": false, "native": true, "framework": "eino", "model_ready": false}})
	}
	run, err := r.prepareEinoRun(ctx, config, params, profile)
	if err != nil {
		return err
	}
	tools, cleanup, err := r.enabledEinoTools(ctx, config, params)
	if err != nil {
		return err
	}
	defer cleanup()
	text, reasoning, toolCalls, produced, err := r.streamEinoAgent(ctx, profile, run.inputMessages, run.session, tools, emit, run.maxSteps)
	if err != nil {
		return err
	}
	r.rememberEinoMessages(ctx, config, params, profile, run, produced)
	trace := buildAgentTrace(run, produced, toolCalls, text)
	if err := emit(Event{Event: "trace", Data: trace}); err != nil {
		return err
	}
	done := map[string]any{
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
	if reasoning != "" {
		done["reasoning_content"] = reasoning
	}
	if workload := cloudWorkloadSummaryFromContext(ctx); workload != nil {
		done["cloud_workload"] = workload
	}
	return emit(Event{Event: "done", Data: done})
}

func (r *Runtime) ensureDataDirs() error {
	for _, dir := range []string{
		r.dataDir,
		filepath.Join(r.dataDir, "skills"),
		filepath.Join(r.dataDir, "mcp"),
		filepath.Join(r.dataDir, "runtime"),
		filepath.Join(r.dataDir, "runtime", "bin"),
		filepath.Join(r.dataDir, "runtime", "home"),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) runtimeInspect(ctx context.Context) (map[string]any, error) {
	config, exists, err := r.agentConfig(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":              true,
		"native":          true,
		"framework":       "eino",
		"configured":      exists,
		"data_dir":        r.dataDir,
		"skills":          configList(config, "skills"),
		"built_in_skills": r.builtInSkills(),
		"mcp_servers":     configList(config, "mcp_servers"),
		"runtime_tools":   configList(config, "runtime_tools"),
		"time":            time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func (r *Runtime) agentConfig(ctx context.Context) (map[string]any, bool, error) {
	if r.store == nil {
		return map[string]any{}, false, nil
	}
	config, exists, err := r.store.Load(ctx)
	return cloneAnyMap(config), exists, err
}

func (r *Runtime) updateAgentConfig(ctx context.Context, mutate func(map[string]any)) error {
	if r.store == nil {
		return fmt.Errorf("config store is unavailable")
	}
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return err
	}
	mutate(config)
	return r.store.Save(ctx, sanitizeConfig(config))
}
