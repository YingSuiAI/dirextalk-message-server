package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Runner interface {
	ApplyPlugin(context.Context, RunnerOperation) error
	InvokePlugin(context.Context, InvokeRequest) (map[string]any, error)
	StreamPlugin(context.Context, InvokeRequest, func(StreamEvent) error) error
}

type RunnerOperation struct {
	Action        string
	PluginID      string
	Name          string
	Version       string
	Image         string
	Digest        string
	ContainerName string
	Network       string
	Config        map[string]any
	Env           map[string]string
	Volumes       []string
}

type InvokeRequest struct {
	PluginID      string
	ContainerName string
	Action        string
	Params        map[string]any
}

type StreamEvent struct {
	Event string
	Data  map[string]any
}

type NoopRunner struct{}

func (NoopRunner) ApplyPlugin(context.Context, RunnerOperation) error { return nil }

func (NoopRunner) InvokePlugin(context.Context, InvokeRequest) (map[string]any, error) {
	return nil, fmt.Errorf("plugin runner is not enabled")
}

func (NoopRunner) StreamPlugin(context.Context, InvokeRequest, func(StreamEvent) error) error {
	return fmt.Errorf("plugin runner is not enabled")
}

func (NoopRunner) PluginRunnerEnabled() bool { return false }

func NewEnvironmentRunner() Runner {
	if !envBool("P2P_PLUGIN_DOCKER_ENABLED") {
		return NoopRunner{}
	}
	return DockerRunner{
		binary:  fallback(strings.TrimSpace(os.Getenv("P2P_PLUGIN_DOCKER_BIN")), "docker"),
		network: strings.TrimSpace(os.Getenv("P2P_PLUGIN_DOCKER_NETWORK")),
	}
}

func envBool(name string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func RunnerEnabled(runner Runner) bool {
	if runner == nil {
		return false
	}
	if reporter, ok := runner.(interface{ PluginRunnerEnabled() bool }); ok {
		return reporter.PluginRunnerEnabled()
	}
	return true
}

type DockerRunner struct {
	binary  string
	network string
	client  *http.Client
}

func NewDockerRunner(binary, network string, client *http.Client) Runner {
	return DockerRunner{binary: fallback(strings.TrimSpace(binary), "docker"), network: strings.TrimSpace(network), client: client}
}

func (DockerRunner) PluginRunnerEnabled() bool { return true }

func (r DockerRunner) ApplyPlugin(ctx context.Context, op RunnerOperation) error {
	if err := ValidateOfficialOperation(op); err != nil {
		return err
	}
	imageRef := ImageReference(op.Image, op.Digest)
	containerName := op.ContainerName
	if containerName == "" {
		containerName = ContainerName(op.PluginID)
	}
	network := op.Network
	if network == "" {
		network = r.network
	}
	switch op.Action {
	case "install":
		return r.run(ctx, "pull", imageRef)
	case "enable":
		_ = r.run(ctx, "rm", "-f", containerName)
		envFile, cleanup, err := writeEnvFile(op.Env)
		if err != nil {
			return err
		}
		if cleanup != nil {
			defer cleanup()
		}
		args := []string{
			"run", "-d",
			"--name", containerName,
			"--label", "io.dirextalk.plugin.id=" + op.PluginID,
			"--label", "io.dirextalk.plugin.official=true",
			"--restart", "unless-stopped",
		}
		if envFile != "" {
			args = append(args, "--env-file", envFile)
		}
		if network != "" {
			args = append(args, "--network", network)
		}
		for _, volume := range op.Volumes {
			args = append(args, "-v", volume)
		}
		args = append(args, imageRef)
		if err := r.run(ctx, args...); err != nil {
			return err
		}
		return r.waitReady(ctx, containerName)
	case "disable":
		return r.run(ctx, "stop", containerName)
	case "uninstall":
		return r.run(ctx, "rm", "-f", containerName)
	default:
		return fmt.Errorf("unsupported plugin runner action %q", op.Action)
	}
}

func (r DockerRunner) InvokePlugin(ctx context.Context, req InvokeRequest) (map[string]any, error) {
	body, err := r.invokeHTTP(ctx, req)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(body).Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (r DockerRunner) StreamPlugin(ctx context.Context, req InvokeRequest, emit func(StreamEvent) error) error {
	if strings.TrimSpace(req.PluginID) == "" || !strings.HasPrefix(req.PluginID, "io.dirextalk.") {
		return fmt.Errorf("plugin id %q is not official", req.PluginID)
	}
	containerName := strings.TrimSpace(req.ContainerName)
	if containerName == "" {
		containerName = ContainerName(req.PluginID)
	}
	conn, _, err := websocket.Dial(ctx, "ws://"+containerName+":8080/ws", nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err := wsjson.Write(ctx, conn, map[string]any{
		"type":   "plugin.invoke.stream",
		"action": req.Action,
		"params": req.Params,
	}); err != nil {
		return err
	}
	for {
		var frame map[string]any
		if err := wsjson.Read(ctx, conn, &frame); err != nil {
			return err
		}
		switch stringValue(frame["type"]) {
		case "plugin.stream.event":
			data, _ := frame["data"].(map[string]any)
			if data == nil {
				data = map[string]any{}
			}
			if err := emit(StreamEvent{
				Event: fallback(stringValue(frame["event"]), "message"),
				Data:  data,
			}); err != nil {
				return err
			}
		case "plugin.stream.done":
			data, _ := frame["data"].(map[string]any)
			if data == nil {
				data = map[string]any{}
			}
			if err := emit(StreamEvent{Event: "done", Data: data}); err != nil {
				return err
			}
			return nil
		case "plugin.stream.error":
			return fmt.Errorf("plugin stream failed: %s", fallback(stringValue(frame["error"]), "M_UNKNOWN"))
		}
	}
}

func (r DockerRunner) invokeHTTP(ctx context.Context, req InvokeRequest) (io.ReadCloser, error) {
	if strings.TrimSpace(req.PluginID) == "" || !strings.HasPrefix(req.PluginID, "io.dirextalk.") {
		return nil, fmt.Errorf("plugin id %q is not official", req.PluginID)
	}
	containerName := strings.TrimSpace(req.ContainerName)
	if containerName == "" {
		containerName = ContainerName(req.PluginID)
	}
	payload, err := json.Marshal(map[string]any{"action": req.Action, "params": req.Params})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+containerName+":8080/invoke", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := r.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("plugin invoke failed: %s", message)
	}
	return resp.Body, nil
}

func (r DockerRunner) waitReady(ctx context.Context, containerName string) error {
	if strings.TrimSpace(containerName) == "" {
		return nil
	}
	client := r.client
	if client == nil {
		client = http.DefaultClient
	}
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+containerName+":8080/health", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("%s", resp.Status)
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("plugin container %s is not ready: %w", containerName, lastErr)
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r DockerRunner) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, r.binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("docker %s failed: %s", strings.Join(args, " "), message)
	}
	return nil
}

func ValidateOfficialOperation(op RunnerOperation) error {
	switch op.Action {
	case "install", "enable", "disable", "uninstall":
	default:
		return fmt.Errorf("plugin runner action %q is not allowed", op.Action)
	}
	if !strings.HasPrefix(op.PluginID, "io.dirextalk.") {
		return fmt.Errorf("plugin id %q is not official", op.PluginID)
	}
	if !OfficialImage(op.Image) {
		return fmt.Errorf("plugin image %q is not official", op.Image)
	}
	digest := strings.TrimSpace(op.Digest)
	if digest != "" && (!strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64) {
		return fmt.Errorf("plugin digest must be empty or a pinned sha256 digest")
	}
	for _, volume := range op.Volumes {
		if err := validateOfficialVolume(op.PluginID, volume); err != nil {
			return err
		}
	}
	return nil
}

func validateOfficialVolume(pluginID, volume string) error {
	volume = strings.TrimSpace(volume)
	if volume == "" {
		return nil
	}
	if pluginID != OpsPluginID {
		return fmt.Errorf("plugin %s cannot request privileged volume %q", pluginID, volume)
	}
	switch {
	case volume == "/var/run/docker.sock:/var/run/docker.sock":
		return nil
	case strings.HasSuffix(volume, ":/var/lib/dirextalk-ops"):
		source := strings.TrimSuffix(volume, ":/var/lib/dirextalk-ops")
		if source == "" || strings.ContainsAny(source, `/\`) || strings.Contains(source, "..") {
			return fmt.Errorf("invalid ops backup volume %q", volume)
		}
		return nil
	default:
		return fmt.Errorf("ops plugin volume %q is not allowed", volume)
	}
}

func OfficialImage(image string) bool {
	image = strings.TrimSpace(image)
	if image == "" || strings.Contains(image, "@") {
		return false
	}
	return strings.HasPrefix(image, "docker.io/dirextalk/") || strings.HasPrefix(image, "dirextalk/")
}

func ImageReference(image, digest string) string {
	image = strings.TrimSpace(image)
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return image
	}
	return image + "@" + digest
}

var containerSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func ContainerName(pluginID string) string {
	suffix := strings.TrimPrefix(strings.TrimSpace(pluginID), "io.dirextalk.")
	suffix = containerSanitizer.ReplaceAllString(suffix, "-")
	suffix = strings.Trim(suffix, "-_.")
	if suffix == "" {
		suffix = "plugin"
	}
	return "dirextalk-plugin-" + strings.ToLower(suffix)
}

var envNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

func writeEnvFile(env map[string]string) (string, func(), error) {
	if len(env) == 0 {
		return "", nil, nil
	}
	file, err := os.CreateTemp("", "dirextalk-plugin-*.env")
	if err != nil {
		return "", nil, err
	}
	path := file.Name()
	cleanup := func() { _ = os.Remove(path) }
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		cleanup()
		return "", nil, err
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, strings.TrimSpace(key))
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			continue
		}
		if !envNamePattern.MatchString(key) {
			_ = file.Close()
			cleanup()
			return "", nil, fmt.Errorf("invalid plugin env var %q", key)
		}
		value := env[key]
		if strings.ContainsAny(value, "\r\n") {
			_ = file.Close()
			cleanup()
			return "", nil, fmt.Errorf("plugin env var %q contains a newline", key)
		}
		if _, err := fmt.Fprintf(file, "%s=%s\n", key, value); err != nil {
			_ = file.Close()
			cleanup()
			return "", nil, err
		}
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return ""
	}
}
