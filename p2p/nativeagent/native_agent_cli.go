package nativeagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (r *Runtime) runtimeInstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	if err := r.ensureDataDirs(); err != nil {
		return nil, err
	}
	tool := nestedOrSelf(params, "tool")
	id := sanitizeNativeID(fallbackString(trimString(tool["id"]), fallbackString(trimString(tool["name"]), trimString(tool["command"]))))
	if id == "" {
		id = randomToken("tool")
	}
	binDir := filepath.Join(r.dataDir, "runtime", "bin")
	if content := trimString(tool["content"]); content != "" {
		filename := sanitizeNativeID(fallbackString(trimString(tool["filename"]), id))
		target := filepath.Join(binDir, filename)
		if err := os.WriteFile(target, []byte(content), 0o700); err != nil {
			return nil, err
		}
		tool["path"] = target
		tool["command"] = target
	}
	var installResult map[string]any
	if installCommand := trimString(tool["install_command"]); installCommand != "" {
		result, err := r.runShell(ctx, installCommand, durationSeconds(tool["timeout_seconds"], 60))
		if err != nil {
			return nil, err
		}
		installResult = result
	}
	record := cloneAnyMap(tool)
	record["id"] = id
	record["installed_at"] = time.Now().UTC().UnixMilli()
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		config["runtime_tools"] = upsertConfigRecord(configList(config, "runtime_tools"), record)
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true, "tool": record, "install": installResult}, nil
}

func (r *Runtime) runtimeWhich(ctx context.Context, params map[string]any) (map[string]any, error) {
	command := trimString(params["command"])
	if command == "" {
		command = trimString(params["name"])
	}
	if command == "" {
		return nil, fmt.Errorf("command is required")
	}
	path, err := exec.LookPath(command)
	if err != nil {
		path, err = lookPathInPATH(command, runtimePATH(r.dataDir))
	}
	if err != nil {
		return map[string]any{"ok": false, "command": command}, nil
	}
	return map[string]any{"ok": true, "command": command, "path": path}, nil
}

func (r *Runtime) runtimeRun(ctx context.Context, params map[string]any) (map[string]any, error) {
	command := trimString(params["command"])
	if command == "" {
		command = trimString(params["path"])
	}
	if command == "" {
		return nil, fmt.Errorf("command is required")
	}
	if !filepath.IsAbs(command) && !strings.Contains(command, string(os.PathSeparator)) {
		if resolved, err := lookPathInPATH(command, runtimePATH(r.dataDir)); err == nil {
			command = resolved
		}
	}
	args := stringSliceParam(params["args"])
	timeout := durationSeconds(params["timeout_seconds"], 30)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, command, args...)
	cmd.Dir = filepath.Join(r.dataDir, "runtime")
	cmd.Env = runtimeEnv(r.dataDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if runCtx.Err() != nil {
			return map[string]any{"ok": false, "timed_out": true, "stdout": stdout.String(), "stderr": stderr.String(), "exit_code": -1}, nil
		} else {
			return nil, err
		}
	}
	return map[string]any{
		"ok":        exitCode == 0,
		"command":   command,
		"args":      args,
		"stdout":    stdout.String(),
		"stderr":    stderr.String(),
		"exit_code": exitCode,
	}, nil
}

func (r *Runtime) runShell(ctx context.Context, command string, timeout time.Duration) (map[string]any, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "bash", "-lc", command)
	cmd.Dir = filepath.Join(r.dataDir, "runtime")
	cmd.Env = runtimeEnv(r.dataDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if runCtx.Err() != nil {
			return map[string]any{"ok": false, "timed_out": true, "stdout": stdout.String(), "stderr": stderr.String(), "exit_code": -1}, nil
		} else {
			return nil, err
		}
	}
	return map[string]any{"ok": exitCode == 0, "stdout": stdout.String(), "stderr": stderr.String(), "exit_code": exitCode}, nil
}

func runtimePATH(dataDir string) string {
	binDir := filepath.Join(dataDir, "runtime", "bin")
	if current := os.Getenv("PATH"); current != "" {
		return binDir + string(os.PathListSeparator) + current
	}
	return binDir
}

func runtimeEnv(dataDir string) []string {
	env := os.Environ()
	pathSet := false
	for i, value := range env {
		if strings.HasPrefix(value, "PATH=") {
			env[i] = "PATH=" + runtimePATH(dataDir)
			pathSet = true
			break
		}
	}
	if !pathSet {
		env = append(env, "PATH="+runtimePATH(dataDir))
	}
	return env
}

func lookPathInPATH(command, pathValue string) (string, error) {
	for _, dir := range filepath.SplitList(pathValue) {
		if dir == "" {
			continue
		}
		candidate := filepath.Join(dir, command)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", exec.ErrNotFound
}
