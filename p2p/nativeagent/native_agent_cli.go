package nativeagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
		filename := sanitizeRuntimeFilename(fallbackString(trimString(tool["filename"]), id), id)
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
		if result["ok"] != true {
			return nil, fmt.Errorf("runtime install command failed with exit code %v: %s", result["exit_code"], strings.TrimSpace(trimString(result["stderr"])))
		}
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
	prepareRuntimeCommand(cmd)
	done := make(chan struct{})
	defer close(done)
	go killRuntimeCommandOnCancel(runCtx, cmd, done)
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
	shell, shellArgs := runtimeShell(command)
	cmd := exec.CommandContext(runCtx, shell, shellArgs...)
	cmd.Dir = filepath.Join(r.dataDir, "runtime")
	cmd.Env = runtimeEnv(r.dataDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	prepareRuntimeCommand(cmd)
	done := make(chan struct{})
	defer close(done)
	go killRuntimeCommandOnCancel(runCtx, cmd, done)
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

func runtimeShell(command string) (string, []string) {
	if bash, err := exec.LookPath("bash"); err == nil {
		return bash, []string{"-lc", command}
	}
	if sh, err := exec.LookPath("sh"); err == nil {
		return sh, []string{"-c", command}
	}
	if runtime.GOOS == "windows" {
		if comspec := strings.TrimSpace(os.Getenv("COMSPEC")); comspec != "" {
			return comspec, []string{"/d", "/s", "/c", command}
		}
		return "cmd.exe", []string{"/d", "/s", "/c", command}
	}
	return "sh", []string{"-c", command}
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
		for _, candidateName := range executableCandidateNames(command) {
			candidate := filepath.Join(dir, candidateName)
			if info, err := os.Stat(candidate); err == nil && runtimeExecutable(info) {
				return candidate, nil
			}
		}
	}
	return "", exec.ErrNotFound
}

func executableCandidateNames(command string) []string {
	if runtime.GOOS != "windows" || filepath.Ext(command) != "" {
		return []string{command}
	}
	exts := strings.Split(os.Getenv("PATHEXT"), ";")
	if len(exts) == 0 || strings.TrimSpace(strings.Join(exts, "")) == "" {
		exts = []string{".COM", ".EXE", ".BAT", ".CMD"}
	}
	candidates := make([]string, 0, len(exts)+1)
	candidates = append(candidates, command)
	for _, ext := range exts {
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		candidates = append(candidates, command+ext)
		candidates = append(candidates, command+strings.ToLower(ext))
	}
	return candidates
}

func runtimeExecutable(info os.FileInfo) bool {
	if info.IsDir() {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	return info.Mode()&0o111 != 0
}

func sanitizeRuntimeFilename(value, fallback string) string {
	value = strings.TrimSpace(filepath.Base(value))
	if value == "." || value == string(os.PathSeparator) {
		value = ""
	}
	if value == "" {
		value = sanitizeNativeID(fallback)
	}
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(value) {
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.'
		if !allowed {
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
			continue
		}
		b.WriteRune(r)
		lastDash = r == '-'
	}
	filename := strings.Trim(b.String(), "-_.")
	if filename == "" {
		filename = sanitizeNativeID(fallback)
	}
	return filename
}
