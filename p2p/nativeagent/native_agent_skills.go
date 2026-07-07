package nativeagent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func (r *Runtime) skillsList(ctx context.Context) (map[string]any, error) {
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"skills": configList(config, "skills")}, nil
}

func (r *Runtime) skillInstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	if err := r.ensureDataDirs(); err != nil {
		return nil, err
	}
	item := nestedOrSelf(params, "skill")
	id := sanitizeNativeID(fallbackString(trimString(item["id"]), trimString(item["name"])))
	if id == "" {
		id = sanitizeNativeID(fallbackString(trimString(item["path"]), trimString(item["url"])))
	}
	if id == "" {
		id = randomToken("skill")
	}
	content := trimString(item["content"])
	sourceURL := trimString(item["url"])
	if content == "" {
		sourceURL = fallbackString(sourceURL, githubRawSkillURL(item))
		if sourceURL == "" {
			return nil, fmt.Errorf("skill content or url is required")
		}
		fetched, err := r.fetchText(ctx, sourceURL)
		if err != nil {
			return nil, err
		}
		content = fetched
	}
	if !strings.Contains(strings.ToLower(content), "skill") {
		return nil, fmt.Errorf("skill content must look like SKILL.md")
	}
	dir := filepath.Join(r.dataDir, "skills", id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	file := filepath.Join(dir, "SKILL.md")
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		return nil, err
	}
	record := cloneAnyMap(item)
	delete(record, "content")
	record["id"] = id
	record["path"] = file
	record["source_url"] = sourceURL
	if _, ok := record["enabled"]; !ok {
		record["enabled"] = true
	}
	record["installed_at"] = time.Now().UTC().UnixMilli()
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		config["skills"] = upsertConfigRecord(configList(config, "skills"), record)
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true, "skill": record}, nil
}

func (r *Runtime) skillSetEnabled(ctx context.Context, params map[string]any, enabled bool) (map[string]any, error) {
	id := sanitizeNativeID(fallbackString(trimString(params["id"]), trimString(params["name"])))
	if id == "" {
		return nil, fmt.Errorf("skill id is required")
	}
	var updated map[string]any
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, "skills")
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				record["enabled"] = enabled
				updated = record
				break
			}
		}
		config["skills"] = records
	}); err != nil {
		return nil, err
	}
	if updated == nil {
		return nil, fmt.Errorf("skill %q is not installed", id)
	}
	return map[string]any{"ok": true, "skill": updated}, nil
}

func (r *Runtime) skillUninstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	id := sanitizeNativeID(fallbackString(trimString(params["id"]), trimString(params["name"])))
	if id == "" {
		return nil, fmt.Errorf("skill id is required")
	}
	removed := false
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, "skills")
		filtered := records[:0]
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				removed = true
				continue
			}
			filtered = append(filtered, record)
		}
		config["skills"] = filtered
	}); err != nil {
		return nil, err
	}
	_ = os.RemoveAll(filepath.Join(r.dataDir, "skills", id))
	return map[string]any{"ok": removed, "id": id}, nil
}

func (r *Runtime) enabledSkillsPrompt(ctx context.Context, config map[string]any) string {
	records := configList(config, "skills")
	parts := make([]string, 0, len(records))
	for _, record := range records {
		if !boolParam(record["enabled"]) {
			continue
		}
		file := trimString(record["path"])
		if file == "" {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(data))
		if text == "" {
			continue
		}
		parts = append(parts, "Enabled skill "+trimString(record["id"])+":\n"+text)
	}
	return strings.Join(parts, "\n\n")
}

func githubRawSkillURL(item map[string]any) string {
	repo := strings.TrimSpace(trimString(item["repo_url"]))
	if repo == "" || !strings.Contains(repo, "github.com/") {
		return ""
	}
	repo = strings.TrimSuffix(repo, ".git")
	parts := strings.Split(strings.Trim(strings.TrimPrefix(strings.TrimPrefix(repo, "https://github.com/"), "git@github.com:"), "/"), "/")
	if len(parts) < 2 {
		return ""
	}
	ref := fallbackString(trimString(item["ref"]), "main")
	skillPath := strings.Trim(trimString(item["path"]), "/")
	if skillPath == "" {
		skillPath = "SKILL.md"
	} else if !strings.HasSuffix(strings.ToLower(skillPath), ".md") {
		skillPath = path.Join(skillPath, "SKILL.md")
	}
	return "https://raw.githubusercontent.com/" + parts[0] + "/" + parts[1] + "/" + ref + "/" + skillPath
}

func (r *Runtime) fetchText(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("fetch %s returned %d", url, resp.StatusCode)
	}
	return string(body), nil
}
