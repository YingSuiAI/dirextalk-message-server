package nativeagent

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
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
		if sourceURL != "" {
			fetched, err := r.fetchText(ctx, sourceURL)
			if err != nil {
				return nil, err
			}
			content = fetched
		} else {
			var lastErr error
			for _, candidateURL := range githubRawSkillURLs(item) {
				fetched, err := r.fetchText(ctx, candidateURL)
				if err == nil {
					sourceURL = candidateURL
					content = fetched
					break
				}
				lastErr = err
			}
			if content == "" {
				if lastErr != nil {
					return nil, fmt.Errorf("fetch skill from repository failed: %w", lastErr)
				}
				return nil, fmt.Errorf("skill content, url, or repo_url is required")
			}
		}
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
	return r.setManagedConfigRecordEnabled(ctx, params, enabled, skillConfigRecord)
}

func (r *Runtime) skillUninstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	return r.uninstallManagedConfigRecord(ctx, params, skillConfigRecord)
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

func githubRawSkillURLs(item map[string]any) []string {
	repo := strings.TrimSpace(trimString(item["repo_url"]))
	parts := githubRepoParts(repo)
	if len(parts) < 2 {
		return nil
	}
	ref := fallbackString(trimString(item["ref"]), "main")
	rawURL := func(skillPath string) string {
		skillPath = strings.Trim(skillPath, "/")
		if skillPath == "" {
			skillPath = "SKILL.md"
		} else if !strings.HasSuffix(strings.ToLower(skillPath), ".md") {
			skillPath = path.Join(skillPath, "SKILL.md")
		}
		return "https://raw.githubusercontent.com/" + parts[0] + "/" + parts[1] + "/" + ref + "/" + skillPath
	}
	if explicitPath := strings.Trim(trimString(item["path"]), "/"); explicitPath != "" {
		return []string{rawURL(explicitPath)}
	}
	names := skillCandidateNames(item)
	candidatePaths := make([]string, 0, len(names)*3+1)
	for _, name := range names {
		candidatePaths = append(candidatePaths,
			path.Join("skills", name),
			name,
			path.Join("skill", name),
		)
	}
	candidatePaths = append(candidatePaths, "SKILL.md")
	seen := map[string]bool{}
	urls := make([]string, 0, len(candidatePaths))
	for _, candidate := range candidatePaths {
		url := rawURL(candidate)
		if !seen[url] {
			seen[url] = true
			urls = append(urls, url)
		}
	}
	return urls
}

func githubRepoParts(repo string) []string {
	repo = strings.TrimSpace(strings.TrimSuffix(repo, ".git"))
	switch {
	case strings.HasPrefix(repo, "https://github.com/"):
		repo = strings.TrimPrefix(repo, "https://github.com/")
	case strings.HasPrefix(repo, "http://github.com/"):
		repo = strings.TrimPrefix(repo, "http://github.com/")
	case strings.HasPrefix(repo, "git@github.com:"):
		repo = strings.TrimPrefix(repo, "git@github.com:")
	case strings.Contains(repo, "://"):
		return nil
	}
	parts := strings.Split(strings.Trim(repo, "/"), "/")
	if len(parts) < 2 {
		return nil
	}
	return parts[:2]
}

func skillCandidateNames(item map[string]any) []string {
	rawNames := []string{
		trimString(item["name"]),
		trimString(item["id"]),
	}
	names := make([]string, 0, len(rawNames)*2)
	seen := map[string]bool{}
	add := func(value string) {
		value = strings.Trim(value, "/")
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		names = append(names, value)
	}
	for _, name := range rawNames {
		add(name)
		add(sanitizeNativeID(name))
	}
	return names
}

func (r *Runtime) fetchText(ctx context.Context, url string) (string, error) {
	if err := validateFetchTextURL(url); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	client := *r.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
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

func validateFetchTextURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return err
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("skill url must use https")
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("skill url host is required")
	}
	if privateHostName(host) {
		return fmt.Errorf("skill url host is not allowed")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if privateAddress(ip) {
			return fmt.Errorf("skill url host is not allowed")
		}
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		addr, err := netip.ParseAddr(ip.String())
		if err != nil {
			return err
		}
		if privateAddress(addr) {
			return fmt.Errorf("skill url host resolves to a private address")
		}
	}
	return nil
}

func privateHostName(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "localhost" || strings.HasSuffix(host, ".localhost")
}

func privateAddress(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified()
}
