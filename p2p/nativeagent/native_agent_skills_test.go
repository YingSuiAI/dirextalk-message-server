package nativeagent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestSkillInstallListsAndInjectsStaticSkillPrompt(t *testing.T) {
	store := &testConfigStore{config: map[string]any{}}
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: store})
	ctx := context.Background()

	install, err := runtime.Invoke(ctx, "agent.skills.install", map[string]any{
		"id":      "answer-style",
		"content": "# Skill\n\nAlways answer with the marker SKILL_USED.",
	})
	if err != nil {
		t.Fatalf("install skill: %v", err)
	}
	if install["ok"] != true {
		t.Fatalf("expected skill install ok, got %#v", install)
	}
	list, err := runtime.Invoke(ctx, "agent.skills.list", nil)
	if err != nil {
		t.Fatalf("list skills: %v", err)
	}
	skills, ok := list["skills"].([]map[string]any)
	if !ok || len(skills) != 1 {
		t.Fatalf("expected one skill, got %#v", list)
	}
	config, _, err := runtime.agentConfig(ctx)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	systemPrompt := runtime.agentSystemPrompt(ctx, config, map[string]any{"prompt": "hello"}, "")
	if !strings.Contains(systemPrompt, "SKILL_USED") {
		t.Fatalf("expected static skill text in system prompt, got %q", systemPrompt)
	}
	if _, err := runtime.Invoke(ctx, "agent.skills.disable", map[string]any{"id": "answer-style"}); err != nil {
		t.Fatalf("disable skill: %v", err)
	}
	config, _, err = runtime.agentConfig(ctx)
	if err != nil {
		t.Fatalf("load disabled config: %v", err)
	}
	systemPrompt = runtime.agentSystemPrompt(ctx, config, map[string]any{"prompt": "hello"}, "")
	if strings.Contains(systemPrompt, "SKILL_USED") {
		t.Fatalf("disabled skill must not be injected, got %q", systemPrompt)
	}
	if _, err := runtime.Invoke(ctx, "agent.skills.enable", map[string]any{"id": "answer-style"}); err != nil {
		t.Fatalf("enable skill: %v", err)
	}
	if _, err := runtime.Invoke(ctx, "agent.skills.install", map[string]any{
		"id":      "second-skill",
		"content": "# Skill\n\nSecond skill marker SECOND_SKILL_USED.",
	}); err != nil {
		t.Fatalf("install second skill: %v", err)
	}
	if _, err := runtime.Invoke(ctx, "agent.skills.uninstall", map[string]any{"id": "answer-style"}); err != nil {
		t.Fatalf("uninstall skill: %v", err)
	}
	list, err = runtime.Invoke(ctx, "agent.skills.list", nil)
	if err != nil {
		t.Fatalf("list skills after uninstall: %v", err)
	}
	skills = list["skills"].([]map[string]any)
	if len(skills) != 1 || skills[0]["id"] != "second-skill" {
		t.Fatalf("expected only second skill after uninstall, got %#v", list)
	}
}

func TestGithubRawSkillURLsPreferNamedMonorepoSkill(t *testing.T) {
	urls := githubRawSkillURLs(map[string]any{
		"repo_url": "https://github.com/vercel-labs/skills",
		"name":     "find-skills",
	})
	wantFirst := "https://raw.githubusercontent.com/vercel-labs/skills/main/skills/find-skills/SKILL.md"
	if len(urls) == 0 || urls[0] != wantFirst {
		t.Fatalf("first GitHub skill URL = %#v, want first %q", urls, wantFirst)
	}
	if !containsString(urls, "https://raw.githubusercontent.com/vercel-labs/skills/main/find-skills/SKILL.md") {
		t.Fatalf("expected direct skill directory fallback in %#v", urls)
	}
	if !containsString(urls, "https://raw.githubusercontent.com/vercel-labs/skills/main/SKILL.md") {
		t.Fatalf("expected root SKILL.md fallback in %#v", urls)
	}
}

func TestGithubRawSkillURLsSupportOwnerRepoShorthandAndExplicitPath(t *testing.T) {
	urls := githubRawSkillURLs(map[string]any{
		"repo_url": "mattpocock/skills",
		"path":     "skills/engineering/code-review",
		"ref":      "main",
	})
	want := []string{"https://raw.githubusercontent.com/mattpocock/skills/main/skills/engineering/code-review/SKILL.md"}
	if len(urls) != len(want) || urls[0] != want[0] {
		t.Fatalf("GitHub skill URLs = %#v, want %#v", urls, want)
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
