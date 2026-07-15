package containerinit

import (
	"reflect"
	"testing"
)

func TestParseAcceptsOnlyFixedOpenClawSecretExec(t *testing.T) {
	arguments := []string{
		"container-init",
		"--run-as=1000:1000",
		"--secret-env=OPENAI_API_KEY=/run/secrets/model-token",
		"--",
		"/usr/bin/tini",
		"-s", "--", "node", "openclaw.mjs", "gateway",
	}
	parsed, err := parse(arguments)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.uid != 1000 || parsed.gid != 1000 || parsed.entrypoint != "/usr/bin/tini" ||
		!reflect.DeepEqual(parsed.arguments, []string{"-s", "--", "node", "openclaw.mjs", "gateway"}) ||
		!reflect.DeepEqual(parsed.secrets, []secretEnvironment{{key: "OPENAI_API_KEY", file: "/run/secrets/model-token"}}) {
		t.Fatalf("parsed=%#v", parsed)
	}
}

func TestParseRejectsShellPlaintextAndAmbiguousBindings(t *testing.T) {
	valid := []string{"container-init", "--run-as=1000:1000", "--secret-env=OPENAI_API_KEY=/run/secrets/model-token", "--", "/usr/bin/tini", "node"}
	for name, mutate := range map[string]func([]string) []string{
		"root identity": func(value []string) []string { value[1] = "--run-as=0:0"; return value },
		"shell":         func(value []string) []string { value[4] = "/bin/sh"; return value },
		"relative exec": func(value []string) []string { value[4] = "node"; return value },
		"plain value":   func(value []string) []string { value[2] = "--secret-env=OPENAI_API_KEY=sk-test-value"; return value },
		"file key": func(value []string) []string {
			value[2] = "--secret-env=OPENAI_API_KEY_FILE=/run/secrets/model-token"
			return value
		},
		"newline": func(value []string) []string { value[5] = "node\nunsafe"; return value },
		"duplicate": func(value []string) []string {
			return append(value[:3], append([]string{"--secret-env=OPENAI_API_KEY=/run/secrets/model-token"}, value[3:]...)...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := mutate(append([]string(nil), valid...))
			if _, err := parse(candidate); err == nil {
				t.Fatalf("unsafe arguments accepted: %q", candidate)
			}
		})
	}
}
