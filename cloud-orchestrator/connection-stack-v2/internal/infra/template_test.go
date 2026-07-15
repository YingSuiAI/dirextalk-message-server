package infra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemplateKeepsGoOnlyFailClosedPermissions(t *testing.T) {
	path := filepath.Join("..", "..", "infra", "template.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	template := string(raw)
	for _, required := range []string{
		"Runtime: provided.al2023",
		"Handler: bootstrap",
		"POST /v2/commands",
		"DIREXTALK_NODE_PUBLIC_KEY_SPKI_B64",
		"BrokerArtifactBucket",
	} {
		if !strings.Contains(template, required) {
			t.Fatalf("template is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"AWS::Serverless::",
		"nodejs",
		"ec2:",
		"dynamodb:",
		"iam:PassRole",
		"secretsmanager:",
		"s3:",
	} {
		if strings.Contains(strings.ToLower(template), strings.ToLower(forbidden)) {
			t.Fatalf("template unexpectedly grants or depends on %q", forbidden)
		}
	}
}
