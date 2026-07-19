package infra

import (
	"os"
	"strings"
	"testing"
)

func TestDynamicArtifactInfrastructureIsVersionedEncryptedAndDefaultOff(t *testing.T) {
	raw, err := os.ReadFile("../../infra/template.yaml")
	if err != nil {
		t.Fatal(err)
	}
	template := string(raw)
	for _, required := range []string{"EnableDynamicArtifacts:", `Default: "false"`, `DynamicArtifactsEnabled: !Equals [!Ref EnableDynamicArtifacts, "true"]`, "DynamicArtifactBucket:", "VersioningConfiguration: {Status: Enabled}", "SSEAlgorithm: aws:kms", "DynamicArtifactsTable:", "PointInTimeRecoveryEnabled: true", "DIREXTALK_DYNAMIC_ARTIFACTS_ENABLED: !Ref EnableDynamicArtifacts", `Resource: !Sub "${DynamicArtifactBucket.Arn}/artifacts/*"`} {
		if !strings.Contains(template, required) {
			t.Fatalf("missing dynamic artifact boundary %q", required)
		}
	}
}
