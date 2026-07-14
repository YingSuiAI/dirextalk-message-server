package mcp

import (
	"testing"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestCloudMCPProjectionNeverIncludesPlanNarrativeOrSecretReferences(t *testing.T) {
	summary := cloudPlanSummary(cloudmodule.Plan{
		PlanID: "cloud_plan_1", Summary: "credential slot secret_ref:cloud_secret_1",
	})
	if _, found := summary["summary"]; found {
		t.Fatalf("public MCP Cloud projection must not include plan narrative: %#v", summary)
	}
}
