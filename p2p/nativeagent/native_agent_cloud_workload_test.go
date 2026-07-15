package nativeagent

import (
	"context"
	"testing"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestCloudWorkloadSummaryFromPlannerResultIsStrictlyDeSecretsed(t *testing.T) {
	validResult := map[string]any{
		"goal": cloudmodule.GoalSummary{
			GoalID: "cloud_goal_1", PlanID: "cloud_plan_1", Status: cloudmodule.GoalStatusResearching, Revision: 1,
		},
		"plan": cloudmodule.Plan{
			PlanID: "cloud_plan_1", GoalID: "cloud_goal_1", Status: cloudmodule.PlanStatusResearching, Revision: 1,
		},
	}

	tests := []struct {
		name string
		edit func(map[string]any)
		want bool
	}{
		{name: "typed result", want: true},
		{
			name: "unknown secret-like result field",
			edit: func(result map[string]any) {
				result["goal"] = map[string]any{
					"goal_id": "cloud_goal_1", "plan_id": "cloud_plan_1", "status": "researching", "revision": int64(1),
					"secret_ref": "must-not-create-a-card",
				}
			},
		},
		{
			name: "mismatched plan relation",
			edit: func(result map[string]any) {
				result["plan"] = cloudmodule.Plan{
					PlanID: "cloud_plan_2", GoalID: "cloud_goal_1", Status: cloudmodule.PlanStatusResearching, Revision: 1,
				}
			},
		},
		{
			name: "unknown plan status",
			edit: func(result map[string]any) {
				result["plan"] = cloudmodule.Plan{
					PlanID: "cloud_plan_1", GoalID: "cloud_goal_1", Status: "worker_ready", Revision: 1,
				}
			},
		},
		{
			name: "unsafe plan identifier",
			edit: func(result map[string]any) {
				result["plan"] = cloudmodule.Plan{
					PlanID: " cloud_plan_1", GoalID: "cloud_goal_1", Status: cloudmodule.PlanStatusResearching, Revision: 1,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := map[string]any{"goal": validResult["goal"], "plan": validResult["plan"]}
			if test.edit != nil {
				test.edit(result)
			}
			summary, ok := cloudWorkloadSummaryFromPlannerResult(result)
			if ok != test.want {
				t.Fatalf("summary accepted=%v want=%v result=%#v", ok, test.want, result)
			}
			if test.want && summary.toMap()["plan_id"] != "cloud_plan_1" {
				t.Fatalf("summary = %#v", summary)
			}
		})
	}
}

func TestRestrictedCloudDialogueCreatesAtMostOnePlanPerRequest(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner})
	ctx, err := prepareCloudDialogueRequest(context.Background(), map[string]any{
		"cloud_dialogue_mode": true,
		"cloud_connection_id": "connection-1",
	})
	if err != nil {
		t.Fatalf("prepare cloud dialogue: %v", err)
	}
	tool, ok := nativeToolByName(runtime.cloudDialoguePlanningTools(), nativeAgentCloudDeploymentPlanTool)
	if !ok {
		t.Fatal("expected restricted cloud planning tool")
	}
	if _, err := tool.Handler(ctx, map[string]any{"goal": "Deploy a private knowledge node."}); err != nil {
		t.Fatalf("first cloud planning call: %v", err)
	}
	if _, err := tool.Handler(ctx, map[string]any{"goal": "Deploy a different service."}); err == nil {
		t.Fatal("second distinct Cloud plan in one dialogue must be rejected")
	}
	if planner.calls != 1 {
		t.Fatalf("planner calls=%d want one", planner.calls)
	}
	assertCloudWorkloadSummary(t, cloudWorkloadSummaryFromContext(ctx), "cloud_plan_1", "cloud_goal_1", "researching", 1)
}
