package nativeagent

import (
	"context"
	"strings"
	"sync"
	"unicode/utf8"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

const cloudAgentWorkloadSchema = "dirextalk.cloud-agent-workload/v1"

type cloudWorkloadCollectorContextKey struct{}

// cloudAgentWorkloadSummary is the fixed, de-secretsed navigation payload that
// may be exposed only after the restricted Cloud planning tool succeeds.
// It intentionally has no Connection, provider, quote, prompt, or recipe data.
type cloudAgentWorkloadSummary struct {
	Schema   string
	PlanID   string
	GoalID   string
	Status   string
	Revision int64
}

type cloudWorkloadCollector struct {
	mu          sync.Mutex
	planningKey string
	invalid     bool
	workload    *cloudAgentWorkloadSummary
}

func withCloudWorkloadCollector(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := cloudWorkloadCollectorFromContext(ctx); ok {
		return ctx
	}
	return context.WithValue(ctx, cloudWorkloadCollectorContextKey{}, &cloudWorkloadCollector{})
}

func cloudWorkloadCollectorFromContext(ctx context.Context) (*cloudWorkloadCollector, bool) {
	if ctx == nil {
		return nil, false
	}
	collector, ok := ctx.Value(cloudWorkloadCollectorContextKey{}).(*cloudWorkloadCollector)
	return collector, ok && collector != nil
}

// reserve permits Eino retries of the same tool call but refuses a second,
// different research goal in one restricted dialogue request. A later owner
// turn gets a fresh request scope and may intentionally create another plan.
func (c *cloudWorkloadCollector) reserve(planningKey string) bool {
	if c == nil || strings.TrimSpace(planningKey) == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.planningKey == "" {
		c.planningKey = planningKey
		return true
	}
	return c.planningKey == planningKey
}

func (c *cloudWorkloadCollector) record(result map[string]any) {
	if c == nil {
		return
	}
	workload, ok := cloudWorkloadSummaryFromPlannerResult(result)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok || c.invalid {
		c.invalid = true
		return
	}
	if c.workload == nil {
		c.workload = &workload
		return
	}
	if *c.workload != workload {
		c.invalid = true
	}
}

func (c *cloudWorkloadCollector) summary() map[string]any {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.invalid || c.workload == nil {
		return nil
	}
	return c.workload.toMap()
}

func cloudWorkloadSummaryFromContext(ctx context.Context) map[string]any {
	collector, ok := cloudWorkloadCollectorFromContext(ctx)
	if !ok {
		return nil
	}
	return collector.summary()
}

func (s cloudAgentWorkloadSummary) toMap() map[string]any {
	return map[string]any{
		"schema":   s.Schema,
		"plan_id":  s.PlanID,
		"goal_id":  s.GoalID,
		"status":   s.Status,
		"revision": s.Revision,
	}
}

func cloudWorkloadSummaryFromPlannerResult(result map[string]any) (cloudAgentWorkloadSummary, bool) {
	if len(result) != 2 {
		return cloudAgentWorkloadSummary{}, false
	}
	goal, ok := cloudPlannerGoalFromAny(result["goal"])
	if !ok {
		return cloudAgentWorkloadSummary{}, false
	}
	plan, ok := cloudPlannerPlanFromAny(result["plan"])
	if !ok || goal.planID != plan.planID || plan.goalID != goal.goalID {
		return cloudAgentWorkloadSummary{}, false
	}
	if !validCloudWorkloadID(goal.goalID) || !validCloudWorkloadID(plan.planID) || !validCloudWorkloadStatus(plan.status) || plan.revision <= 0 {
		return cloudAgentWorkloadSummary{}, false
	}
	return cloudAgentWorkloadSummary{
		Schema:   cloudAgentWorkloadSchema,
		PlanID:   plan.planID,
		GoalID:   goal.goalID,
		Status:   plan.status,
		Revision: plan.revision,
	}, true
}

type cloudPlannerGoalResult struct {
	goalID string
	planID string
}

func cloudPlannerGoalFromAny(value any) (cloudPlannerGoalResult, bool) {
	switch typed := value.(type) {
	case cloudmodule.GoalSummary:
		return cloudPlannerGoalResult{goalID: typed.GoalID, planID: typed.PlanID}, true
	case *cloudmodule.GoalSummary:
		if typed == nil {
			return cloudPlannerGoalResult{}, false
		}
		return cloudPlannerGoalResult{goalID: typed.GoalID, planID: typed.PlanID}, true
	case map[string]any:
		if !hasOnlyCloudWorkloadKeys(typed, "goal_id", "plan_id", "status", "revision") {
			return cloudPlannerGoalResult{}, false
		}
		goalID, goalOK := strictCloudWorkloadString(typed["goal_id"])
		planID, planOK := strictCloudWorkloadString(typed["plan_id"])
		status, statusOK := strictCloudWorkloadString(typed["status"])
		_, revisionOK := strictCloudWorkloadRevision(typed["revision"])
		if !goalOK || !planOK || !statusOK || !revisionOK || status != cloudmodule.GoalStatusResearching {
			return cloudPlannerGoalResult{}, false
		}
		return cloudPlannerGoalResult{goalID: goalID, planID: planID}, true
	default:
		return cloudPlannerGoalResult{}, false
	}
}

type cloudPlannerPlanResult struct {
	planID   string
	goalID   string
	status   string
	revision int64
}

func cloudPlannerPlanFromAny(value any) (cloudPlannerPlanResult, bool) {
	switch typed := value.(type) {
	case cloudmodule.Plan:
		return cloudPlannerPlanResult{
			planID: typed.PlanID, goalID: typed.GoalID, status: typed.Status, revision: typed.Revision,
		}, true
	case *cloudmodule.Plan:
		if typed == nil {
			return cloudPlannerPlanResult{}, false
		}
		return cloudPlannerPlanResult{
			planID: typed.PlanID, goalID: typed.GoalID, status: typed.Status, revision: typed.Revision,
		}, true
	case map[string]any:
		if !hasOnlyCloudWorkloadKeys(typed, "plan_id", "goal_id", "status", "revision") {
			return cloudPlannerPlanResult{}, false
		}
		planID, planOK := strictCloudWorkloadString(typed["plan_id"])
		goalID, goalOK := strictCloudWorkloadString(typed["goal_id"])
		status, statusOK := strictCloudWorkloadString(typed["status"])
		revision, revisionOK := strictCloudWorkloadRevision(typed["revision"])
		if !planOK || !goalOK || !statusOK || !revisionOK {
			return cloudPlannerPlanResult{}, false
		}
		return cloudPlannerPlanResult{planID: planID, goalID: goalID, status: status, revision: revision}, true
	default:
		return cloudPlannerPlanResult{}, false
	}
}

func hasOnlyCloudWorkloadKeys(values map[string]any, expected ...string) bool {
	if len(values) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, ok := values[key]; !ok {
			return false
		}
	}
	return true
}

func strictCloudWorkloadString(value any) (string, bool) {
	text, ok := value.(string)
	if !ok || !validCloudWorkloadID(text) {
		return "", false
	}
	return text, true
}

func strictCloudWorkloadRevision(value any) (int64, bool) {
	switch revision := value.(type) {
	case int:
		return int64(revision), revision > 0
	case int8:
		return int64(revision), revision > 0
	case int16:
		return int64(revision), revision > 0
	case int32:
		return int64(revision), revision > 0
	case int64:
		return revision, revision > 0
	case uint:
		if uint64(revision) > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(revision), revision > 0
	case uint8:
		return int64(revision), revision > 0
	case uint16:
		return int64(revision), revision > 0
	case uint32:
		return int64(revision), revision > 0
	case uint64:
		if revision > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(revision), revision > 0
	default:
		return 0, false
	}
}

func validCloudWorkloadID(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n\t")
}

func validCloudWorkloadStatus(value string) bool {
	switch value {
	case cloudmodule.PlanStatusResearching,
		cloudmodule.PlanStatusQuoting,
		cloudmodule.PlanStatusReadyForConfirmation,
		cloudmodule.PlanStatusApproved,
		cloudmodule.PlanStatusExpired,
		cloudmodule.PlanStatusSuperseded:
		return true
	default:
		return false
	}
}
