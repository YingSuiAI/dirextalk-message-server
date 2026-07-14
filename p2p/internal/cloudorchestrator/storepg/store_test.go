package storepg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestStoreClaimsOnceAndFencesAnExpiredWorker(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})

	first, found, err := store.ClaimResearchGoal(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim found=%v err=%v", found, err)
	}
	if _, found, err := store.ClaimResearchGoal(ctx, "orchestrator-b", time.Minute); err != nil || found {
		t.Fatalf("overlapping claim found=%v err=%v", found, err)
	}
	now = now.Add(2 * time.Minute)
	second, found, err := store.ClaimResearchGoal(ctx, "orchestrator-b", time.Minute)
	if err != nil || !found || second.LeaseToken == first.LeaseToken {
		t.Fatalf("expired lease takeover=%#v found=%v err=%v", second, found, err)
	}
	if err := store.DeferResearch(ctx, first, "temporary", now.Add(time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old worker defer error = %v, want ErrLeaseLost", err)
	}
	if err := store.DeferResearch(ctx, second, "temporary", now.Add(time.Minute)); err != nil {
		t.Fatalf("new worker defer: %v", err)
	}
}

func TestStorePersistsResearchJobLeaseRetryAndFailure(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimResearchGoal(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if err := store.MarkResearchStarted(ctx, claim); err != nil {
		t.Fatalf("mark research started: %v", err)
	}
	assertResearchJob(t, ctx, database, claim, "queued", "pending", "research_leased", "", 2)
	if err := store.DeferResearch(ctx, claim, "researcher_unavailable", now.Add(time.Minute)); err != nil {
		t.Fatalf("defer research: %v", err)
	}
	assertResearchJob(t, ctx, database, claim, "queued", "pending", "research_retry_scheduled", "researcher_unavailable", 3)

	now = now.Add(2 * time.Minute)
	claim, found, err = store.ClaimResearchGoal(ctx, "orchestrator-b", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry claim found=%v err=%v", found, err)
	}
	if err := store.MarkResearchStarted(ctx, claim); err != nil {
		t.Fatalf("mark retry started: %v", err)
	}
	if err := store.FailResearch(ctx, claim, "research_planner_failed"); err != nil {
		t.Fatalf("fail research: %v", err)
	}
	assertResearchJob(t, ctx, database, claim, "finished", "failed", "research_failed", "research_planner_failed", 5)

	var stepStatus, stepCheckpoint, stepError string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT status, checkpoint, error_code FROM p2p_cloud_job_steps
		WHERE job_id = $1 AND step_id = 'research'`, cloudmodule.ResearchJobID(claim.OutboxID)).Scan(&stepStatus, &stepCheckpoint, &stepError); err != nil {
		t.Fatal(err)
	}
	if stepStatus != "failed" || stepCheckpoint != "research_failed" || stepError != "research_planner_failed" {
		t.Fatalf("research step terminal state = status:%q checkpoint:%q error:%q", stepStatus, stepCheckpoint, stepError)
	}
}

func TestStoreCommitResearchAtomicallyPublishesSafeProjection(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimResearchGoal(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if err := store.MarkResearchStarted(ctx, claim); err != nil {
		t.Fatalf("mark research started: %v", err)
	}
	output := testResearchOutput(t, now, claim)
	if err := store.CommitResearch(ctx, claim, output); err != nil {
		t.Fatalf("commit research: %v", err)
	}

	var status, title, summary, planHash string
	var revision int64
	if err := database.DB().QueryRowContext(ctx, `
		SELECT status, title, summary, plan_hash, revision
		FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&status, &title, &summary, &planHash, &revision); err != nil {
		t.Fatal(err)
	}
	if status != string(cloudcontracts.PlanReadyForConfirmation) || revision != 2 || title != output.Title || summary != output.Summary || planHash == "" {
		t.Fatalf("plan projection = status:%q revision:%d title:%q summary:%q hash:%q", status, revision, title, summary, planHash)
	}
	var planSummary string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT summary_json FROM p2p_cloud_events
		WHERE aggregate_type = 'plan' AND aggregate_id = $1 AND revision = 2 AND type = 'cloud.plan.changed'`, claim.PlanID).Scan(&planSummary); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(planSummary, `"goal_id":"`+claim.GoalID+`"`) {
		t.Fatalf("plan event must preserve its goal_id for strict realtime projection: %s", planSummary)
	}
	for _, table := range []string{"p2p_cloud_plan_versions", "p2p_cloud_recipe_versions", "p2p_cloud_quotes", "p2p_cloud_jobs", "p2p_cloud_job_steps", "p2p_cloud_projection_outbox"} {
		var count int
		if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count == 0 {
			t.Fatalf("%s rows=%d err=%v", table, count, err)
		}
	}
	var rawProjection string
	if err := database.DB().QueryRowContext(ctx, `SELECT payload_json FROM p2p_cloud_projection_outbox ORDER BY created_at, projection_id LIMIT 1`).Scan(&rawProjection); err != nil {
		t.Fatal(err)
	}
	if containsAny(rawProjection, []string{"private deployment intent", `"goal"`, "secret_ref"}) {
		t.Fatalf("projection payload leaked a private planning field: %s", rawProjection)
	}
	var completedAt int64
	if err := database.DB().QueryRowContext(ctx, `SELECT completed_at FROM p2p_cloud_outbox WHERE outbox_id = $1`, claim.OutboxID).Scan(&completedAt); err != nil || completedAt == 0 {
		t.Fatalf("outbox completion=%d err=%v", completedAt, err)
	}
	if err := store.CommitResearch(ctx, claim, output); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("replayed commit error=%v, want ErrLeaseLost", err)
	}
}

func TestStoreResearchEventsPassTheStrictProjectionRelay(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	seedResearchGoal(t, ctx, database)
	now := time.Now().UTC().Truncate(time.Millisecond)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimResearchGoal(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if err := store.MarkResearchStarted(ctx, claim); err != nil {
		t.Fatalf("mark research started: %v", err)
	}
	if err := store.CommitResearch(ctx, claim, testResearchOutput(t, now, claim)); err != nil {
		t.Fatalf("commit research: %v", err)
	}

	type publishedEvent struct {
		id      string
		typ     string
		payload map[string]any
	}
	published := []publishedEvent{}
	relay := cloudmodule.NewProjectionRelay(database, func(_ context.Context, eventID, eventType string, payload map[string]any) error {
		published = append(published, publishedEvent{id: eventID, typ: eventType, payload: payload})
		return nil
	}, cloudmodule.ProjectionRelayConfig{WorkerID: "projection-test"})
	for index := 0; index < 7; index++ {
		processed, err := relay.RunOnce(ctx)
		if err != nil {
			t.Fatalf("relay iteration %d: %v", index, err)
		}
		if !processed {
			break
		}
	}
	if len(published) != 6 {
		rows, err := database.DB().QueryContext(ctx, `
			SELECT type, last_error_code FROM p2p_cloud_projection_outbox ORDER BY created_at, projection_id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		rejected := []string{}
		for rows.Next() {
			var eventType, code string
			if err := rows.Scan(&eventType, &code); err != nil {
				t.Fatal(err)
			}
			if code != "" {
				rejected = append(rejected, eventType+":"+code)
			}
		}
		t.Fatalf("strict relay published %d events, want six; rejected=%#v events=%#v", len(published), rejected, published)
	}
	for _, event := range published {
		if containsAny(event.typ+" "+event.id, []string{"private deployment intent", "secret_ref"}) {
			t.Fatalf("published event identity leaked private material: %#v", event)
		}
	}
	var readyPlan map[string]any
	for _, event := range published {
		if event.typ == "cloud.plan.changed" && event.payload["revision"] == int64(2) {
			readyPlan = event.payload
			break
		}
	}
	if readyPlan == nil || readyPlan["goal_id"] != claim.GoalID || readyPlan["created_at"] != int64(100) {
		t.Fatalf("ready plan relay payload = %#v", readyPlan)
	}
}

func openMigratedStore(t *testing.T) (context.Context, *p2pstorage.DatabaseStore, func()) {
	t.Helper()
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	options := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	database, err := p2pstorage.NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, options), &options)
	if err != nil {
		closeDB()
		t.Fatal(err)
	}
	return ctx, database, func() {
		_ = database.Close()
		closeDB()
	}
}

func seedResearchGoal(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore) {
	t.Helper()
	outboxID := "outbox-1"
	jobID := cloudmodule.ResearchJobID(outboxID)
	request := cloudmodule.CreateGoalRequest{
		Goal: cloudmodule.Goal{
			GoalID: "goal-1", OwnerMXID: "@owner:example.com", Prompt: "private deployment intent", ConnectionID: "connection-1", PlanID: "plan-1",
			Status: cloudmodule.GoalStatusResearching, IdempotencyHash: "idem-1", RequestDigest: "digest-1", Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		},
		Plan: cloudmodule.Plan{
			PlanID: "plan-1", GoalID: "goal-1", ConnectionID: "connection-1", Status: cloudmodule.PlanStatusResearching,
			Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		},
		Job: cloudmodule.Job{
			JobID: jobID, PlanID: "plan-1", Kind: "research", Execution: "queued", Outcome: "pending",
			Checkpoint: "research_queued", Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		},
		Events: []cloudmodule.Event{
			{EventID: "event-goal-1", Type: "cloud.goal.changed", AggregateType: "goal", AggregateID: "goal-1", Revision: 1, SummaryJSON: `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","status":"researching","revision":1,"created_at":100,"updated_at":100}`, CreatedAt: 100},
			{EventID: "event-plan-1", Type: "cloud.plan.changed", AggregateType: "plan", AggregateID: "plan-1", Revision: 1, SummaryJSON: `{"plan_id":"plan-1","goal_id":"goal-1","cloud_connection_id":"connection-1","status":"researching","title":"","summary":"","recipe_digest":"","quote_id":"","plan_hash":"","revision":1,"created_at":100,"updated_at":100}`, CreatedAt: 100},
			{EventID: "event-job-1", Type: "cloud.job.changed", AggregateType: "job", AggregateID: jobID, Revision: 1, SummaryJSON: `{"job_id":"cloud_job_research_outbox-1","plan_id":"plan-1","deployment_id":"","kind":"research","execution_status":"queued","outcome_status":"pending","checkpoint":"research_queued","error_code":"","revision":1,"created_at":100,"updated_at":100}`, CreatedAt: 100},
		},
		Outbox: cloudmodule.OutboxEntry{
			OutboxID: outboxID, Kind: runtime.ResearchGoalRequested, AggregateType: "goal", AggregateID: "goal-1",
			PayloadJSON: `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","goal":"private deployment intent"}`, CreatedAt: 100,
		},
	}
	if _, err := database.CreateCloudGoal(ctx, request); err != nil {
		t.Fatalf("seed research goal: %v", err)
	}
}

func testResearchOutput(t *testing.T, now time.Time, claim runtime.Claim) runtime.ResearchOutput {
	t.Helper()
	recipe := cloudcontracts.RecipeV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, RecipeID: "recipe-knowledge-1", Name: "Private knowledge workload", Maturity: cloudcontracts.RecipeExperimental,
		Sources:      []cloudcontracts.RecipeSourceV1{{URL: "https://github.com/example/knowledge-workload", Version: "v1.0.0", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", License: "Apache-2.0", RetrievedAt: now, Official: true}},
		Requirements: cloudcontracts.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		Install:      cloudcontracts.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"artifact-ready", "service-ready"}, Steps: []cloudcontracts.InstallStepV1{{ID: "install", Summary: "Install the signed workload artifact", TimeoutSeconds: 900}}},
		Health:       cloudcontracts.HealthContractV1{Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"}, Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "verify-service"}},
		Lifecycle:    cloudcontracts.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
	}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	quote := cloudcontracts.QuoteV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, QuoteID: "quote-knowledge-1", CloudConnectionID: claim.ConnectionID, Region: "ap-south-1", Currency: "USD", QuotedAt: now, ValidUntil: now.Add(15 * time.Minute),
		Candidates: []cloudcontracts.QuoteCandidateV1{{CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, HourlyMinor: 2000, ThirtyDayMinor: 1440000, EstimatedDiskGiB: 80, AvailabilityZones: []string{"ap-south-1a"}}},
	}
	quoteDigest, err := quote.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return runtime.ResearchOutput{
		Plan: cloudcontracts.PlanV1{
			SchemaVersion: cloudcontracts.SchemaVersionV1, PlanID: claim.PlanID, Revision: uint64(claim.PlanRevision + 1), Status: cloudcontracts.PlanReadyForConfirmation, CloudConnectionID: claim.ConnectionID,
			Recipe: cloudcontracts.RecipeBindingV1{RecipeID: recipe.RecipeID, Digest: recipeDigest, Maturity: recipe.Maturity}, Quote: cloudcontracts.QuoteBindingV1{QuoteID: quote.QuoteID, Digest: quoteDigest, ValidUntil: quote.ValidUntil, CandidateID: "recommended"},
			ResourceScope: cloudcontracts.ResourceScopeV1{Region: quote.Region, AvailabilityZones: []string{"ap-south-1a"}, InstanceType: "m7i.xlarge", Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384, DiskGiB: 80, PurchaseOption: cloudcontracts.PurchaseOnDemand},
			NetworkScope:  cloudcontracts.NetworkScopeV1{PublicIngress: false, EntryPoint: cloudcontracts.EntryPointNone},
		},
		Recipe: recipe, Quote: quote, Title: "Private knowledge workload", Summary: "Official-source private single-VM proposal; review the quote before creating billable resources.",
	}
}

func assertResearchJob(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore, claim runtime.Claim, execution, outcome, checkpoint, errorCode string, revision int64) {
	t.Helper()
	items, err := database.ListCloudJobs(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("cloud jobs = %#v err=%v", items, err)
	}
	job := items[0]
	if job.JobID != cloudmodule.ResearchJobID(claim.OutboxID) || job.PlanID != claim.PlanID || job.Kind != "research" ||
		job.Execution != execution || job.Outcome != outcome || job.Checkpoint != checkpoint || job.ErrorCode != errorCode || job.Revision != revision {
		t.Fatalf("cloud job = %#v", job)
	}
}

func containsAny(value string, forbidden []string) bool {
	for _, item := range forbidden {
		if len(item) > 0 && strings.Contains(value, item) {
			return true
		}
	}
	return false
}
