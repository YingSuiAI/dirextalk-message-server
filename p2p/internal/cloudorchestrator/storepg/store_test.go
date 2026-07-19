package storepg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
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

func TestStoreCommitResearchAtomicallyQueuesVerifiedQuote(t *testing.T) {
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
	output := testResearchOutput(t, now)
	if err := store.CommitResearch(ctx, claim, output); err != nil {
		t.Fatalf("commit research: %v", err)
	}

	recipeDigest, err := output.Recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var status, title, summary, storedRecipeDigest, quoteID, planHash string
	var revision int64
	if err := database.DB().QueryRowContext(ctx, `
		SELECT status, title, summary, recipe_digest, quote_id, plan_hash, revision
		FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&status, &title, &summary, &storedRecipeDigest, &quoteID, &planHash, &revision); err != nil {
		t.Fatal(err)
	}
	if status != string(cloudcontracts.PlanQuoting) || revision != 2 || title != output.Title || summary != output.Summary || storedRecipeDigest != recipeDigest || quoteID != "" || planHash != "" {
		t.Fatalf("plan projection = status:%q revision:%d title:%q summary:%q recipe_digest:%q quote_id:%q hash:%q", status, revision, title, summary, storedRecipeDigest, quoteID, planHash)
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
	for _, table := range []string{"p2p_cloud_recipe_versions", "p2p_cloud_jobs", "p2p_cloud_job_steps", "p2p_cloud_projection_outbox"} {
		var count int
		if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count == 0 {
			t.Fatalf("%s rows=%d err=%v", table, count, err)
		}
	}
	for _, table := range []string{"p2p_cloud_plan_versions", "p2p_cloud_quotes"} {
		var count int
		if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatal(err)
		} else if count != 0 {
			t.Fatalf("%s rows=%d, want no model-produced approval plan or price quote", table, count)
		}
	}

	var quoteOutboxID, quoteKind, quoteAggregateType, quoteAggregateID, rawQuoteRequest string
	var quoteCompletedAt int64
	if err := database.DB().QueryRowContext(ctx, `
		SELECT outbox_id, kind, aggregate_type, aggregate_id, payload_json, completed_at
		FROM p2p_cloud_outbox
		WHERE kind = $1`, runtime.QuotePlanRequested).Scan(
		&quoteOutboxID, &quoteKind, &quoteAggregateType, &quoteAggregateID, &rawQuoteRequest, &quoteCompletedAt,
	); err != nil {
		t.Fatal(err)
	}
	if quoteOutboxID == "" || quoteKind != runtime.QuotePlanRequested || quoteAggregateType != "plan" || quoteAggregateID != claim.PlanID || quoteCompletedAt != 0 {
		t.Fatalf("quote outbox = id:%q kind:%q aggregate:%s/%s completed_at:%d", quoteOutboxID, quoteKind, quoteAggregateType, quoteAggregateID, quoteCompletedAt)
	}
	decoder := json.NewDecoder(strings.NewReader(rawQuoteRequest))
	decoder.DisallowUnknownFields()
	var quoteRequest cloudcontracts.QuoteRequestV1
	if err := decoder.Decode(&quoteRequest); err != nil {
		t.Fatalf("decode quote request payload: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("quote request payload has trailing JSON: %v", err)
	}
	if err := quoteRequest.Validate(); err != nil {
		t.Fatalf("quote request payload validation: %v", err)
	}
	if quoteRequest.SchemaVersion != cloudcontracts.SchemaVersionV1 || quoteRequest.QuoteRequestID == "" || quoteRequest.PlanID != claim.PlanID || quoteRequest.PlanRevision != uint64(claim.PlanRevision+1) || quoteRequest.CloudConnectionID != claim.ConnectionID || quoteRequest.RecipeDigest != recipeDigest || quoteRequest.Region != output.Draft.Region || !reflect.DeepEqual(quoteRequest.Candidates, output.Draft.Candidates) {
		t.Fatalf("quote request payload = %#v", quoteRequest)
	}
	if containsAny(rawQuoteRequest, []string{"\"quote_id\"", "\"plan_hash\"", "\"hourly_minor\"", "\"thirty_day_minor\"", "\"startup_upper_minor\""}) {
		t.Fatalf("quote request must remain pre-price and pre-approval: %s", rawQuoteRequest)
	}
	var quoteJobKind, quoteJobExecution, quoteJobOutcome, quoteJobCheckpoint string
	var quoteJobRevision int64
	if err := database.DB().QueryRowContext(ctx, `
		SELECT kind, execution_status, outcome_status, checkpoint, revision
		FROM p2p_cloud_jobs WHERE job_id = $1`, cloudmodule.QuoteJobID(quoteOutboxID)).Scan(
		&quoteJobKind, &quoteJobExecution, &quoteJobOutcome, &quoteJobCheckpoint, &quoteJobRevision,
	); err != nil {
		t.Fatal(err)
	}
	if quoteJobKind != "quote" || quoteJobExecution != "queued" || quoteJobOutcome != "pending" || quoteJobCheckpoint != "quote_queued" || quoteJobRevision != 1 {
		t.Fatalf("quote job = kind:%q execution:%q outcome:%q checkpoint:%q revision:%d", quoteJobKind, quoteJobExecution, quoteJobOutcome, quoteJobCheckpoint, quoteJobRevision)
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

func TestStoreCommitResearchReusesExactManagedSelectedRecipe(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, 7, 15, 15, 0, 0, 0, time.UTC)
	output := testResearchOutput(t, now)
	output.Recipe.Maturity = cloudcontracts.RecipeManaged
	digest, err := output.Recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := output.Recipe.CanonicalRecipeCBOR()
	if err != nil {
		t.Fatal(err)
	}
	display, _ := json.Marshal(output.Recipe)
	if _, err = database.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at)VALUES($1,$2,'v2',$3,'managed',2,$4,$4)`, output.Recipe.RecipeID, output.Recipe.Name, digest, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = database.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at)VALUES($1,2,$2,$3,$4,'managed',$5)`, output.Recipe.RecipeID, canonical, string(display), digest, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	payload := fmt.Sprintf(`{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","goal":"private deployment intent","recipe_id":"%s","recipe_revision":2,"recipe_digest":"%s"}`, output.Recipe.RecipeID, digest)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`UPDATE p2p_cloud_goals SET selected_recipe_id=$1,selected_recipe_revision=2,selected_recipe_digest=$2 WHERE goal_id='goal-1'`, []any{output.Recipe.RecipeID, digest}},
		{`UPDATE p2p_cloud_plans SET recipe_id=$1,recipe_revision=2,recipe_digest=$2 WHERE plan_id='plan-1'`, []any{output.Recipe.RecipeID, digest}},
		{`UPDATE p2p_cloud_outbox SET payload_json=$1 WHERE outbox_id='outbox-1'`, []any{payload}},
	} {
		if _, err = database.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimResearchGoal(ctx, "orchestrator-selected", time.Minute)
	if err != nil || !found || claim.SelectedRecipe == nil || claim.SelectedRecipe.Revision != 2 {
		t.Fatalf("claim=%#v found=%v err=%v", claim, found, err)
	}
	if err = store.MarkResearchStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	output.Draft.Candidates = []cloudcontracts.QuoteRequestCandidateV1{{CandidateID: "economy", Tier: cloudcontracts.QuoteTierEconomy, InstanceType: "m7i.large", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80}, {CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80}, {CandidateID: "performance", Tier: cloudcontracts.QuoteTierPerformance, InstanceType: "m7i.2xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80}}
	if _, err = database.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_versions SET canonical_cbor=$2 WHERE recipe_id=$1 AND revision=2`, output.Recipe.RecipeID, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitResearch(ctx, claim, output); err == nil {
		t.Fatal("commit accepted a tampered authoritative selected recipe")
	}
	if _, err = database.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_versions SET canonical_cbor=$2 WHERE recipe_id=$1 AND revision=2`, output.Recipe.RecipeID, canonical); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitResearch(ctx, claim, output); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_recipe_versions WHERE recipe_id=$1`, output.Recipe.RecipeID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("managed versions=%d err=%v", count, err)
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
	if err := store.CommitResearch(ctx, claim, testResearchOutput(t, now)); err != nil {
		t.Fatalf("commit research: %v", err)
	}
	var quoteOutboxID string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT outbox_id FROM p2p_cloud_outbox WHERE kind = $1`, runtime.QuotePlanRequested,
	).Scan(&quoteOutboxID); err != nil {
		t.Fatal(err)
	}
	quoteJobID := cloudmodule.QuoteJobID(quoteOutboxID)

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
	const expectedProjectionEvents = 7
	for index := 0; index <= expectedProjectionEvents; index++ {
		processed, err := relay.RunOnce(ctx)
		if err != nil {
			t.Fatalf("relay iteration %d: %v", index, err)
		}
		if !processed {
			break
		}
	}
	if len(published) != expectedProjectionEvents {
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
		t.Fatalf("strict relay published %d events, want %d; rejected=%#v events=%#v", len(published), expectedProjectionEvents, rejected, published)
	}
	for _, event := range published {
		if containsAny(event.typ+" "+event.id, []string{"private deployment intent", "secret_ref"}) {
			t.Fatalf("published event identity leaked private material: %#v", event)
		}
	}
	var quotingPlan map[string]any
	var quoteJob map[string]any
	for _, event := range published {
		if event.typ == "cloud.plan.changed" && event.payload["revision"] == int64(2) {
			quotingPlan = event.payload
		}
		if event.typ == "cloud.job.changed" && event.payload["job_id"] == quoteJobID {
			quoteJob = event.payload
		}
	}
	if quotingPlan == nil || quotingPlan["goal_id"] != claim.GoalID || quotingPlan["created_at"] != int64(100) || quotingPlan["status"] != string(cloudcontracts.PlanQuoting) || quotingPlan["quote_id"] != "" || quotingPlan["plan_hash"] != "" {
		t.Fatalf("quoting plan relay payload = %#v", quotingPlan)
	}
	if quoteJob == nil || quoteJob["plan_id"] != claim.PlanID || quoteJob["kind"] != "quote" || quoteJob["checkpoint"] != "quote_queued" || quoteJob["execution_status"] != "queued" || quoteJob["outcome_status"] != "pending" {
		t.Fatalf("quote job relay payload = %#v", quoteJob)
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

func testResearchOutput(t *testing.T, now time.Time) runtime.ResearchOutput {
	t.Helper()
	recipe := cloudcontracts.RecipeV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, RecipeID: "recipe-knowledge-1", Name: "Private knowledge workload", Maturity: cloudcontracts.RecipeExperimental,
		Sources:      []cloudcontracts.RecipeSourceV1{{URL: "https://github.com/example/knowledge-workload", Version: "v1.0.0", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", License: "Apache-2.0", RetrievedAt: now, Official: true}},
		Requirements: cloudcontracts.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		Install:      cloudcontracts.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"artifact-ready", "service-ready"}, Steps: []cloudcontracts.InstallStepV1{{ID: "install", Summary: "Install the signed workload artifact", TimeoutSeconds: 900}}},
		Health:       cloudcontracts.HealthContractV1{Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"}, Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "verify-service"}},
		Lifecycle:    cloudcontracts.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
	}
	return runtime.ResearchOutput{
		Recipe: recipe,
		Draft: cloudcontracts.ResearchDraftV1{
			SchemaVersion: cloudcontracts.SchemaVersionV1,
			Region:        "ap-south-1",
			Candidates: []cloudcontracts.QuoteRequestCandidateV1{{
				CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80,
			}},
		},
		Title: "Private knowledge workload", Summary: "Official-source private single-VM proposal; obtain a verified price estimate before creating billable resources.",
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
