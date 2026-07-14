package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseStoreCreateCloudGoalIsAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	request := cloudGoalCreateRequest("cloud-goal-1", "cloud-plan-1", "idem-1", "digest-1", "event-1", "outbox-1")
	created, err := store.CreateCloudGoal(ctx, request)
	if err != nil || !created.Created || created.Goal.GoalID != request.Goal.GoalID || created.Plan.PlanID != request.Plan.PlanID {
		t.Fatalf("first create = %#v, err=%v", created, err)
	}
	replay, err := store.CreateCloudGoal(ctx, request)
	if err != nil || replay.Created || replay.Goal.GoalID != request.Goal.GoalID || replay.Plan.PlanID != request.Plan.PlanID {
		t.Fatalf("replay = %#v, err=%v", replay, err)
	}
	conflict := request
	conflict.Goal.RequestDigest = "different-digest"
	if _, err := store.CreateCloudGoal(ctx, conflict); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("different request under the same idempotency key = %v", err)
	}
	plans, err := store.ListCloudPlans(ctx)
	if err != nil || len(plans) != 1 || plans[0].PlanID != request.Plan.PlanID {
		t.Fatalf("plans after replay = %#v, err=%v", plans, err)
	}
	events, err := store.ListCloudEvents(ctx, 10)
	if err != nil || len(events) != 2 || events[0].SummaryJSON == "" || events[0].SummaryJSON == request.Goal.Prompt || events[0].Summary == nil {
		t.Fatalf("durable cloud events = %#v, err=%v", events, err)
	}
	var projections int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_projection_outbox`).Scan(&projections); err != nil || projections != 2 {
		t.Fatalf("initial cloud events must atomically create two projection records: count=%d err=%v", projections, err)
	}

	broken := cloudGoalCreateRequest("cloud-goal-2", "cloud-plan-2", "idem-2", "digest-2", "duplicate-event", "outbox-2")
	broken.Events[1].EventID = broken.Events[0].EventID
	if _, err := store.CreateCloudGoal(ctx, broken); err == nil {
		t.Fatal("duplicate durable cloud event must roll back the whole goal creation")
	}
	goals, err := store.ListCloudGoals(ctx)
	if err != nil || len(goals) != 1 || goals[0].GoalID != request.Goal.GoalID {
		t.Fatalf("failed create leaked a cloud goal: %#v, err=%v", goals, err)
	}
}

func TestDatabaseStoreGetCloudQuoteReturnsStrictSafeView(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	quotedAt := time.Date(2026, time.July, 14, 8, 0, 0, 0, time.UTC)
	validUntil := quotedAt.Add(15 * time.Minute)
	safeDisplay := `{"schema_version":"cloud-orchestrator/v1","quote_id":"quote-safe-1","cloud_connection_id":"connection-safe-1","region":"ap-south-1","currency":"USD","quoted_at":"` + quotedAt.Format(time.RFC3339Nano) + `","valid_until":"` + validUntil.Format(time.RFC3339Nano) + `","candidates":[{"candidate_id":"recommended-private-binding","tier":"recommended","instance_type":"m7i.xlarge","purchase_option":"on_demand","hourly_minor":2000,"thirty_day_minor":1440000,"startup_upper_minor":500,"estimated_disk_gib":80,"availability_zones":["ap-south-1a"]}],"included_items":["ec2_linux_ondemand"],"unincluded_items":["ebs_gp3","taxes"]}`
	insertCloudQuoteDisplay(t, store, "quote-safe-1", "connection-safe-1", "digest-safe-1", safeDisplay, quotedAt, validUntil)

	quote, found, err := store.GetCloudQuote(ctx, "quote-safe-1")
	if err != nil || !found || quote.QuoteID != "quote-safe-1" || quote.ConnectionID != "connection-safe-1" || quote.Region != "ap-south-1" || quote.Currency != "USD" || len(quote.Candidates) != 1 {
		t.Fatalf("safe cloud quote = %#v, found=%v err=%v", quote, found, err)
	}
	if quote.Candidates[0].Tier != "recommended" || quote.Candidates[0].InstanceType != "m7i.xlarge" || quote.Candidates[0].HourlyMinor != 2000 || len(quote.IncludedItems) != 1 || len(quote.UnincludedItems) != 2 {
		t.Fatalf("safe cloud quote content = %#v", quote)
	}
	encoded, err := json.Marshal(quote)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"schema_version", "candidate_id", "command_id", "request_sha256", "receipt", "envelope", "endpoint", "key", "secret"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("safe quote view leaked %q: %s", forbidden, encoded)
		}
	}

	privateCanary := "private-command-receipt-canary"
	unsafeDisplay := strings.Replace(safeDisplay, `"quote_id":"quote-safe-1"`, `"quote_id":"quote-unsafe-1"`, 1)
	unsafeDisplay = strings.TrimSuffix(unsafeDisplay, `}`) + `,"command_id":"` + privateCanary + `"}`
	insertCloudQuoteDisplay(t, store, "quote-unsafe-1", "connection-safe-1", "digest-unsafe-1", unsafeDisplay, quotedAt, validUntil)
	_, found, err = store.GetCloudQuote(ctx, "quote-unsafe-1")
	if err == nil || found {
		t.Fatalf("unsafe cloud quote result found=%v err=%v", found, err)
	}
	if strings.Contains(err.Error(), privateCanary) {
		t.Fatalf("unsafe quote error leaked stored display data: %v", err)
	}
}

func TestMemoryStoreGetCloudQuoteReturnsClonedSafeProjection(t *testing.T) {
	store := NewMemoryStore()
	request := cloudGoalCreateRequest("cloud-goal-memory-quote", "cloud-plan-memory-quote", "idem-memory-quote", "digest-memory-quote", "event-memory-quote", "outbox-memory-quote")
	quotedAt := time.Date(2026, time.July, 14, 8, 0, 0, 0, time.UTC)
	request.Plan.QuoteID = "quote-memory-1"
	request.Plan.Quote = &cloudmodule.QuoteView{
		QuoteID: "quote-memory-1", ConnectionID: "connection-memory-1", Region: "us-east-1", Currency: "USD",
		QuotedAt: quotedAt, ValidUntil: quotedAt.Add(15 * time.Minute),
		Candidates: []cloudmodule.QuoteCandidateView{{
			Tier: "economy", InstanceType: "t3.medium", PurchaseOption: "on_demand", EstimatedDiskGiB: 40,
			AvailabilityZones: []string{"us-east-1a"},
		}},
	}
	if _, err := store.CreateCloudGoal(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	first, found, err := store.GetCloudQuote(context.Background(), request.Plan.QuoteID)
	if err != nil || !found || first.QuoteID != request.Plan.QuoteID || len(first.Candidates) != 1 {
		t.Fatalf("memory cloud quote = %#v found=%v err=%v", first, found, err)
	}
	first.Candidates[0].AvailabilityZones[0] = "mutated"
	second, found, err := store.GetCloudQuote(context.Background(), request.Plan.QuoteID)
	if err != nil || !found || second.Candidates[0].AvailabilityZones[0] != "us-east-1a" {
		t.Fatalf("memory cloud quote must be cloned: %#v found=%v err=%v", second, found, err)
	}
}

func insertCloudQuoteDisplay(t *testing.T, store *DatabaseStore, quoteID, connectionID, digest, displayJSON string, quotedAt, validUntil time.Time) {
	t.Helper()
	_, err := store.DB().ExecContext(context.Background(), `
		INSERT INTO p2p_cloud_quotes (
			quote_id, cloud_connection_id, region, currency, digest, canonical_cbor, display_json,
			quoted_at, valid_until, created_at
		) VALUES ($1, $2, 'ap-south-1', 'USD', $3, $4, $5, $6, $7, $6)
	`, quoteID, connectionID, digest, []byte{1}, displayJSON, quotedAt.UnixMilli(), validUntil.UnixMilli())
	if err != nil {
		t.Fatalf("insert cloud quote display: %v", err)
	}
}

func TestDatabaseStoreCloudProjectionUsesAuthoritativeEventAndFencesLeases(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	request := cloudGoalCreateRequest("cloud-goal-projection", "cloud-plan-projection", "idem-projection", "digest-projection", "event-projection", "outbox-projection")
	request.Events = request.Events[:1]
	if _, err := store.CreateCloudGoal(ctx, request); err != nil {
		t.Fatalf("seed cloud projection: %v", err)
	}
	projectionID := cloudProjectionID(request.Events[0].EventID)
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_projection_outbox SET payload_json = $1 WHERE projection_id = $2`, `{"raw_worker_log":"sk-0123456789abcdefghijklmnop"}`, projectionID); err != nil {
		t.Fatal(err)
	}

	first, found, err := store.ClaimCloudProjection(ctx, "message-server-a", time.Minute, "lease-a")
	if err != nil || !found || first.ProjectionID != projectionID || first.LeaseToken != "lease-a" {
		t.Fatalf("first projection claim = %#v, found=%v err=%v", first, found, err)
	}
	if first.PayloadJSON != request.Events[0].SummaryJSON {
		t.Fatalf("claim must use authoritative cloud event summary, got %q want %q", first.PayloadJSON, request.Events[0].SummaryJSON)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_projection_outbox SET lease_until = 0 WHERE projection_id = $1`, projectionID); err != nil {
		t.Fatal(err)
	}
	second, found, err := store.ClaimCloudProjection(ctx, "message-server-b", time.Minute, "lease-b")
	if err != nil || !found || second.ProjectionID != projectionID || second.LeaseToken != "lease-b" {
		t.Fatalf("takeover projection claim = %#v, found=%v err=%v", second, found, err)
	}
	if err := store.CompleteCloudProjection(ctx, first); !errors.Is(err, cloudmodule.ErrProjectionLeaseLost) {
		t.Fatalf("expired worker completion = %v, want lease loss", err)
	}
	if err := store.CompleteCloudProjection(ctx, second); err != nil {
		t.Fatalf("current worker completion: %v", err)
	}
}

func TestDatabaseStoreCreateCloudGoalConcurrentReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Delay every insert so independent transactions all observe the empty
	// idempotency slot before one can commit. This reproduces a retry race that
	// a single in-process writer cannot hide in production PostgreSQL.
	if _, err := store.DB().ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION p2p_delay_cloud_goal_insert() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_sleep(0.05);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER p2p_delay_cloud_goal_insert
		BEFORE INSERT ON p2p_cloud_goals
		FOR EACH ROW EXECUTE FUNCTION p2p_delay_cloud_goal_insert();
	`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.DB().ExecContext(context.Background(), `DROP TRIGGER IF EXISTS p2p_delay_cloud_goal_insert ON p2p_cloud_goals; DROP FUNCTION IF EXISTS p2p_delay_cloud_goal_insert()`)
	})

	start := make(chan struct{})
	results := make(chan cloudmodule.CreateGoalResult, 8)
	errs := make(chan error, 8)
	var wait sync.WaitGroup
	for index := range 8 {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			// ProductCore creates fresh entity IDs for each HTTP retry. The
			// idempotency key, rather than an accidental primary-key collision,
			// must select the original winner.
			request := cloudGoalCreateRequest(
				fmt.Sprintf("cloud-goal-concurrent-%d", index),
				fmt.Sprintf("cloud-plan-concurrent-%d", index),
				"idem-concurrent", "digest-concurrent",
				fmt.Sprintf("event-concurrent-%d", index),
				fmt.Sprintf("outbox-concurrent-%d", index),
			)
			result, err := store.CreateCloudGoal(ctx, request)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent idempotent create returned %v", err)
	}
	created := 0
	var winner cloudmodule.CreateGoalResult
	allResults := make([]cloudmodule.CreateGoalResult, 0, 8)
	for result := range results {
		allResults = append(allResults, result)
		if result.Created {
			created++
			winner = result
		}
	}
	if created != 1 {
		t.Fatalf("concurrent idempotent create count = %d, want 1", created)
	}
	for _, result := range allResults {
		if result.Goal.GoalID != winner.Goal.GoalID || result.Plan.PlanID != winner.Plan.PlanID {
			t.Fatalf("concurrent replay returned a different entity: winner=%#v replay=%#v", winner, result)
		}
	}
	plans, err := store.ListCloudPlans(ctx)
	if err != nil || len(plans) != 1 || plans[0].PlanID != winner.Plan.PlanID {
		t.Fatalf("concurrent replay persisted plans = %#v, err=%v", plans, err)
	}
}

func cloudGoalCreateRequest(goalID, planID, idempotencyHash, requestDigest, eventID, outboxID string) cloudmodule.CreateGoalRequest {
	return cloudmodule.CreateGoalRequest{
		Goal: cloudmodule.Goal{
			GoalID: goalID, OwnerMXID: "@owner:example.com", Prompt: "private deployment intent",
			PlanID: planID, Status: cloudmodule.GoalStatusResearching,
			IdempotencyHash: idempotencyHash, RequestDigest: requestDigest,
			Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		},
		Plan: cloudmodule.Plan{
			PlanID: planID, GoalID: goalID, Status: cloudmodule.PlanStatusResearching,
			Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		},
		Events: []cloudmodule.Event{
			{EventID: eventID + "-goal", Type: "cloud.goal.changed", AggregateType: "goal", AggregateID: goalID, Revision: 1, SummaryJSON: `{"goal_id":"` + goalID + `"}`, CreatedAt: 100},
			{EventID: eventID + "-plan", Type: "cloud.plan.changed", AggregateType: "plan", AggregateID: planID, Revision: 1, SummaryJSON: `{"plan_id":"` + planID + `"}`, CreatedAt: 100},
		},
		Outbox: cloudmodule.OutboxEntry{
			OutboxID: outboxID, Kind: cloudmodule.OutboxKindResearchGoalRequested,
			AggregateType: "goal", AggregateID: goalID, PayloadJSON: `{"goal_id":"` + goalID + `"}`, CreatedAt: 100,
		},
	}
}
