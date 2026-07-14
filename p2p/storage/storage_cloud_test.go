package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

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
