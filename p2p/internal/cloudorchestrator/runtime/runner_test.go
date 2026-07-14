package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRunnerCommitsValidatedResearchOutputExactlyOnce(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testResearchClaim()
	store := &fakeStore{claims: []Claim{claim}}
	planner := &fakePlanner{output: validResearchOutput(t, now, claim)}
	runner := New(store, planner, Config{
		WorkerID:   "orchestrator-1",
		Lease:      time.Minute,
		RetryDelay: time.Minute,
		Now:        func() time.Time { return now },
	})

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("first RunOnce = processed:%v err:%v", processed, err)
	}
	if planner.inputs[0].GoalID != claim.GoalID || planner.inputs[0].Prompt == "" {
		t.Fatalf("planner input = %#v", planner.inputs)
	}
	if len(store.committed) != 1 || store.committed[0].claim.LeaseToken != claim.LeaseToken {
		t.Fatalf("commit records = %#v", store.committed)
	}
	if len(store.deferred) != 0 || len(store.failed) != 0 {
		t.Fatalf("unexpected recovery state: deferred=%#v failed=%#v", store.deferred, store.failed)
	}
	if len(store.started) != 1 || store.started[0].LeaseToken != claim.LeaseToken {
		t.Fatalf("research start must be durable before planner invocation: %#v", store.started)
	}

	processed, err = runner.RunOnce(context.Background())
	if err != nil || processed {
		t.Fatalf("restarted RunOnce = processed:%v err:%v; a committed claim must not run twice", processed, err)
	}
}

func TestRunnerDefersOnlyClassifiedRetryablePlannerErrors(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testResearchClaim()
	store := &fakeStore{claims: []Claim{claim}}
	planner := &fakePlanner{err: Retryable("official_source_unavailable", errors.New("temporary source outage"))}
	runner := New(store, planner, Config{
		WorkerID:   "orchestrator-1",
		Lease:      time.Minute,
		RetryDelay: 90 * time.Second,
		Now:        func() time.Time { return now },
	})

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "official_source_unavailable" || !store.deferred[0].availableAt.Equal(now.Add(90*time.Second)) {
		t.Fatalf("deferred = %#v", store.deferred)
	}
	if len(store.committed) != 0 || len(store.failed) != 0 {
		t.Fatalf("retry must not commit or terminally fail: committed=%#v failed=%#v", store.committed, store.failed)
	}
}

func TestRunnerBoundsPlannerAttemptBeforeLeaseExpiry(t *testing.T) {
	claim := testResearchClaim()
	store := &fakeStore{claims: []Claim{claim}}
	planner := &fakePlanner{research: func(ctx context.Context, input ResearchInput) (ResearchOutput, error) {
		<-ctx.Done()
		return ResearchOutput{}, ctx.Err()
	}}
	runner := New(store, planner, Config{
		WorkerID:       "orchestrator-1",
		Lease:          time.Second,
		AttemptTimeout: time.Millisecond,
		RetryDelay:     time.Minute,
	})

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "research_attempt_timed_out" {
		t.Fatalf("attempt timeout must defer with a fixed code, got %#v", store.deferred)
	}
	if len(store.committed) != 0 || len(store.failed) != 0 {
		t.Fatalf("attempt timeout must not settle a plan: committed=%#v failed=%#v", store.committed, store.failed)
	}
}

func TestRunnerRejectsAttemptTimeoutThatCanOutliveItsLease(t *testing.T) {
	runner := New(&fakeStore{}, &fakePlanner{}, Config{
		WorkerID:       "orchestrator-1",
		Lease:          time.Minute,
		AttemptTimeout: time.Minute,
	})
	if _, err := runner.RunOnce(context.Background()); err == nil {
		t.Fatal("attempt timeout equal to lease must be rejected before claiming work")
	}
}

func TestRunnerFailsInvalidPayloadAndOutputWithoutLeakingPlannerError(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	t.Run("payload", func(t *testing.T) {
		claim := testResearchClaim()
		claim.PayloadJSON += `{}`
		store := &fakeStore{claims: []Claim{claim}}
		runner := New(store, &fakePlanner{}, Config{WorkerID: "orchestrator-1", Now: func() time.Time { return now }})

		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed || len(store.failed) != 1 || store.failed[0].code != "invalid_research_payload" {
			t.Fatalf("payload result processed=%v err=%v failed=%#v", processed, err, store.failed)
		}
	})
	t.Run("output", func(t *testing.T) {
		claim := testResearchClaim()
		output := validResearchOutput(t, now, claim)
		output.Draft.Candidates[0].PurchaseOption = cloudcontracts.PurchaseSpot
		store := &fakeStore{claims: []Claim{claim}}
		runner := New(store, &fakePlanner{output: output}, Config{WorkerID: "orchestrator-1", Now: func() time.Time { return now }})

		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed || len(store.failed) != 1 || store.failed[0].code != "invalid_research_output" {
			t.Fatalf("output result processed=%v err=%v failed=%#v", processed, err, store.failed)
		}
	})
}

type fakeStore struct {
	claims    []Claim
	started   []Claim
	committed []commitRecord
	deferred  []deferRecord
	failed    []failRecord
}

func (s *fakeStore) ClaimResearchGoal(context.Context, string, time.Duration) (Claim, bool, error) {
	if len(s.claims) == 0 {
		return Claim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeStore) MarkResearchStarted(_ context.Context, claim Claim) error {
	s.started = append(s.started, claim)
	return nil
}

func (s *fakeStore) CommitResearch(_ context.Context, claim Claim, output ResearchOutput) error {
	s.committed = append(s.committed, commitRecord{claim: claim, output: output})
	return nil
}

func (s *fakeStore) DeferResearch(_ context.Context, claim Claim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, deferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeStore) FailResearch(_ context.Context, claim Claim, code string) error {
	s.failed = append(s.failed, failRecord{claim: claim, code: code})
	return nil
}

type commitRecord struct {
	claim  Claim
	output ResearchOutput
}

type deferRecord struct {
	claim       Claim
	code        string
	availableAt time.Time
}

type failRecord struct {
	claim Claim
	code  string
}

type fakePlanner struct {
	output   ResearchOutput
	err      error
	inputs   []ResearchInput
	research func(context.Context, ResearchInput) (ResearchOutput, error)
}

func (p *fakePlanner) Research(ctx context.Context, input ResearchInput) (ResearchOutput, error) {
	p.inputs = append(p.inputs, input)
	if p.research != nil {
		return p.research(ctx, input)
	}
	return p.output, p.err
}

func testResearchClaim() Claim {
	return Claim{
		OutboxID:      "cloud_outbox_1",
		Kind:          "cloud.goal.research.requested",
		AggregateType: "goal",
		AggregateID:   "goal-1",
		GoalID:        "goal-1",
		PlanID:        "plan-1",
		ConnectionID:  "connection-1",
		PlanRevision:  1,
		LeaseToken:    "lease-1",
		PayloadJSON:   `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","goal":"Deploy a private knowledge workload."}`,
		Attempt:       1,
	}
}

func validResearchOutput(t *testing.T, now time.Time, claim Claim) ResearchOutput {
	t.Helper()
	recipe := cloudcontracts.RecipeV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1,
		RecipeID:      "recipe-knowledge-1",
		Name:          "Private knowledge workload",
		Maturity:      cloudcontracts.RecipeExperimental,
		Sources: []cloudcontracts.RecipeSourceV1{{
			URL:            "https://github.com/example/knowledge-workload",
			Version:        "v1.0.0",
			Commit:         "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			License:        "Apache-2.0",
			RetrievedAt:    now,
			Official:       true,
		}},
		Requirements: cloudcontracts.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		Install: cloudcontracts.InstallContractV1{
			RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"artifact-ready", "service-ready"},
			Steps: []cloudcontracts.InstallStepV1{{ID: "install", Summary: "Install the signed workload artifact", TimeoutSeconds: 900}},
		},
		Health: cloudcontracts.HealthContractV1{
			Liveness:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"},
			Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"},
			Semantic:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "verify-service"},
		},
		Lifecycle: cloudcontracts.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
	}
	return ResearchOutput{
		Recipe: recipe,
		Draft: cloudcontracts.ResearchDraftV1{
			SchemaVersion: cloudcontracts.SchemaVersionV1,
			Region:        "ap-south-1",
			Candidates: []cloudcontracts.QuoteRequestCandidateV1{{
				CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge",
				PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80,
			}},
		},
		Title: "Private knowledge workload", Summary: "Official-source private single-VM proposal; await a verified provider quote before any approval surface is created.",
	}
}
