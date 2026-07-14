package cloud

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProjectionRelayPublishesOnlyWhitelistedCloudSummaries(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		claim       ProjectionClaim
		wantPayload map[string]any
	}{
		{
			name: "goal",
			claim: ProjectionClaim{
				ProjectionID: "projection-goal", CloudEventID: "event-goal", LeaseToken: "lease-goal", Type: "cloud.goal.changed",
				PayloadJSON: `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","status":"researching","revision":1,"created_at":100,"updated_at":100}`,
			},
			wantPayload: map[string]any{"goal_id": "goal-1", "plan_id": "plan-1", "cloud_connection_id": "connection-1", "status": "researching", "revision": int64(1), "created_at": int64(100), "updated_at": int64(100)},
		},
		{
			name: "plan",
			claim: ProjectionClaim{
				ProjectionID: "projection-plan", CloudEventID: "event-plan", LeaseToken: "lease-plan", Type: "cloud.plan.changed",
				PayloadJSON: `{"plan_id":"plan-1","goal_id":"goal-1","cloud_connection_id":"","status":"ready_for_confirmation","title":"Private knowledge node","summary":"Review the hourly estimate before creating billable resources.","recipe_digest":"recipe-digest","quote_id":"quote-1","plan_hash":"plan-hash","revision":2,"created_at":100,"updated_at":101}`,
			},
			wantPayload: map[string]any{"plan_id": "plan-1", "goal_id": "goal-1", "cloud_connection_id": "", "status": "ready_for_confirmation", "title": "Private knowledge node", "summary": "Review the hourly estimate before creating billable resources.", "recipe_digest": "recipe-digest", "quote_id": "quote-1", "plan_hash": "plan-hash", "revision": int64(2), "created_at": int64(100), "updated_at": int64(101)},
		},
		{
			name: "job",
			claim: ProjectionClaim{
				ProjectionID: "projection-job", CloudEventID: "event-job", LeaseToken: "lease-job", Type: "cloud.job.changed",
				PayloadJSON: `{"job_id":"job-1","plan_id":"plan-1","kind":"research","execution_status":"finished","outcome_status":"succeeded","checkpoint":"quote_ready","error_code":"","revision":1,"created_at":100,"updated_at":101}`,
			},
			wantPayload: map[string]any{"job_id": "job-1", "plan_id": "plan-1", "deployment_id": "", "kind": "research", "execution_status": "finished", "outcome_status": "succeeded", "checkpoint": "quote_ready", "error_code": "", "revision": int64(1), "created_at": int64(100), "updated_at": int64(101)},
		},
		{
			name: "deployment",
			claim: ProjectionClaim{
				ProjectionID: "projection-deployment", CloudEventID: "event-deployment", LeaseToken: "lease-deployment", Type: "cloud.deployment.changed",
				PayloadJSON: `{"deployment_id":"deployment-1","plan_id":"plan-1","cloud_connection_id":"connection-1","execution_status":"queued","outcome_status":"pending","resource_status":"none","revision":1,"created_at":100,"updated_at":101}`,
			},
			wantPayload: map[string]any{"deployment_id": "deployment-1", "plan_id": "plan-1", "cloud_connection_id": "connection-1", "execution_status": "queued", "outcome_status": "pending", "resource_status": "none", "revision": int64(1), "created_at": int64(100), "updated_at": int64(101)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeProjectionStore{claims: []ProjectionClaim{test.claim}}
			publisher := &recordingProjectionPublisher{}
			relay := NewProjectionRelay(store, publisher.publish, ProjectionRelayConfig{
				WorkerID: "message-server-1", Lease: time.Minute, RetryDelay: time.Minute,
				Now: func() time.Time { return now }, NewLeaseToken: func() string { return "new-lease" },
			})

			processed, err := relay.RunOnce(context.Background())
			if err != nil || !processed {
				t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
			}
			if len(publisher.events) != 1 || publisher.events[0].eventID != test.claim.CloudEventID || publisher.events[0].eventType != test.claim.Type {
				t.Fatalf("published events = %#v", publisher.events)
			}
			if !equalProjectionPayload(publisher.events[0].payload, test.wantPayload) {
				t.Fatalf("published payload = %#v, want %#v", publisher.events[0].payload, test.wantPayload)
			}
			if len(store.completed) != 1 || store.completed[0].ProjectionID != test.claim.ProjectionID || len(store.deferred) != 0 || len(store.rejected) != 0 {
				t.Fatalf("settlement = completed:%#v deferred:%#v rejected:%#v", store.completed, store.deferred, store.rejected)
			}
		})
	}
}

func TestProjectionRelayRejectsUnsafeOrMalformedPayloadWithoutPublishing(t *testing.T) {
	tests := []struct {
		name  string
		claim ProjectionClaim
	}{
		{
			name:  "unknown type",
			claim: ProjectionClaim{ProjectionID: "projection-1", CloudEventID: "event-1", LeaseToken: "lease-1", Type: "cloud.worker.raw_log", PayloadJSON: `{}`},
		},
		{
			name:  "unknown field",
			claim: ProjectionClaim{ProjectionID: "projection-2", CloudEventID: "event-2", LeaseToken: "lease-2", Type: "cloud.job.changed", PayloadJSON: `{"job_id":"job-1","plan_id":"plan-1","kind":"research","execution_status":"finished","outcome_status":"failed","checkpoint":"","error_code":"safe_code","revision":1,"created_at":1,"updated_at":1,"raw_worker_log":"must never project"}`},
		},
		{
			name:  "secret shaped text",
			claim: ProjectionClaim{ProjectionID: "projection-3", CloudEventID: "event-3", LeaseToken: "lease-3", Type: "cloud.plan.changed", PayloadJSON: `{"plan_id":"plan-1","goal_id":"goal-1","cloud_connection_id":"connection-1","status":"ready_for_confirmation","title":"sk-0123456789abcdefghijklmnop","summary":"safe","recipe_digest":"recipe","quote_id":"quote","plan_hash":"hash","revision":2,"created_at":1,"updated_at":1}`},
		},
		{
			name:  "deployment enrollment leak",
			claim: ProjectionClaim{ProjectionID: "projection-4", CloudEventID: "event-4", LeaseToken: "lease-4", Type: "cloud.deployment.changed", PayloadJSON: `{"deployment_id":"deployment-1","plan_id":"plan-1","cloud_connection_id":"connection-1","execution_status":"queued","outcome_status":"pending","resource_status":"none","revision":1,"created_at":1,"updated_at":1,"worker_enrollment":"must never project"}`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeProjectionStore{claims: []ProjectionClaim{test.claim}}
			publisher := &recordingProjectionPublisher{}
			relay := NewProjectionRelay(store, publisher.publish, ProjectionRelayConfig{WorkerID: "message-server-1"})

			processed, err := relay.RunOnce(context.Background())
			if err != nil || !processed {
				t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
			}
			if len(publisher.events) != 0 || len(store.rejected) != 1 || store.rejected[0].code != "invalid_cloud_projection" {
				t.Fatalf("unsafe projection must be terminally rejected: published=%#v rejected=%#v", publisher.events, store.rejected)
			}
		})
	}
}

func TestProjectionRelayDefersTransientPublishAndDedupesAfterAckFailure(t *testing.T) {
	claim := ProjectionClaim{
		ProjectionID: "projection-1", CloudEventID: "event-1", LeaseToken: "lease-1", Type: "cloud.goal.changed",
		PayloadJSON: `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","status":"researching","revision":1,"created_at":1,"updated_at":1}`,
	}
	t.Run("publish failure", func(t *testing.T) {
		store := &fakeProjectionStore{claims: []ProjectionClaim{claim}}
		publisher := &recordingProjectionPublisher{err: errors.New("temporary event store outage")}
		relay := NewProjectionRelay(store, publisher.publish, ProjectionRelayConfig{WorkerID: "message-server-1", RetryDelay: time.Minute})

		processed, err := relay.RunOnce(context.Background())
		if err != nil || !processed || len(store.deferred) != 1 || store.deferred[0].code != "cloud_projection_publish_failed" || len(store.completed) != 0 {
			t.Fatalf("transient publish result processed=%v err=%v deferred=%#v completed=%#v", processed, err, store.deferred, store.completed)
		}
	})
	t.Run("append succeeds before ack failure", func(t *testing.T) {
		store := &fakeProjectionStore{claims: []ProjectionClaim{claim, claim}, completeErr: errors.New("ack interrupted")}
		publisher := &recordingProjectionPublisher{dedupe: map[string]struct{}{}}
		relay := NewProjectionRelay(store, publisher.publish, ProjectionRelayConfig{WorkerID: "message-server-1"})

		if processed, err := relay.RunOnce(context.Background()); !processed || err == nil {
			t.Fatalf("first RunOnce = processed:%v err:%v; simulated ack loss must surface", processed, err)
		}
		if processed, err := relay.RunOnce(context.Background()); !processed || err != nil {
			t.Fatalf("replayed RunOnce = processed:%v err:%v", processed, err)
		}
		if len(publisher.events) != 1 || len(store.completed) != 1 {
			t.Fatalf("deduped replay = events:%#v completed:%#v", publisher.events, store.completed)
		}
	})
}

type fakeProjectionStore struct {
	claims      []ProjectionClaim
	completed   []ProjectionClaim
	deferred    []deferredProjection
	rejected    []rejectedProjection
	completeErr error
}

func (s *fakeProjectionStore) ClaimCloudProjection(_ context.Context, _ string, _ time.Duration, _ string) (ProjectionClaim, bool, error) {
	if len(s.claims) == 0 {
		return ProjectionClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeProjectionStore) CompleteCloudProjection(_ context.Context, claim ProjectionClaim) error {
	if s.completeErr != nil {
		err := s.completeErr
		s.completeErr = nil
		return err
	}
	s.completed = append(s.completed, claim)
	return nil
}

func (s *fakeProjectionStore) DeferCloudProjection(_ context.Context, claim ProjectionClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, deferredProjection{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeProjectionStore) RejectCloudProjection(_ context.Context, claim ProjectionClaim, code string) error {
	s.rejected = append(s.rejected, rejectedProjection{claim: claim, code: code})
	return nil
}

type deferredProjection struct {
	claim       ProjectionClaim
	code        string
	availableAt time.Time
}

type rejectedProjection struct {
	claim ProjectionClaim
	code  string
}

type recordedProjection struct {
	eventID   string
	eventType string
	payload   map[string]any
}

type recordingProjectionPublisher struct {
	events []recordedProjection
	err    error
	dedupe map[string]struct{}
}

func (p *recordingProjectionPublisher) publish(_ context.Context, eventID, eventType string, payload map[string]any) error {
	if p.err != nil {
		return p.err
	}
	if p.dedupe != nil {
		if _, found := p.dedupe[eventID]; found {
			return nil
		}
		p.dedupe[eventID] = struct{}{}
	}
	p.events = append(p.events, recordedProjection{eventID: eventID, eventType: eventType, payload: payload})
	return nil
}

func equalProjectionPayload(left, right map[string]any) bool {
	if len(left) != len(right) {
		return false
	}
	for key, want := range right {
		if left[key] != want {
			return false
		}
	}
	return true
}
