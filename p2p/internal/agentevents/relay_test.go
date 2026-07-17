package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRelayReplaysFromDurableCursorWithoutDuplicateProjection(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	store := &memoryProjectionStore{}
	first := projectablePlanEvent(1, 1, source.CallerID)
	second := projectablePlanEvent(2, 2, source.CallerID)
	client := &recordingClient{streams: []EventStream{
		&sliceStream{events: []Event{first}, terminal: io.EOF},
		&sliceStream{events: []Event{first, second}, terminal: context.Canceled},
	}}
	relay := New(client, store, source, Config{})

	if err := relay.consumeConnection(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("first consume error = %v", err)
	}
	if err := relay.consumeConnection(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("second consume error = %v", err)
	}
	if len(client.afterSeq) != 2 || client.afterSeq[0] != 0 || client.afterSeq[1] != 1 {
		t.Fatalf("WatchEvents cursors = %#v", client.afterSeq)
	}
	if store.cursor != 2 || len(store.projected) != 2 || store.projected[0].SourceEventSeq != 1 || store.projected[1].SourceEventSeq != 2 {
		t.Fatalf("durable projection state cursor=%d projected=%#v", store.cursor, store.projected)
	}
}

func TestRelaySecretCanaryFailsClosedBeforeCursorAdvance(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	for name, canary := range map[string]string{
		"model token":    "sk-0123456789abcdefghijklmnop",
		"AWS access key": "AKIA0123456789ABCDEF",
		"private key":    "-----BEGIN PRIVATE KEY-----",
	} {
		t.Run(name, func(t *testing.T) {
			store := &memoryProjectionStore{}
			event := projectablePlanEvent(1, 1, source.CallerID)
			event.SummaryJSON = []byte(`{"plan_id":"` + event.AggregateID + `","owner_id":"` + source.CallerID + `","revision":1,"status":"planning","note":"` + canary + `"}`)
			relay := New(&recordingClient{streams: []EventStream{&sliceStream{events: []Event{event}, terminal: io.EOF}}}, store, source, Config{})

			err := relay.consumeConnection(context.Background())
			if !errors.Is(err, ErrUnsafeEvent) || strings.Contains(err.Error(), canary) {
				t.Fatalf("secret validation error = %v", err)
			}
			if store.commitCalls != 0 || store.cursor != 0 || len(store.projected) != 0 {
				t.Fatalf("unsafe event reached persistence: calls=%d cursor=%d projections=%d", store.commitCalls, store.cursor, len(store.projected))
			}
		})
	}
}

func TestPrepareProjectionPublishesOnlyReviewedPlanFieldsAndSourceEpoch(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	event := projectablePlanEvent(1, 1, source.CallerID)

	projection, err := prepareProjection(source, event)
	if err != nil {
		t.Fatal(err)
	}
	if projection == nil || projection.Payload["source_agent_instance_id"] != source.AgentInstanceID {
		t.Fatalf("projection source epoch = %#v", projection)
	}
	if _, present := projection.Payload["actor"]; present {
		t.Fatalf("internal actor leaked into ProductCore projection: %#v", projection.Payload)
	}
	if got, want := len(projection.Payload), 12; got != want {
		t.Fatalf("projected field count = %d, want %d: %#v", got, want, projection.Payload)
	}
}

func TestPrepareProjectionPublishesOnlyReviewedCloudTaskAndStepFields(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	for name, event := range map[string]Event{
		"task": projectableCloudTaskEvent(1, 1, source.CallerID),
		"step": projectableCloudStepEvent(2, 1, source.CallerID),
	} {
		t.Run(name, func(t *testing.T) {
			projection, err := prepareProjection(source, event)
			if err != nil {
				t.Fatal(err)
			}
			if projection == nil || projection.Type != event.EventType || projection.Payload["source_agent_instance_id"] != source.AgentInstanceID {
				t.Fatalf("projection = %#v", projection)
			}
			if projection.Payload["schema_version"] != cloudTaskEventSummarySchemaV1 || projection.Payload["revision"] != event.Revision {
				t.Fatalf("schema/revision drifted: %#v", projection.Payload)
			}
			if _, present := projection.Payload["related_plan_id"]; present {
				t.Fatalf("empty plan binding was projected: %#v", projection.Payload)
			}
			for _, forbidden := range []string{"goal", "connection_id", "step_name", "checkpoint_ref", "result_ref", "worker_id", "actor"} {
				if _, present := projection.Payload[forbidden]; present {
					t.Fatalf("unreviewed field %q reached ProductCore: %#v", forbidden, projection.Payload)
				}
			}
			wantKeys := map[string]struct{}{
				"schema_version": {}, "task_id": {}, "owner_id": {}, "execution_status": {}, "outcome_status": {},
				"current_stage": {}, "revision": {}, "updated_at": {}, "source_agent_instance_id": {},
			}
			if name == "step" {
				wantKeys["step_id"] = struct{}{}
			}
			gotKeys := map[string]struct{}{}
			for key := range projection.Payload {
				gotKeys[key] = struct{}{}
			}
			if !reflect.DeepEqual(gotKeys, wantKeys) {
				t.Fatalf("projected keys = %#v, want %#v", gotKeys, wantKeys)
			}
		})
	}
}

func TestCloudTaskAndStepProjectionRejectUnknownAndInvalidSummaryFields(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	for name, test := range map[string]struct {
		event  Event
		mutate func(map[string]any)
		want   error
	}{
		"task unknown secret field": {
			event:  projectableCloudTaskEvent(1, 1, source.CallerID),
			mutate: func(summary map[string]any) { summary["api_key"] = "not-a-real-key" }, want: ErrUnsafeEvent,
		},
		"step unknown worker field": {
			event:  projectableCloudStepEvent(1, 1, source.CallerID),
			mutate: func(summary map[string]any) { summary["worker_id"] = "worker-1" }, want: ErrUnsafeEvent,
		},
		"task invalid stage": {
			event:  projectableCloudTaskEvent(1, 1, source.CallerID),
			mutate: func(summary map[string]any) { summary["current_stage"] = "arbitrary_agent_text" }, want: ErrInvalidEvent,
		},
		"step invalid error code": {
			event:  projectableCloudStepEvent(1, 1, source.CallerID),
			mutate: func(summary map[string]any) { summary["error_code"] = "unreviewed_error" }, want: ErrInvalidEvent,
		},
		"task invalid related plan": {
			event:  projectableCloudTaskEvent(1, 1, source.CallerID),
			mutate: func(summary map[string]any) { summary["related_plan_id"] = "not-a-uuid" }, want: ErrInvalidEvent,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var summary map[string]any
			if err := json.Unmarshal(test.event.SummaryJSON, &summary); err != nil {
				t.Fatal(err)
			}
			test.mutate(summary)
			encoded, err := json.Marshal(summary)
			if err != nil {
				t.Fatal(err)
			}
			test.event.SummaryJSON = encoded
			projection, err := prepareProjection(source, test.event)
			if projection != nil || !errors.Is(err, test.want) {
				t.Fatalf("projection/error = %#v / %v, want %v", projection, err, test.want)
			}
		})
	}
}

func TestRelayCancelsStreamWhenProjectionValidationFails(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	event := projectablePlanEvent(1, 1, source.CallerID)
	event.SummaryJSON = append(event.SummaryJSON[:len(event.SummaryJSON)-1], []byte(`,"api_key":"not-a-real-key"}`)...)
	client := &recordingClient{streams: []EventStream{&sliceStream{events: []Event{event}, terminal: io.EOF}}}
	relay := New(client, &memoryProjectionStore{}, source, Config{})

	if err := relay.consumeConnection(context.Background()); !errors.Is(err, ErrUnsafeEvent) {
		t.Fatalf("consume error = %v", err)
	}
	if len(client.contexts) != 1 {
		t.Fatalf("stream contexts = %d", len(client.contexts))
	}
	select {
	case <-client.contexts[0].Done():
	default:
		t.Fatal("failed connection left Agent event stream context active")
	}
}

func TestRelayIgnoresOtherOwnerAndNonProjectableFactsWhileAdvancingCursor(t *testing.T) {
	source := Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	otherOwner := projectablePlanEvent(1, 1, "dirextalk-project:other.example")
	nonCloud := Event{
		Seq: 2, EventID: uuid.NewString(), EventType: "agent.task.changed", AggregateType: "task",
		AggregateID: uuid.NewString(), Revision: 1, SummaryJSON: []byte(`{"revision":1}`), OccurredAt: time.Now().UTC(),
	}
	store := &memoryProjectionStore{}
	relay := New(&recordingClient{streams: []EventStream{&sliceStream{events: []Event{otherOwner, nonCloud}, terminal: io.EOF}}}, store, source, Config{})

	if err := relay.consumeConnection(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("consume error = %v", err)
	}
	if store.cursor != 2 || len(store.projected) != 0 {
		t.Fatalf("ignored events cursor=%d projected=%#v", store.cursor, store.projected)
	}
}

func projectablePlanEvent(seq, revision int64, owner string) Event {
	id := uuid.NewString()
	summary, err := json.Marshal(planEventSummary{
		PlanID: id, OwnerID: owner, Status: "ready_for_confirmation", Revision: revision,
		PlanHash: "sha256:" + strings.Repeat("a", 64), QuoteID: uuid.NewString(),
		QuoteValidUntil: time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano),
		Region:          "ap-south-1", InstanceType: "t3.small", SecretReferenceCount: 0,
		Actor: planEventActor{ClientID: "message-server", CredentialID: uuid.NewString()},
	})
	if err != nil {
		panic(err)
	}
	return Event{
		Seq: seq, EventID: uuid.NewString(), EventType: "cloud.plan.changed", AggregateType: "cloud_plan",
		AggregateID: id, Revision: revision, OccurredAt: time.Now().UTC(),
		SummaryJSON: summary,
	}
}

func projectableCloudTaskEvent(seq, revision int64, owner string) Event {
	id := uuid.NewString()
	summary, err := json.Marshal(cloudTaskEventSummary{
		SchemaVersion: cloudTaskEventSummarySchemaV1, TaskID: id, OwnerID: owner,
		ExecutionStatus: "planning", OutcomeStatus: "pending", CurrentStage: "research",
		Revision: revision, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		panic(err)
	}
	return Event{
		Seq: seq, EventID: uuid.NewString(), EventType: "cloud.task.changed", AggregateType: "cloud_task",
		AggregateID: id, Revision: revision, OccurredAt: time.Now().UTC(), SummaryJSON: summary,
	}
}

func projectableCloudStepEvent(seq, revision int64, owner string) Event {
	stepID, taskID := uuid.NewString(), uuid.NewString()
	summary, err := json.Marshal(cloudStepEventSummary{
		SchemaVersion: cloudTaskEventSummarySchemaV1, TaskID: taskID, StepID: stepID, OwnerID: owner,
		ExecutionStatus: "running", OutcomeStatus: "pending", CurrentStage: "recipe",
		Revision: revision, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		panic(err)
	}
	return Event{
		Seq: seq, EventID: uuid.NewString(), EventType: "cloud.step.changed", AggregateType: "cloud_step",
		AggregateID: stepID, Revision: revision, OccurredAt: time.Now().UTC(), SummaryJSON: summary,
	}
}

type recordingClient struct {
	streams  []EventStream
	afterSeq []int64
	contexts []context.Context
}

func (client *recordingClient) WatchEvents(ctx context.Context, afterSeq int64) (EventStream, error) {
	client.afterSeq = append(client.afterSeq, afterSeq)
	client.contexts = append(client.contexts, ctx)
	if len(client.streams) == 0 {
		return nil, io.EOF
	}
	stream := client.streams[0]
	client.streams = client.streams[1:]
	return stream, nil
}

type sliceStream struct {
	events   []Event
	terminal error
}

func (stream *sliceStream) Recv() (Event, error) {
	if len(stream.events) == 0 {
		return Event{}, stream.terminal
	}
	event := stream.events[0]
	stream.events = stream.events[1:]
	return event, nil
}

type memoryProjectionStore struct {
	cursor      int64
	commitCalls int
	projected   []Projection
	revisions   map[string]int64
}

func (store *memoryProjectionStore) Cursor(context.Context, Source) (int64, error) {
	return store.cursor, nil
}

func (store *memoryProjectionStore) Commit(_ context.Context, request CommitRequest) (CommitResult, error) {
	store.commitCalls++
	if request.Event.Seq <= store.cursor {
		return CommitResult{Cursor: store.cursor}, nil
	}
	result := CommitResult{Cursor: request.Event.Seq}
	store.cursor = request.Event.Seq
	if request.Projection == nil {
		return result, nil
	}
	if store.revisions == nil {
		store.revisions = map[string]int64{}
	}
	key := request.Event.EventType + "\x00" + request.Event.AggregateID
	if request.Event.Revision <= store.revisions[key] {
		return result, nil
	}
	store.revisions[key] = request.Event.Revision
	store.projected = append(store.projected, *request.Projection)
	result.Inserted = true
	return result, nil
}
