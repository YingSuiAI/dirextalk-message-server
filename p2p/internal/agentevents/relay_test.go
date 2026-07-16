package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
