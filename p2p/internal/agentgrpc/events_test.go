package agentgrpc

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWatchEventsUsesPersistentCursorAndMapsEventV1(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	aggregateID := uuid.NewString()
	server.tasks.event = &agentv1.Event{
		Seq: 42, EventId: uuid.NewString(), EventType: "cloud.plan.changed", AggregateType: "cloud_plan",
		AggregateId: aggregateID, Revision: 3, SummaryJson: []byte(`{"plan_id":"` + aggregateID + `","revision":3}`),
		OccurredAt: timestamppb.New(time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)),
	}

	stream, err := runner.WatchEvents(context.Background(), 41)
	if err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if server.tasks.afterSeq != 41 || server.tasks.authorization != authorizationScheme+" "+testServiceKey {
		t.Fatalf("WatchEvents request cursor=%d authorization=%q", server.tasks.afterSeq, server.tasks.authorization)
	}
	if event.Seq != 42 || event.EventID != server.tasks.event.GetEventId() || event.AggregateID != aggregateID || event.Revision != 3 || string(event.SummaryJSON) != string(server.tasks.event.GetSummaryJson()) {
		t.Fatalf("mapped Agent event=%#v", event)
	}
	source := runner.AgentEventSource()
	if source.AgentInstanceID != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" || source.CallerID != "owner-from-config" {
		t.Fatalf("Agent event source=%#v", source)
	}
}

type taskTestService struct {
	agentv1.UnimplementedTaskServiceServer
	afterSeq      int64
	authorization string
	event         *agentv1.Event
}

func (service *taskTestService) WatchEvents(request *agentv1.WatchEventsRequest, stream grpc.ServerStreamingServer[agentv1.WatchEventsResponse]) error {
	service.afterSeq = request.GetAfterSeq()
	if values := metadata.ValueFromIncomingContext(stream.Context(), "authorization"); len(values) == 1 {
		service.authorization = values[0]
	}
	return stream.Send(&agentv1.WatchEventsResponse{Event: service.event})
}
