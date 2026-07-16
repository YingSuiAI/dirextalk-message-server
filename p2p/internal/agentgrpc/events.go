package agentgrpc

import (
	"context"
	"errors"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
	"google.golang.org/grpc"
)

func (runner *Runner) AgentEventSource() agentevents.Source {
	if runner == nil {
		return agentevents.Source{}
	}
	return agentevents.Source{AgentInstanceID: runner.agentInstanceID, CallerID: runner.ownerID}
}

func (runner *Runner) WatchEvents(ctx context.Context, afterSeq int64) (agentevents.EventStream, error) {
	if runner == nil || runner.tasks == nil || afterSeq < 0 {
		return nil, errors.New("Agent event stream is unavailable")
	}
	stream, err := runner.tasks.WatchEvents(ctx, &agentv1.WatchEventsRequest{AfterSeq: afterSeq})
	if err != nil {
		return nil, sanitizeRPCError(ctx, err)
	}
	return &agentEventStream{stream: stream}, nil
}

type agentEventStream struct {
	stream grpc.ServerStreamingClient[agentv1.WatchEventsResponse]
}

func (stream *agentEventStream) Recv() (agentevents.Event, error) {
	if stream == nil || stream.stream == nil {
		return agentevents.Event{}, errors.New("Agent event stream is unavailable")
	}
	response, err := stream.stream.Recv()
	if err != nil {
		return agentevents.Event{}, err
	}
	remote := response.GetEvent()
	if remote == nil || remote.GetOccurredAt() == nil || remote.GetOccurredAt().CheckValid() != nil {
		return agentevents.Event{}, nil
	}
	return agentevents.Event{
		Seq: remote.GetSeq(), EventID: remote.GetEventId(), EventType: remote.GetEventType(),
		AggregateType: remote.GetAggregateType(), AggregateID: remote.GetAggregateId(), Revision: remote.GetRevision(),
		SummaryJSON: append([]byte(nil), remote.GetSummaryJson()...), OccurredAt: remote.GetOccurredAt().AsTime().UTC(),
	}, nil
}
