package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/nats-io/nats.go"
)

func TestOutputRoomEventConsumerRecordsProcessingMetrics(t *testing.T) {
	ctx := context.Background()
	metrics := newP2PProjectorConsumerMetrics()
	projectErr := errors.New("project failed")
	consumer := &OutputRoomEventConsumer{
		metrics: metrics,
		projectOutputEvent: func(ctx context.Context, output roomserverAPI.OutputEvent) error {
			if output.Type == roomserverAPI.OutputTypeNewRoomEvent {
				return projectErr
			}
			return nil
		},
	}

	valid, err := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeOldRoomEvent})
	if err != nil {
		t.Fatal(err)
	}
	if !consumer.onMessage(ctx, []*nats.Msg{{Data: valid}}) {
		t.Fatalf("expected valid ignored output event to ack")
	}
	snapshot := metrics.snapshot()
	if snapshot.Received != 1 || snapshot.Processed != 1 || snapshot.Discarded != 0 || snapshot.Failed != 0 || snapshot.ConsecutiveFailures != 0 || snapshot.LastSuccessUnix <= 0 {
		t.Fatalf("expected successful processing metrics, got %#v", snapshot)
	}

	if !consumer.onMessage(ctx, []*nats.Msg{{Data: []byte("{")}}) {
		t.Fatalf("expected malformed output event to be discarded and acked")
	}
	snapshot = metrics.snapshot()
	if snapshot.Received != 2 || snapshot.Processed != 1 || snapshot.Discarded != 1 || snapshot.Failed != 0 || snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("expected discard metrics without failure backoff, got %#v", snapshot)
	}

	failing, err := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeNewRoomEvent})
	if err != nil {
		t.Fatal(err)
	}
	if consumer.onMessage(ctx, []*nats.Msg{{Data: failing}}) {
		t.Fatalf("expected projector failure to NAK")
	}
	snapshot = metrics.snapshot()
	if snapshot.Received != 3 || snapshot.Processed != 1 || snapshot.Discarded != 1 || snapshot.Failed != 1 || snapshot.ConsecutiveFailures != 1 || snapshot.LastFailureUnix <= 0 {
		t.Fatalf("expected failure and backoff metrics, got %#v", snapshot)
	}
}
