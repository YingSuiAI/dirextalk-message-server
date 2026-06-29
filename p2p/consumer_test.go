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

func TestOutputRoomEventConsumerProcessesBatchSequentially(t *testing.T) {
	ctx := context.Background()
	processed := []roomserverAPI.OutputType{}
	consumer := &OutputRoomEventConsumer{
		metrics: newP2PProjectorConsumerMetrics(),
		projectOutputEvent: func(ctx context.Context, output roomserverAPI.OutputEvent) error {
			processed = append(processed, output.Type)
			if output.Type == roomserverAPI.OutputTypeNewRoomEvent {
				return errors.New("second failed")
			}
			return nil
		},
	}
	first, err := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeOldRoomEvent})
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeNewRoomEvent})
	if err != nil {
		t.Fatal(err)
	}

	if consumer.onMessage(ctx, []*nats.Msg{{Data: first}, {Data: second}}) {
		t.Fatalf("expected batch to NAK when a later message fails")
	}
	if len(processed) != 2 || processed[0] != roomserverAPI.OutputTypeOldRoomEvent || processed[1] != roomserverAPI.OutputTypeNewRoomEvent {
		t.Fatalf("expected batch to process messages sequentially, got %#v", processed)
	}
	snapshot := consumer.metrics.snapshot()
	if snapshot.Received != 2 || snapshot.Processed != 1 || snapshot.Failed != 1 || snapshot.ConsecutiveFailures != 1 {
		t.Fatalf("expected sequential batch metrics, got %#v", snapshot)
	}
}

func TestP2PProjectorBatchSizeFromEnv(t *testing.T) {
	t.Setenv("P2P_PROJECTOR_BATCH_SIZE", "")
	if got := p2pProjectorBatchSizeFromEnv(); got != 1 {
		t.Fatalf("expected default batch size 1, got %d", got)
	}
	t.Setenv("P2P_PROJECTOR_BATCH_SIZE", "25")
	if got := p2pProjectorBatchSizeFromEnv(); got != 25 {
		t.Fatalf("expected configured batch size 25, got %d", got)
	}
	t.Setenv("P2P_PROJECTOR_BATCH_SIZE", "0")
	if got := p2pProjectorBatchSizeFromEnv(); got != 1 {
		t.Fatalf("expected invalid low batch size to fall back to 1, got %d", got)
	}
	t.Setenv("P2P_PROJECTOR_BATCH_SIZE", "250")
	if got := p2pProjectorBatchSizeFromEnv(); got != 100 {
		t.Fatalf("expected high batch size to be capped at 100, got %d", got)
	}
}
