package projector

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/nats-io/nats.go"
)

func TestOutputRoomEventConsumerRecordsProcessingMetrics(t *testing.T) {
	ctx := context.Background()
	metrics := newConsumerMetrics()
	projectErr := errors.New("project failed")
	consumer := &OutputRoomEventConsumer{
		metrics: metrics,
		projectOutputEvent: func(context.Context, roomserverAPI.OutputEvent) error {
			return projectErr
		},
	}

	valid, err := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeOldRoomEvent})
	if err != nil {
		t.Fatal(err)
	}
	consumer.projectOutputEvent = func(context.Context, roomserverAPI.OutputEvent) error { return nil }
	if !consumer.onMessage(ctx, []*nats.Msg{{Data: valid}}) {
		t.Fatal("valid output event was not acknowledged")
	}
	if got := metrics.snapshot(); got.Received != 1 || got.Processed != 1 || got.LastSuccessUnix <= 0 {
		t.Fatalf("successful metrics = %#v", got)
	}

	if !consumer.onMessage(ctx, []*nats.Msg{{Data: []byte("{")}}) {
		t.Fatal("malformed output event was not discarded")
	}
	if got := metrics.snapshot(); got.Received != 2 || got.Discarded != 1 || got.Failed != 0 {
		t.Fatalf("discard metrics = %#v", got)
	}

	consumer.projectOutputEvent = func(context.Context, roomserverAPI.OutputEvent) error { return projectErr }
	if consumer.onMessage(ctx, []*nats.Msg{{Data: valid}}) {
		t.Fatal("projector failure was acknowledged")
	}
	if got := metrics.snapshot(); got.Received != 3 || got.Processed != 1 || got.Discarded != 1 || got.Failed != 1 || got.ConsecutiveFailures != 1 || got.LastFailureUnix <= 0 {
		t.Fatalf("failure metrics = %#v", got)
	}
}

func TestOutputRoomEventConsumerProcessesBatchSequentially(t *testing.T) {
	processed := []roomserverAPI.OutputType{}
	consumer := &OutputRoomEventConsumer{
		metrics: newConsumerMetrics(),
		projectOutputEvent: func(_ context.Context, output roomserverAPI.OutputEvent) error {
			processed = append(processed, output.Type)
			if output.Type == roomserverAPI.OutputTypeNewRoomEvent {
				return errors.New("second failed")
			}
			return nil
		},
	}
	first, _ := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeOldRoomEvent})
	second, _ := json.Marshal(roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeNewRoomEvent})
	if consumer.onMessage(context.Background(), []*nats.Msg{{Data: first}, {Data: second}}) {
		t.Fatal("batch with a failed message was acknowledged")
	}
	if len(processed) != 2 || processed[0] != roomserverAPI.OutputTypeOldRoomEvent || processed[1] != roomserverAPI.OutputTypeNewRoomEvent {
		t.Fatalf("processing order = %#v", processed)
	}
	if got := consumer.metrics.snapshot(); got.Received != 2 || got.Processed != 1 || got.Failed != 1 || got.ConsecutiveFailures != 1 {
		t.Fatalf("batch metrics = %#v", got)
	}
}

func TestProjectorBatchSizeFromEnv(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
	}{{"", 1}, {"25", 25}, {"0", 1}, {"250", 100}} {
		t.Run(test.value, func(t *testing.T) {
			t.Setenv("P2P_PROJECTOR_BATCH_SIZE", test.value)
			if got := batchSizeFromEnv(); got != test.want {
				t.Fatalf("batch size = %d, want %d", got, test.want)
			}
		})
	}
}
