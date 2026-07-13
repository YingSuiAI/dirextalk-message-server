package jetstream

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestJetStreamConsumerWithNakDelayDefersRedelivery(t *testing.T) {
	server, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "jetstream-helper-test",
		JetStream:  true,
		StoreDir:   t.TempDir(),
		DontListen: true,
		NoLog:      true,
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go server.Start()
	t.Cleanup(func() {
		server.Shutdown()
		server.WaitForShutdown()
	})
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}

	connection, err := nats.Connect("", nats.InProcessServer(server))
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := connection.JetStream()
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	if _, err = js.AddStream(&nats.StreamConfig{
		Name:     "HELPER_RETRY",
		Subjects: []string{"helper.retry"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	attempts := make(chan time.Time, 4)
	const retryDelay = 250 * time.Millisecond
	if err = JetStreamConsumerWithNakDelay(
		ctx,
		js,
		"helper.retry",
		"HelperRetry",
		1,
		func(_ context.Context, _ []*nats.Msg) bool {
			attempts <- time.Now()
			return false
		},
		func(_ *nats.Msg) time.Duration { return retryDelay },
		nats.DeliverAll(),
		nats.ManualAck(),
	); err != nil {
		t.Fatalf("start delayed consumer: %v", err)
	}
	if _, err = js.Publish("helper.retry", []byte("retry")); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	first := receiveAttempt(t, attempts, 2*time.Second)
	select {
	case second := <-attempts:
		t.Fatalf("message was redelivered without the requested delay: %v", second.Sub(first))
	case <-time.After(125 * time.Millisecond):
	}
	second := receiveAttempt(t, attempts, 2*time.Second)
	if elapsed := second.Sub(first); elapsed < 200*time.Millisecond {
		t.Fatalf("redelivery delay = %v, want approximately %v", elapsed, retryDelay)
	}
	cancel()
}

func receiveAttempt(t *testing.T, attempts <-chan time.Time, timeout time.Duration) time.Time {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message delivery")
		return time.Time{}
	}
}
