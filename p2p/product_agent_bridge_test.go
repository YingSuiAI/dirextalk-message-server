package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	productagentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/productagent"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestProductAgentBridgeSendsOneMatrixReplyForReplayedEvent(t *testing.T) {
	requests := make(chan productagentmodule.MessageRequest, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload productagentmodule.MessageRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Error(err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- payload
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"reply":"completed"}`))
	}))
	defer server.Close()

	transport := newProductAgentTestTransport()
	service := NewServiceWithTransport(Config{
		ServerName:             "test",
		ProductAgentURL:        server.URL,
		ProductAgentHTTPClient: server.Client(),
	}, transport)
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	service.mu.Lock()
	service.ownerMXID = owner.ID
	service.agentRoomID = room.ID
	service.mu.Unlock()
	event := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{
		"msgtype": "m.text",
		"body":    "run this",
	})

	if err := service.ProjectRoomEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.ProjectRoomEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}

	select {
	case request := <-requests:
		if request.MessageID != event.EventID() || request.RoomID != room.ID || request.Content != "run this" {
			t.Fatalf("unexpected Product Agent request %#v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Product Agent request")
	}
	select {
	case request := <-requests:
		t.Fatalf("replayed event created a second Product Agent request: %#v", request)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case message := <-transport.sent:
		if message.SenderMXID != "@agent:test" || message.RoomID != room.ID || message.Content["body"] != "completed" {
			t.Fatalf("unexpected Matrix reply %#v", message)
		}
		if message.Content[AgentGatewayContentKey] != true || message.Content[AgentGatewaySourceContentKey] != productAgentGatewaySource {
			t.Fatalf("Product Agent reply was not loop-protected: %#v", message.Content)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Matrix reply")
	}
	select {
	case message := <-transport.sent:
		t.Fatalf("replayed event created a second Matrix reply: %#v", message)
	case <-time.After(100 * time.Millisecond):
	}
}

type productAgentTestTransport struct {
	*recordingTransport
	mu   sync.Mutex
	sent chan SendMessageRequest
}

func newProductAgentTestTransport() *productAgentTestTransport {
	return &productAgentTestTransport{
		recordingTransport: &recordingTransport{},
		sent:               make(chan SendMessageRequest, 2),
	}
}

func (t *productAgentTestTransport) SendMessage(ctx context.Context, request SendMessageRequest) (SendMessageResult, error) {
	t.mu.Lock()
	result, err := t.recordingTransport.SendMessage(ctx, request)
	t.mu.Unlock()
	if err == nil {
		t.sent <- request
	}
	return result, err
}
