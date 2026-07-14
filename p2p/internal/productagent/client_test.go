package productagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPClientHandlesMessageWithStableIdentity(t *testing.T) {
	var received MessageRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/message-server/new-message" {
			t.Fatalf("unexpected path %q", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"reply":"done"}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.HandleMessage(context.Background(), MessageRequest{
		NodeID:           "node.example",
		RoomID:           "!agent:node.example",
		ConversationID:   "!agent:node.example",
		ConversationType: "agent",
		SenderID:         "@owner:node.example",
		SenderKind:       "user",
		MessageID:        "$event",
		Content:          "run it",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Reply != "done" || received.MessageID != "$event" || received.ConversationID != received.RoomID {
		t.Fatalf("unexpected bridge exchange: response=%#v request=%#v", response, received)
	}
}

func TestHTTPClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"reply":"` + strings.Repeat("x", 128) + `"}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client(), 32)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.HandleMessage(context.Background(), MessageRequest{})
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("expected bounded response error, got %v", err)
	}
}

func TestModuleDeduplicatesTurnAndSendsStructuredCardOnce(t *testing.T) {
	client := &stubClient{response: MessageResponse{
		Reply: "Status card",
		OutboundMessage: &OutboundMessage{Content: `{
			"schema":"direxio.agent_action_result.v1",
			"title":"Today",
			"summary":"Ready"
		}`},
	}}
	sender := &stubSender{}
	module := New(Config{
		NodeID:   "node.example",
		Client:   client,
		Sender:   sender,
		Dispatch: func(run func()) { run() },
	})
	message := Message{RoomID: "!agent:node.example", EventID: "$same", SenderMXID: "@owner:node.example", Body: "status"}

	module.Handle(context.Background(), message)
	module.Handle(context.Background(), message)

	if len(client.requests) != 1 || len(sender.replies) != 1 {
		t.Fatalf("expected one request and one reply, got %d and %d", len(client.requests), len(sender.replies))
	}
	reply := sender.replies[0]
	if reply.Body != "Status card" || reply.Fields[actionResultHideBodyKey] != true {
		t.Fatalf("unexpected structured reply %#v", reply)
	}
	if strings.Contains(reply.Body, "direxio.agent_action_result.v1") {
		t.Fatalf("raw card JSON leaked into the Matrix body: %q", reply.Body)
	}
}

func TestModuleIsolatesClientFailure(t *testing.T) {
	client := &stubClient{err: errors.New("offline")}
	sender := &stubSender{}
	module := New(Config{
		Client:   client,
		Sender:   sender,
		Dispatch: func(run func()) { run() },
	})

	module.Handle(context.Background(), Message{RoomID: "!agent:test", SenderMXID: "@owner:test", Body: "hello"})
	if len(sender.replies) != 0 {
		t.Fatalf("failed Product Agent request must not send a reply: %#v", sender.replies)
	}
}

type stubClient struct {
	requests []MessageRequest
	response MessageResponse
	err      error
}

func (c *stubClient) HandleMessage(_ context.Context, request MessageRequest) (MessageResponse, error) {
	c.requests = append(c.requests, request)
	return c.response, c.err
}

type stubSender struct {
	replies []Reply
}

func (s *stubSender) SendProductAgentReply(_ context.Context, reply Reply) error {
	s.replies = append(s.replies, reply)
	return nil
}
