package projector

import (
	"context"
	"testing"

	productagentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/productagent"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestAgentRoomOwnerTextReachesProductAgentSink(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{
		"msgtype": "m.text",
		"body":    "run code",
	})
	sink := &capturingAgentMessageSink{}
	module := New(Dependencies{
		AgentMessages: sink,
		Identity: func() IdentitySnapshot {
			return IdentitySnapshot{OwnerMXID: owner.ID, AgentMXID: "@agent:test", AgentRoomID: room.ID}
		},
	}, Config{})

	if err := module.ProjectRoomEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(sink.messages) != 1 {
		t.Fatalf("expected one Product Agent message, got %#v", sink.messages)
	}
	got := sink.messages[0]
	if got.EventID != event.EventID() || got.RoomID != room.ID || got.Body != "run code" {
		t.Fatalf("unexpected Product Agent message %#v", got)
	}
}

func TestAgentRoomBridgeRejectsIneligibleMessages(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	tests := []struct {
		name     string
		identity IdentitySnapshot
		content  map[string]any
	}{
		{
			name:     "wrong sender",
			identity: IdentitySnapshot{OwnerMXID: "@someone-else:test", AgentMXID: "@agent:test", AgentRoomID: room.ID},
			content:  map[string]any{"msgtype": "m.text", "body": "hello"},
		},
		{
			name:     "gateway marked",
			identity: IdentitySnapshot{OwnerMXID: owner.ID, AgentMXID: "@agent:test", AgentRoomID: room.ID},
			content:  map[string]any{"msgtype": "m.text", "body": "hello", "io.dirextalk.agent_gateway": true},
		},
		{
			name:     "media message",
			identity: IdentitySnapshot{OwnerMXID: owner.ID, AgentMXID: "@agent:test", AgentRoomID: room.ID},
			content:  map[string]any{"msgtype": "m.image", "body": "image"},
		},
		{
			name:     "empty body",
			identity: IdentitySnapshot{OwnerMXID: owner.ID, AgentMXID: "@agent:test", AgentRoomID: room.ID},
			content:  map[string]any{"msgtype": "m.text", "body": " "},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			event := room.CreateAndInsert(t, owner, "m.room.message", testCase.content)
			sink := &capturingAgentMessageSink{}
			module := New(Dependencies{
				AgentMessages: sink,
				Identity:      func() IdentitySnapshot { return testCase.identity },
			}, Config{})
			if err := module.ProjectRoomEvent(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if len(sink.messages) != 0 {
				t.Fatalf("ineligible message reached Product Agent: %#v", sink.messages)
			}
		})
	}
}

type capturingAgentMessageSink struct {
	messages []productagentmodule.Message
}

func (s *capturingAgentMessageSink) Handle(_ context.Context, message productagentmodule.Message) {
	s.messages = append(s.messages, message)
}
