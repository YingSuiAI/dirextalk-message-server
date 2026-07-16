package projector

import (
	"context"
	"reflect"
	"strings"
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

func TestAgentRoomImageActionIsNormalizedForProductAgent(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{
		"msgtype": "m.text",
		"body":    "A rainy Shanghai night",
		"io.dirextalk.agent_action": map[string]any{
			"schema": "direxio.agent_action_request.v2",
			"action": "generate_image",
			"input": map[string]any{
				"prompt":  "A rainy Shanghai night",
				"size":    "1024x1024",
				"count":   1,
				"ignored": "must not cross the boundary",
			},
			"ignored": true,
		},
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
	want := map[string]any{
		"schema": "direxio.agent_action_request.v2",
		"action": "generate_image",
		"input": map[string]any{
			"prompt": "A rainy Shanghai night",
			"size":   "1024x1024",
			"count":  1,
		},
	}
	if !reflect.DeepEqual(sink.messages[0].AgentAction, want) {
		t.Fatalf("unexpected normalized Agent action: got %#v want %#v", sink.messages[0].AgentAction, want)
	}
}

func TestAgentRoomRejectsMalformedImageActions(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	validInput := func() map[string]any {
		return map[string]any{
			"prompt": "A rainy Shanghai night",
			"size":   "1024x1024",
			"count":  1,
		}
	}
	tests := []struct {
		name   string
		body   string
		action any
	}{
		{name: "not object", body: "prompt", action: "generate_image"},
		{name: "wrong schema", body: "A rainy Shanghai night", action: map[string]any{
			"schema": "direxio.agent_action_request.v1", "action": "generate_image", "input": validInput(),
		}},
		{name: "wrong action", body: "A rainy Shanghai night", action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "delete_everything", "input": validInput(),
		}},
		{name: "wrong size", body: "A rainy Shanghai night", action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "generate_image", "input": map[string]any{
				"prompt": "A rainy Shanghai night", "size": "512x512", "count": 1,
			},
		}},
		{name: "wrong count", body: "A rainy Shanghai night", action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "generate_image", "input": map[string]any{
				"prompt": "A rainy Shanghai night", "size": "1024x1024", "count": 2,
			},
		}},
		{name: "empty prompt", body: "prompt", action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "generate_image", "input": map[string]any{
				"prompt": " ", "size": "1024x1024", "count": 1,
			},
		}},
		{name: "oversized prompt", body: strings.Repeat("界", 2001), action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "generate_image", "input": map[string]any{
				"prompt": strings.Repeat("界", 2001), "size": "1024x1024", "count": 1,
			},
		}},
		{name: "body mismatch", body: "visible prompt", action: map[string]any{
			"schema": "direxio.agent_action_request.v2", "action": "generate_image", "input": validInput(),
		}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			event := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{
				"msgtype":                   "m.text",
				"body":                      testCase.body,
				"io.dirextalk.agent_action": testCase.action,
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
			if len(sink.messages) != 0 {
				t.Fatalf("malformed image action reached Product Agent: %#v", sink.messages)
			}
		})
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
			name:     "wrong room",
			identity: IdentitySnapshot{OwnerMXID: owner.ID, AgentMXID: "@agent:test", AgentRoomID: "!other:test"},
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
