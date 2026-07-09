package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPProductAgentClientPostsMessageServerEvent(t *testing.T) {
	var got ProductAgentMessageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/message-server/new-message" {
			t.Fatalf("unexpected product-agent request route: %s %s", r.Method, r.URL.Path)
		}
		if contentType := r.Header.Get("Content-Type"); !strings.Contains(contentType, "application/json") {
			t.Fatalf("expected JSON content-type, got %q", contentType)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(ProductAgentMessageResponse{
			Reply: "card ready",
			OutboundMessage: &ProductAgentOutboundMessage{
				ConversationID: got.RoomID,
				Content:        `{"schema":"direxio.agent_action_result.v1","title":"Skill"}`,
			},
		})
	}))
	defer server.Close()

	client := httpProductAgentClient{baseURL: server.URL, client: server.Client()}
	response, err := client.HandleMessage(context.Background(), ProductAgentMessageRequest{
		NodeID:           "node-a",
		RoomID:           "!agents:example.com",
		ConversationType: "agent",
		SenderID:         "@owner:example.com",
		SenderKind:       "user",
		Content:          "run capsule",
		AgentConfig: map[string]any{
			"skills": []any{map[string]any{
				"schema": "direxio.prompt_skill.v1",
				"kind":   "prompt",
				"id":     "prompt-capsule",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "node-a" || got.RoomID != "!agents:example.com" || got.ConversationType != "agent" || got.Content != "run capsule" {
		t.Fatalf("unexpected bridged request body: %#v", got)
	}
	skills, ok := got.AgentConfig["skills"].([]any)
	if !ok || len(skills) != 1 {
		t.Fatalf("expected agent_config.skills to be preserved, got %#v", got.AgentConfig)
	}
	if response.Reply != "card ready" || response.OutboundMessage == nil || response.OutboundMessage.Content == "" {
		t.Fatalf("unexpected product-agent response: %#v", response)
	}
}

func TestHTTPProductAgentClientReturnsProductAgentErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ProductAgentMessageResponse{
			Error: &ProductAgentError{Code: "setup_needed", Message: "Direxio AI is not enabled for this node."},
		})
	}))
	defer server.Close()

	client := httpProductAgentClient{baseURL: server.URL, client: server.Client()}
	_, err := client.HandleMessage(context.Background(), ProductAgentMessageRequest{
		NodeID:           "node-a",
		RoomID:           "!agents:example.com",
		ConversationType: "agent",
		Content:          "hello",
	})
	if err == nil || !strings.Contains(err.Error(), "Direxio AI is not enabled") {
		t.Fatalf("expected product-agent error message, got %v", err)
	}
}

func TestHTTPProductAgentClientUsesMemoryEndpoints(t *testing.T) {
	var calls []string
	var saved ProductAgentMemorySaveRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.String())
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/agent/memory":
			if got := r.URL.Query().Get("conversation_id"); got != "!agents:example.com" {
				t.Fatalf("expected memory list conversation id, got %q", got)
			}
			_ = json.NewEncoder(w).Encode(ProductAgentMemoryListResponse{
				Schema: "direxio.agent_memory_list.v1",
				Items: []ProductAgentMemoryItem{{
					ID:             "memory-1",
					ConversationID: "!agents:example.com",
					Type:           "card_memory",
					Text:           "Saved card",
					Tags:           []string{"card_memory"},
					Source:         "agent_card_save",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/agent/memory":
			if err := json.NewDecoder(r.Body).Decode(&saved); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(ProductAgentMemoryItemResponse{
				Schema: "direxio.agent_memory_item.v1",
				Item: &ProductAgentMemoryItem{
					ID:             "memory-2",
					ConversationID: saved.ConversationID,
					Type:           saved.Type,
					Text:           saved.Text,
					Tags:           saved.Tags,
					Source:         saved.Source,
				},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/agent/memory/memory-2":
			if got := r.URL.Query().Get("conversation_id"); got != "!agents:example.com" {
				t.Fatalf("expected memory delete conversation id, got %q", got)
			}
			_ = json.NewEncoder(w).Encode(ProductAgentMemoryDeleteResponse{
				Schema:  "direxio.agent_memory_delete.v1",
				ID:      "memory-2",
				Deleted: true,
			})
		default:
			t.Fatalf("unexpected memory endpoint request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client := httpProductAgentClient{baseURL: server.URL, client: server.Client()}
	list, err := client.ListMemory(context.Background(), "!agents:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].ID != "memory-1" {
		t.Fatalf("expected memory list item, got %#v", list)
	}
	savedResponse, err := client.SaveMemory(context.Background(), ProductAgentMemorySaveRequest{
		ConversationID: "!agents:example.com",
		Text:           "Saved card",
		Type:           "card_memory",
		Tags:           []string{"card", "agent"},
		Source:         "agent_card_save",
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.ConversationID != "!agents:example.com" || saved.Type != "card_memory" || saved.Source != "agent_card_save" {
		t.Fatalf("expected save payload to preserve memory fields, got %#v", saved)
	}
	if savedResponse.Item == nil || savedResponse.Item.ID != "memory-2" {
		t.Fatalf("expected saved memory item, got %#v", savedResponse)
	}
	deleted, err := client.DeleteMemory(context.Background(), "!agents:example.com", "memory-2")
	if err != nil {
		t.Fatal(err)
	}
	if !deleted.Deleted || deleted.ID != "memory-2" {
		t.Fatalf("expected delete result, got %#v", deleted)
	}
	if len(calls) != 3 {
		t.Fatalf("expected list/save/delete calls, got %#v", calls)
	}
}

func TestHTTPProductAgentClientReturnsMemoryErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ProductAgentMemoryListResponse{
			Error: &ProductAgentError{Code: "memory_unavailable", Message: "memory store is unavailable"},
		})
	}))
	defer server.Close()

	client := httpProductAgentClient{baseURL: server.URL, client: server.Client()}
	_, err := client.ListMemory(context.Background(), "!agents:example.com")
	if err == nil || !strings.Contains(err.Error(), "memory store is unavailable") {
		t.Fatalf("expected structured memory error message, got %v", err)
	}
}

func TestProductAgentClientFromConfigUsesExplicitURLAndEnvFallback(t *testing.T) {
	if productAgentClientFromConfig(Config{}) != nil {
		t.Fatalf("expected no product-agent client without config or env")
	}
	t.Setenv("DIREXIO_PRODUCT_AGENT_URL", "http://env-agent:8797/")
	envClient, ok := productAgentClientFromConfig(Config{}).(httpProductAgentClient)
	if !ok || envClient.baseURL != "http://env-agent:8797" {
		t.Fatalf("expected env product-agent URL to be normalized, got %#v", envClient)
	}
	explicitClient, ok := productAgentClientFromConfig(Config{
		ProductAgentURL: "http://explicit-agent:8797/",
	}).(httpProductAgentClient)
	if !ok || explicitClient.baseURL != "http://explicit-agent:8797" {
		t.Fatalf("expected explicit product-agent URL to win, got %#v", explicitClient)
	}
}

func TestProductAgentReplyMatrixPayloadPromotesActionResultCard(t *testing.T) {
	body, content := productAgentReplyMatrixPayload(ProductAgentMessageResponse{
		Reply: "Short visible summary.",
		OutboundMessage: &ProductAgentOutboundMessage{
			Content: `{"schema":"direxio.agent_action_result.v1","action":"mood_card","title":"Mood","summary":"fallback summary"}`,
		},
	})
	if body != "Short visible summary." {
		t.Fatalf("expected reply summary body, got %q", body)
	}
	card, ok := content[agentActionResultContentKey].(map[string]any)
	if !ok || card["schema"] != agentActionResultSchema || card["action"] != "mood_card" {
		t.Fatalf("expected structured action result card, got %#v", content)
	}
	if content[agentActionResultHideBodyKey] != true {
		t.Fatalf("expected hide-body marker, got %#v", content)
	}
}

func TestProductAgentReplyMatrixPayloadFallsBackForPlainText(t *testing.T) {
	body, content := productAgentReplyMatrixPayload(ProductAgentMessageResponse{
		Reply: "Plain reply.",
		OutboundMessage: &ProductAgentOutboundMessage{
			Content: "Plain outbound content.",
		},
	})
	if body != "Plain outbound content." {
		t.Fatalf("expected plain outbound body, got %q", body)
	}
	if len(content) != 0 {
		t.Fatalf("expected no structured content for plain text, got %#v", content)
	}
}
