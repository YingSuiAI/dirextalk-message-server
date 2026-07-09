package p2p

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestPluginActionAllowlistIncludesAgentMemoryActions(t *testing.T) {
	entry, ok := findOfficialPlugin("io.dirextalk.agent")
	if !ok {
		t.Fatalf("expected agent plugin in official catalog")
	}
	for _, action := range []string{
		agentMemoryListActionName,
		agentMemorySaveActionName,
		agentMemoryDeleteActionName,
	} {
		if !pluginActionAllowed(entry, action) {
			t.Fatalf("expected agent memory action %q to be allowed by catalog %#v", action, entry.Actions)
		}
	}
}

func TestPluginInvokeAgentMemorySaveUsesProductAgentBridge(t *testing.T) {
	runner := &recordingPluginRunner{}
	productAgent := &recordingMemoryProductAgentClient{
		saveResponse: ProductAgentMemoryItemResponse{
			Schema: "direxio.agent_memory_item.v1",
			Item: &ProductAgentMemoryItem{
				ID:             "memory-1",
				ConversationID: "!agents:example.com",
				Type:           "card_memory",
				Text:           "Launch card",
				Source:         "agent_card_save",
			},
		},
	}
	service := NewService(Config{
		ServerName:   "example.com",
		PluginRunner: runner,
		ProductAgent: productAgent,
	})
	enableOfficialAgentPlugin(t, service)

	response := mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    agentMemorySaveActionName,
		"params": map[string]any{
			"conversation_id": "!agents:example.com",
			"text":            "Launch card",
			"type":            "card_memory",
			"source":          "agent_card_save",
			"tags":            []any{"launch", "agent"},
		},
	})
	if len(runner.invokes) != 0 {
		t.Fatalf("agent memory action must not call plugin runner, got %#v", runner.invokes)
	}
	if productAgent.saved.ConversationID != "!agents:example.com" ||
		productAgent.saved.Text != "Launch card" ||
		productAgent.saved.Type != "card_memory" ||
		productAgent.saved.Source != "agent_card_save" ||
		len(productAgent.saved.Tags) != 2 {
		t.Fatalf("expected memory save request to reach product-agent, got %#v", productAgent.saved)
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected result map, got %#v", response)
	}
	item, ok := result["item"].(*ProductAgentMemoryItem)
	if !ok || item == nil || item.ID != "memory-1" {
		t.Fatalf("expected saved memory item in response, got %#v", response)
	}
}

func TestPluginInvokeAgentMemoryListAndDeleteUseConversationFallbacks(t *testing.T) {
	runner := &recordingPluginRunner{}
	productAgent := &recordingMemoryProductAgentClient{
		listResponse: ProductAgentMemoryListResponse{
			Schema: "direxio.agent_memory_list.v1",
			Items: []ProductAgentMemoryItem{{
				ID:             "memory-1",
				ConversationID: "!agents:example.com",
				Type:           "fact",
				Text:           "User likes concise replies",
				Source:         "user_explicit",
			}},
		},
		deleteResponse: ProductAgentMemoryDeleteResponse{
			Schema:  "direxio.agent_memory_delete.v1",
			ID:      "memory-1",
			Deleted: true,
		},
	}
	service := NewService(Config{
		ServerName:   "example.com",
		PluginRunner: runner,
		ProductAgent: productAgent,
	})
	service.agentRoomID = "!agents:example.com"
	enableOfficialAgentPlugin(t, service)

	list := mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    agentMemoryListActionName,
		"params":    map[string]any{},
	})
	if productAgent.listConversationID != "!agents:example.com" {
		t.Fatalf("expected memory list to fall back to agent room id, got %q", productAgent.listConversationID)
	}
	listResult := list["result"].(map[string]any)
	items, ok := listResult["items"].([]ProductAgentMemoryItem)
	if !ok || len(items) != 1 || items[0].ID != "memory-1" {
		t.Fatalf("expected typed memory items, got %#v", list)
	}

	deleted := mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    agentMemoryDeleteActionName,
		"params": map[string]any{
			"room_id": "!room:example.com",
			"id":      "memory-1",
		},
	})
	if productAgent.deleteConversationID != "!room:example.com" || productAgent.deleteID != "memory-1" {
		t.Fatalf("expected memory delete request to preserve room/id, got room=%q id=%q", productAgent.deleteConversationID, productAgent.deleteID)
	}
	deleteResult := deleted["result"].(map[string]any)
	if deleteResult["deleted"] != true || deleteResult["id"] != "memory-1" {
		t.Fatalf("expected delete result, got %#v", deleted)
	}
	if len(runner.invokes) != 0 {
		t.Fatalf("agent memory actions must not call plugin runner, got %#v", runner.invokes)
	}
}

func TestPluginInvokeAgentMemoryRequiresProductAgentBridge(t *testing.T) {
	service := NewService(Config{ServerName: "example.com", PluginRunner: &recordingPluginRunner{}})
	enableOfficialAgentPlugin(t, service)

	_, apiErr := service.Handle(context.Background(), "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    agentMemoryListActionName,
		"params": map[string]any{
			"conversation_id": "!agents:example.com",
		},
	})
	if apiErr == nil || apiErr.Status != http.StatusConflict {
		t.Fatalf("expected missing product-agent bridge conflict, got %#v", apiErr)
	}
}

/**
 * Function: Installs and enables the official Agent plugin for owner-only invoke tests.
 * Inputs:
 * - t: Test handle for failure reporting.
 * - service: In-memory service under test.
 * Output:
 * - The service has an enabled `io.dirextalk.agent` plugin.
 * Side effects:
 * - Mutates service plugin state through normal action handlers.
 * Errors:
 * - Fails the test if install or enable does not succeed.
 */
func enableOfficialAgentPlugin(t *testing.T, service *Service) {
	t.Helper()
	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
}

type recordingMemoryProductAgentClient struct {
	listConversationID   string
	saved                ProductAgentMemorySaveRequest
	deleteConversationID string
	deleteID             string
	listResponse         ProductAgentMemoryListResponse
	saveResponse         ProductAgentMemoryItemResponse
	deleteResponse       ProductAgentMemoryDeleteResponse
	err                  error
}

func (c *recordingMemoryProductAgentClient) HandleMessage(context.Context, ProductAgentMessageRequest) (ProductAgentMessageResponse, error) {
	return ProductAgentMessageResponse{}, nil
}

func (c *recordingMemoryProductAgentClient) ListMemory(_ context.Context, conversationID string) (ProductAgentMemoryListResponse, error) {
	c.listConversationID = conversationID
	return c.listResponse, c.err
}

func (c *recordingMemoryProductAgentClient) SaveMemory(_ context.Context, req ProductAgentMemorySaveRequest) (ProductAgentMemoryItemResponse, error) {
	c.saved = req
	return c.saveResponse, c.err
}

func (c *recordingMemoryProductAgentClient) DeleteMemory(_ context.Context, conversationID, id string) (ProductAgentMemoryDeleteResponse, error) {
	c.deleteConversationID = conversationID
	c.deleteID = id
	return c.deleteResponse, c.err
}

func (c *recordingProductAgentClient) ListMemory(context.Context, string) (ProductAgentMemoryListResponse, error) {
	return ProductAgentMemoryListResponse{}, errors.New("unexpected product-agent memory list")
}

func (c *recordingProductAgentClient) SaveMemory(context.Context, ProductAgentMemorySaveRequest) (ProductAgentMemoryItemResponse, error) {
	return ProductAgentMemoryItemResponse{}, errors.New("unexpected product-agent memory save")
}

func (c *recordingProductAgentClient) DeleteMemory(context.Context, string, string) (ProductAgentMemoryDeleteResponse, error) {
	return ProductAgentMemoryDeleteResponse{}, errors.New("unexpected product-agent memory delete")
}
