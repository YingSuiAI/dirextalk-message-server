package p2p

import (
	"context"
	"net/http"
	"strings"
)

const (
	agentMemoryListActionName   = "agent.memory.list"
	agentMemorySaveActionName   = "agent.memory.save"
	agentMemoryDeleteActionName = "agent.memory.delete"
)

/**
 * Function: Handles official Agent plugin memory actions through the configured product-agent bridge.
 * Inputs:
 * - ctx: Request context from the owner-authenticated plugin invocation.
 * - action: Client-facing action such as `agent.memory.save`.
 * - params: Action params after normal Agent plugin invoke enrichment.
 * Output:
 * - A compact result map that Flutter can render or inspect inside the existing plugin invoke envelope.
 * Side effects:
 * - Calls product-agent memory HTTP APIs when the bridge is configured.
 * Errors:
 * - Returns 400 for invalid params, 409 when product-agent is not configured, or 502 when the bridge call fails.
 */
func (s *Service) invokeProductAgentMemory(ctx context.Context, action string, params map[string]any) (map[string]any, *apiError) {
	if s.productAgent == nil {
		return nil, statusError(http.StatusConflict, "product-agent bridge is not configured")
	}
	switch strings.TrimSpace(action) {
	case agentMemoryListActionName:
		return s.invokeProductAgentMemoryList(ctx, params)
	case agentMemorySaveActionName:
		return s.invokeProductAgentMemorySave(ctx, params)
	case agentMemoryDeleteActionName:
		return s.invokeProductAgentMemoryDelete(ctx, params)
	default:
		return nil, badRequest("unsupported agent memory action")
	}
}

/**
 * Function: Lists product-agent memory for the requested conversation.
 * Inputs:
 * - ctx: Request context.
 * - params: Optional `conversation_id` or `room_id`; empty values fall back to the service agent room.
 * Output:
 * - Memory list result with schema and active items.
 * Side effects:
 * - Performs one product-agent bridge call.
 * Errors:
 * - Returns 400 when no conversation can be resolved, or 502 when product-agent fails.
 */
func (s *Service) invokeProductAgentMemoryList(ctx context.Context, params map[string]any) (map[string]any, *apiError) {
	conversationID, apiErr := s.agentMemoryConversationID(params)
	if apiErr != nil {
		return nil, apiErr
	}
	response, err := s.productAgent.ListMemory(ctx, conversationID)
	if err != nil {
		return nil, statusError(http.StatusBadGateway, err.Error())
	}
	items := response.Items
	if items == nil {
		items = []ProductAgentMemoryItem{}
	}
	return map[string]any{
		"schema": fallbackString(response.Schema, "direxio.agent_memory_list.v1"),
		"items":  items,
	}, nil
}

/**
 * Function: Saves one explicit product-agent memory for the requested conversation.
 * Inputs:
 * - ctx: Request context.
 * - params: `text`, optional `conversation_id`/`room_id`, `type`, `source`, and `tags`.
 * Output:
 * - Saved memory item result.
 * Side effects:
 * - Performs one product-agent bridge call that can persist memory on disk.
 * Errors:
 * - Returns 400 for missing text or invalid type/source, or 502 when product-agent fails.
 */
func (s *Service) invokeProductAgentMemorySave(ctx context.Context, params map[string]any) (map[string]any, *apiError) {
	req, apiErr := s.productAgentMemorySaveRequest(params)
	if apiErr != nil {
		return nil, apiErr
	}
	response, err := s.productAgent.SaveMemory(ctx, req)
	if err != nil {
		return nil, statusError(http.StatusBadGateway, err.Error())
	}
	return map[string]any{
		"schema": fallbackString(response.Schema, "direxio.agent_memory_item.v1"),
		"item":   response.Item,
	}, nil
}

/**
 * Function: Deletes one explicit product-agent memory for the requested conversation.
 * Inputs:
 * - ctx: Request context.
 * - params: `id` plus optional `conversation_id` or `room_id`.
 * Output:
 * - Delete result with the id and whether an active memory was removed.
 * Side effects:
 * - Performs one product-agent bridge call that can update the memory file.
 * Errors:
 * - Returns 400 for missing id/conversation, or 502 when product-agent fails.
 */
func (s *Service) invokeProductAgentMemoryDelete(ctx context.Context, params map[string]any) (map[string]any, *apiError) {
	conversationID, apiErr := s.agentMemoryConversationID(params)
	if apiErr != nil {
		return nil, apiErr
	}
	id := trimString(params["id"])
	if id == "" {
		return nil, badRequest("id is required")
	}
	response, err := s.productAgent.DeleteMemory(ctx, conversationID, id)
	if err != nil {
		return nil, statusError(http.StatusBadGateway, err.Error())
	}
	return map[string]any{
		"schema":  fallbackString(response.Schema, "direxio.agent_memory_delete.v1"),
		"id":      fallbackString(response.ID, id),
		"deleted": response.Deleted,
	}, nil
}

/**
 * Function: Builds a product-agent memory save request from plugin action params.
 * Inputs:
 * - params: Client-supplied plugin invoke params.
 * Output:
 * - ProductAgentMemorySaveRequest with normalized conversation id, text, type, tags, and source.
 * Side effects:
 * - Reads the current agent room id as a fallback conversation id.
 * Errors:
 * - Returns apiError for missing text/conversation or invalid type/source values.
 */
func (s *Service) productAgentMemorySaveRequest(params map[string]any) (ProductAgentMemorySaveRequest, *apiError) {
	conversationID, apiErr := s.agentMemoryConversationID(params)
	if apiErr != nil {
		return ProductAgentMemorySaveRequest{}, apiErr
	}
	text := fallbackString(trimString(params["text"]), trimString(params["body"]))
	if text == "" {
		return ProductAgentMemorySaveRequest{}, badRequest("text is required")
	}
	memoryType := trimString(params["type"])
	if memoryType != "" && !validAgentMemoryType(memoryType) {
		return ProductAgentMemorySaveRequest{}, badRequest("invalid memory type")
	}
	source := trimString(params["source"])
	if source != "" && !validAgentMemorySource(source) {
		return ProductAgentMemorySaveRequest{}, badRequest("invalid memory source")
	}
	return ProductAgentMemorySaveRequest{
		ConversationID: conversationID,
		Text:           text,
		Type:           memoryType,
		Tags:           stringSliceParam(params["tags"]),
		Source:         source,
	}, nil
}

/**
 * Function: Resolves the product-agent conversation id for memory actions.
 * Inputs:
 * - params: May contain `conversation_id` or `room_id`.
 * Output:
 * - Conversation id used by product-agent memory APIs.
 * Side effects:
 * - Reads service state under lock when params omit both ids.
 * Errors:
 * - Returns 400 when no usable conversation id exists.
 */
func (s *Service) agentMemoryConversationID(params map[string]any) (string, *apiError) {
	conversationID := fallbackString(trimString(params["conversation_id"]), trimString(params["room_id"]))
	if conversationID == "" {
		s.mu.Lock()
		conversationID = strings.TrimSpace(s.agentRoomID)
		s.mu.Unlock()
	}
	if conversationID == "" {
		return "", badRequest("conversation_id is required")
	}
	return conversationID, nil
}

/**
 * Function: Identifies official Agent plugin actions that should be served by product-agent memory APIs.
 * Inputs:
 * - action: Client-facing plugin action name.
 * Output:
 * - True for memory actions handled by message-server's product-agent bridge.
 * Side effects:
 * - None.
 * Errors:
 * - None.
 */
func isProductAgentMemoryPluginAction(action string) bool {
	switch strings.TrimSpace(action) {
	case agentMemoryListActionName, agentMemorySaveActionName, agentMemoryDeleteActionName:
		return true
	default:
		return false
	}
}

/**
 * Function: Checks whether a client-supplied memory type is supported by product-agent.
 * Inputs:
 * - value: Raw type string from plugin invoke params.
 * Output:
 * - True when product-agent accepts the memory type.
 * Side effects:
 * - None.
 * Errors:
 * - None.
 */
func validAgentMemoryType(value string) bool {
	switch strings.TrimSpace(value) {
	case "preference", "fact", "card_memory", "skill_result", "thread_summary":
		return true
	default:
		return false
	}
}

/**
 * Function: Checks whether a client-supplied memory source is supported by product-agent.
 * Inputs:
 * - value: Raw source string from plugin invoke params.
 * Output:
 * - True when product-agent accepts the source value.
 * Side effects:
 * - None.
 * Errors:
 * - None.
 */
func validAgentMemorySource(value string) bool {
	switch strings.TrimSpace(value) {
	case "user_explicit", "agent_card_save", "prompt_skill", "migration", "auto_compression":
		return true
	default:
		return false
	}
}
