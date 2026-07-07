package nativeagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const nativeAgentDefaultMemoryWindow = 12

type nativeAgentMemory struct {
	ConversationID string                    `json:"conversation_id"`
	Summary        string                    `json:"summary,omitempty"`
	Turns          []nativeAgentMemoryTurn   `json:"turns,omitempty"`
	UpdatedAt      int64                     `json:"updated_at"`
	Metadata       map[string]map[string]any `json:"metadata,omitempty"`
}

type nativeAgentMemoryTurn struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"created_at"`
}

func (r *Runtime) memoryContext(ctx context.Context, config map[string]any, params map[string]any) (string, []map[string]any) {
	key := nativeAgentConversationKey(params)
	if key == "" {
		return "", nil
	}
	memory, err := r.loadMemory(ctx, key)
	if err != nil {
		return "", nil
	}
	messages := make([]map[string]any, 0, len(memory.Turns))
	for _, turn := range memory.Turns {
		role := strings.TrimSpace(turn.Role)
		if role != "assistant" {
			role = "user"
		}
		content := strings.TrimSpace(turn.Content)
		if content == "" {
			continue
		}
		messages = append(messages, map[string]any{"role": role, "content": content})
	}
	summary := strings.TrimSpace(memory.Summary)
	if summary != "" {
		summary = "Conversation memory summary:\n" + summary
	}
	return summary, messages
}

func (r *Runtime) rememberTurn(ctx context.Context, config map[string]any, params map[string]any, assistantText string) {
	if boolParam(params["memory_disabled"]) || boolParam(config["memory_disabled"]) {
		return
	}
	key := nativeAgentConversationKey(params)
	if key == "" {
		return
	}
	userText := fallbackString(trimString(params["prompt"]), trimString(params["message"]))
	if userText == "" && len(params) == 0 {
		return
	}
	memory, err := r.loadMemory(ctx, key)
	if err != nil {
		memory = nativeAgentMemory{ConversationID: key}
	}
	now := time.Now().UTC().UnixMilli()
	if userText != "" {
		memory.Turns = append(memory.Turns, nativeAgentMemoryTurn{Role: "user", Content: userText, CreatedAt: now})
	}
	if strings.TrimSpace(assistantText) != "" {
		memory.Turns = append(memory.Turns, nativeAgentMemoryTurn{Role: "assistant", Content: assistantText, CreatedAt: now})
	}
	window := int(int64Param(params["memory_window"]))
	if window <= 0 {
		window = int(int64Param(config["memory_window"]))
	}
	if window <= 0 {
		window = nativeAgentDefaultMemoryWindow
	}
	memory = compactNativeAgentMemory(memory, window)
	_ = r.saveMemory(ctx, memory)
}

func (r *Runtime) compressMemory(ctx context.Context, params map[string]any) (map[string]any, error) {
	key := nativeAgentConversationKey(params)
	if key == "" {
		return r.summarize(ctx, params)
	}
	memory, err := r.loadMemory(ctx, key)
	if err != nil {
		return nil, err
	}
	window := int(int64Param(params["memory_window"]))
	if window <= 0 {
		window = nativeAgentDefaultMemoryWindow
	}
	memory = compactNativeAgentMemory(memory, window)
	if err := r.saveMemory(ctx, memory); err != nil {
		return nil, err
	}
	return map[string]any{
		"conversation_id": key,
		"summary":         memory.Summary,
		"turns":           memory.Turns,
		"updated_at":      memory.UpdatedAt,
	}, nil
}

func compactNativeAgentMemory(memory nativeAgentMemory, window int) nativeAgentMemory {
	if window <= 0 {
		window = nativeAgentDefaultMemoryWindow
	}
	if len(memory.Turns) <= window {
		memory.UpdatedAt = time.Now().UTC().UnixMilli()
		return memory
	}
	overflow := memory.Turns[:len(memory.Turns)-window]
	memory.Turns = append([]nativeAgentMemoryTurn{}, memory.Turns[len(memory.Turns)-window:]...)
	parts := make([]string, 0, len(overflow)+1)
	if strings.TrimSpace(memory.Summary) != "" {
		parts = append(parts, memory.Summary)
	}
	for _, turn := range overflow {
		content := strings.TrimSpace(turn.Content)
		if content == "" {
			continue
		}
		parts = append(parts, turn.Role+": "+content)
	}
	summary := strings.Join(parts, "\n")
	runes := []rune(summary)
	if len(runes) > 4000 {
		summary = string(runes[len(runes)-4000:])
	}
	memory.Summary = strings.TrimSpace(summary)
	memory.UpdatedAt = time.Now().UTC().UnixMilli()
	return memory
}

func nativeAgentConversationKey(params map[string]any) string {
	for _, key := range []string{"conversation_id", "thread_id", "room_id", "memory_key"} {
		if value := sanitizeNativeID(trimString(params[key])); value != "" {
			return value
		}
	}
	return "default"
}

func (r *Runtime) loadMemory(ctx context.Context, conversationID string) (nativeAgentMemory, error) {
	select {
	case <-ctx.Done():
		return nativeAgentMemory{}, ctx.Err()
	default:
	}
	file := r.memoryFile(conversationID)
	data, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nativeAgentMemory{ConversationID: conversationID}, nil
		}
		return nativeAgentMemory{}, err
	}
	var memory nativeAgentMemory
	if err := json.Unmarshal(data, &memory); err != nil {
		return nativeAgentMemory{}, err
	}
	if memory.ConversationID == "" {
		memory.ConversationID = conversationID
	}
	return memory, nil
}

func (r *Runtime) saveMemory(ctx context.Context, memory nativeAgentMemory) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := os.MkdirAll(filepath.Dir(r.memoryFile(memory.ConversationID)), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(memory, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.memoryFile(memory.ConversationID), data, 0o600)
}

func (r *Runtime) memoryFile(conversationID string) string {
	return filepath.Join(r.dataDir, "memory", sanitizeNativeID(conversationID)+".json")
}
