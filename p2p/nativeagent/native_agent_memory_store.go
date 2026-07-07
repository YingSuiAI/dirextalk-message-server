package nativeagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

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
	memory.Messages = compactEinoMessagesForMemory(memory.Messages)
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
	memory.Messages = compactEinoMessagesForMemory(memory.Messages)
	data, err := json.MarshalIndent(memory, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.memoryFile(memory.ConversationID), data, 0o600)
}

func (r *Runtime) memoryFile(conversationID string) string {
	return filepath.Join(r.dataDir, "memory", sanitizeNativeID(conversationID)+".json")
}
