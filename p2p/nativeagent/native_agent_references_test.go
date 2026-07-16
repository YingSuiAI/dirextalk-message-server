package nativeagent

import (
	"testing"

	"github.com/cloudwego/eino/schema"
)

func TestNativeAgentReferencesExtractRoomsAndPostsInToolOrder(t *testing.T) {
	produced := []*schema.Message{
		schema.ToolMessage(`{"result":{"contacts":[{"display_name":"Ada","room_id":"!direct:example.com"},{"display_name":"Ada duplicate","room_id":"!direct:example.com"}]}}`, "call_contacts", schema.WithToolName("dirextalk_contacts_search")),
		schema.ToolMessage(`{"result":{"rooms":[{"type":"group","name":"Team","room_id":"!group:example.com"},{"type":"channel","name":"News","room_id":"!channel:example.com"}]}}`, "call_rooms", schema.WithToolName("dirextalk_rooms_search")),
		schema.ToolMessage(`{"result":{"room_id":"!group:example.com","name":"Team","messages":[{"msg":"matching message"}]}}`, "call_messages", schema.WithToolName("dirextalk_messages_list")),
		schema.ToolMessage(`{"result":{"room_id":"!channel:example.com","channel_id":"news","name":"News","posts":[{"post_id":"post_1","msg":"first post"},{"post_id":"post_2","msg":"second post"}]}}`, "call_posts", schema.WithToolName("dirextalk_channel_posts_list")),
		schema.ToolMessage(`{"result":{"room_id":"!migrated-channel:example.com","channel_id":"news","name":"News duplicate","posts":[{"post_id":"post_1","msg":"duplicate post"}]}}`, "call_duplicate_posts", schema.WithToolName("dirextalk_channel_posts_list")),
	}

	got := nativeAgentReferences(produced)
	if len(got) != 5 {
		t.Fatalf("references len = %d, want 5: %#v", len(got), got)
	}
	want := []map[string]any{
		{"kind": "room", "room_id": "!direct:example.com", "room_type": "direct", "title": "Ada"},
		{"kind": "room", "room_id": "!group:example.com", "room_type": "group", "title": "Team"},
		{"kind": "room", "room_id": "!channel:example.com", "room_type": "channel", "title": "News"},
		{"kind": "channel_post", "room_id": "!channel:example.com", "channel_id": "news", "post_id": "post_1", "title": "News", "preview": "first post"},
		{"kind": "channel_post", "room_id": "!channel:example.com", "channel_id": "news", "post_id": "post_2", "title": "News", "preview": "second post"},
	}
	for index := range want {
		for key, value := range want[index] {
			if got[index][key] != value {
				t.Fatalf("reference %d %s = %#v, want %#v; full=%#v", index, key, got[index][key], value, got)
			}
		}
	}
}

func TestNativeAgentReferencesUseMessagePreviewAndIgnoreInvalidResults(t *testing.T) {
	produced := []*schema.Message{
		schema.ToolMessage(`{"result":{"room_id":"!room:example.com","name":"Room","messages":[{"msg":" matched text "}]}}`, "call_messages", schema.WithToolName("dirextalk_messages_list")),
		schema.ToolMessage(`{"error":"denied"}`, "call_error", schema.WithToolName("dirextalk_rooms_search")),
		schema.ToolMessage(`not-json`, "call_bad", schema.WithToolName("dirextalk_contacts_list")),
		schema.ToolMessage(`{"result":{"room_id":"","name":"Missing"}}`, "call_missing", schema.WithToolName("dirextalk_messages_list")),
		schema.ToolMessage(`{"result":{"rooms":[{"type":"unknown","name":"Unknown","room_id":"!unknown:example.com"}]}}`, "call_unknown", schema.WithToolName("dirextalk_rooms_search")),
		schema.ToolMessage(`{"result":{"room_id":"!channel:example.com","posts":[{"post_id":"post_without_channel","msg":"body"}]}}`, "call_incomplete_post", schema.WithToolName("dirextalk_channel_posts_list")),
		schema.ToolMessage(`{"result":{"room_id":"!ignored:example.com"}}`, "call_other", schema.WithToolName("runtime__shell")),
	}

	got := nativeAgentReferences(produced)
	if len(got) != 1 {
		t.Fatalf("references = %#v, want one valid room reference", got)
	}
	if got[0]["room_id"] != "!room:example.com" || got[0]["preview"] != "matched text" {
		t.Fatalf("unexpected message reference: %#v", got[0])
	}
}
