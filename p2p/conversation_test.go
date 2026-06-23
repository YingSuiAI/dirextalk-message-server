package p2p

import "testing"

func TestConversationKindFromContentUsesExplicitRoomType(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		want conversationKind
	}{
		{
			name: "direct profile with invite policy stays direct",
			in: map[string]any{
				"room_type":     DirexioRoomTypeDirect,
				"invite_policy": "member",
			},
			want: conversationKindDirect,
		},
		{
			name: "group profile",
			in:   map[string]any{"room_type": DirexioRoomTypeGroup},
			want: conversationKindGroup,
		},
		{
			name: "channel profile",
			in:   map[string]any{"room_type": DirexioRoomTypeChannel},
			want: conversationKindChannel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := conversationKindFromContent(tt.in)
			if got != tt.want {
				t.Fatalf("expected %q, got %q reason=%q", tt.want, got, reason)
			}
			if reason != "" {
				t.Fatalf("expected no diagnostic reason, got %q", reason)
			}
		})
	}
}

func TestConversationKindFromContentRejectsImplicitGroupGuess(t *testing.T) {
	got, reason := conversationKindFromContent(map[string]any{
		"invite_policy": "member",
		"kind":          "group",
		"topic":         "looks like a group",
	})
	if got != "" {
		t.Fatalf("expected unknown kind without explicit room_type, got %q", got)
	}
	if reason == "" {
		t.Fatal("expected diagnostic reason for missing explicit room kind")
	}
}
