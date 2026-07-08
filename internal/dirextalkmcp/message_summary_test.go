package dirextalkmcp

import "testing"

func TestOrdinaryMessageSummaryFormatsSenderAndBody(t *testing.T) {
	summary, ok := OrdinaryMessageSummary(
		"m.room.message",
		"$event",
		1710000300000,
		" @alice:remote.example ",
		map[string]any{"body": " hello "},
		Page{SnapshotTS: 1710000400000, Limit: 10},
	)
	if !ok {
		t.Fatalf("expected ordinary message to be summarized")
	}
	want := MessageSummary{
		EventID:         "$event",
		OriginServerTS:  1710000300000,
		CreatedAt:       "2024-03-09T16:05:00Z",
		Sender:          "alice",
		SenderMXID:      "@alice:remote.example",
		SenderDomain:    "remote.example",
		SenderLocalpart: "alice",
		Msg:             "hello",
	}
	if summary != want {
		t.Fatalf("unexpected summary:\n got %#v\nwant %#v", summary, want)
	}
}

func TestOrdinaryMessageSummaryRejectsNonOrdinaryOrOutOfPageEvents(t *testing.T) {
	page := Page{SnapshotTS: 1710000400000, CursorTS: 1710000300000, CursorID: "$m", Limit: 10}
	tests := []struct {
		name           string
		eventType      string
		eventID        string
		originServerTS int64
		content        map[string]any
	}{
		{
			name:           "non message event",
			eventType:      "m.reaction",
			eventID:        "$old",
			originServerTS: 1710000200000,
			content:        map[string]any{"body": "hello"},
		},
		{
			name:           "channel post product event",
			eventType:      "m.room.message",
			eventID:        "$old",
			originServerTS: 1710000200000,
			content:        map[string]any{"body": "post", "p2p_kind": "channel_post"},
		},
		{
			name:           "blank body",
			eventType:      "m.room.message",
			eventID:        "$old",
			originServerTS: 1710000200000,
			content:        map[string]any{"body": " "},
		},
		{
			name:           "outside cursor",
			eventType:      "m.room.message",
			eventID:        "$z",
			originServerTS: 1710000300000,
			content:        map[string]any{"body": "same timestamp but after cursor"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if summary, ok := OrdinaryMessageSummary(tt.eventType, tt.eventID, tt.originServerTS, "@alice:example.com", tt.content, page); ok {
				t.Fatalf("expected event to be rejected, got %#v", summary)
			}
		})
	}
}
