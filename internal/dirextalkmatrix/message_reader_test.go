package dirextalkmatrix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPMessageReaderListOrdinaryMessagesScansPagesAndExcludesProductEvents(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_matrix/client/v3/rooms/!channel:example.com/messages") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("unexpected authorization header: %q", r.Header.Get("Authorization"))
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("from") == "" {
			_ = json.NewEncoder(w).Encode(messagesResponse{
				Chunk: []Event{
					{
						EventID:        "$ordinary_newer",
						Type:           "m.room.message",
						Sender:         "@owner:example.com",
						OriginServerTS: 1710000300000,
						Content:        map[string]any{"body": "newer chat"},
					},
					{
						EventID:        "$post",
						Type:           "m.room.message",
						Sender:         "@owner:example.com",
						OriginServerTS: 1710000200000,
						Content:        map[string]any{"body": "post body", "p2p_kind": "channel_post"},
					},
					{
						EventID:        "$comment",
						Type:           "m.room.message",
						Sender:         "@owner:example.com",
						OriginServerTS: 1710000100000,
						Content:        map[string]any{"body": "comment body", "p2p_kind": "channel_comment"},
					},
				},
				End: "next",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(messagesResponse{
			Chunk: []Event{
				{
					EventID:        "$ordinary_older",
					Type:           "m.room.message",
					Sender:         "@alice:remote.example",
					OriginServerTS: 1710000000000,
					Content:        map[string]any{"body": "older chat"},
				},
			},
		})
	}))
	defer server.Close()

	reader := NewHTTPMessageReader(server.URL, func(context.Context) (string, error) {
		return "token", nil
	}, server.Client())
	result, err := reader.ListOrdinaryMessages(context.Background(), "!channel:example.com", Page{
		SnapshotTS: 1710000400000,
		Limit:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("expected reader to scan a second Matrix page, got %d requests", requests)
	}
	if result.HasMore {
		t.Fatalf("did not expect has_more for two ordinary messages: %#v", result)
	}
	if len(result.Messages) != 2 ||
		result.Messages[0].EventID != "$ordinary_newer" ||
		result.Messages[0].CreatedAt != "2024-03-09T16:05:00Z" ||
		result.Messages[1].EventID != "$ordinary_older" {
		t.Fatalf("expected only ordinary channel chat messages newest-first, got %#v", result.Messages)
	}
}

func TestHTTPMessageReaderListChannelContentIncludesPostsCommentsAndReactions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(messagesResponse{
			Chunk: []Event{
				{
					EventID:        "$ordinary",
					Type:           "m.room.message",
					Sender:         "@owner:example.com",
					OriginServerTS: 1710000300000,
					Content:        map[string]any{"body": "ordinary chat"},
				},
				{
					EventID:        "$post",
					Type:           "m.room.message",
					Sender:         "@owner:example.com",
					OriginServerTS: 1710000200000,
					Content:        map[string]any{"body": "post body", "p2p_kind": "channel_post"},
				},
				{
					EventID:        "$comment",
					Type:           "m.room.message",
					Sender:         "@owner:example.com",
					OriginServerTS: 1710000100000,
					Content:        map[string]any{"body": "comment body", "p2p_kind": "channel_comment"},
				},
				{
					EventID:        "$reaction",
					Type:           "m.reaction",
					Sender:         "@owner:example.com",
					OriginServerTS: 1710000400000,
					Content:        map[string]any{"m.relates_to": map[string]any{"event_id": "$post"}},
				},
				{
					EventID:        "$unrelated_reaction",
					Type:           "m.reaction",
					Sender:         "@owner:example.com",
					OriginServerTS: 1710000500000,
					Content:        map[string]any{"body": "ignore"},
				},
			},
		})
	}))
	defer server.Close()

	reader := NewHTTPMessageReader(server.URL, func(context.Context) (string, error) {
		return "token", nil
	}, server.Client())
	events, err := reader.ListChannelContent(context.Background(), "!channel:example.com", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("expected post, comment, and related reaction only, got %#v", events)
	}
	if events[0].EventID != "$post" || events[0].RoomID != "!channel:example.com" {
		t.Fatalf("unexpected post event: %#v", events[0])
	}
	if events[1].EventID != "$comment" || events[2].EventID != "$reaction" {
		t.Fatalf("unexpected channel content order: %#v", events)
	}
}
