package routing

import (
	"context"
	"testing"

	rsapi "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	rstypes "github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
)

type redactedEventRoomserver struct {
	rsapi.SyncRoomserverAPI
	event *rstypes.HeaderedEvent
}

func (r *redactedEventRoomserver) QueryEventsByID(ctx context.Context, req *rsapi.QueryEventsByIDRequest, res *rsapi.QueryEventsByIDResponse) error {
	res.Events = []*rstypes.HeaderedEvent{r.event}
	return nil
}

func TestPreferRoomserverRedactedEvent(t *testing.T) {
	stale := mustGetEventTestEvent(t, `$event:test`, `{
		"type": "m.room.message",
		"room_id": "!room:test",
		"sender": "@alice:test",
		"content": {"body": "expected message body", "msgtype": "m.text"}
	}`, false)
	redacted := mustGetEventTestEvent(t, `$event:test`, `{
		"type": "m.room.message",
		"room_id": "!room:test",
		"sender": "@alice:test",
		"content": {},
		"unsigned": {
			"redacted_because": {
				"type": "m.room.redaction",
				"room_id": "!room:test",
				"sender": "@alice:test",
				"content": {},
				"redacts": "$event:test"
			}
		}
	}`, true)

	got := preferRoomserverRedactedEvent(context.Background(), &redactedEventRoomserver{event: redacted}, stale)
	if body := string(got.Content()); body != "{}" {
		t.Fatalf("expected redacted event content, got %s", body)
	}
}

func TestPreferRoomserverRedactedEventKeepsCurrentEvent(t *testing.T) {
	current := mustGetEventTestEvent(t, `$event:test`, `{
		"type": "m.room.message",
		"room_id": "!room:test",
		"sender": "@alice:test",
		"content": {"body": "expected message body", "msgtype": "m.text"}
	}`, false)

	got := preferRoomserverRedactedEvent(context.Background(), &redactedEventRoomserver{event: current}, current)
	if got != current {
		t.Fatal("expected current event to be preserved when roomserver has no redaction")
	}
}

func mustGetEventTestEvent(t *testing.T, eventID, raw string, redacted bool) *rstypes.HeaderedEvent {
	t.Helper()
	pdu, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSONWithEventID(eventID, []byte(raw), redacted)
	if err != nil {
		t.Fatalf("failed to create event: %s", err)
	}
	return &rstypes.HeaderedEvent{PDU: pdu}
}
