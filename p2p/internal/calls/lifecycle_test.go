package calls

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func TestCallEventValidationLifecycleAndTerminalRules(t *testing.T) {
	current := time.Date(2026, 7, 12, 0, 0, 1, 0, time.UTC)
	store := newTestStore()
	store.calls["call_1"] = dirextalkdomain.CallRecord{
		CallID:    "call_1",
		RoomID:    "!room:example.com",
		RoomType:  "direct",
		MediaType: "video",
		State:     "ringing",
		CreatedAt: "2026-07-12T00:00:00Z",
	}
	events := &eventRecorder{}
	handlers := New(store, Config{
		OwnerMXID:    "@owner:example.com",
		Now:          func() time.Time { return current },
		PublishEvent: events.publish,
	}).Handlers()

	for _, tt := range []struct {
		name    string
		params  map[string]any
		status  int
		message string
	}{
		{name: "missing id", params: map[string]any{"event": "connected"}, status: 400, message: "call_id is required"},
		{name: "missing event", params: map[string]any{"call_id": "call_1"}, status: 400, message: "event must be connected, ended, rejected, missed, or failed"},
		{name: "invalid event", params: map[string]any{"call_id": "call_1", "event": "ringing"}, status: 400, message: "event must be connected, ended, rejected, missed, or failed"},
		{name: "missing call", params: map[string]any{"call_id": "missing", "event": "ended"}, status: 404, message: "call not found"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, apiErr := handlers["calls.event"](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != tt.status || apiErr.Error != tt.message {
				t.Fatalf("calls.event = (%#v, %#v), want %d %q", result, apiErr, tt.status, tt.message)
			}
		})
	}

	result, apiErr := handlers["calls.event"](context.Background(), map[string]any{
		"call_id":     " call_1 ",
		"event":       "connected",
		"media_type":  " voice ",
		"answered_at": "2026-07-12T08:00:00+08:00",
	})
	if apiErr != nil {
		t.Fatalf("connected error = %#v", apiErr)
	}
	connected := result.(dirextalkdomain.CallRecord)
	if connected.State != "connected" || connected.MediaType != "voice" || connected.AnsweredAt != "2026-07-12T00:00:00Z" {
		t.Fatalf("connected call = %#v", connected)
	}

	current = time.Date(2026, 7, 12, 0, 0, 4, 0, time.UTC)
	result, apiErr = handlers["calls.event"](context.Background(), map[string]any{
		"call_id":  "call_1",
		"event":    "ended",
		"ended_at": "2026-07-12T00:00:03.250Z",
		"reason":   " completed ",
	})
	if apiErr != nil {
		t.Fatalf("ended error = %#v", apiErr)
	}
	ended := result.(dirextalkdomain.CallRecord)
	if ended.State != "ended" || ended.EndedAt != "2026-07-12T00:00:03.25Z" ||
		ended.EndedByMXID != "@owner:example.com" || ended.EndReason != "completed" || ended.DurationMS != 3250 {
		t.Fatalf("ended call = %#v", ended)
	}

	upsertsBefore, eventsBefore := len(store.upserts), len(events.events)
	for _, request := range []struct {
		action string
		params map[string]any
	}{
		{action: "calls.incoming", params: map[string]any{"call_id": "call_1", "media_type": "screen"}},
		{action: "calls.event", params: map[string]any{"call_id": "call_1", "event": "connected"}},
		{action: "calls.event", params: map[string]any{"call_id": "call_1", "event": "rejected"}},
	} {
		result, apiErr = handlers[request.action](context.Background(), request.params)
		if apiErr != nil || result.(dirextalkdomain.CallRecord) != ended {
			t.Fatalf("terminal %s = (%#v, %#v), want unchanged %#v", request.action, result, apiErr, ended)
		}
	}
	if len(store.upserts) != upsertsBefore || len(events.events) != eventsBefore {
		t.Fatalf("terminal no-op wrote or published: upserts=%d/%d events=%d/%d", len(store.upserts), upsertsBefore, len(events.events), eventsBefore)
	}

	result, apiErr = handlers["calls.event"](context.Background(), map[string]any{
		"call_id":       "call_1",
		"event":         "ended",
		"ended_by_mxid": " @alice:example.com ",
		"reason":        " corrected ",
		"duration_ms":   float64(5000),
	})
	if apiErr != nil {
		t.Fatalf("same terminal event error = %#v", apiErr)
	}
	covered := result.(dirextalkdomain.CallRecord)
	if covered.EndedAt != ended.EndedAt || covered.EndedByMXID != "@alice:example.com" || covered.EndReason != "corrected" || covered.DurationMS != 5000 {
		t.Fatalf("same terminal event coverage = %#v", covered)
	}
	if len(store.upserts) != upsertsBefore+1 || len(events.events) != eventsBefore+1 {
		t.Fatalf("same terminal event did not persist and publish")
	}
	wantPayload := map[string]any{"call": covered}
	if !reflect.DeepEqual(events.events[len(events.events)-1].Payload, wantPayload) {
		t.Fatalf("same terminal event payload = %#v, want %#v", events.events[len(events.events)-1].Payload, wantPayload)
	}
}

func TestCallTimeParsingPreservesRFC3339AndMillisecondCompatibility(t *testing.T) {
	fixed := time.Date(2026, 7, 12, 0, 0, 10, 0, time.UTC)
	tests := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{
			name:   "RFC3339 offset normalizes to UTC",
			params: map[string]any{"created_at": "2026-07-11T20:00:00-04:00"},
			want:   "2026-07-12T00:00:00Z",
		},
		{
			name:   "invalid primary falls back to numeric milliseconds",
			params: map[string]any{"created_at": "invalid", "created_at_ms": float64(1000)},
			want:   "1970-01-01T00:00:01Z",
		},
		{
			name:   "numeric primary is milliseconds",
			params: map[string]any{"created_at": json.Number("2000")},
			want:   "1970-01-01T00:00:02Z",
		},
		{
			name:   "numeric text is not accepted as milliseconds",
			params: map[string]any{"created_at_ms": "3000"},
			want:   fixed.Format(time.RFC3339Nano),
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore()
			params := make(map[string]any, len(tt.params)+1)
			for key, value := range tt.params {
				params[key] = value
			}
			params["call_id"] = "call_" + string(rune('a'+index))
			result, apiErr := New(store, Config{Now: func() time.Time { return fixed }}).
				Handlers()["calls.create"](context.Background(), params)
			if apiErr != nil {
				t.Fatalf("calls.create error = %#v", apiErr)
			}
			if got := result.(dirextalkdomain.CallRecord).CreatedAt; got != tt.want {
				t.Fatalf("CreatedAt = %q, want %q", got, tt.want)
			}
		})
	}

	store := newTestStore()
	store.calls["lifecycle"] = dirextalkdomain.CallRecord{CallID: "lifecycle", State: "ringing"}
	lifecycleNow := time.UnixMilli(10000).UTC()
	handler := New(store, Config{Now: func() time.Time { return lifecycleNow }}).Handlers()["calls.event"]
	result, apiErr := handler(context.Background(), map[string]any{
		"call_id":        "lifecycle",
		"event":          "connected",
		"answered_at":    "invalid",
		"answered_at_ms": float64(4000),
	})
	if apiErr != nil || result.(dirextalkdomain.CallRecord).AnsweredAt != "1970-01-01T00:00:04Z" {
		t.Fatalf("answered_at_ms fallback = (%#v, %#v)", result, apiErr)
	}
	result, apiErr = handler(context.Background(), map[string]any{"call_id": "lifecycle", "event": "ended"})
	ended := result.(dirextalkdomain.CallRecord)
	if apiErr != nil || ended.EndedAt != lifecycleNow.Format(time.RFC3339Nano) || ended.DurationMS != 6000 {
		t.Fatalf("default ended time/duration = (%#v, %#v)", ended, apiErr)
	}
	if callDurationMS("2026-07-12T00:00:10Z", "2026-07-12T00:00:05Z") != 0 || callDurationMS("invalid", "2026-07-12T00:00:05Z") != 0 {
		t.Fatal("invalid or negative duration must remain zero")
	}
}

func TestMutationLockSerializesThroughPublishAndProtectsTerminalState(t *testing.T) {
	store := newTestStore()
	store.calls["call_1"] = dirextalkdomain.CallRecord{CallID: "call_1", State: "ringing"}
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var enteredOnce sync.Once
	module := New(store, Config{PublishEvent: func(context.Context, dirextalkdomain.Event) error {
		enteredOnce.Do(func() { close(publishEntered) })
		<-releasePublish
		return nil
	}})
	handler := module.Handlers()["calls.event"]
	type outcome struct {
		result any
		err    *actionbase.Error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		result, apiErr := handler(context.Background(), map[string]any{"call_id": "call_1", "event": "ended"})
		if apiErr != nil {
			firstDone <- outcome{err: apiErr}
			return
		}
		firstDone <- outcome{result: result}
	}()
	<-publishEntered

	secondStarted := make(chan struct{})
	secondDone := make(chan outcome, 1)
	go func() {
		close(secondStarted)
		result, apiErr := handler(context.Background(), map[string]any{"call_id": "call_1", "event": "connected"})
		if apiErr != nil {
			secondDone <- outcome{err: apiErr}
			return
		}
		secondDone <- outcome{result: result}
	}()
	<-secondStarted
	select {
	case got := <-secondDone:
		t.Fatalf("second transition escaped mutation lock before publish completed: %#v", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(releasePublish)
	for name, done := range map[string]<-chan outcome{"first": firstDone, "second": secondDone} {
		got := <-done
		if got.err != nil {
			t.Fatalf("%s transition error = %v", name, got.err)
		}
		call := got.result.(dirextalkdomain.CallRecord)
		if call.State != "ended" {
			t.Fatalf("%s transition returned %#v, want terminal ended", name, call)
		}
	}
	if got := store.calls["call_1"].State; got != "ended" {
		t.Fatalf("final stored state = %q, want ended", got)
	}
}
