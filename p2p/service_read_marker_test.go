package p2p

import (
	"context"
	"encoding/json"
	"testing"
)

type resolvedReadMarker struct {
	roomID              string
	topologicalPosition int64
	streamPosition      int64
	originServerTS      int64
}

type staticReadMarkerResolver map[string]resolvedReadMarker

func (r staticReadMarkerResolver) ResolveReadMarkerPosition(
	_ context.Context, roomID, eventID string,
) (int64, int64, int64, bool, error) {
	resolved, ok := r[eventID]
	if !ok || resolved.roomID != roomID {
		return 0, 0, 0, false, nil
	}
	return resolved.topologicalPosition, resolved.streamPosition, resolved.originServerTS, true, nil
}

func TestSyncBootstrapReturnsMetadataOnlyMonotonicReadMarkers(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})
	service.SetReadMarkerPositionResolver(staticReadMarkerResolver{
		"$z-current": {roomID: "!z:example.com", topologicalPosition: 2, streamPosition: 20, originServerTS: 200},
		"$a-current": {roomID: "!a:example.com", topologicalPosition: 5, streamPosition: 10, originServerTS: 100},
		"$a-stale":   {roomID: "!a:example.com", topologicalPosition: 4, streamPosition: 99, originServerTS: 500},
		"$a-equal":   {roomID: "!a:example.com", topologicalPosition: 5, streamPosition: 11, originServerTS: 100},
		"$a-missing": {roomID: "!a:example.com", topologicalPosition: 6, streamPosition: 12, originServerTS: 101},
		"$a-invalid": {roomID: "!a:example.com", topologicalPosition: 7, streamPosition: 13, originServerTS: 102},
	})

	for _, params := range []map[string]any{
		{"room_id": "!z:example.com", "event_id": "$z-current", "origin_server_ts": int64(-1)},
		{"room_id": "!a:example.com", "event_id": "$a-current", "origin_server_ts": int64(9_999_999_999_999)},
		{"room_id": "!a:example.com", "event_id": "$a-stale"},
		{"room_id": "!a:example.com", "event_id": "$a-equal"},
		{"room_id": "!a:example.com", "event_id": "$a-missing"},
		{"room_id": "!a:example.com", "event_id": "$a-invalid", "origin_server_ts": "invalid"},
		{"room_id": "!a:example.com", "event_id": "$a-stale", "origin_server_ts": int64(9_999_999_999_999)},
	} {
		mustHandle[map[string]any](t, service, "sync.read_marker", params)
	}

	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	markers, ok := bootstrap["read_markers"].([]readMarker)
	if !ok {
		t.Fatalf("sync.bootstrap read_markers type = %T, want []readMarker", bootstrap["read_markers"])
	}
	if len(markers) != 2 {
		t.Fatalf("sync.bootstrap read_markers = %#v, want two metadata records", markers)
	}
	if markers[0] != (readMarker{
		RoomID: "!a:example.com", EventID: "$a-invalid", OriginServerTS: 102,
		TopologicalPosition: 7, StreamPosition: 13,
	}) ||
		markers[1] != (readMarker{
			RoomID: "!z:example.com", EventID: "$z-current", OriginServerTS: 200,
			TopologicalPosition: 2, StreamPosition: 20,
		}) {
		t.Fatalf("sync.bootstrap returned unordered or regressed read markers: %#v", markers)
	}
	encoded, err := json.Marshal(markers)
	if err != nil {
		t.Fatal(err)
	}
	var wire []map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	for _, marker := range wire {
		if len(marker) != 3 || marker["room_id"] == nil || marker["event_id"] == nil || marker["origin_server_ts"] == nil {
			t.Fatalf("sync.bootstrap read marker wire shape must be metadata-only: %#v", marker)
		}
	}
}

func TestReadMarkerRejectsEventOutsideRequestedRoom(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})
	service.SetReadMarkerPositionResolver(staticReadMarkerResolver{
		"$event": {roomID: "!actual:example.com", topologicalPosition: 1, streamPosition: 1},
	})

	for _, testCase := range []struct {
		name, roomID, eventID string
	}{
		{name: "cross-room", roomID: "!other:example.com", eventID: "$event"},
		{name: "unauthorized", roomID: "!actual:example.com", eventID: "$not-visible"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, apiErr := service.Handle(context.Background(), "sync.read_marker", map[string]any{
				"room_id": testCase.roomID, "event_id": testCase.eventID,
			}); apiErr == nil || apiErr.Status != 400 || apiErr.Error != "event_id cannot be resolved in room_id" {
				t.Fatalf("non-visible read marker error = %#v, want stable non-leaking HTTP 400", apiErr)
			}
			if _, found, err := service.store.GetReadMarker(context.Background(), testCase.roomID); err != nil || found {
				t.Fatalf("non-visible marker persisted = (%v, %v), want (false, nil)", found, err)
			}
		})
	}
}

func TestReadMarkerCanonicalizesLegacyBoundaryBeforeAdvancing(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})
	service.SetReadMarkerPositionResolver(staticReadMarkerResolver{
		"$legacy-current": {
			roomID: "!room:example.com", topologicalPosition: 8, streamPosition: 20,
			originServerTS: 100,
		},
		"$older": {
			roomID: "!room:example.com", topologicalPosition: 7, streamPosition: 99,
			originServerTS: 9_999_999_999_999,
		},
	})
	if err := service.store.SaveReadMarker(context.Background(), readMarker{
		RoomID: "!room:example.com", EventID: "$legacy-current", OriginServerTS: 200,
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "sync.read_marker", map[string]any{
		"room_id": "!room:example.com", "event_id": "$older",
	})
	marker, found, err := service.store.GetReadMarker(context.Background(), "!room:example.com")
	if err != nil || !found {
		t.Fatalf("GetReadMarker = (%#v, %v, %v), want canonicalized legacy marker", marker, found, err)
	}
	if marker.EventID != "$legacy-current" || marker.OriginServerTS != 100 ||
		marker.TopologicalPosition != 8 || marker.StreamPosition != 20 {
		t.Fatalf("legacy marker regressed during canonicalization: %#v", marker)
	}
}

func TestSyncBootstrapReturnsEmptyReadMarkerArray(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	markers, ok := bootstrap["read_markers"].([]readMarker)
	if !ok || markers == nil || len(markers) != 0 {
		t.Fatalf("sync.bootstrap read_markers = %#v (%T), want non-nil empty metadata array", bootstrap["read_markers"], bootstrap["read_markers"])
	}
}
