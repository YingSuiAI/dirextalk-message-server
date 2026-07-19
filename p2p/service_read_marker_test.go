package p2p

import (
	"encoding/json"
	"testing"
)

func TestSyncBootstrapReturnsMetadataOnlyMonotonicReadMarkers(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})

	for _, params := range []map[string]any{
		{"room_id": "!z:example.com", "event_id": "$z-current", "origin_server_ts": int64(200)},
		{"room_id": "!a:example.com", "event_id": "$a-current", "origin_server_ts": int64(100)},
		{"room_id": "!a:example.com", "event_id": "$a-stale", "origin_server_ts": int64(99)},
		{"room_id": "!a:example.com", "event_id": "$a-equal", "origin_server_ts": int64(100)},
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
	if markers[0] != (readMarker{RoomID: "!a:example.com", EventID: "$a-current", OriginServerTS: 100}) ||
		markers[1] != (readMarker{RoomID: "!z:example.com", EventID: "$z-current", OriginServerTS: 200}) {
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

func TestSyncBootstrapReturnsEmptyReadMarkerArray(t *testing.T) {
	t.Parallel()
	service := NewService(Config{ServerName: "example.com"})
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	markers, ok := bootstrap["read_markers"].([]readMarker)
	if !ok || markers == nil || len(markers) != 0 {
		t.Fatalf("sync.bootstrap read_markers = %#v (%T), want non-nil empty metadata array", bootstrap["read_markers"], bootstrap["read_markers"])
	}
}
