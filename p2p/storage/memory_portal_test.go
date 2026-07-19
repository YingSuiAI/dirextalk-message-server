package storage

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestMemoryStorePortalCASAndCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()

	if _, ok, err := store.LoadPortal(ctx); err != nil || ok {
		t.Fatalf("LoadPortal before save = (_, %v, %v), want (_, false, nil)", ok, err)
	}

	state := portalState{
		Initialized:    true,
		MatrixDeviceID: "DEVICE_A",
		AgentConfig: dirextalkdomain.AgentConfig{
			MCPBlockedRoomIDs: []string{"!blocked:example.com"},
			Native: map[string]any{
				"nested":      map[string]any{"enabled": true},
				"typed_map":   map[string]string{"value": "original"},
				"typed_slice": []map[string]any{{"value": "original"}},
			},
		},
		ClientBuild: clientBuild{Version: "1.0.0", BuildNumber: "1"},
	}
	if err := store.SavePortal(ctx, state); err != nil {
		t.Fatalf("SavePortal: %v", err)
	}

	state.AgentConfig.MCPBlockedRoomIDs[0] = "mutated"
	state.AgentConfig.Native["nested"].(map[string]any)["enabled"] = false
	state.AgentConfig.Native["typed_map"].(map[string]string)["value"] = "mutated"
	state.AgentConfig.Native["typed_slice"].([]map[string]any)[0]["value"] = "mutated"
	loaded, ok, err := store.LoadPortal(ctx)
	if err != nil || !ok {
		t.Fatalf("LoadPortal after save = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if got := loaded.AgentConfig.MCPBlockedRoomIDs[0]; got != "!blocked:example.com" {
		t.Fatalf("stored blocked rooms aliased caller input: %q", got)
	}
	if got := loaded.AgentConfig.Native["nested"].(map[string]any)["enabled"]; got != true {
		t.Fatalf("stored native config aliased caller input: %v", got)
	}
	if got := loaded.AgentConfig.Native["typed_map"].(map[string]string)["value"]; got != "original" {
		t.Fatalf("stored typed map aliased caller input: %v", got)
	}
	if got := loaded.AgentConfig.Native["typed_slice"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("stored typed slice aliased caller input: %v", got)
	}

	loaded.AgentConfig.MCPBlockedRoomIDs[0] = "returned mutation"
	loaded.AgentConfig.Native["nested"].(map[string]any)["enabled"] = false
	loaded.AgentConfig.Native["typed_map"].(map[string]string)["value"] = "returned mutation"
	loaded.AgentConfig.Native["typed_slice"].([]map[string]any)[0]["value"] = "returned mutation"
	reloaded, _, err := store.LoadPortal(ctx)
	if err != nil {
		t.Fatalf("LoadPortal after returned-value mutation: %v", err)
	}
	if got := reloaded.AgentConfig.MCPBlockedRoomIDs[0]; got != "!blocked:example.com" {
		t.Fatalf("LoadPortal returned aliased blocked rooms: %q", got)
	}
	if got := reloaded.AgentConfig.Native["nested"].(map[string]any)["enabled"]; got != true {
		t.Fatalf("LoadPortal returned aliased native config: %v", got)
	}
	if got := reloaded.AgentConfig.Native["typed_map"].(map[string]string)["value"]; got != "original" {
		t.Fatalf("LoadPortal returned aliased typed map: %v", got)
	}
	if got := reloaded.AgentConfig.Native["typed_slice"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("LoadPortal returned aliased typed slice: %v", got)
	}

	updated, err := store.SaveClientBuild(ctx, "WRONG_DEVICE", clientBuild{Version: "2.0.0"})
	if err != nil || updated {
		t.Fatalf("SaveClientBuild wrong device = (%v, %v), want (false, nil)", updated, err)
	}
	updated, err = store.SaveClientBuild(ctx, "DEVICE_A", clientBuild{Version: "2.0.0", BuildNumber: "2"})
	if err != nil || !updated {
		t.Fatalf("SaveClientBuild matching device = (%v, %v), want (true, nil)", updated, err)
	}

	loaded, _, _ = store.LoadPortal(ctx)
	if loaded.ClientBuild.Version != "2.0.0" {
		t.Fatalf("client build version = %q, want 2.0.0", loaded.ClientBuild.Version)
	}

	// SavePortal preserves client build while the device is unchanged, matching
	// the durable store's compare-and-swap boundary.
	loaded.Password = "rotated"
	loaded.ClientBuild = clientBuild{Version: "stale"}
	if err := store.SavePortal(ctx, loaded); err != nil {
		t.Fatalf("SavePortal with unchanged device: %v", err)
	}
	loaded, _, _ = store.LoadPortal(ctx)
	if loaded.ClientBuild.Version != "2.0.0" {
		t.Fatalf("unchanged-device SavePortal replaced client build: %q", loaded.ClientBuild.Version)
	}

	loaded.MatrixDeviceID = "DEVICE_B"
	loaded.ClientBuild = clientBuild{Version: "3.0.0"}
	if err := store.SavePortal(ctx, loaded); err != nil {
		t.Fatalf("SavePortal with changed device: %v", err)
	}
	loaded, _, _ = store.LoadPortal(ctx)
	if loaded.ClientBuild.Version != "3.0.0" {
		t.Fatalf("changed-device SavePortal kept old client build: %q", loaded.ClientBuild.Version)
	}
}

func TestMemoryStoreSaveClientBuildRequiresPortal(t *testing.T) {
	t.Parallel()
	updated, err := NewMemoryStore().SaveClientBuild(context.Background(), "DEVICE", clientBuild{Version: "1"})
	if err != nil || updated {
		t.Fatalf("SaveClientBuild without portal = (%v, %v), want (false, nil)", updated, err)
	}
}

func TestMemoryStoreReadMarkersAdvanceMonotonicallyAndListDeterministically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()

	for _, marker := range []readMarker{
		{RoomID: "!z:example.com", EventID: "$z-new", OriginServerTS: 200},
		{RoomID: "!a:example.com", EventID: "$a-current", OriginServerTS: 100},
		{RoomID: "!a:example.com", EventID: "$a-stale", OriginServerTS: 99},
		{RoomID: "!a:example.com", EventID: "$a-equal", OriginServerTS: 100},
	} {
		if err := store.SaveReadMarker(ctx, marker); err != nil {
			t.Fatalf("SaveReadMarker(%#v): %v", marker, err)
		}
	}

	marker, found, err := store.GetReadMarker(ctx, "!a:example.com")
	if err != nil || !found {
		t.Fatalf("GetReadMarker = (%#v, %v, %v), want current marker", marker, found, err)
	}
	if marker.EventID != "$a-current" || marker.OriginServerTS != 100 {
		t.Fatalf("stale or equal timestamp replaced current marker: %#v", marker)
	}
	if _, found, err := store.GetReadMarker(ctx, "!missing:example.com"); err != nil || found {
		t.Fatalf("missing GetReadMarker = (_, %v, %v), want (_, false, nil)", found, err)
	}

	markers, err := store.ListReadMarkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 2 || markers[0].RoomID != "!a:example.com" || markers[1].RoomID != "!z:example.com" {
		t.Fatalf("ListReadMarkers = %#v, want room-id order", markers)
	}
}
