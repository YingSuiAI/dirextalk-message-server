package storage

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseStoreReadMarkersAdvanceMonotonicallyAndSurviveReopen(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}

	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, marker := range []readMarker{
		{RoomID: "!z:example.com", EventID: "$z-current", OriginServerTS: 200},
		{RoomID: "!a:example.com", EventID: "$a-current", OriginServerTS: 100},
		{RoomID: "!a:example.com", EventID: "$a-stale", OriginServerTS: 99},
		{RoomID: "!a:example.com", EventID: "$a-equal", OriginServerTS: 100},
	} {
		if err := store.SaveReadMarker(ctx, marker); err != nil {
			t.Fatalf("SaveReadMarker(%#v): %v", marker, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()

	marker, found, err := reloaded.GetReadMarker(ctx, "!a:example.com")
	if err != nil || !found {
		t.Fatalf("GetReadMarker after reopen = (%#v, %v, %v), want current marker", marker, found, err)
	}
	if marker.EventID != "$a-current" || marker.OriginServerTS != 100 {
		t.Fatalf("stale or equal timestamp replaced durable marker: %#v", marker)
	}
	if _, found, err := reloaded.GetReadMarker(ctx, "!missing:example.com"); err != nil || found {
		t.Fatalf("missing GetReadMarker = (_, %v, %v), want (_, false, nil)", found, err)
	}

	markers, err := reloaded.ListReadMarkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(markers) != 2 || markers[0].RoomID != "!a:example.com" || markers[1].RoomID != "!z:example.com" {
		t.Fatalf("ListReadMarkers after reopen = %#v, want room-id order", markers)
	}
}
