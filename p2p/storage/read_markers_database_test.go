package storage

import (
	"context"
	"sync"
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
	markersToSave := []readMarker{
		{RoomID: "!z:example.com", EventID: "$z-current", OriginServerTS: 200, TopologicalPosition: 2, StreamPosition: 20},
		{RoomID: "!a:example.com", EventID: "$a-current", OriginServerTS: 100, TopologicalPosition: 5, StreamPosition: 10},
		{RoomID: "!a:example.com", EventID: "$a-stale", OriginServerTS: 999, TopologicalPosition: 4, StreamPosition: 99},
		{RoomID: "!a:example.com", EventID: "$a-equal", OriginServerTS: 100, TopologicalPosition: 5, StreamPosition: 11},
		{RoomID: "!a:example.com", EventID: "$a-replay", OriginServerTS: 9999, TopologicalPosition: 5, StreamPosition: 10},
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(markersToSave))
	for _, marker := range markersToSave {
		marker := marker
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.SaveReadMarker(ctx, marker)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SaveReadMarker: %v", err)
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
	if marker.EventID != "$a-equal" || marker.OriginServerTS != 100 ||
		marker.TopologicalPosition != 5 || marker.StreamPosition != 11 {
		t.Fatalf("authoritatively older marker replaced durable marker: %#v", marker)
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
