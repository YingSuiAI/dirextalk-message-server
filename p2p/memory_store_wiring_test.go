package p2p

import (
	"context"
	"reflect"
	"testing"

	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestNoDatabaseConstructorsUseIndependentSeededMemoryStores(t *testing.T) {
	transport := &recordingTransport{}
	services := []*Service{
		NewService(Config{ServerName: "example.com"}),
		NewServiceWithTransport(Config{ServerName: "example.com"}, transport),
	}
	stores := make([]*p2pstorage.MemoryStore, len(services))
	for i, service := range services {
		store, ok := service.store.(*p2pstorage.MemoryStore)
		if !ok {
			t.Fatalf("service %d store = %T, want *storage.MemoryStore", i, service.store)
		}
		stores[i] = store

		state, found, err := store.LoadPortal(context.Background())
		if err != nil {
			t.Fatalf("service %d LoadPortal: %v", i, err)
		}
		if !found {
			t.Fatalf("service %d MemoryStore portal was not seeded", i)
		}
		service.mu.Lock()
		want := service.portalStateLocked()
		service.mu.Unlock()
		if !reflect.DeepEqual(state, want) {
			t.Fatalf("service %d seeded portal = %#v, want %#v", i, state, want)
		}

		value, apiErr := service.Handle(context.Background(), "portal.status", nil)
		if apiErr != nil {
			t.Fatalf("portal status: %#v", apiErr)
		}
		status := value.(map[string]any)
		if status["store_mode"] != "memory" {
			t.Fatalf("service %d store_mode = %#v, want memory", i, status["store_mode"])
		}
	}
	if stores[0] == stores[1] {
		t.Fatal("no-database constructors shared one MemoryStore")
	}
	if got := storeMode(&DatabaseStore{}); got != "database" {
		t.Fatalf("storeMode(non-memory Store) = %q, want database", got)
	}

	identity, authorized := services[0].authorizeProductAction(services[0].AccessToken(), "client.version.report")
	if !authorized {
		t.Fatal("owner access token was not authorized")
	}
	_, apiErr := services[0].Handle(withPortalActionSession(context.Background(), identity), "client.version.report", map[string]any{
		"client_version": "1.2.3",
	})
	if apiErr != nil {
		t.Fatalf("first client.version.report = %#v", apiErr)
	}
	persisted, found, err := stores[0].LoadPortal(context.Background())
	if err != nil || !found || persisted.ClientBuild.Version != "v1.2.3" {
		t.Fatalf("persisted client build = (%#v, %v, %v)", persisted.ClientBuild, found, err)
	}
}

func TestNoDatabaseConstructorWithTransportRemainsLightweight(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	if service.transport != transport {
		t.Fatalf("service transport = %T, want supplied transport", service.transport)
	}
	if len(transport.createRooms) != 0 || len(transport.messages) != 0 || len(transport.stateEvents) != 0 ||
		len(transport.inviteRequests) != 0 || len(transport.joinRequests) != 0 || len(transport.leaves) != 0 ||
		len(transport.kicks) != 0 || len(transport.profiles) != 0 || len(transport.redactions) != 0 {
		t.Fatalf("lightweight constructor called transport: %#v", transport)
	}
}

func TestNoDatabaseEventRetentionAllowsDedupeKeyReuseAfterPrune(t *testing.T) {
	service := NewService(Config{
		ServerName:                    "example.com",
		P2PEventRetentionMaxRows:      1,
		P2PEventRetentionPruneOnWrite: true,
	})
	ctx := context.Background()

	for _, event := range []p2pEvent{
		{Seq: 1, Type: "first", DedupeKey: "reusable"},
		{Seq: 2, Type: "second", DedupeKey: "other"},
		{Seq: 3, Type: "third", DedupeKey: "reusable"},
	} {
		if err := service.appendP2PEvent(ctx, event); err != nil {
			t.Fatalf("append %s: %v", event.Type, err)
		}
	}
	events, err := service.listP2PEvents(ctx, 0, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 || events[0].Type != "third" || events[0].DedupeKey != "reusable" {
		t.Fatalf("retained events = %#v", events)
	}
}
