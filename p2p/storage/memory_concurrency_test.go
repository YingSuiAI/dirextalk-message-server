package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryStoreConcurrentAccessAndEventDedupe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.SavePortal(ctx, portalState{MatrixDeviceID: "DEVICE"}); err != nil {
		t.Fatalf("SavePortal: %v", err)
	}

	const workers = 64
	var inserted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("%02d", i)
			roomID := "!room-" + id + ":example.com"
			userID := "@user-" + id + ":example.com"
			if err := store.UpsertChannel(ctx, channel{ChannelID: "ch_" + id, RoomID: roomID}); err != nil {
				t.Errorf("UpsertChannel: %v", err)
			}
			if _, _, err := store.GetChannelByIDOrRoom(ctx, "ch_"+id, roomID); err != nil {
				t.Errorf("GetChannelByIDOrRoom: %v", err)
			}
			if err := store.UpsertMember(ctx, memberRecord{RoomID: roomID, UserID: userID, Membership: "join"}); err != nil {
				t.Errorf("UpsertMember: %v", err)
			}
			if _, _, err := store.LookupMember(ctx, roomID, userID); err != nil {
				t.Errorf("LookupMember: %v", err)
			}
			if ok, err := store.InsertEvent(ctx, p2pEvent{Seq: int64(i + 1), DedupeKey: "shared", Payload: map[string]any{"worker": id}}); err != nil {
				t.Errorf("InsertEvent: %v", err)
			} else if ok {
				inserted.Add(1)
			}
			if ok, err := store.SaveClientBuild(ctx, "DEVICE", clientBuild{Version: id}); err != nil || !ok {
				t.Errorf("SaveClientBuild = (%v, %v)", ok, err)
			}
		}()
	}
	wg.Wait()

	if got := inserted.Load(); got != 1 {
		t.Fatalf("deduplicated inserts = %d, want 1", got)
	}
	channels, err := store.ListChannels(ctx)
	if err != nil || len(channels) != workers {
		t.Fatalf("ListChannels = (%d, %v), want (%d, nil)", len(channels), err, workers)
	}
	members, err := store.ListMembers(ctx, "", "")
	if err != nil || len(members) != workers {
		t.Fatalf("ListMembers = (%d, %v), want (%d, nil)", len(members), err, workers)
	}
}

func TestMemoryStorePreservesLegacyWritesAfterContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewMemoryStore()

	want := channel{ChannelID: "ch_cancelled", RoomID: "!cancelled:example.com"}
	if err := store.UpsertChannel(ctx, want); err != nil {
		t.Fatalf("UpsertChannel with cancelled context: %v", err)
	}
	got, ok, err := store.GetChannelByIDOrRoom(ctx, want.ChannelID, "")
	if err != nil || !ok || got != want {
		t.Fatalf("GetChannelByIDOrRoom with cancelled context = (%#v, %v, %v), want (%#v, true, nil)", got, ok, err, want)
	}
}
