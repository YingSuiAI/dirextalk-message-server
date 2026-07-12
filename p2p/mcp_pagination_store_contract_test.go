package p2p

import (
	"context"
	"testing"
)

func TestMCPPaginationUsesSocialStoreForFavorites(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.store.UpsertFavorite(context.Background(), favoriteRecord{
		ID:          7,
		EventID:     "$post:example.com",
		RoomID:      "!channel:example.com",
		MessageType: "channel_post",
	}); err != nil {
		t.Fatalf("seed favorite: %v", err)
	}

	count, byMe := service.mcpFavoriteStateForPost(context.Background(), channelPostRecord{
		EventID: "$post:example.com",
		RoomID:  "!channel:example.com",
	})
	if count != 1 || !byMe {
		t.Fatalf("favorite state = (%d, %t), want (1, true)", count, byMe)
	}
}
