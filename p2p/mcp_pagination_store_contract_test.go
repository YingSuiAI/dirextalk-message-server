package p2p

import (
	"context"
	"testing"
)

func TestMCPPaginationUsesProjectedFavoriteReactions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.ownerMXID = "@owner:example.com"
	if err := service.store.UpsertReaction(context.Background(), reactionRecord{
		TargetType: "post",
		TargetID:   "post",
		ChannelID:  "channel",
		PostID:     "post",
		Reaction:   "favorite",
		UserID:     "@owner:example.com",
		Active:     true,
	}); err != nil {
		t.Fatalf("seed favorite reaction: %v", err)
	}

	count, byMe := service.mcpFavoriteStateForPost(context.Background(), channelPostRecord{
		PostID: "post",
		RoomID: "!channel:example.com",
	})
	if count != 1 || !byMe {
		t.Fatalf("favorite state = (%d, %t), want (1, true)", count, byMe)
	}
}
