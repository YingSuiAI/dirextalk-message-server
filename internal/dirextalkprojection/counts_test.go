package dirextalkprojection

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestProductMemberCountsCountsJoinedAndPendingOnly(t *testing.T) {
	joined, pending := ProductMemberCounts([]dirextalkdomain.MemberRecord{
		{UserID: "@owner:example.com", Membership: "join"},
		{UserID: "@alice:example.com", Membership: "join"},
		{UserID: "@bob:example.com", Membership: " pending "},
		{UserID: "@left:example.com", Membership: "left"},
		{UserID: "@removed:example.com", Membership: "remove"},
		{UserID: "@empty:example.com"},
	})

	if joined != 2 || pending != 1 {
		t.Fatalf("expected joined=2 pending=1, got joined=%d pending=%d", joined, pending)
	}
}
