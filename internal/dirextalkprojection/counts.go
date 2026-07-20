package dirextalkprojection

import (
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func ProductMemberCounts(members []dirextalkdomain.MemberRecord) (joined, pending int64) {
	for _, member := range members {
		switch strings.ToLower(strings.TrimSpace(member.Membership)) {
		case "join":
			joined++
		case "pending":
			pending++
		}
	}
	return joined, pending
}
