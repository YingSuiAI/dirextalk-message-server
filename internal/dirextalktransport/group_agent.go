package dirextalktransport

import (
	"context"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

// GroupAgentRoom is one current Matrix state snapshot, never a ProductStore
// role projection. Ownership must be unique; ambiguous ownership fails closed.
type GroupAgentRoom struct {
	RoomID    string
	IsGroup   bool
	Dissolved bool
	OwnerMXID string
	Joined    map[string]bool
	JoinedAt  map[string]int64
	Binding   *dirextalkdomain.GroupAgentBinding
}

type GroupAgentMessage struct {
	RoomID         string   `json:"-"`
	EventID        string   `json:"event_id"`
	SenderMXID     string   `json:"sender_mxid"`
	Body           string   `json:"body"`
	OriginServerTS int64    `json:"origin_server_ts"`
	Mentions       []string `json:"-"`
	Generated      bool     `json:"-"`
}

type GroupAgentReadPort interface {
	ReadGroupAgentRoom(context.Context, string) (GroupAgentRoom, error)
	ReadGroupAgentMessage(context.Context, string, string) (GroupAgentMessage, error)
}
