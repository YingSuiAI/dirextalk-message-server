package dirextalkdomain

import (
	"context"
	"errors"
)

const GroupAgentStateEventType = "io.dirextalk.group_agent"

var ErrGroupAgentConflict = errors.New("group agent binding or request conflict")
var ErrGroupAgentQueueFull = errors.New("group Agent request queue is full; retry after pending requests settle")

// GroupAgentBinding is the authoritative, revocable grant for one group's
// owner's Native Ying. Matrix state publishes this grant; it does not grant
// owner tools, credentials, memory or access to other rooms.
type GroupAgentBinding struct {
	RoomID            string `json:"room_id"`
	Enabled           bool   `json:"enabled"`
	OwnerMXID         string `json:"owner_mxid"`
	AgentMXID         string `json:"agent_mxid"`
	DisplayName       string `json:"display_name"`
	AvatarURL         string `json:"avatar_url"`
	Revision          int64  `json:"revision"`
	MemberPolicy      string `json:"member_policy"`
	Status            string `json:"status"`
	AccountGeneration int64  `json:"-"`
	EnabledAt         int64  `json:"-"`
}

// GroupAgentRequest stores only trusted event references, never room history
// or model text. Body is hydrated from Matrix only at delivery time.
type GroupAgentRequest struct {
	RequestID         string `json:"request_id"`
	RoomID            string `json:"room_id"`
	EventID           string `json:"event_id"`
	SenderMXID        string `json:"sender_mxid"`
	OwnerMXID         string `json:"owner_mxid"`
	AgentMXID         string `json:"agent_mxid"`
	BindingRevision   int64  `json:"binding_revision"`
	AccountGeneration int64  `json:"account_generation"`
	OriginServerTS    int64  `json:"origin_server_ts"`
	Body              string `json:"body,omitempty"`
	// SenderDisplayName is hydrated with the body at delivery time, sanitized,
	// and used only to attribute the message inside the group conversation.
	SenderDisplayName string `json:"sender_display_name,omitempty"`
	Status            string `json:"-"`
	ReplyEventID      string `json:"-"`
	ReplyDigest       string `json:"-"`
}

// GroupAgentStore keeps grant updates and reply submission serialized by the
// binding row. Callbacks must not reenter this interface for the same room.
// PostgreSQL holds that row lock until the callback and durable update finish.
type GroupAgentStore interface {
	GetGroupAgentBinding(context.Context, string) (GroupAgentBinding, bool, error)
	// ListEnabledGroupAgentBindings powers the Agent's own rolling group
	// summary sweep: it returns only bindings that are enabled for this owner
	// and account generation.
	ListEnabledGroupAgentBindings(context.Context, string, int64, int) ([]GroupAgentBinding, error)
	MutateGroupAgentBinding(context.Context, string, func(*GroupAgentBinding) error) error
	EnqueueGroupAgentRequest(context.Context, GroupAgentRequest) (bool, error)
	ListGroupAgentRequests(context.Context, string, int64, int, string) ([]GroupAgentRequest, error)
	GetGroupAgentRequest(context.Context, string) (GroupAgentRequest, bool, error)
	CancelGroupAgentRequests(context.Context, string, string, string) error
	AdmitGroupAgentPublication(context.Context, string, GroupAgentRequest, []byte) error
	MutateGroupAgentRequest(context.Context, string, func(GroupAgentBinding, *GroupAgentRequest) error) error
}
