package transportapi

import (
	"context"
	"time"

	"github.com/YingSuiAI/direxio-message-server/p2p/domain"
)

type Transport interface {
	CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error)
	SendMessage(ctx context.Context, req SendMessageRequest) (SendMessageResult, error)
	SendStateEvent(ctx context.Context, req SendStateEventRequest) error
	InviteUser(ctx context.Context, req InviteUserRequest) error
	JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error)
	LeaveRoom(ctx context.Context, req LeaveRoomRequest) error
	KickUser(ctx context.Context, req KickUserRequest) error
	GetRoomChannel(ctx context.Context, roomID string) (domain.Channel, bool, error)
	ListRoomMembers(ctx context.Context, roomID string) ([]domain.MemberRecord, error)
	UpdateMemberProfile(ctx context.Context, req UpdateMemberProfileRequest) error
	RedactEvent(ctx context.Context, req RedactEventRequest) (RedactEventResult, error)
}

type CreateRoomRequest struct {
	CreatorMXID        string
	CreatorDisplayName string
	CreatorAvatarURL   string
	Name               string
	Topic              string
	Visibility         string
	RoomType           string
	CreationContent    map[string]any
	IsDirect           bool
	InviteMXIDs        []string
	InitialState       []RoomStateEvent
}

type RoomStateEvent struct {
	Type     string
	StateKey string
	Content  map[string]any
}

type SendStateEventRequest struct {
	RoomID     string
	SenderMXID string
	Event      RoomStateEvent
	Timestamp  time.Time
}

type CreateRoomResult struct {
	RoomID string
}

type SendMessageRequest struct {
	SenderMXID  string
	RoomID      string
	EventType   string
	MessageType string
	Content     map[string]any
	Timestamp   time.Time
}

type SendMessageResult struct {
	EventID        string
	OriginServerTS int64
}

type InviteUserRequest struct {
	RoomID          string
	InviterMXID     string
	InviteeMXID     string
	Reason          string
	IsDirect        bool
	InviteRoomState []RoomStateEvent
}

type JoinRoomRequest struct {
	RoomIDOrAlias             string
	UserMXID                  string
	DisplayName               string
	AvatarURL                 string
	ServerNames               []string
	DirectContactReactivation bool
}

type JoinRoomResult struct {
	RoomID    string
	JoinedVia string
}

type LeaveRoomRequest struct {
	RoomID   string
	UserMXID string
}

type KickUserRequest struct {
	RoomID     string
	SenderMXID string
	TargetMXID string
	Reason     string
	Timestamp  time.Time
}

type UpdateMemberProfileRequest struct {
	RoomID      string
	UserMXID    string
	DisplayName string
	AvatarURL   string
	Timestamp   time.Time
}

type RedactEventRequest struct {
	RoomID     string
	EventID    string
	SenderMXID string
	Reason     string
	Timestamp  time.Time
}

type RedactEventResult struct {
	EventID string
}
