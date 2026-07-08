package dendrite

import (
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/transportapi"
)

const (
	DirextalkRoomTypeChannel       = productpolicy.DirextalkRoomTypeChannel
	DirextalkRoomProfileEventType  = productpolicy.DirextalkRoomProfileEventType
	DirextalkJoinRequestEventType  = productpolicy.DirextalkJoinRequestEventType
	DirextalkMemberPolicyEventType = productpolicy.DirextalkMemberPolicyEventType
)

type channel = transportapi.RoomChannel
type memberRecord = transportapi.RoomMember

type CreateRoomRequest = transportapi.CreateRoomRequest
type RoomStateEvent = transportapi.RoomStateEvent
type SendStateEventRequest = transportapi.SendStateEventRequest
type CreateRoomResult = transportapi.CreateRoomResult
type SendMessageRequest = transportapi.SendMessageRequest
type SendMessageResult = transportapi.SendMessageResult
type InviteUserRequest = transportapi.InviteUserRequest
type JoinRoomRequest = transportapi.JoinRoomRequest
type JoinRoomResult = transportapi.JoinRoomResult
type LeaveRoomRequest = transportapi.LeaveRoomRequest
type KickUserRequest = transportapi.KickUserRequest
type UpdateMemberProfileRequest = transportapi.UpdateMemberProfileRequest
type RedactEventRequest = transportapi.RedactEventRequest
type RedactEventResult = transportapi.RedactEventResult
