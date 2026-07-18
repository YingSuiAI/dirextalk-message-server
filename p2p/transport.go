package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport"

type Transport = dirextalktransport.Transport
type RoomCreatorReader = dirextalktransport.RoomCreatorReader
type CreateRoomRequest = dirextalktransport.CreateRoomRequest
type RoomStateEvent = dirextalktransport.RoomStateEvent
type SendStateEventRequest = dirextalktransport.SendStateEventRequest
type CreateRoomResult = dirextalktransport.CreateRoomResult
type SendMessageRequest = dirextalktransport.SendMessageRequest
type SendMessageResult = dirextalktransport.SendMessageResult
type InviteUserRequest = dirextalktransport.InviteUserRequest
type JoinRoomRequest = dirextalktransport.JoinRoomRequest
type JoinRoomResult = dirextalktransport.JoinRoomResult
type LeaveRoomRequest = dirextalktransport.LeaveRoomRequest
type KickUserRequest = dirextalktransport.KickUserRequest
type UpdateMemberProfileRequest = dirextalktransport.UpdateMemberProfileRequest
type RedactEventRequest = dirextalktransport.RedactEventRequest
type RedactEventResult = dirextalktransport.RedactEventResult
type RoomChannel = dirextalktransport.RoomChannel
type RoomMember = dirextalktransport.RoomMember
