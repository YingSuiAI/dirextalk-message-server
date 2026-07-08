package dirextalkdomain

import "encoding/json"

type Channel struct {
	ChannelID        string `json:"channel_id"`
	RoomID           string `json:"room_id"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	AvatarURL        string `json:"avatar_url"`
	Visibility       string `json:"visibility"`
	JoinPolicy       string `json:"join_policy"`
	ChannelType      string `json:"channel_type"`
	CommentsEnabled  bool   `json:"comments_enabled"`
	Muted            bool   `json:"muted"`
	MemberCount      int64  `json:"member_count"`
	PendingJoinCount int64  `json:"pending_join_count"`
	IsOwned          bool   `json:"is_owned,omitempty"`
	Role             string `json:"role,omitempty"`
	MemberStatus     string `json:"member_status,omitempty"`
}

type GroupRecord struct {
	RoomID       string `json:"room_id"`
	Name         string `json:"name"`
	Topic        string `json:"topic"`
	AvatarURL    string `json:"avatar_url"`
	MemberCount  int64  `json:"member_count"`
	InvitePolicy string `json:"invite_policy"`
	Muted        bool   `json:"muted"`
}

type MemberRecord struct {
	RoomID               string `json:"room_id"`
	ChannelID            string `json:"channel_id"`
	UserID               string `json:"user_id"`
	DisplayName          string `json:"display_name"`
	AvatarURL            string `json:"avatar_url"`
	Domain               string `json:"domain"`
	Membership           string `json:"membership"`
	Role                 string `json:"role"`
	Muted                bool   `json:"muted"`
	JoinedAt             int64  `json:"joined_at"`
	RequesterNodeBaseURL string `json:"-"`
}

func (m MemberRecord) MarshalJSON() ([]byte, error) {
	type memberAlias MemberRecord
	return json.Marshal(struct {
		memberAlias
		UserMXID string `json:"user_mxid"`
		Status   string `json:"status"`
	}{
		memberAlias: memberAlias(m),
		UserMXID:    m.UserID,
		Status:      m.Membership,
	})
}
