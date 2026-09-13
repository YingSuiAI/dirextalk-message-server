package dirextalktransport

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

// GroupAgentDisplayNameMaxRunes bounds an untrusted room display name before it
// reaches the shared group conversation.
const GroupAgentDisplayNameMaxRunes = 64

// SanitizeGroupAgentDisplayName turns a member-controlled profile name into a
// single bounded line. Members choose their own names, so the value is display
// data for the model and can never carry policy text, line breaks or control
// characters. Callers keep the authenticated MXID as the real identity.
func SanitizeGroupAgentDisplayName(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !utf8.ValidString(trimmed) {
		return ""
	}
	var out strings.Builder
	space := false
	written := 0
	for _, r := range trimmed {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == utf8.RuneError {
			space = out.Len() > 0
			continue
		}
		if space {
			out.WriteByte(' ')
			written++
			space = false
		}
		if written >= GroupAgentDisplayNameMaxRunes {
			break
		}
		out.WriteRune(r)
		written++
	}
	return strings.TrimSpace(out.String())
}

// GroupAgentRoom is one current Matrix state snapshot, never a ProductStore
// role projection. Ownership must be unique; ambiguous ownership fails closed.
// GroupAgentMember is one currently joined member of the group, as the Agent
// may see it: the authenticated MXID plus the sanitized in-room display name.
// It never carries avatars, contacts, phone numbers or any other private data.
type GroupAgentMember struct {
	MXID        string `json:"mxid"`
	DisplayName string `json:"display_name,omitempty"`
}

type GroupAgentRoom struct {
	RoomID    string
	IsGroup   bool
	Dissolved bool
	OwnerMXID string
	// OwnerDisplayName is the owner's current in-room profile name. The shared
	// group Agent is labelled with it so every client (including builds that
	// only know room members) shows the owner's Ying instead of a bare service
	// account name.
	OwnerDisplayName string
	// Members is the current joined roster in reader order; the service bounds
	// and orders it before it reaches a model.
	Members  []GroupAgentMember
	Joined   map[string]bool
	JoinedAt map[string]int64
	Binding  *dirextalkdomain.GroupAgentBinding
}

type GroupAgentMessage struct {
	RoomID     string `json:"-"`
	EventID    string `json:"event_id"`
	SenderMXID string `json:"sender_mxid"`
	// SenderDisplayName is the sender's current in-room profile name, already
	// sanitized for model use. It is display data for attributing the shared
	// group conversation, never an authorization input.
	SenderDisplayName string   `json:"sender_display_name,omitempty"`
	Body              string   `json:"body"`
	OriginServerTS    int64    `json:"origin_server_ts"`
	Mentions          []string `json:"-"`
	Generated         bool     `json:"-"`
}

type GroupAgentReadPort interface {
	ReadGroupAgentRoom(context.Context, string) (GroupAgentRoom, error)
	ReadGroupAgentMessage(context.Context, string, string) (GroupAgentMessage, error)
}
