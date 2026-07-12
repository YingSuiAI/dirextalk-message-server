package contacts

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const actionList = "contacts.list"

// View is the public ProductCore contact response.
type View struct {
	PeerMXID            string                            `json:"peer_mxid"`
	DisplayName         string                            `json:"display_name"`
	DisplayNameOverride bool                              `json:"display_name_override,omitempty"`
	AvatarURL           string                            `json:"avatar_url"`
	Domain              string                            `json:"domain"`
	RoomID              string                            `json:"room_id"`
	Status              string                            `json:"status"`
	Remark              string                            `json:"remark,omitempty"`
	Operation           map[string]any                    `json:"operation,omitempty"`
	Conversation        *dirextalkdomain.ConversationView `json:"conversation,omitempty"`
}

func ViewFromRecord(record dirextalkdomain.ContactRecord) View {
	return View{
		PeerMXID:            record.PeerMXID,
		DisplayName:         record.DisplayName,
		DisplayNameOverride: record.DisplayNameOverride,
		AvatarURL:           record.AvatarURL,
		Domain:              record.Domain,
		RoomID:              record.RoomID,
		Status:              record.Status,
		Remark:              record.Remark,
	}
}

func RecordFromView(view View) dirextalkdomain.ContactRecord {
	return dirextalkdomain.ContactRecord{
		PeerMXID:            view.PeerMXID,
		DisplayName:         view.DisplayName,
		DisplayNameOverride: view.DisplayNameOverride,
		AvatarURL:           view.AvatarURL,
		Domain:              view.Domain,
		RoomID:              view.RoomID,
		Status:              view.Status,
		Remark:              view.Remark,
	}
}

func ViewsFromRecords(records []dirextalkdomain.ContactRecord) []View {
	views := make([]View, 0, len(records))
	for _, record := range records {
		views = append(views, ViewFromRecord(record))
	}
	return views
}

func RecordsFromViews(views []View) []dirextalkdomain.ContactRecord {
	records := make([]dirextalkdomain.ContactRecord, 0, len(views))
	for _, view := range views {
		records = append(records, RecordFromView(view))
	}
	return records
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{actionList: m.handleList}
}

func (m *Module) handleList(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	contacts, err := m.ListVisible(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"contacts": ViewsFromRecords(contacts)}, nil
}
