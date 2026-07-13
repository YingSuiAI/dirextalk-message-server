package contacts

import (
	"context"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionDelete        = "contacts.delete"
	actionList          = "contacts.list"
	actionReactivate    = "contacts.reactivate"
	actionRequest       = "contacts.request"
	actionUpdate        = "contacts.update"
	actionRequestAccept = "contacts.requests.accept"
	actionRequestDelete = "contacts.requests.delete"
	actionRequestReject = "contacts.requests.reject"
)

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
	OperationID         string                            `json:"operation_id,omitempty"`
	CurrentRoomID       string                            `json:"current_room_id,omitempty"`
	ErrorCode           string                            `json:"error_code,omitempty"`
	RequestID           string                            `json:"-"`
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
		RequestID:           record.RequestID,
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
		RequestID:           view.RequestID,
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
	return map[string]actionbase.Handler{
		actionDelete:        m.Delete,
		actionList:          m.handleList,
		actionReactivate:    m.handleReactivate,
		actionRequest:       m.Request,
		actionUpdate:        m.handleUpdate,
		actionRequestAccept: m.handleRequestAccept,
		actionRequestDelete: m.handleRequestDelete,
		actionRequestReject: m.handleRequestReject,
	}
}

func (m *Module) ListRaw(ctx context.Context) ([]dirextalkdomain.ContactRecord, error) {
	return m.store.ListContacts(ctx)
}

func (m *Module) ListVisible(ctx context.Context) ([]dirextalkdomain.ContactRecord, error) {
	contacts, err := m.ListRaw(ctx)
	if err != nil {
		return nil, err
	}
	visible := make([]dirextalkdomain.ContactRecord, 0, len(contacts))
	for _, contact := range contacts {
		if dirextalkdomain.ContactDeleted(contact.Status) {
			continue
		}
		visible = append(visible, contact)
	}
	return dedupeByPeer(visible), nil
}

func (m *Module) LookupByRoom(ctx context.Context, roomID string) (dirextalkdomain.ContactRecord, bool, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return dirextalkdomain.ContactRecord{}, false, nil
	}
	contacts, err := m.ListRaw(ctx)
	if err != nil {
		return dirextalkdomain.ContactRecord{}, false, err
	}
	for _, contact := range contacts {
		if contact.RoomID == roomID {
			return contact, true, nil
		}
	}
	return dirextalkdomain.ContactRecord{}, false, nil
}

func (m *Module) LookupByPeer(ctx context.Context, peerMXID string) (dirextalkdomain.ContactRecord, bool, error) {
	peerMXID = strings.TrimSpace(peerMXID)
	if peerMXID == "" {
		return dirextalkdomain.ContactRecord{}, false, nil
	}
	contacts, err := m.ListRaw(ctx)
	if err != nil {
		return dirextalkdomain.ContactRecord{}, false, err
	}
	var found dirextalkdomain.ContactRecord
	for _, contact := range contacts {
		if contact.PeerMXID == peerMXID &&
			(found.PeerMXID == "" || statusRank(contact.Status) > statusRank(found.Status)) {
			found = contact
		}
	}
	if found.PeerMXID != "" {
		return found, true, nil
	}
	return dirextalkdomain.ContactRecord{}, false, nil
}

func (m *Module) lookupDecisionContact(
	ctx context.Context,
	roomID,
	peerMXID string,
) (dirextalkdomain.ContactRecord, bool, error) {
	var contact dirextalkdomain.ContactRecord
	var found bool
	var err error
	if strings.TrimSpace(roomID) != "" {
		contact, found, err = m.LookupByRoom(ctx, roomID)
	}
	if !found && strings.TrimSpace(peerMXID) != "" && err == nil {
		contact, found, err = m.LookupByPeer(ctx, peerMXID)
	}
	if err != nil || !found || strings.TrimSpace(contact.RoomID) == "" {
		return dirextalkdomain.ContactRecord{}, false, err
	}
	return contact, true, nil
}

func dedupeByPeer(contacts []dirextalkdomain.ContactRecord) []dirextalkdomain.ContactRecord {
	if len(contacts) <= 1 {
		return contacts
	}
	byPeer := make(map[string]dirextalkdomain.ContactRecord, len(contacts))
	for _, contact := range contacts {
		key := strings.TrimSpace(contact.PeerMXID)
		if key == "" {
			key = strings.TrimSpace(contact.RoomID)
		}
		if key == "" {
			continue
		}
		existing, ok := byPeer[key]
		if !ok || statusRank(contact.Status) > statusRank(existing.Status) {
			byPeer[key] = contact
			continue
		}
		if statusRank(contact.Status) == statusRank(existing.Status) {
			if existing.DisplayName == "" && contact.DisplayName != "" {
				existing.DisplayName = contact.DisplayName
			}
			if existing.AvatarURL == "" && contact.AvatarURL != "" {
				existing.AvatarURL = contact.AvatarURL
			}
			if existing.Domain == "" && contact.Domain != "" {
				existing.Domain = contact.Domain
			}
			if existing.Remark == "" && contact.Remark != "" {
				existing.Remark = contact.Remark
			}
			byPeer[key] = existing
		}
	}
	result := make([]dirextalkdomain.ContactRecord, 0, len(byPeer))
	for _, contact := range byPeer {
		result = append(result, contact)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left := strings.ToLower(result[i].DisplayName)
		right := strings.ToLower(result[j].DisplayName)
		if left == right {
			return result[i].PeerMXID < result[j].PeerMXID
		}
		return left < right
	})
	return result
}

func statusRank(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted":
		return 4
	case "pending_inbound", "joining":
		return 3
	case "pending_outbound":
		return 2
	case "rejected", "reject":
		return 1
	default:
		return 0
	}
}

func (m *Module) viewWithOperation(ctx context.Context, action string, contact dirextalkdomain.ContactRecord) (View, *actionbase.Error) {
	view, err := m.HydrateView(ctx, action, contact)
	if err != nil {
		return View{}, actionbase.InternalError(err)
	}
	return view, nil
}

// HydrateView adds the legacy operation and conversation presentation to one
// already-persisted contact fact. It is read-only and is also used by durable
// recovery responses that must retain the normal ProductCore success shape.
func (m *Module) HydrateView(ctx context.Context, action string, contact dirextalkdomain.ContactRecord) (View, error) {
	operation, conversation, err := m.conversation.Operation(ctx, action, contact.Status, contact.RoomID)
	if err != nil {
		return View{}, err
	}
	view := ViewFromRecord(contact)
	view.Operation = operation
	view.Conversation = conversation
	return view, nil
}

func (m *Module) handleList(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	contacts, err := m.ListVisible(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"contacts": ViewsFromRecords(contacts)}, nil
}
