package contacts

import (
	"context"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

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
	case "pending_inbound":
		return 3
	case "pending_outbound":
		return 2
	case "rejected", "reject":
		return 1
	default:
		return 0
	}
}
