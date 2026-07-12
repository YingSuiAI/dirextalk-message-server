// Package contacts owns contact reads and durable contact-save orchestration.
package contacts

import (
	"context"
	"sync"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

// Store is the durable contact repository used by Module.
type Store interface {
	UpsertContact(ctx context.Context, contact dirextalkdomain.ContactRecord) error
	ListContacts(ctx context.Context) ([]dirextalkdomain.ContactRecord, error)
}

// ConversationPort owns durable conversation projection records.
type ConversationPort interface {
	ListRecords(ctx context.Context) ([]dirextalkdomain.ConversationRecord, error)
	Save(ctx context.Context, record dirextalkdomain.ConversationRecord) error
	DeleteKindByRoom(ctx context.Context, roomID string, kind dirextalkdomain.ConversationKind) error
}

type Config struct {
	DeleteGroup func(ctx context.Context, roomID string) error
}

type Module struct {
	store        Store
	conversation ConversationPort
	deleteGroup  func(context.Context, string) error
	mutationMu   sync.Mutex
}

func New(store Store, conversation ConversationPort, cfg Config) *Module {
	return &Module{
		store:        store,
		conversation: conversation,
		deleteGroup:  cfg.DeleteGroup,
	}
}

// Save serializes the contact and conversation persistence orchestration. It
// does not cover Matrix room creation or any work performed before this call.
func (m *Module) Save(ctx context.Context, contact dirextalkdomain.ContactRecord) error {
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()

	existingContacts, err := m.store.ListContacts(ctx)
	if err != nil {
		return err
	}
	existingConversations, err := m.conversation.ListRecords(ctx)
	if err != nil {
		return err
	}
	replacedDirectRoomIDs := replacementDirectRoomIDs(contact, existingContacts, existingConversations)

	if err := m.store.UpsertContact(ctx, contact); err != nil {
		return err
	}
	for _, roomID := range replacedDirectRoomIDs {
		if err := m.conversation.DeleteKindByRoom(ctx, roomID, dirextalkdomain.ConversationKindDirect); err != nil {
			return err
		}
	}
	if contact.RoomID != "" {
		if m.deleteGroup != nil {
			if err := m.deleteGroup(ctx, contact.RoomID); err != nil {
				return err
			}
		}
		if err := m.conversation.DeleteKindByRoom(ctx, contact.RoomID, dirextalkdomain.ConversationKindGroup); err != nil {
			return err
		}
	}
	return m.conversation.Save(ctx, dirextalkdomain.ConversationFromContact(contact))
}

func replacementDirectRoomIDs(
	contact dirextalkdomain.ContactRecord,
	existingContacts []dirextalkdomain.ContactRecord,
	existingConversations []dirextalkdomain.ConversationRecord,
) []string {
	roomIDs := make([]string, 0)
	if contact.PeerMXID == "" {
		return roomIDs
	}
	seen := make(map[string]struct{})
	appendRoomID := func(roomID string) {
		if roomID == "" || roomID == contact.RoomID {
			return
		}
		if _, ok := seen[roomID]; ok {
			return
		}
		seen[roomID] = struct{}{}
		roomIDs = append(roomIDs, roomID)
	}
	for _, existing := range existingContacts {
		if existing.PeerMXID == contact.PeerMXID {
			appendRoomID(existing.RoomID)
		}
	}
	for _, existing := range existingConversations {
		if existing.Kind == dirextalkdomain.ConversationKindDirect && existing.PeerMXID == contact.PeerMXID {
			appendRoomID(existing.MatrixRoomID)
		}
	}
	return roomIDs
}
