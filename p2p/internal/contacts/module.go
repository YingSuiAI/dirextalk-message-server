// Package contacts owns contact reads and durable contact-save orchestration.
package contacts

import (
	"context"
	"strings"
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

type peerMutationEntry struct {
	mu   sync.Mutex
	refs int
}

type Module struct {
	store        Store
	conversation ConversationPort
	deleteGroup  func(context.Context, string) error
	mutationMu   sync.Mutex

	peerMutationsMu sync.Mutex
	peerMutations   map[string]*peerMutationEntry
}

func New(store Store, conversation ConversationPort, cfg Config) *Module {
	return &Module{
		store:        store,
		conversation: conversation,
		deleteGroup:  cfg.DeleteGroup,
	}
}

// SerializePeer runs fn under a process-local lock scoped to the trimmed peer
// Matrix ID. It coordinates callers sharing this Module only; it is not a
// cross-Module, cross-process, or durable compare-and-swap boundary.
func (m *Module) SerializePeer(peerMXID string, fn func()) {
	if fn == nil {
		return
	}

	key := strings.TrimSpace(peerMXID)
	mutation := m.acquirePeerMutation(key)
	mutation.mu.Lock()
	defer m.releasePeerMutation(key, mutation)

	fn()
}

func (m *Module) acquirePeerMutation(key string) *peerMutationEntry {
	m.peerMutationsMu.Lock()
	defer m.peerMutationsMu.Unlock()

	if m.peerMutations == nil {
		m.peerMutations = make(map[string]*peerMutationEntry)
	}
	mutation := m.peerMutations[key]
	if mutation == nil {
		mutation = &peerMutationEntry{}
		m.peerMutations[key] = mutation
	}
	mutation.refs++
	return mutation
}

func (m *Module) releasePeerMutation(key string, mutation *peerMutationEntry) {
	mutation.mu.Unlock()

	m.peerMutationsMu.Lock()
	defer m.peerMutationsMu.Unlock()

	mutation.refs--
	if mutation.refs == 0 && m.peerMutations[key] == mutation {
		delete(m.peerMutations, key)
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
