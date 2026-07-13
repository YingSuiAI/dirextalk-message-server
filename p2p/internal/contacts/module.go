// Package contacts owns contact reads and durable contact-save orchestration.
package contacts

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// Store is the durable contact repository used by Module.
type Store interface {
	UpsertContact(ctx context.Context, contact dirextalkdomain.ContactRecord) error
	ListContacts(ctx context.Context) ([]dirextalkdomain.ContactRecord, error)
}

type compareAndSwapContactStore interface {
	CompareAndSwapContact(ctx context.Context, contact, expected dirextalkdomain.ContactRecord) (bool, error)
}

type compareAndSwapContactProjectionStore interface {
	CompareAndSwapContactProjection(ctx context.Context, contact, expected dirextalkdomain.ContactRecord) (bool, error)
}

// ConversationPort owns durable conversation projection records.
type ConversationPort interface {
	ListRecords(ctx context.Context) ([]dirextalkdomain.ConversationRecord, error)
	Save(ctx context.Context, record dirextalkdomain.ConversationRecord) error
	DeleteKindByRoom(ctx context.Context, roomID string, kind dirextalkdomain.ConversationKind) error
	Operation(ctx context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error)
}

// LocalProfileSnapshot is one atomic local identity snapshot used for direct
// room Matrix writes.
type LocalProfileSnapshot struct {
	MXID        string
	DisplayName string
	AvatarURL   string
}

// DirectRoomAcceptor resolves the final Matrix room for an accepted contact.
// The returned room ID is authoritative even when empty.
type DirectRoomAcceptor func(ctx context.Context, contact dirextalkdomain.ContactRecord, serverNames []string) (roomID string, actionErr *actionbase.Error)

// DirectRoomCreateRequest describes one private direct invite room. The
// fallback room is returned when no Matrix transport is configured.
type DirectRoomCreateRequest struct {
	PeerMXID       string
	DisplayName    string
	Remark         string
	FallbackRoomID string
}

type DirectRoomCreator func(ctx context.Context, request DirectRoomCreateRequest) (roomID string, actionErr *actionbase.Error)

// DirectRoomInviteRequest describes a repeated invite to an existing direct
// contact room.
type DirectRoomInviteRequest struct {
	Contact dirextalkdomain.ContactRecord
}

type DirectRoomInviter func(ctx context.Context, request DirectRoomInviteRequest) *actionbase.Error

type DirectRoomJoinMode uint8

const (
	DirectRoomJoinNormal DirectRoomJoinMode = iota
	DirectRoomJoinReactivation
)

type DirectRoomJoinKind uint8

const (
	DirectRoomJoinUnknown DirectRoomJoinKind = iota
	DirectRoomJoinSucceeded
	DirectRoomJoinInviteRequired
	DirectRoomJoinRetainedUnavailable
	DirectRoomJoinFailed
)

// DirectRoomJoinRequest describes one Matrix join attempt using a workflow's
// atomic local identity snapshot.
type DirectRoomJoinRequest struct {
	RoomID                string
	Profile               LocalProfileSnapshot
	ServerNames           []string
	Mode                  DirectRoomJoinMode
	UseRoomServerFallback bool
}

// DirectRoomJoinOutcome classifies Matrix-specific failures for contact
// workflows without exposing Dendrite error strings to the module.
type DirectRoomJoinOutcome struct {
	Kind    DirectRoomJoinKind
	RoomID  string
	Failure *actionbase.Error
}

type DirectRoomJoiner func(context.Context, DirectRoomJoinRequest) DirectRoomJoinOutcome

// DirectRoomReactivator invites a retained accepted peer back to its room.
type DirectRoomReactivator func(ctx context.Context, profile LocalProfileSnapshot, roomID, requesterMXID string) *actionbase.Error

// PeerReactivationRequest contains the typed contact and profile state needed
// to ask a peer node whether it still retains a direct contact room.
type PeerReactivationRequest struct {
	Contact           dirextalkdomain.ContactRecord
	RequesterMXID     string
	RemoteNodeBaseURL string
	DisplayName       string
	AvatarURL         string
	Domain            string
	Remark            string
}

// PeerReactivationResult describes the peer node's retained-room decision.
type PeerReactivationResult struct {
	PendingInbound bool
	NotRetained    bool
	RoomID         string
}

// PeerReactivator is the protocol-independent port used by contact workflows.
type PeerReactivator func(context.Context, PeerReactivationRequest) (PeerReactivationResult, *actionbase.Error)

// PeerBlockChecker reports whether a contact request target is already
// blocked. The blocks module remains the owner of the underlying records.
type PeerBlockChecker func(context.Context, string) (bool, error)

type Config struct {
	ServerName           string
	DeleteGroup          func(ctx context.Context, roomID string) error
	LeaveRoom            func(ctx context.Context, roomID string) *actionbase.Error
	AcceptDirectRoom     DirectRoomAcceptor
	VerifyAcceptedRoom   bool
	CreateDirectRoom     DirectRoomCreator
	InviteDirectRoom     DirectRoomInviter
	JoinDirectRoom       DirectRoomJoiner
	NewDirectRoomID      func() string
	LocalProfile         func() LocalProfileSnapshot
	ReactivatePeer       PeerReactivator
	ReactivateDirectRoom DirectRoomReactivator
	CheckPeerBlocked     PeerBlockChecker
	MatrixJoined         func(context.Context, string, string) (bool, error)
}

type peerMutationEntry struct {
	mu   sync.Mutex
	refs int
}

type Module struct {
	serverName       string
	store            Store
	conversation     ConversationPort
	deleteGroup      func(context.Context, string) error
	leaveRoom        func(context.Context, string) *actionbase.Error
	acceptRoom       DirectRoomAcceptor
	verifyAccepted   bool
	createRoom       DirectRoomCreator
	inviteRoom       DirectRoomInviter
	joinRoom         DirectRoomJoiner
	newDirectRoomID  func() string
	localProfile     func() LocalProfileSnapshot
	reactivatePeer   PeerReactivator
	reactivateRoom   DirectRoomReactivator
	checkPeerBlocked PeerBlockChecker
	matrixJoined     func(context.Context, string, string) (bool, error)
	mutationMu       sync.Mutex

	peerMutationsMu sync.Mutex
	peerMutations   map[string]*peerMutationEntry
}

func New(store Store, conversation ConversationPort, cfg Config) *Module {
	return &Module{
		serverName:       cfg.ServerName,
		store:            store,
		conversation:     conversation,
		deleteGroup:      cfg.DeleteGroup,
		leaveRoom:        cfg.LeaveRoom,
		acceptRoom:       cfg.AcceptDirectRoom,
		verifyAccepted:   cfg.VerifyAcceptedRoom,
		createRoom:       cfg.CreateDirectRoom,
		inviteRoom:       cfg.InviteDirectRoom,
		joinRoom:         cfg.JoinDirectRoom,
		newDirectRoomID:  cfg.NewDirectRoomID,
		localProfile:     cfg.LocalProfile,
		reactivatePeer:   cfg.ReactivatePeer,
		reactivateRoom:   cfg.ReactivateDirectRoom,
		checkPeerBlocked: cfg.CheckPeerBlocked,
		matrixJoined:     cfg.MatrixJoined,
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

func acceptedStatus(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "accepted")
}

// Save serializes the contact and conversation persistence orchestration. It
// does not cover Matrix room creation or any work performed before this call.
func (m *Module) Save(ctx context.Context, contact dirextalkdomain.ContactRecord) error {
	_, err := m.save(ctx, contact, nil)
	return err
}

// SaveProjectionIfCurrent advances one contact generation together with its
// direct-conversation projection. Supported production stores provide the
// atomic repository boundary; the fallback preserves compatibility for narrow
// in-package stores while retaining generation CAS.
func (m *Module) SaveProjectionIfCurrent(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
) (bool, error) {
	store, ok := m.store.(compareAndSwapContactProjectionStore)
	if !ok {
		return m.save(ctx, contact, &expected)
	}
	return store.CompareAndSwapContactProjection(ctx, contact, expected)
}

// EnsureAcceptedProjection restores the durable direct-conversation projection
// for an already accepted contact without dispatching another Matrix join. The
// compare-and-swap fence preserves a concurrent newer contact generation.
func (m *Module) EnsureAcceptedProjection(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
) (dirextalkdomain.ContactRecord, error) {
	if !acceptedStatus(contact.Status) {
		return contact, nil
	}
	saved, err := m.SaveProjectionIfCurrent(ctx, contact, contact)
	if err != nil || saved {
		return contact, err
	}
	current, found, err := m.LookupByPeer(ctx, contact.PeerMXID)
	if err != nil {
		return current, err
	}
	if !found {
		return current, errors.New("accepted contact disappeared while restoring conversation projection")
	}
	return current, nil
}

func (m *Module) saveDecision(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
	authoritativeAccepted bool,
) (dirextalkdomain.ContactRecord, bool, error) {
	if expected.PeerMXID == "" {
		if err := m.Save(ctx, contact); err != nil {
			return dirextalkdomain.ContactRecord{}, false, err
		}
		return contact, true, nil
	}
	saved, err := m.SaveProjectionIfCurrent(ctx, contact, expected)
	if err != nil || saved {
		return contact, saved, err
	}
	current, found, err := m.LookupByPeer(ctx, expected.PeerMXID)
	if err == nil && !found && contact.PeerMXID != expected.PeerMXID {
		current, found, err = m.LookupByPeer(ctx, contact.PeerMXID)
	}
	if err != nil || !found {
		return current, false, err
	}
	if authoritativeAccepted && acceptedStatus(contact.Status) && current.RequestID == expected.RequestID &&
		!strings.EqualFold(strings.TrimSpace(current.Status), "deleted") {
		saved, err = m.SaveProjectionIfCurrent(ctx, contact, current)
		if err != nil || saved {
			return contact, saved, err
		}
		current, _, err = m.LookupByPeer(ctx, contact.PeerMXID)
	}
	return current, false, err
}

func (m *Module) save(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	expected *dirextalkdomain.ContactRecord,
) (bool, error) {
	m.mutationMu.Lock()
	defer m.mutationMu.Unlock()

	existingContacts, err := m.store.ListContacts(ctx)
	if err != nil {
		return false, err
	}
	existingConversations, err := m.conversation.ListRecords(ctx)
	if err != nil {
		return false, err
	}
	replacedDirectRoomIDs := replacementDirectRoomIDs(contact, existingContacts, existingConversations)

	if expected != nil {
		if store, ok := m.store.(compareAndSwapContactStore); ok {
			saved, err := store.CompareAndSwapContact(ctx, contact, *expected)
			if err != nil || !saved {
				return saved, err
			}
		} else {
			current, found := contactByPeer(existingContacts, expected.PeerMXID)
			if !found || current.RoomID != expected.RoomID || current.RequestID != expected.RequestID ||
				!strings.EqualFold(strings.TrimSpace(current.Status), strings.TrimSpace(expected.Status)) {
				return false, nil
			}
			if err := m.store.UpsertContact(ctx, contact); err != nil {
				return false, err
			}
		}
	} else if err := m.store.UpsertContact(ctx, contact); err != nil {
		return false, err
	}
	for _, roomID := range replacedDirectRoomIDs {
		if err := m.conversation.DeleteKindByRoom(ctx, roomID, dirextalkdomain.ConversationKindDirect); err != nil {
			return false, err
		}
	}
	if contact.RoomID != "" {
		if m.deleteGroup != nil {
			if err := m.deleteGroup(ctx, contact.RoomID); err != nil {
				return false, err
			}
		}
		if err := m.conversation.DeleteKindByRoom(ctx, contact.RoomID, dirextalkdomain.ConversationKindGroup); err != nil {
			return false, err
		}
	}
	if err := m.conversation.Save(ctx, dirextalkdomain.ConversationFromContact(contact)); err != nil {
		return false, err
	}
	return true, nil
}

func contactByPeer(contacts []dirextalkdomain.ContactRecord, peerMXID string) (dirextalkdomain.ContactRecord, bool) {
	for _, contact := range contacts {
		if contact.PeerMXID == peerMXID {
			return contact, true
		}
	}
	return dirextalkdomain.ContactRecord{}, false
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
