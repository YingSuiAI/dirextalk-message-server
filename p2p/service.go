package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	"github.com/YingSuiAI/direxio-message-server/p2p/domain"
	"github.com/YingSuiAI/direxio-message-server/p2p/serviceapi"
)

type Config struct {
	ServerName                      string
	Homeserver                      string
	RemoteNodeInsecureSkipTLSVerify bool
	RemoteNodeAllowPrivateBaseURLs  bool
}

const (
	ownerLocalpart = "owner"
	agentLocalpart = "agent"
	agentRoomName  = "Agents"
)

func transportWriteError(err error) *apiError {
	if err == nil {
		return nil
	}
	var policyErr *productpolicy.PolicyError
	if errors.As(err, &policyErr) {
		status := policyErr.Code
		if status <= 0 {
			status = http.StatusForbidden
		}
		return statusError(status, policyErr.Message)
	}
	return internalError(err)
}

type Service struct {
	mu              sync.Mutex
	matrixSessionMu sync.Mutex

	serverName         string
	homeserver         string
	store              Store
	transport          Transport
	sessions           MatrixSessionIssuer
	matrixMessages     matrixMessageReader
	matrixProfiles     matrixProfileResolver
	remoteHTTPClient   *http.Client
	remoteAllowPrivate bool
	storeMode          string
	projectorStarted   bool

	initialized    bool
	password       string
	accessToken    string
	matrixDeviceID string
	agentToken     string
	ownerMXID      string
	agentRoomID    string
	profile        ownerProfile
	agentConfig    agentConfig
	actions        map[string]actionHandler

	readMarkers   map[string]readMarker
	channels      map[string]channel
	posts         []channelPostRecord
	comments      []channelCommentRecord
	contacts      map[string]contactRecord
	groups        map[string]groupRecord
	calls         map[string]callRecord
	favorites     map[int64]favoriteRecord
	reports       map[string]reportRecord
	follows       map[string]followRecord
	reactions     map[string]reactionRecord
	members       map[string]memberRecord
	conversations map[string]conversationRecord
	inviteGrants  map[string]channelInviteGrant
	events        []p2pEvent
	nextEventSeq  int64
	eventNotify   chan struct{}
}

type Store interface {
	LoadPortal(ctx context.Context) (portalState, bool, error)
	SavePortal(ctx context.Context, state portalState) error
	SaveReadMarker(ctx context.Context, marker readMarker) error
	UpsertChannel(ctx context.Context, ch channel) error
	DeleteChannel(ctx context.Context, channelID string) error
	ListChannels(ctx context.Context) ([]channel, error)
	GetChannelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error)
	ListJoinedChannelsForUser(ctx context.Context, userID string) ([]channel, error)
	SearchPublicChannels(ctx context.Context, query string, limit int) ([]channel, error)
	ListPublicChannelsForOwner(ctx context.Context, userID string) ([]channel, error)
	InsertChannelPost(ctx context.Context, post channelPostRecord) error
	ListChannelPosts(ctx context.Context, channelID string) ([]channelPostRecord, error)
	InsertChannelComment(ctx context.Context, comment channelCommentRecord) error
	ListChannelComments(ctx context.Context, postID string) ([]channelCommentRecord, error)
	UpsertContact(ctx context.Context, contact contactRecord) error
	ListContacts(ctx context.Context) ([]contactRecord, error)
	DeleteContact(ctx context.Context, roomID string) error
	UpsertGroup(ctx context.Context, group groupRecord) error
	DeleteGroup(ctx context.Context, roomID string) error
	ListGroups(ctx context.Context) ([]groupRecord, error)
	GetGroupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error)
	ListJoinedGroupsForUser(ctx context.Context, userID string) ([]groupRecord, error)
	UpsertCall(ctx context.Context, call callRecord) error
	ListCalls(ctx context.Context, roomID string, activeOnly bool) ([]callRecord, error)
	UpsertFavorite(ctx context.Context, favorite favoriteRecord) error
	FindFavoriteByEvent(ctx context.Context, eventID, roomID string) (favoriteRecord, bool, error)
	ListFavorites(ctx context.Context, messageType string) ([]favoriteRecord, error)
	DeleteFavorite(ctx context.Context, id int64) error
	InsertReport(ctx context.Context, report reportRecord) error
	UpsertFollow(ctx context.Context, follow followRecord) error
	ListFollows(ctx context.Context) ([]followRecord, error)
	DeleteFollow(ctx context.Context, domain string) error
	UpsertReaction(ctx context.Context, reaction reactionRecord) error
	GetReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error)
	CountActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error)
	ListReactions(ctx context.Context, userID string) ([]reactionRecord, error)
	UpsertMember(ctx context.Context, member memberRecord) error
	LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error)
	ListMembers(ctx context.Context, roomID, channelID string) ([]memberRecord, error)
	ListMembersForUser(ctx context.Context, userID string) ([]memberRecord, error)
	CountProductMembers(ctx context.Context, roomID, channelID string) (joined, pending int64, err error)
	CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error)
	UpsertConversation(ctx context.Context, record conversationRecord) error
	GetConversationByID(ctx context.Context, conversationID string) (conversationRecord, bool, error)
	GetConversationByRoomID(ctx context.Context, matrixRoomID string) (conversationRecord, bool, error)
	ListConversations(ctx context.Context) ([]conversationRecord, error)
	DeleteConversationByRoomID(ctx context.Context, matrixRoomID string) error
	DeleteChannelPost(ctx context.Context, postID string) error
	DeleteChannelComment(ctx context.Context, commentID string) error
	InsertEvent(ctx context.Context, event p2pEvent) error
	ListEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error)
	UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error
	ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error)
}

type portalState = domain.PortalState
type ownerProfile = domain.OwnerProfile
type agentConfig = domain.AgentConfig

const matrixPortalDeviceID = "P2P_PORTAL"

type readMarker = domain.ReadMarker
type channel = domain.Channel
type channelInviteGrant = domain.ChannelInviteGrant
type channelPostRecord = domain.ChannelPostRecord
type channelCommentRecord = domain.ChannelCommentRecord
type contactRecord = domain.ContactRecord
type groupRecord = domain.GroupRecord
type callRecord = domain.CallRecord
type favoriteRecord = domain.FavoriteRecord
type reportRecord = domain.ReportRecord
type followRecord = domain.FollowRecord
type reactionRecord = domain.ReactionRecord
type channelReactionHistory = domain.ChannelReactionHistory
type memberRecord = domain.MemberRecord

func NewService(cfg Config) *Service {
	return newService(cfg, nil, nil, portalState{}, false)
}

func NewServiceWithTransport(cfg Config, transport Transport) *Service {
	return newService(cfg, nil, transport, portalState{}, false)
}

func NewServiceWithStore(ctx context.Context, cfg Config, store Store) (*Service, error) {
	return NewServiceWithStoreAndTransport(ctx, cfg, store, nil)
}

func NewServiceWithStoreAndTransport(ctx context.Context, cfg Config, store Store, transport Transport) (*Service, error) {
	state, ok, err := store.LoadPortal(ctx)
	if err != nil {
		return nil, err
	}
	shouldPersist := !ok || !state.Initialized || strings.TrimSpace(state.Password) == ""
	service := newService(cfg, store, transport, state, ok)
	agentRoomChanged, err := service.ensureAgentRoom(ctx)
	if err != nil {
		return nil, err
	}
	if shouldPersist || agentRoomChanged {
		service.mu.Lock()
		state = service.portalStateLocked()
		service.mu.Unlock()
		if err := store.SavePortal(ctx, state); err != nil {
			return nil, err
		}
	}
	if err := service.writePortalCredentialsFile(); err != nil {
		return nil, err
	}
	if err := service.repairLocalChannelOwnerRoles(ctx); err != nil {
		return nil, err
	}
	return service, nil
}

func (s *Service) ensureAgentRoom(ctx context.Context) (bool, error) {
	s.mu.Lock()
	currentRoomID := strings.TrimSpace(s.agentRoomID)
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	agentMXID := s.agentMXIDLocked()
	agentDisplayName := s.agentDisplayNameLocked()
	agentOnline := s.agentConfig.Enabled
	s.mu.Unlock()
	if s.transport == nil {
		return false, nil
	}
	if !needsAgentRoomCreate(currentRoomID, s.serverName) {
		if currentRoomID != "" {
			if err := s.ensureAgentRoomAgentMember(ctx, currentRoomID, ownerMXID, agentMXID, agentDisplayName); err != nil {
				return false, err
			}
			if err := s.ensureAgentRoomOwnerMember(ctx, currentRoomID, ownerMXID, ownerDisplayName, agentMXID); err != nil {
				return false, err
			}
			if err := s.publishAgentStatusState(ctx, currentRoomID, agentMXID, agentMXID, agentOnline); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	res, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
		CreatorMXID:        ownerMXID,
		CreatorDisplayName: ownerDisplayName,
		CreatorAvatarURL:   ownerAvatarURL,
		Name:               agentRoomName,
		Topic:              "Direxio agents room",
		Visibility:         "private",
		InviteMXIDs:        []string{agentMXID},
		InitialState:       []RoomStateEvent{agentRoomPowerLevelsStateEvent(ownerMXID, agentMXID)},
	})
	if err != nil {
		return false, err
	}
	roomID := strings.TrimSpace(res.RoomID)
	if roomID == "" {
		return false, errors.New("agent room creation returned empty room_id")
	}
	s.mu.Lock()
	s.agentRoomID = roomID
	s.mu.Unlock()
	if err := s.ensureAgentRoomAgentMember(ctx, roomID, ownerMXID, agentMXID, agentDisplayName); err != nil {
		return false, err
	}
	if err := s.publishAgentStatusState(ctx, roomID, agentMXID, agentMXID, agentOnline); err != nil {
		return false, err
	}
	return roomID != currentRoomID, nil
}

func (s *Service) ensureAgentRoomAgentMember(ctx context.Context, roomID, ownerMXID, agentMXID, agentDisplayName string) error {
	if strings.TrimSpace(roomID) == "" || strings.TrimSpace(agentMXID) == "" {
		return nil
	}
	if _, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: roomID,
		UserMXID:      agentMXID,
		DisplayName:   agentDisplayName,
	}); err == nil {
		return nil
	}
	if strings.TrimSpace(ownerMXID) != "" {
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      roomID,
			InviterMXID: ownerMXID,
			InviteeMXID: agentMXID,
			Reason:      "Direxio agents gateway",
		}); err != nil {
			return err
		}
	}
	_, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: roomID,
		UserMXID:      agentMXID,
		DisplayName:   agentDisplayName,
	})
	return err
}

func (s *Service) ensureAgentRoomOwnerMember(ctx context.Context, roomID, ownerMXID, ownerDisplayName, agentMXID string) error {
	if strings.TrimSpace(roomID) == "" || strings.TrimSpace(ownerMXID) == "" {
		return nil
	}
	if _, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: roomID,
		UserMXID:      ownerMXID,
		DisplayName:   ownerDisplayName,
	}); err == nil {
		return nil
	}
	if strings.TrimSpace(agentMXID) != "" {
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      roomID,
			InviterMXID: agentMXID,
			InviteeMXID: ownerMXID,
			Reason:      "Direxio agents owner",
		}); err != nil {
			return err
		}
	}
	_, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: roomID,
		UserMXID:      ownerMXID,
		DisplayName:   ownerDisplayName,
	})
	return err
}

func (s *Service) agentMXIDLocked() string {
	return "@" + agentLocalpart + ":" + strings.TrimSpace(s.serverName)
}

func (s *Service) agentDisplayNameLocked() string {
	return fallbackString(strings.TrimSpace(s.agentConfig.DisplayName), "Agent")
}

func needsAgentRoomCreate(roomID, serverName string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return true
	}
	if strings.EqualFold(roomID, "!agent:"+strings.TrimSpace(serverName)) {
		return true
	}
	return strings.HasPrefix(roomID, "!agent:")
}

func storeMode(store Store) string {
	if store == nil {
		return "memory"
	}
	return "database"
}

func ownerMXIDForServer(serverName string) string {
	return "@" + ownerLocalpart + ":" + serverName
}

func (s *Service) SetMatrixSessionIssuer(issuer MatrixSessionIssuer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = issuer
}

func (s *Service) SetMatrixMessageReader(reader matrixMessageReader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matrixMessages = reader
}

func (s *Service) SetMatrixProfileResolver(resolver matrixProfileResolver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matrixProfiles = resolver
}

func (s *Service) SetProjectorStarted(started bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projectorStarted = started
}

type portalCredentialsFile struct {
	Version     int       `json:"version"`
	GeneratedAt time.Time `json:"generated_at"`
	OwnerUserID string    `json:"owner_user_id"`
	UserID      string    `json:"user_id"`
	Homeserver  string    `json:"homeserver"`
	AccessToken string    `json:"access_token"`
	DeviceID    string    `json:"device_id"`
	AgentToken  string    `json:"agent_token"`
	Password    string    `json:"password"`
	AgentRoomID string    `json:"agent_room_id"`
}

func newService(cfg Config, store Store, transport Transport, state portalState, hasPortal bool) *Service {
	serverName := strings.TrimSpace(cfg.ServerName)
	if serverName == "" {
		serverName = "localhost"
	}
	homeserver := strings.TrimSpace(cfg.Homeserver)
	if homeserver == "" {
		homeserver = "https://" + serverName
	}
	if !hasPortal {
		ownerMXID := ownerMXIDForServer(serverName)
		state = portalState{
			Initialized:    false,
			Password:       defaultPortalPassword(),
			AccessToken:    randomToken("p2p_access"),
			MatrixDeviceID: matrixPortalDeviceID,
			AgentToken:     randomToken("p2p_agent"),
			OwnerMXID:      ownerMXID,
			Profile: ownerProfile{
				UserID: ownerMXID,
				Domain: serverName,
			},
		}
	}
	if strings.TrimSpace(state.Password) == "" {
		state.Password = defaultPortalPassword()
	}
	if strings.TrimSpace(state.AccessToken) == "" {
		state.AccessToken = randomToken("p2p_access")
	}
	if state.MatrixDeviceID == "" {
		state.MatrixDeviceID = matrixPortalDeviceID
	}
	if state.AgentToken == "" {
		state.AgentToken = randomToken("p2p_agent")
	}
	if state.OwnerMXID == "" {
		state.OwnerMXID = ownerMXIDForServer(serverName)
	}
	if state.Profile.UserID == "" {
		state.Profile.UserID = state.OwnerMXID
	}
	if state.Profile.Domain == "" {
		state.Profile.Domain = serverName
	}
	service := &Service{
		serverName:         serverName,
		homeserver:         homeserver,
		store:              store,
		transport:          transport,
		remoteHTTPClient:   newRemotePublicHTTPClient(cfg.RemoteNodeInsecureSkipTLSVerify),
		remoteAllowPrivate: cfg.RemoteNodeAllowPrivateBaseURLs,
		storeMode:          storeMode(store),
		initialized:        state.Initialized,
		password:           state.Password,
		accessToken:        state.AccessToken,
		matrixDeviceID:     state.MatrixDeviceID,
		agentToken:         state.AgentToken,
		ownerMXID:          state.OwnerMXID,
		agentRoomID:        state.AgentRoomID,
		profile:            state.Profile,
		agentConfig: agentConfig{
			DisplayName:   "Agent",
			ContextWindow: 30,
			Enabled:       true,
		},
		readMarkers:   map[string]readMarker{},
		channels:      map[string]channel{},
		contacts:      map[string]contactRecord{},
		groups:        map[string]groupRecord{},
		calls:         map[string]callRecord{},
		favorites:     map[int64]favoriteRecord{},
		reports:       map[string]reportRecord{},
		follows:       map[string]followRecord{},
		reactions:     map[string]reactionRecord{},
		members:       map[string]memberRecord{},
		conversations: map[string]conversationRecord{},
		inviteGrants:  map[string]channelInviteGrant{},
		eventNotify:   make(chan struct{}),
	}
	service.actions = service.actionHandlers()
	return service
}

func (s *Service) AccessToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accessToken
}

func (s *Service) AgentToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.agentToken
}

func (s *Service) Handle(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	action = strings.TrimSpace(action)
	handler, ok := s.actions[action]
	if !ok {
		return nil, badRequest("unknown action")
	}
	return handler(ctx, params)
}

func (s *Service) Authenticate(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return token != "" && token == s.accessToken
}

func (s *Service) Authorize(token, action string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == "" {
		return false
	}
	if token == s.accessToken {
		return true
	}
	return token == s.agentToken && serviceapi.AgentAction(action)
}

func (s *Service) AuthorizeEventStream(token string) bool {
	return s.authorizeEventStream(token)
}

func (s *Service) authorizeEventStream(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == "" {
		return false
	}
	if token == s.accessToken {
		return true
	}
	return token == s.agentToken
}

func (s *Service) publishCurrentAgentStatusState(ctx context.Context) error {
	s.mu.Lock()
	roomID := s.agentRoomID
	agentMXID := s.agentMXIDLocked()
	online := s.agentConfig.Enabled
	s.mu.Unlock()
	return s.publishAgentStatusState(ctx, roomID, agentMXID, agentMXID, online)
}

func (s *Service) publishAgentStatusState(ctx context.Context, roomID, senderMXID, agentMXID string, online bool) error {
	if s.transport == nil || strings.TrimSpace(roomID) == "" || strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     strings.TrimSpace(roomID),
		SenderMXID: strings.TrimSpace(senderMXID),
		Event:      agentStatusStateEvent(agentMXID, online),
	})
}

func publicAction(action string) bool {
	return serviceapi.PublicAction(action)
}
