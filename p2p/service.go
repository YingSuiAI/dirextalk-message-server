package p2p

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
)

type Config struct {
	ServerName                      string
	Homeserver                      string
	RemoteNodeInsecureSkipTLSVerify bool
	RemoteNodeAllowPrivateBaseURLs  bool
}

const (
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
	mcpMessages        mcpMessageReader
	remoteHTTPClient   *http.Client
	remoteAllowPrivate bool
	storeMode          string
	projectorStarted   bool

	initialized         bool
	password            string
	adminToken          string
	matrixDeviceID      string
	agentToken          string
	ownerMXID           string
	agentRoomID         string
	passwordInitialized bool
	profileInitialized  bool
	profile             ownerProfile
	agentConfig         agentConfig
	apiPerms            map[string]apiPermission

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

type portalState struct {
	Initialized         bool
	Password            string
	AdminToken          string
	MatrixToken         string
	MatrixDeviceID      string
	AgentToken          string
	OwnerMXID           string
	AgentRoomID         string
	PasswordInitialized bool
	ProfileInitialized  bool
	Profile             ownerProfile
}

type ownerProfile struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Domain      string `json:"domain"`
	AvatarURL   string `json:"avatar_url"`
	Gender      string `json:"gender"`
	Birthday    string `json:"birthday"`
	Phone       string `json:"phone"`
	Email       string `json:"email"`
}

type agentConfig struct {
	DisplayName    string   `json:"display_name"`
	ContextWindow  int64    `json:"context_window"`
	Enabled        bool     `json:"enabled"`
	Model          string   `json:"model"`
	SystemPrompt   string   `json:"system_prompt"`
	AllowedActions []string `json:"allowed_actions"`
}

type apiPermission struct {
	Action      string `json:"action"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
}

const matrixPortalDeviceID = "P2P_PORTAL"

type readMarker struct {
	RoomID         string `json:"room_id"`
	EventID        string `json:"event_id"`
	OriginServerTS int64  `json:"origin_server_ts"`
}

type channel struct {
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

type channelInviteGrant struct {
	GrantID     string `json:"grant_id"`
	ChannelID   string `json:"channel_id"`
	RoomID      string `json:"room_id"`
	ShareRoomID string `json:"share_room_id"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   int64  `json:"created_at"`
}

type channelPostRecord struct {
	PostID         string            `json:"post_id"`
	ChannelID      string            `json:"channel_id"`
	RoomID         string            `json:"room_id"`
	EventID        string            `json:"event_id"`
	AuthorMXID     string            `json:"author_mxid"`
	AuthorName     string            `json:"author_name"`
	Body           string            `json:"body"`
	MessageType    string            `json:"message_type"`
	MediaJSON      string            `json:"media_json"`
	OriginServerTS int64             `json:"origin_server_ts"`
	CommentCount   int64             `json:"comment_count"`
	ReactionCount  int64             `json:"reaction_count"`
	ReactedByMe    bool              `json:"reacted_by_me"`
	Operation      map[string]any    `json:"operation,omitempty"`
	Conversation   *conversationView `json:"conversation,omitempty"`
}

type channelCommentRecord struct {
	CommentID         string            `json:"comment_id"`
	PostID            string            `json:"post_id"`
	ChannelID         string            `json:"channel_id"`
	EventID           string            `json:"event_id"`
	AuthorMXID        string            `json:"author_mxid"`
	AuthorName        string            `json:"author_name"`
	Body              string            `json:"body"`
	MessageType       string            `json:"message_type"`
	MediaJSON         string            `json:"media_json"`
	ReplyToCommentID  string            `json:"reply_to_comment_id"`
	ReplyToAuthorMXID string            `json:"reply_to_author_mxid"`
	MentionsJSON      string            `json:"mentions_json"`
	OriginServerTS    int64             `json:"origin_server_ts"`
	ReactionCount     int64             `json:"reaction_count"`
	ReactedByMe       bool              `json:"reacted_by_me"`
	Operation         map[string]any    `json:"operation,omitempty"`
	Conversation      *conversationView `json:"conversation,omitempty"`
}

type contactRecord struct {
	PeerMXID     string            `json:"peer_mxid"`
	DisplayName  string            `json:"display_name"`
	AvatarURL    string            `json:"avatar_url"`
	Domain       string            `json:"domain"`
	RoomID       string            `json:"room_id"`
	Status       string            `json:"status"`
	Remark       string            `json:"remark,omitempty"`
	Operation    map[string]any    `json:"operation,omitempty"`
	Conversation *conversationView `json:"conversation,omitempty"`
}

type groupRecord struct {
	RoomID       string            `json:"room_id"`
	Name         string            `json:"name"`
	Topic        string            `json:"topic"`
	AvatarURL    string            `json:"avatar_url"`
	MemberCount  int64             `json:"member_count"`
	InvitePolicy string            `json:"invite_policy"`
	Muted        bool              `json:"muted"`
	Operation    map[string]any    `json:"operation,omitempty"`
	Conversation *conversationView `json:"conversation,omitempty"`
}

type callRecord struct {
	CallID        string `json:"call_id"`
	RoomID        string `json:"room_id"`
	RoomType      string `json:"room_type"`
	MediaType     string `json:"media_type"`
	CreatedByMXID string `json:"created_by_mxid"`
	State         string `json:"state"`
	CreatedAt     string `json:"created_at"`
	AnsweredAt    string `json:"answered_at,omitempty"`
	EndedAt       string `json:"ended_at,omitempty"`
	EndedByMXID   string `json:"ended_by_mxid,omitempty"`
	EndReason     string `json:"end_reason,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
}

type favoriteRecord struct {
	ID             int64  `json:"id"`
	EventID        string `json:"event_id"`
	RoomID         string `json:"room_id"`
	SenderID       string `json:"sender_id"`
	SenderName     string `json:"sender_name"`
	Content        string `json:"content"`
	MessageType    string `json:"message_type"`
	OriginServerTS int64  `json:"origin_server_ts"`
	CreatedAt      string `json:"created_at"`
}

type reportRecord struct {
	ID             string `json:"id"`
	ReporterDomain string `json:"reporter_domain"`
	ReportedDomain string `json:"reported_domain"`
	TargetType     int64  `json:"target_type"`
	Reason         string `json:"reason"`
	ImagesJSON     string `json:"images_json"`
	CreatedAt      string `json:"created_at"`
}

type followRecord struct {
	Domain    string `json:"domain"`
	CreatedAt string `json:"created_at"`
}

type reactionRecord struct {
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	ChannelID  string `json:"channel_id"`
	PostID     string `json:"post_id"`
	CommentID  string `json:"comment_id"`
	Reaction   string `json:"reaction"`
	UserID     string `json:"user_id"`
	Active     bool   `json:"active"`
	CreatedAt  string `json:"created_at"`
}

type memberRecord struct {
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

func (m memberRecord) MarshalJSON() ([]byte, error) {
	type memberAlias memberRecord
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

func (s *Service) SetMatrixSessionIssuer(issuer MatrixSessionIssuer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = issuer
}

func (s *Service) SetMCPMessageReader(reader mcpMessageReader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mcpMessages = reader
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
		state = portalState{
			Initialized:         true,
			Password:            defaultPortalPassword(),
			AdminToken:          randomToken("p2p_access"),
			MatrixDeviceID:      matrixPortalDeviceID,
			AgentToken:          randomToken("p2p_agent"),
			OwnerMXID:           "@owner:" + serverName,
			AgentRoomID:         "!agent:" + serverName,
			PasswordInitialized: false,
			Profile: ownerProfile{
				UserID: "@owner:" + serverName,
				Domain: serverName,
			},
		}
	}
	if !state.Initialized {
		state.Initialized = true
	}
	if strings.TrimSpace(state.Password) == "" {
		state.Password = defaultPortalPassword()
	}
	accessToken := strings.TrimSpace(state.MatrixToken)
	if accessToken == "" {
		accessToken = strings.TrimSpace(state.AdminToken)
	}
	if accessToken == "" {
		accessToken = randomToken("p2p_access")
	}
	state.AdminToken = accessToken
	state.MatrixToken = accessToken
	if state.ProfileInitialized && !state.PasswordInitialized {
		state.PasswordInitialized = true
	}
	if state.MatrixDeviceID == "" {
		state.MatrixDeviceID = matrixPortalDeviceID
	}
	if state.AgentToken == "" {
		state.AgentToken = randomToken("p2p_agent")
	}
	if state.OwnerMXID == "" {
		state.OwnerMXID = "@owner:" + serverName
	}
	if state.AgentRoomID == "" {
		state.AgentRoomID = "!agent:" + serverName
	}
	if state.Profile.UserID == "" {
		state.Profile.UserID = state.OwnerMXID
	}
	if state.Profile.Domain == "" {
		state.Profile.Domain = serverName
	}
	return &Service{
		serverName:          serverName,
		homeserver:          homeserver,
		store:               store,
		transport:           transport,
		remoteHTTPClient:    newRemotePublicHTTPClient(cfg.RemoteNodeInsecureSkipTLSVerify),
		remoteAllowPrivate:  cfg.RemoteNodeAllowPrivateBaseURLs,
		storeMode:           storeMode(store),
		initialized:         state.Initialized,
		password:            state.Password,
		adminToken:          state.AdminToken,
		matrixDeviceID:      state.MatrixDeviceID,
		agentToken:          state.AgentToken,
		ownerMXID:           state.OwnerMXID,
		agentRoomID:         state.AgentRoomID,
		passwordInitialized: state.PasswordInitialized,
		profileInitialized:  state.ProfileInitialized,
		profile:             state.Profile,
		agentConfig: agentConfig{
			DisplayName:   "Agent",
			ContextWindow: 30,
			Enabled:       true,
		},
		apiPerms:      defaultAPIPermissions(),
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
}

func (s *Service) AdminToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adminToken
}

func (s *Service) AgentToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.agentToken
}

//nolint:gocyclo // Product commands stay in one router until the command layer is split into typed handlers.
func (s *Service) Handle(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	action = strings.TrimSpace(action)
	switch action {
	case "portal.bootstrap":
		return s.bootstrap(ctx, params)
	case "portal.auth":
		return s.auth(ctx, params)
	case "portal.status":
		return s.portalStatus(), nil
	case "portal.password":
		return s.changePortalPassword(ctx, params)
	case "agent.password":
		return s.agentPassword(), nil
	case "agent.matrix_session.create":
		return s.agentMatrixSession(ctx, params)
	case "profile.get":
		return s.getProfile(), nil
	case "profile.update":
		return s.updateProfile(ctx, params)
	case "sync.bootstrap":
		return s.syncBootstrap(ctx)
	case "conversations.list":
		return s.conversationList(ctx)
	case "conversations.get":
		return s.conversationGet(ctx, params)
	case "mcp.rooms.search":
		return s.mcpRoomsSearch(ctx, params)
	case "mcp.messages.send":
		return s.mcpMessagesSend(ctx, params)
	case "mcp.messages.list":
		return s.mcpMessagesList(ctx, params)
	case "mcp.channel_posts.list":
		return s.mcpChannelPostsList(ctx, params)
	case "mcp.channel_comments.list":
		return s.mcpChannelCommentsList(ctx, params)
	case "mcp.channel_comments.create":
		return s.mcpChannelCommentCreate(ctx, params)
	case "sync.read_marker":
		return s.updateReadMarker(ctx, params)
	case "apis.list":
		return s.apiPermissionList(), nil
	case "apis.status":
		return s.apiPermissionStatus(params)
	case "agent.config.get":
		return s.getAgentConfig(), nil
	case "agent.config.update":
		return s.updateAgentConfig(params), nil
	case "agent.status":
		return s.agentStatus(), nil
	case "follows.list":
		return s.followList(ctx), nil
	case "follows.add":
		return s.followAdd(ctx, params)
	case "follows.remove":
		return s.followRemove(ctx, params)
	case "favorites.list":
		return s.favoriteList(ctx, params), nil
	case "favorites.add":
		return s.favoriteMessage(ctx, params)
	case "favorites.delete":
		return s.favoriteDelete(ctx, params)
	case "favorites.delete_batch":
		return s.favoriteDeleteBatch(ctx, params)
	case "reports.submit":
		return s.reportSubmit(ctx, params)
	case "contacts.list":
		return s.contactList(ctx)
	case "contacts.request":
		return s.contactRequest(ctx, params)
	case "contacts.reactivate":
		return s.contactReactivate(ctx, params)
	case "contacts.requests.accept", "contacts.requests.reject", "contacts.requests.delete", "contacts.delete":
		return s.contactMutation(ctx, action, params)
	case "contacts.update":
		return s.contactUpdate(ctx, params)
	case "calls.create", "calls.incoming":
		return s.callSession(ctx, params)
	case "calls.get":
		return s.callGet(ctx, params)
	case "calls.event":
		return s.callEvent(ctx, params)
	case "calls.active", "calls.list":
		return s.callList(ctx, params, action == "calls.active"), nil
	case "groups.create":
		return s.groupResult(ctx, params)
	case "groups.update":
		return s.groupUpdate(ctx, params)
	case "groups.invite":
		return s.inviteMembers(ctx, "group", params)
	case "groups.join":
		return s.joinMember(ctx, "group", params)
	case "groups.list":
		return s.groupList(ctx), nil
	case "groups.members":
		return s.memberList(ctx, params), nil
	case "groups.dissolve":
		return s.dissolveGroup(ctx, params)
	case "groups.leave", "groups.invite.reject", "groups.member.remove", "groups.member.mute", "groups.member.unmute":
		return s.memberMutation(ctx, "group", action, params)
	case "groups.mute", "groups.unmute", "groups.invite_policy.update":
		return s.groupPolicyMutation(ctx, action, params)
	case "channels.create":
		return s.channelResult(ctx, params)
	case "channels.update":
		return s.channelUpdate(ctx, params)
	case "channels.join":
		return s.joinMember(ctx, "channel", params)
	case "channels.invite_grant.create":
		return s.channelInviteGrantCreate(ctx, params)
	case "channels.invite":
		return s.inviteMembers(ctx, "channel", params)
	case "channels.dissolve":
		return s.dissolveChannel(ctx, params)
	case "channels.leave", "channels.member.remove", "channels.member.mute", "channels.member.unmute", "channels.join_request.approve", "channels.join_request.reject":
		return s.memberMutation(ctx, "channel", action, params)
	case "channels.mute", "channels.unmute":
		return s.channelPolicyMutation(ctx, action, params)
	case "channels.read_marker":
		return s.updateReadMarker(ctx, params)
	case "channels.list":
		return s.channelList(ctx), nil
	case "channels.members":
		return s.memberList(ctx, params), nil
	case "channels.public.search":
		return s.channelPublicSearch(ctx, params)
	case "channels.public.get":
		return s.channelPublicGet(ctx, params)
	case "channels.public.join_request":
		return s.channelJoinRequest(ctx, params)
	case "channels.public.join_result":
		return s.channelJoinResult(ctx, params)
	case "users.public_channels":
		return s.userPublicChannels(ctx, params)
	case "channels.posts.list":
		return s.channelPosts(ctx, params), nil
	case "channels.posts.create":
		return s.channelPost(ctx, params)
	case "channels.posts.recall", "channels.comments.recall":
		return s.recallChannelContent(ctx, action, params)
	case "channels.comments.list":
		return s.channelComments(ctx, params), nil
	case "channels.comments.create":
		return s.channelComment(ctx, params)
	case "channels.post_reaction.toggle", "channels.comment_reaction.toggle":
		return s.channelReaction(ctx, action, params)
	case "channels.my_comments":
		return s.myChannelComments(ctx, params), nil
	case "channels.my_reactions":
		return s.myReactions(ctx), nil
	default:
		return nil, badRequest("unknown action")
	}
}

func (s *Service) Authenticate(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return token != "" && (token == s.adminToken || token == s.agentToken)
}

func (s *Service) Authorize(token, action string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == "" {
		return false
	}
	if token == s.adminToken {
		return true
	}
	if token != s.agentToken {
		return false
	}
	perm, ok := s.apiPerms[action]
	return ok && perm.Enabled
}

func publicAction(action string) bool {
	switch action {
	case "portal.bootstrap", "portal.auth", "portal.status", "contacts.reactivate", "channels.public.search", "channels.public.get", "channels.public.join_request", "channels.public.join_result", "users.public_channels":
		return true
	default:
		return false
	}
}

func (s *Service) bootstrap(ctx context.Context, params map[string]any) (any, *apiError) {
	password := trimString(params["password"])
	if password == "" {
		password = trimString(params["token"])
	}
	if password == "" {
		return nil, badRequest("password is required")
	}
	s.mu.Lock()
	if s.initialized {
		if password != s.password {
			s.mu.Unlock()
			return nil, statusError(409, "portal already initialized")
		}
		session := s.sessionLocked()
		state := s.portalStateLocked()
		s.mu.Unlock()
		if s.store != nil {
			if err := s.store.SavePortal(ctx, state); err != nil {
				return nil, internalError(err)
			}
		}
		if err := s.writePortalCredentialsFile(); err != nil {
			return nil, internalError(err)
		}
		return s.refreshMatrixSession(ctx, session, params)
	}
	s.password = password
	s.initialized = true
	session := s.sessionLocked()
	state := s.portalStateLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return s.refreshMatrixSession(ctx, session, params)
}

func (s *Service) auth(ctx context.Context, params map[string]any) (any, *apiError) {
	password := trimString(params["password"])
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return nil, statusError(401, "portal is not initialized")
	}
	if password == "" || password != s.password {
		s.mu.Unlock()
		return nil, statusError(401, "password invalid")
	}
	session := s.sessionLocked()
	s.mu.Unlock()
	return s.refreshMatrixSession(ctx, session, params)
}

func (s *Service) changePortalPassword(ctx context.Context, params map[string]any) (any, *apiError) {
	oldPassword := trimString(params["old_password"])
	newPassword := trimString(params["new_password"])
	if newPassword == "" {
		return nil, badRequest("new_password is required")
	}
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return nil, statusError(401, "portal is not initialized")
	}
	if oldPassword == "" || oldPassword != s.password {
		s.mu.Unlock()
		return nil, statusError(401, "password invalid")
	}
	s.password = newPassword
	s.adminToken = randomToken("p2p_access")
	s.passwordInitialized = true
	s.profileInitialized = s.portalProfileInitializedLocked()
	session := s.sessionLocked()
	state := s.portalStateLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return s.refreshMatrixSession(ctx, session, params)
}

func (s *Service) agentPassword() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"password": s.password}
}

func (s *Service) agentMatrixSession(ctx context.Context, params map[string]any) (any, *apiError) {
	session, apiErr := s.refreshMatrixSession(ctx, map[string]any{}, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"access_token": session["access_token"],
		"device_id":    session["device_id"],
		"user_id":      session["user_id"],
		"homeserver":   session["homeserver"],
	}, nil
}

func (s *Service) refreshMatrixSession(ctx context.Context, session map[string]any, params map[string]any) (map[string]any, *apiError) {
	s.matrixSessionMu.Lock()
	defer s.matrixSessionMu.Unlock()

	requestedDeviceID := requestedMatrixDeviceID(params)
	s.mu.Lock()
	issuer := s.sessions
	userID := s.profile.UserID
	displayName := s.profile.DisplayName
	avatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	if issuer == nil {
		session["device_id"] = requestedDeviceID
		return session, nil
	}
	token, err := issuer.EnsureMatrixSession(ctx, userID, displayName, avatarURL, requestedDeviceID)
	if err != nil {
		return nil, internalError(err)
	}
	s.mu.Lock()
	s.adminToken = token
	s.matrixDeviceID = requestedDeviceID
	state := s.portalStateLocked()
	session = s.sessionLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.writePortalCredentialsFile(); err != nil {
		return nil, internalError(err)
	}
	return session, nil
}

func (s *Service) portalStatus() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	policyIndexMode := "unavailable"
	policyIndexReady := false
	if s.transport != nil {
		policyIndexMode = "matrix_state"
		policyIndexReady = true
	}
	return map[string]any{
		"initialized":        s.initialized,
		"user_id":            s.ownerMXID,
		"homeserver":         s.homeserver,
		"store_mode":         s.storeMode,
		"projector_started":  s.projectorStarted,
		"policy_index_mode":  policyIndexMode,
		"policy_index_ready": policyIndexReady,
		"event_stream_ready": true,
	}
}

func (s *Service) getAgentConfig() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return agentConfigToMap(s.agentConfig)
}

func (s *Service) updateAgentConfig(params map[string]any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if displayName := trimString(params["display_name"]); displayName != "" {
		s.agentConfig.DisplayName = displayName
	}
	if contextWindow := int64Param(params["context_window"]); contextWindow > 0 {
		s.agentConfig.ContextWindow = contextWindow
	}
	if _, ok := params["enabled"]; ok {
		s.agentConfig.Enabled = boolParam(params["enabled"])
	}
	if model := trimString(params["model"]); model != "" {
		s.agentConfig.Model = model
	}
	if systemPrompt := trimString(params["system_prompt"]); systemPrompt != "" {
		s.agentConfig.SystemPrompt = systemPrompt
	}
	if actions := stringSliceParam(params["allowed_actions"]); actions != nil {
		s.agentConfig.AllowedActions = actions
	}
	return agentConfigToMap(s.agentConfig)
}

func (s *Service) agentStatus() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"online":        s.agentConfig.Enabled,
		"connected":     s.agentConfig.Enabled,
		"configured":    s.initialized,
		"display_name":  s.agentConfig.DisplayName,
		"agent_room_id": s.agentRoomID,
	}
}

func agentConfigToMap(cfg agentConfig) map[string]any {
	return map[string]any{
		"display_name":    cfg.DisplayName,
		"context_window":  cfg.ContextWindow,
		"enabled":         cfg.Enabled,
		"model":           cfg.Model,
		"system_prompt":   cfg.SystemPrompt,
		"allowed_actions": append([]string{}, cfg.AllowedActions...),
	}
}

func (s *Service) apiPermissionList() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"items": apiPermissionItemsLocked(s.apiPerms)}
}

func (s *Service) apiPermissionStatus(params map[string]any) (any, *apiError) {
	rawItems, _ := params["items"].([]any)
	if len(rawItems) == 0 {
		return nil, badRequest("items cannot be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range rawItems {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, badRequest("invalid permission item")
		}
		action := trimString(item["action"])
		if action == "" {
			return nil, badRequest("action is required")
		}
		perm, ok := s.apiPerms[action]
		if !ok {
			return nil, badRequest("route is not Agent-permission controlled")
		}
		perm.Enabled = boolParam(item["enabled"])
		s.apiPerms[action] = perm
	}
	return map[string]any{"items": apiPermissionItemsLocked(s.apiPerms)}, nil
}

func apiPermissionItemsLocked(perms map[string]apiPermission) []apiPermission {
	items := make([]apiPermission, 0, len(perms))
	for _, item := range perms {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Action < items[j].Action
	})
	return items
}

func defaultAPIPermissions() map[string]apiPermission {
	items := []apiPermission{
		{Action: "agent.password", Description: "Agent reads current portal password", Enabled: true},
		{Action: "agent.matrix_session.create", Description: "Create an internal Matrix session for Agent tooling", Enabled: true},
		{Action: "agent.config.get", Description: "Read Agent config", Enabled: true},
		{Action: "agent.config.update", Description: "Update Agent config", Enabled: true},
		{Action: "agent.status", Description: "Read Agent status", Enabled: true},
		{Action: "apis.list", Description: "List Agent-controllable API permissions", Enabled: true},
		{Action: "profile.get", Description: "Read owner profile", Enabled: true},
		{Action: "profile.update", Description: "Update owner profile", Enabled: true},
		{Action: "sync.bootstrap", Description: "Read first-screen metadata", Enabled: true},
		{Action: "conversations.list", Description: "List ProductCore conversations", Enabled: true},
		{Action: "conversations.get", Description: "Read ProductCore conversation", Enabled: true},
		{Action: "mcp.rooms.search", Description: "Search MCP room summaries", Enabled: true},
		{Action: "mcp.messages.send", Description: "Send MCP plain text message", Enabled: true},
		{Action: "mcp.messages.list", Description: "List MCP ordinary message summaries", Enabled: true},
		{Action: "mcp.channel_posts.list", Description: "List MCP channel post summaries", Enabled: true},
		{Action: "mcp.channel_comments.list", Description: "List MCP channel comment summaries", Enabled: true},
		{Action: "mcp.channel_comments.create", Description: "Create MCP channel post comment", Enabled: true},
		{Action: "events.stream", Description: "Stream projected P2P events with SSE", Enabled: true},
		{Action: "sync.read_marker", Description: "Update read marker", Enabled: true},
		{Action: "contacts.list", Description: "List contacts", Enabled: true},
		{Action: "contacts.request", Description: "Create contact request", Enabled: true},
		{Action: "contacts.reactivate", Description: "Reinvite a retained peer to an existing direct room", Enabled: true},
		{Action: "contacts.requests.accept", Description: "Accept contact request", Enabled: true},
		{Action: "contacts.requests.reject", Description: "Reject contact request", Enabled: true},
		{Action: "contacts.requests.delete", Description: "Delete contact request", Enabled: true},
		{Action: "contacts.update", Description: "Update contact remark", Enabled: true},
		{Action: "contacts.delete", Description: "Delete contact", Enabled: true},
		{Action: "favorites.list", Description: "List favorite messages", Enabled: true},
		{Action: "favorites.add", Description: "Add favorite message", Enabled: true},
		{Action: "favorites.delete", Description: "Delete favorite message", Enabled: true},
		{Action: "favorites.delete_batch", Description: "Batch delete favorites", Enabled: true},
		{Action: "reports.submit", Description: "Submit user or channel report", Enabled: true},
		{Action: "calls.get", Description: "Read call session detail", Enabled: true},
		{Action: "calls.incoming", Description: "Register incoming call session", Enabled: true},
		{Action: "calls.event", Description: "Update call session state", Enabled: true},
		{Action: "channels.create", Description: "Create channel", Enabled: true},
		{Action: "channels.list", Description: "List channels", Enabled: true},
		{Action: "channels.join", Description: "Join channel by room id", Enabled: true},
		{Action: "channels.update", Description: "Update channel", Enabled: true},
		{Action: "channels.invite", Description: "Invite channel members", Enabled: true},
		{Action: "channels.invite_grant.create", Description: "Create a room-scoped channel invite grant", Enabled: true},
		{Action: "channels.leave", Description: "Leave channel", Enabled: true},
		{Action: "channels.dissolve", Description: "Dissolve owned channel", Enabled: true},
		{Action: "channels.members", Description: "List channel members", Enabled: true},
		{Action: "channels.member.remove", Description: "Remove channel member", Enabled: true},
		{Action: "channels.member.mute", Description: "Mute channel member", Enabled: true},
		{Action: "channels.member.unmute", Description: "Unmute channel member", Enabled: true},
		{Action: "channels.mute", Description: "Mute channel", Enabled: true},
		{Action: "channels.unmute", Description: "Unmute channel", Enabled: true},
		{Action: "channels.posts.list", Description: "List channel posts", Enabled: true},
		{Action: "channels.posts.create", Description: "Create channel post", Enabled: true},
		{Action: "channels.posts.recall", Description: "Recall channel post", Enabled: true},
		{Action: "channels.comments.list", Description: "List channel post comments", Enabled: true},
		{Action: "channels.comments.create", Description: "Create channel post comment", Enabled: true},
		{Action: "channels.comments.recall", Description: "Recall channel comment", Enabled: true},
		{Action: "channels.post_reaction.toggle", Description: "Toggle channel post reaction", Enabled: true},
		{Action: "channels.comment_reaction.toggle", Description: "Toggle channel comment reaction", Enabled: true},
		{Action: "channels.my_comments", Description: "List owner channel comments", Enabled: true},
		{Action: "channels.my_reactions", Description: "List owner channel reactions", Enabled: true},
		{Action: "channels.read_marker", Description: "Update channel read marker", Enabled: true},
		{Action: "channels.join_request.approve", Description: "Approve channel join request", Enabled: true},
		{Action: "channels.join_request.reject", Description: "Reject channel join request", Enabled: true},
		{Action: "channels.public.get", Description: "Read public channel detail", Enabled: true},
		{Action: "channels.public.join_request", Description: "Create public channel join request", Enabled: true},
		{Action: "users.public_channels", Description: "List public channels owned by a user", Enabled: true},
		{Action: "groups.create", Description: "Create group", Enabled: true},
		{Action: "groups.list", Description: "List groups", Enabled: true},
		{Action: "groups.update", Description: "Update group profile", Enabled: true},
		{Action: "groups.invite", Description: "Invite group members", Enabled: true},
		{Action: "groups.invite.reject", Description: "Reject current user's group invite", Enabled: true},
		{Action: "groups.join", Description: "Join group", Enabled: true},
		{Action: "groups.members", Description: "List group members", Enabled: true},
		{Action: "groups.leave", Description: "Leave group", Enabled: true},
		{Action: "groups.dissolve", Description: "Dissolve owned group", Enabled: true},
		{Action: "groups.mute", Description: "Mute group", Enabled: true},
		{Action: "groups.unmute", Description: "Unmute group", Enabled: true},
		{Action: "groups.invite_policy.update", Description: "Update group invite policy", Enabled: true},
		{Action: "groups.member.remove", Description: "Remove group member", Enabled: true},
		{Action: "groups.member.mute", Description: "Mute group member", Enabled: true},
		{Action: "groups.member.unmute", Description: "Unmute group member", Enabled: true},
		{Action: "calls.create", Description: "Create call session", Enabled: true},
		{Action: "calls.list", Description: "List call sessions", Enabled: true},
		{Action: "calls.active", Description: "List active calls", Enabled: true},
		{Action: "follows.list", Description: "List followed domains", Enabled: true},
		{Action: "follows.add", Description: "Add followed domain", Enabled: true},
		{Action: "follows.remove", Description: "Remove followed domain", Enabled: true},
	}
	perms := make(map[string]apiPermission, len(items))
	for _, item := range items {
		perms[item.Action] = item
	}
	return perms
}

func (s *Service) getProfile() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profile
}

func (s *Service) portalOwnerWellKnown() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"matrix_user_id": s.profile.UserID,
		"mxid":           s.profile.UserID,
		"user_id":        s.profile.UserID,
		"display_name":   s.profile.DisplayName,
		"avatar_url":     s.profile.AvatarURL,
	}
}

func (s *Service) updateProfile(ctx context.Context, params map[string]any) (any, *apiError) {
	s.mu.Lock()
	if v := trimString(params["display_name"]); v != "" {
		s.profile.DisplayName = v
	}
	s.profile.AvatarURL = trimString(params["avatar_url"])
	s.profile.Gender = trimString(params["gender"])
	s.profile.Birthday = trimString(params["birthday"])
	s.profile.Phone = trimString(params["phone"])
	s.profile.Email = trimString(params["email"])
	s.profileInitialized = s.portalProfileInitializedLocked()
	profile := s.profile
	state := s.portalStateLocked()
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.updateMatrixProfile(ctx, profile); err != nil {
		return nil, internalError(err)
	}
	if err := s.updateOwnerMemberProfiles(ctx, profile); err != nil {
		return nil, internalError(err)
	}
	return profile, nil
}

func (s *Service) updateMatrixProfile(ctx context.Context, profile ownerProfile) error {
	s.mu.Lock()
	issuer := s.sessions
	s.mu.Unlock()
	updater, ok := issuer.(MatrixProfileUpdater)
	if !ok || updater == nil {
		return nil
	}
	return updater.UpdateMatrixProfile(ctx, profile.UserID, profile.DisplayName, profile.AvatarURL)
}

func (s *Service) updateOwnerMemberProfiles(ctx context.Context, profile ownerProfile) error {
	members, err := s.membersForUser(ctx, profile.UserID)
	if err != nil {
		return err
	}
	for _, member := range members {
		member.DisplayName = profile.DisplayName
		member.AvatarURL = profile.AvatarURL
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
		if s.transport != nil {
			if err := s.transport.UpdateMemberProfile(ctx, UpdateMemberProfileRequest{
				RoomID:      member.RoomID,
				UserMXID:    profile.UserID,
				DisplayName: profile.DisplayName,
				AvatarURL:   profile.AvatarURL,
				Timestamp:   time.Now().UTC(),
			}); err != nil {
				continue
			}
		}
	}
	return nil
}

func (s *Service) membersForUser(ctx context.Context, userID string) ([]memberRecord, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, nil
	}
	if s.store != nil {
		members, err := s.store.ListMembers(ctx, "", "")
		if err != nil {
			return nil, err
		}
		filtered := make([]memberRecord, 0, len(members))
		for _, member := range members {
			if member.UserID == userID {
				filtered = append(filtered, member)
			}
		}
		return filtered, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.UserID == userID && !memberHidden(member.Membership) {
			filtered = append(filtered, member)
		}
	}
	return filtered, nil
}

func (s *Service) syncBootstrap(ctx context.Context) (any, *apiError) {
	contacts, err := s.listContacts(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	groups, err := s.listGroups(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	channels, err := s.listChannels(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	visibleGroups, err := s.joinedGroupsForOwner(ctx, groups)
	if err != nil {
		return nil, internalError(err)
	}
	visibleChannels, err := s.joinedChannelsForOwner(ctx, channels)
	if err != nil {
		return nil, internalError(err)
	}
	s.mu.Lock()
	userID := s.ownerMXID
	agentRoomID := s.agentRoomID
	s.mu.Unlock()
	members, err := s.membersForUser(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	return map[string]any{
		"synced_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"user":          map[string]any{"user_id": userID},
		"agent_room_id": agentRoomID,
		"contacts":      contacts,
		"groups":        visibleGroups,
		"channels":      visibleChannels,
		"pending": map[string]any{
			"friend_requests": pendingFriendRequestsFromContacts(contacts),
			"group_invites":   pendingGroupInvitesFromMembers(members, groups),
			"channel_notices": pendingChannelInvitesFromMembers(members, channels),
		},
	}, nil
}

func pendingFriendRequestsFromContacts(contacts []contactRecord) []map[string]any {
	pending := make([]map[string]any, 0)
	for _, contact := range contacts {
		if !strings.EqualFold(strings.TrimSpace(contact.Status), "pending_inbound") {
			continue
		}
		id := fallbackString(contact.RoomID, contact.PeerMXID)
		title := fallbackString(contact.DisplayName, contact.PeerMXID)
		pending = append(pending, map[string]any{
			"id":     id,
			"title":  title,
			"remark": contact.Remark,
		})
	}
	return pending
}

func pendingGroupInvitesFromMembers(members []memberRecord, groups []groupRecord) []map[string]any {
	groupByRoom := make(map[string]groupRecord, len(groups))
	for _, group := range groups {
		if strings.TrimSpace(group.RoomID) != "" {
			groupByRoom[group.RoomID] = group
		}
	}
	pending := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "invite") || member.ChannelID != "" {
			continue
		}
		roomID := strings.TrimSpace(member.RoomID)
		if roomID == "" || seen[roomID] {
			continue
		}
		seen[roomID] = true
		group := groupByRoom[roomID]
		title := fallbackString(group.Name, roomID)
		pending = append(pending, pendingItem(roomID, title, member.JoinedAt))
	}
	return pending
}

func pendingChannelInvitesFromMembers(members []memberRecord, channels []channel) []map[string]any {
	channelByID := make(map[string]channel, len(channels))
	channelByRoom := make(map[string]channel, len(channels))
	for _, ch := range channels {
		if strings.TrimSpace(ch.ChannelID) != "" {
			channelByID[ch.ChannelID] = ch
		}
		if strings.TrimSpace(ch.RoomID) != "" {
			channelByRoom[ch.RoomID] = ch
		}
	}
	pending := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "invite") || member.ChannelID == "" {
			continue
		}
		ch, ok := channelByID[member.ChannelID]
		if !ok {
			ch = channelByRoom[member.RoomID]
		}
		id := fallbackString(ch.RoomID, member.RoomID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		title := fallbackString(ch.Name, fallbackString(member.ChannelID, id))
		pending = append(pending, pendingItem(id, title, member.JoinedAt))
	}
	return pending
}

func pendingItem(id, title string, ts int64) map[string]any {
	item := map[string]any{
		"id":    id,
		"title": title,
	}
	if ts > 0 {
		item["created_at"] = time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
	}
	return item
}

func (s *Service) updateReadMarker(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	eventID := trimString(params["event_id"])
	if roomID == "" || eventID == "" {
		return nil, badRequest("room_id and event_id are required")
	}
	marker := readMarker{
		RoomID:         roomID,
		EventID:        eventID,
		OriginServerTS: int64Param(params["origin_server_ts"]),
	}
	s.mu.Lock()
	s.readMarkers[roomID] = marker
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.SaveReadMarker(ctx, marker); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}

func (s *Service) contactRequest(ctx context.Context, params map[string]any) (any, *apiError) {
	mxid := trimString(params["mxid"])
	if mxid == "" {
		return nil, badRequest("mxid is required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if mxid == ownerMXID {
		return nil, badRequest("mxid must be a remote peer")
	}
	domain := trimString(params["domain"])
	if domain == "" && strings.Contains(mxid, ":") {
		domain = mxid[strings.Index(mxid, ":")+1:]
	}
	if existing, ok, err := s.lookupContactByPeer(ctx, mxid); err != nil {
		return nil, internalError(err)
	} else if ok && contactDeleted(existing.Status) {
		return s.restoreDeletedContact(ctx, existing, params, domain)
	} else if ok && contactPendingInbound(existing.Status) {
		return s.acceptPendingInboundContact(ctx, existing, params)
	} else if ok && strings.EqualFold(strings.TrimSpace(existing.Status), "pending_outbound") {
		return s.resendPendingOutboundContactRequest(ctx, existing, params, domain)
	} else if ok && contactAccepted(existing.Status) && remoteNodeBaseURLParam(params) != "" && domainFromMXID(existing.PeerMXID) != s.serverName {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		s.mu.Unlock()
		if apiErr := s.requestPeerContactReactivation(ctx, existing, params, ownerMXID); apiErr != nil {
			if contactReactivationNotRetained(apiErr) {
				return s.requestPeerApprovalInExistingDirectRoom(ctx, existing, params, fallbackString(domain, existing.Domain))
			}
			return nil, apiErr
		}
		if err := s.attachContactConversationOperation(ctx, &existing, "contacts.request", existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	} else if ok {
		if err := s.attachContactConversationOperation(ctx, &existing, "contacts.request", existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	}
	return s.createDirectContactRequest(ctx, mxid, params, domain)
}

func (s *Service) createDirectContactRequest(ctx context.Context, mxid string, params map[string]any, domain string) (contactRecord, *apiError) {
	roomID := "!dm-" + randomToken("room") + ":" + s.serverName
	remark := contactRequestRemark(params)
	if s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(trimString(params["display_name"]), mxid)
		res, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
			CreatorMXID:        ownerMXID,
			CreatorDisplayName: ownerDisplayName,
			CreatorAvatarURL:   ownerAvatarURL,
			Name:               directName,
			Visibility:         "private",
			RoomType:           DirexioRoomTypeDirect,
			IsDirect:           true,
			InviteMXIDs:        []string{mxid},
			InitialState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, mxid, ownerDisplayName, ownerAvatarURL, remark, false),
			},
		})
		if err != nil {
			return contactRecord{}, transportWriteError(err)
		}
		roomID = res.RoomID
	}
	contact := contactRecord{
		PeerMXID:    mxid,
		DisplayName: trimString(params["display_name"]),
		AvatarURL:   trimString(params["avatar_url"]),
		Domain:      domain,
		RoomID:      roomID,
		Status:      "pending_outbound",
		Remark:      remark,
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
	}
	return contact, nil
}

func (s *Service) resendPendingOutboundContactRequest(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (contactRecord, *apiError) {
	if remark := contactRequestRemark(params); remark != "" {
		contact.Remark = remark
	}
	if displayName := trimString(params["display_name"]); displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		contact.Domain = domain
	} else if contact.Domain == "" {
		contact.Domain = fallbackDomain
	}
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(contact.DisplayName, contact.PeerMXID)
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      contact.RoomID,
			InviterMXID: ownerMXID,
			InviteeMXID: contact.PeerMXID,
			IsDirect:    true,
			InviteRoomState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark, false),
			},
		}); err != nil {
			return contactRecord{}, transportWriteError(err)
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
	}
	return contact, nil
}

func (s *Service) acceptPendingInboundContact(ctx context.Context, contact contactRecord, params map[string]any) (any, *apiError) {
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		roomID, apiErr := s.joinContactDirectRoom(ctx, contact, params, ownerMXID, ownerDisplayName, ownerAvatarURL)
		if apiErr != nil {
			if contactReactivationNotRetained(apiErr) {
				return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(trimString(params["domain"]), contact.Domain))
			}
			return nil, apiErr
		}
		contact.RoomID = roomID
	}
	if displayName := trimString(params["display_name"]); contact.DisplayName == "" && displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); contact.AvatarURL == "" && avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); contact.Domain == "" && domain != "" {
		contact.Domain = domain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) restoreDeletedContact(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (any, *apiError) {
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		if remoteNodeBaseURLParam(params) != "" && domainFromMXID(contact.PeerMXID) != s.serverName {
			if apiErr := s.requestPeerContactReactivation(ctx, contact, params, ownerMXID); apiErr != nil {
				if contactReactivationNotRetained(apiErr) {
					return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
				}
				return nil, apiErr
			}
			join, err := s.joinReactivatedDirectRoom(ctx, contact.RoomID, ownerMXID, ownerDisplayName, ownerAvatarURL, stringSliceParam(params["server_names"]))
			if err != nil {
				return nil, transportWriteError(err)
			}
			if strings.TrimSpace(join.RoomID) != "" {
				contact.RoomID = join.RoomID
			}
		} else {
			roomID, apiErr := s.joinContactDirectRoom(ctx, contact, params, ownerMXID, ownerDisplayName, ownerAvatarURL)
			if apiErr != nil {
				if contactReactivationNotRetained(apiErr) {
					return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
				}
				return nil, apiErr
			}
			contact.RoomID = roomID
		}
	}
	if displayName := trimString(params["display_name"]); displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		contact.Domain = domain
	} else if contact.Domain == "" {
		contact.Domain = fallbackDomain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) requestPeerApprovalInExistingDirectRoom(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (contactRecord, *apiError) {
	if strings.TrimSpace(contact.RoomID) == "" {
		return s.createDirectContactRequest(ctx, contact.PeerMXID, params, fallbackDomain)
	}
	if remark := contactRequestRemark(params); remark != "" {
		contact.Remark = remark
	}
	if displayName := trimString(params["display_name"]); displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		contact.Domain = domain
	} else if contact.Domain == "" {
		contact.Domain = fallbackDomain
	}
	contact.Status = "pending_outbound"
	if s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(contact.DisplayName, contact.PeerMXID)
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      contact.RoomID,
			InviterMXID: ownerMXID,
			InviteeMXID: contact.PeerMXID,
			IsDirect:    true,
			InviteRoomState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark, false),
			},
		}); err != nil {
			return contactRecord{}, transportWriteError(err)
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
	}
	return contact, nil
}

func (s *Service) joinContactDirectRoom(ctx context.Context, contact contactRecord, params map[string]any, ownerMXID, ownerDisplayName, ownerAvatarURL string) (string, *apiError) {
	serverNames := stringSliceParam(params["server_names"])
	join, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: contact.RoomID,
		UserMXID:      ownerMXID,
		DisplayName:   ownerDisplayName,
		AvatarURL:     ownerAvatarURL,
		ServerNames:   serverNames,
	})
	if err != nil {
		if !isDirectRoomJoinRequiresInvite(err) {
			return "", transportWriteError(err)
		}
		if apiErr := s.requestPeerContactReactivation(ctx, contact, params, ownerMXID); apiErr != nil {
			return "", apiErr
		}
		join, err = s.joinReactivatedDirectRoom(ctx, contact.RoomID, ownerMXID, ownerDisplayName, ownerAvatarURL, serverNames)
		if err != nil {
			return "", transportWriteError(err)
		}
	}
	if strings.TrimSpace(join.RoomID) != "" {
		return join.RoomID, nil
	}
	return contact.RoomID, nil
}

func (s *Service) joinReactivatedDirectRoom(ctx context.Context, roomID, userMXID, displayName, avatarURL string, serverNames []string) (JoinRoomResult, error) {
	const maxAttempts = 6
	for attempt := 0; attempt < maxAttempts; attempt++ {
		join, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
			RoomIDOrAlias: roomID,
			UserMXID:      userMXID,
			DisplayName:   displayName,
			AvatarURL:     avatarURL,
			ServerNames:   serverNames,
		})
		if err == nil || !isDirectRoomJoinRequiresInvite(err) {
			return join, err
		}
		if attempt == maxAttempts-1 {
			return JoinRoomResult{}, err
		}
		select {
		case <-ctx.Done():
			return JoinRoomResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 150 * time.Millisecond):
		}
	}
	return JoinRoomResult{}, productpolicy.Forbidden("direct room join requires invite")
}

func (s *Service) requestPeerContactReactivation(ctx context.Context, contact contactRecord, params map[string]any, requesterMXID string) *apiError {
	peerServer := domainFromMXID(contact.PeerMXID)
	if peerServer == "" || peerServer == s.serverName {
		return statusError(http.StatusForbidden, "peer node is required to reactivate direct room")
	}
	remoteBase := remoteNodeBaseURLParam(params)
	if remoteBase == "" {
		remoteBase = "https://" + peerServer + "/_p2p"
	}
	var result map[string]any
	status, err := s.remotePublicAction(ctx, peerServer, "contacts.reactivate", map[string]any{
		"room_id":              contact.RoomID,
		"requester_mxid":       requesterMXID,
		"remote_node_base_url": remoteBase,
	}, &result)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return statusError(status, err.Error())
		}
		return statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		if status == http.StatusNotFound {
			return statusError(status, "retained contact not found")
		}
		return statusError(status, "target node contact reactivation failed")
	}
	return nil
}

func (s *Service) contactReactivate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	requesterMXID := trimString(params["requester_mxid"])
	if roomID == "" || requesterMXID == "" {
		return nil, badRequest("room_id and requester_mxid are required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if requesterMXID == ownerMXID {
		return nil, badRequest("requester_mxid must be a remote peer")
	}
	contact, ok, err := s.lookupContactByPeer(ctx, requesterMXID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok || contact.RoomID != roomID || !contactAccepted(contact.Status) {
		return nil, statusError(http.StatusNotFound, "retained contact not found")
	}
	if s.transport != nil {
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      roomID,
			InviterMXID: ownerMXID,
			InviteeMXID: requesterMXID,
			IsDirect:    true,
			InviteRoomState: []RoomStateEvent{
				roomProfileForDirect(contact.DisplayName, requesterMXID, ownerMXID, contact.DisplayName, contact.AvatarURL, contact.Remark, false),
			},
		}); err != nil {
			return nil, transportWriteError(err)
		}
	}
	result := map[string]any{"status": "invited", "room_id": roomID}
	if err := s.attachConversationOperation(ctx, result, "contacts.reactivate", "invited", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func contactReactivationNotRetained(apiErr *apiError) bool {
	return apiErr != nil &&
		apiErr.Status == http.StatusNotFound &&
		strings.Contains(strings.ToLower(strings.TrimSpace(apiErr.Error)), "retained contact")
}

func isDirectRoomJoinRequiresInvite(err error) bool {
	var policyErr *productpolicy.PolicyError
	return errors.As(err, &policyErr) &&
		policyErr.Code == http.StatusForbidden &&
		policyErr.Message == "direct room join requires invite"
}

//nolint:gocyclo // Contact mutations share transport and persistence guards in one compatibility endpoint.
func (s *Service) contactMutation(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	peer := trimString(params["peer_mxid"])
	if peer == "" {
		peer = trimString(params["mxid"])
	}
	roomID := trimString(params["room_id"])
	if action == "contacts.delete" || action == "contacts.requests.delete" {
		contact, ok, err := s.lookupContactByRoom(ctx, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			contact = contactRecord{
				RoomID:      roomID,
				PeerMXID:    peer,
				DisplayName: trimString(params["display_name"]),
				AvatarURL:   trimString(params["avatar_url"]),
				Domain:      trimString(params["domain"]),
				Remark:      contactRequestRemark(params),
			}
		}
		if action == "contacts.requests.delete" && contactAccepted(contact.Status) {
			result := map[string]any{"status": "ok"}
			if err := s.attachConversationOperation(ctx, result, action, contact.Status, contact.RoomID); err != nil {
				return nil, internalError(err)
			}
			return result, nil
		}
		wasDeleted := contactDeleted(contact.Status)
		if action == "contacts.delete" && !wasDeleted && contact.RoomID != "" && s.transport != nil {
			s.mu.Lock()
			ownerMXID := s.ownerMXID
			s.mu.Unlock()
			if err := s.transport.LeaveRoom(ctx, LeaveRoomRequest{
				RoomID:   contact.RoomID,
				UserMXID: ownerMXID,
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
		contact.Status = "deleted"
		if contact.DisplayName == "" {
			contact.DisplayName = trimString(params["display_name"])
		}
		if contact.AvatarURL == "" {
			contact.AvatarURL = trimString(params["avatar_url"])
		}
		if contact.Domain == "" {
			contact.Domain = trimString(params["domain"])
		}
		if err := s.saveContact(ctx, contact); err != nil {
			return nil, internalError(err)
		}
		result := map[string]any{"status": "ok"}
		if err := s.attachConversationOperation(ctx, result, action, contact.Status, contact.RoomID); err != nil {
			return nil, internalError(err)
		}
		return result, nil
	}
	status := "accepted"
	if action == "contacts.requests.reject" {
		status = "rejected"
	}
	var existing contactRecord
	if roomID != "" {
		found, ok, err := s.lookupContactByRoom(ctx, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, statusError(http.StatusNotFound, "contact request not found")
		}
		existing = found
		if peer == "" {
			peer = existing.PeerMXID
		}
	}
	if action == "contacts.requests.reject" && contactAccepted(existing.Status) {
		if err := s.attachContactConversationOperation(ctx, &existing, action, existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	}
	if action == "contacts.requests.accept" && contactAccepted(existing.Status) {
		if err := s.attachContactConversationOperation(ctx, &existing, action, existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	}
	if action == "contacts.requests.accept" && s.transport != nil && roomID != "" {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		join, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
			RoomIDOrAlias: roomID,
			UserMXID:      ownerMXID,
			DisplayName:   ownerDisplayName,
			AvatarURL:     ownerAvatarURL,
			ServerNames:   stringSliceParam(params["server_names"]),
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		if strings.TrimSpace(join.RoomID) != "" {
			roomID = join.RoomID
		}
	}
	displayName := trimString(params["display_name"])
	if existing.DisplayName != "" && (action == "contacts.requests.accept" || action == "contacts.requests.reject") {
		displayName = existing.DisplayName
	}
	contact := contactRecord{
		PeerMXID:    peer,
		DisplayName: displayName,
		AvatarURL:   trimString(params["avatar_url"]),
		Domain:      trimString(params["domain"]),
		RoomID:      roomID,
		Status:      status,
		Remark:      existing.Remark,
	}
	if contact.DisplayName == "" {
		contact.DisplayName = existing.DisplayName
	}
	if contact.AvatarURL == "" {
		contact.AvatarURL = existing.AvatarURL
	}
	if contact.Domain == "" {
		contact.Domain = existing.Domain
	}
	if contactAccepted(contact.Status) {
		contact.Remark = ""
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, action, contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) contactUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	displayName := trimString(params["display_name"])
	if displayName == "" {
		return nil, badRequest("display_name is required")
	}
	contact, ok, err := s.lookupContactByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "contact not found")
	}
	if !contactAccepted(contact.Status) {
		return nil, statusError(http.StatusForbidden, "contact is not accepted")
	}
	contact.DisplayName = displayName
	if domain := trimString(params["domain"]); domain != "" {
		contact.Domain = domain
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.update", contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) contactList(ctx context.Context) (any, *apiError) {
	contacts, err := s.listContacts(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"contacts": contacts}, nil
}

func (s *Service) saveContact(ctx context.Context, contact contactRecord) error {
	s.mu.Lock()
	s.contacts[contact.RoomID] = contact
	if contact.RoomID != "" {
		delete(s.groups, contact.RoomID)
		deleteConversationKindByRoomLocked(s.conversations, contact.RoomID, conversationKindGroup)
	}
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertContact(ctx, contact); err != nil {
			return err
		}
		if contact.RoomID != "" {
			if err := s.store.DeleteGroup(ctx, contact.RoomID); err != nil {
				return err
			}
			if err := s.deleteStoredConversationKind(ctx, contact.RoomID, conversationKindGroup); err != nil {
				return err
			}
		}
	}
	return s.saveConversation(ctx, conversationFromContact(contact))
}

func (s *Service) saveChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error {
	s.mu.Lock()
	s.inviteGrants[grant.GrantID] = grant
	s.mu.Unlock()
	if s.store != nil {
		return s.store.UpsertChannelInviteGrant(ctx, grant)
	}
	return nil
}

func (s *Service) listChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error) {
	if s.store != nil {
		return s.store.ListChannelInviteGrants(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	grants := make([]channelInviteGrant, 0, len(s.inviteGrants))
	for _, grant := range s.inviteGrants {
		grants = append(grants, grant)
	}
	sort.SliceStable(grants, func(i, j int) bool {
		if grants[i].CreatedAt == grants[j].CreatedAt {
			return grants[i].GrantID < grants[j].GrantID
		}
		return grants[i].CreatedAt > grants[j].CreatedAt
	})
	return grants, nil
}

func (s *Service) lookupChannelInviteGrantForParams(ctx context.Context, params map[string]any) (channelInviteGrant, bool, error) {
	grantID := trimString(params["grant_id"])
	shareRoomID := trimString(params["share_room_id"])
	if shareRoomID == "" {
		shareRoomID = trimString(params["via_room_id"])
	}
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	grants, err := s.listChannelInviteGrants(ctx)
	if err != nil {
		return channelInviteGrant{}, false, err
	}
	for _, grant := range grants {
		if grantID != "" && grant.GrantID != grantID {
			continue
		}
		if shareRoomID != "" && grant.ShareRoomID != shareRoomID {
			continue
		}
		if roomID != "" && grant.RoomID != roomID {
			continue
		}
		if channelID != "" && grant.ChannelID != channelID {
			continue
		}
		return grant, true, nil
	}
	return channelInviteGrant{}, false, nil
}

func (s *Service) favoriteMessage(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	favorite := favoriteRecord{
		ID:             now.UnixMilli(),
		EventID:        trimString(params["event_id"]),
		RoomID:         trimString(params["room_id"]),
		SenderID:       trimString(params["sender_id"]),
		SenderName:     trimString(params["sender_name"]),
		Content:        trimString(params["content"]),
		MessageType:    trimString(params["message_type"]),
		OriginServerTS: int64Param(params["origin_server_ts"]),
		CreatedAt:      now.Format(time.RFC3339Nano),
	}
	reuseFavoriteID := false
	if favorite.EventID != "" {
		if s.store != nil {
			existing, ok, err := s.store.FindFavoriteByEvent(ctx, favorite.EventID, favorite.RoomID)
			if err != nil {
				return nil, internalError(err)
			}
			if ok {
				favorite.ID = existing.ID
				reuseFavoriteID = true
				if existing.CreatedAt != "" {
					favorite.CreatedAt = existing.CreatedAt
				}
			}
		} else {
			s.mu.Lock()
			for _, existing := range s.favorites {
				if sameFavoriteTarget(existing, favorite) {
					favorite.ID = existing.ID
					reuseFavoriteID = true
					if existing.CreatedAt != "" {
						favorite.CreatedAt = existing.CreatedAt
					}
					break
				}
			}
			s.mu.Unlock()
		}
	}
	s.mu.Lock()
	for !reuseFavoriteID && s.favorites[favorite.ID].ID != 0 {
		favorite.ID++
	}
	s.favorites[favorite.ID] = favorite
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertFavorite(ctx, favorite); err != nil {
			return nil, internalError(err)
		}
	}
	return favorite, nil
}

func sameFavoriteTarget(existing, incoming favoriteRecord) bool {
	if incoming.EventID == "" || existing.EventID != incoming.EventID {
		return false
	}
	if incoming.RoomID != "" && existing.RoomID != "" && incoming.RoomID != existing.RoomID {
		return false
	}
	return true
}

func (s *Service) favoriteList(ctx context.Context, params map[string]any) any {
	messageType := trimString(params["message_type"])
	if s.store != nil {
		favorites, err := s.store.ListFavorites(ctx, messageType)
		if err == nil {
			return map[string]any{"favorites": favorites}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	favorites := make([]favoriteRecord, 0, len(s.favorites))
	for _, favorite := range s.favorites {
		if messageType == "" || favorite.MessageType == messageType {
			favorites = append(favorites, favorite)
		}
	}
	return map[string]any{"favorites": favorites}
}

func (s *Service) reportSubmit(ctx context.Context, params map[string]any) (any, *apiError) {
	reporterDomain := fallbackString(trimString(params["reporter_domain"]), trimString(params["reporterDomain"]))
	reportedDomain := fallbackString(trimString(params["reported_domain"]), trimString(params["reportedDomain"]))
	reason := trimString(params["reason"])
	if reporterDomain == "" {
		return nil, badRequest("reporter_domain is required")
	}
	if reportedDomain == "" {
		return nil, badRequest("reported_domain is required")
	}
	if reason == "" {
		return nil, badRequest("reason is required")
	}
	targetType := int64Param(params["target_type"])
	if targetType == 0 {
		targetType = int64Param(params["targetType"])
	}
	if targetType == 0 {
		targetType = 1
	}
	imagesJSON := "[]"
	if images := stringSliceParam(params["images"]); len(images) > 0 {
		raw, err := json.Marshal(images)
		if err != nil {
			return nil, badRequest("images is invalid")
		}
		imagesJSON = string(raw)
	}
	now := time.Now().UTC()
	report := reportRecord{
		ID:             randomToken("report"),
		ReporterDomain: reporterDomain,
		ReportedDomain: reportedDomain,
		TargetType:     targetType,
		Reason:         reason,
		ImagesJSON:     imagesJSON,
		CreatedAt:      now.Format(time.RFC3339Nano),
	}
	s.mu.Lock()
	s.reports[report.ID] = report
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.InsertReport(ctx, report); err != nil {
			return nil, internalError(err)
		}
	}
	return report, nil
}

func (s *Service) favoriteDelete(ctx context.Context, params map[string]any) (any, *apiError) {
	id := int64Param(params["id"])
	s.mu.Lock()
	delete(s.favorites, id)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.DeleteFavorite(ctx, id); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}

func (s *Service) favoriteDeleteBatch(ctx context.Context, params map[string]any) (any, *apiError) {
	ids := int64SliceParam(params["ids"])
	if len(ids) == 0 {
		return nil, badRequest("ids is required")
	}
	if len(ids) > 500 {
		return nil, badRequest("ids is too large")
	}
	s.mu.Lock()
	for _, id := range ids {
		delete(s.favorites, id)
	}
	s.mu.Unlock()
	if s.store != nil {
		for _, id := range ids {
			if err := s.store.DeleteFavorite(ctx, id); err != nil {
				return nil, internalError(err)
			}
		}
	}
	return map[string]any{"status": "ok", "deleted": ids}, nil
}

func (s *Service) callSession(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	callID := trimString(params["call_id"])
	if callID == "" {
		callID = "call_" + randomToken("p2p")
	}
	roomID := trimString(params["room_id"])
	if roomID == "" {
		roomID = "!call:" + s.serverName
	}
	state := "ringing"
	if event := trimString(params["event"]); event != "" {
		state = event
	}
	createdAt := callTimeParam(params["created_at"], params["created_at_ms"])
	if createdAt == "" {
		createdAt = now.Format(time.RFC3339Nano)
	}
	call := callRecord{
		CallID:        callID,
		RoomID:        roomID,
		RoomType:      "direct",
		MediaType:     fallbackString(trimString(params["media_type"]), "voice"),
		CreatedByMXID: fallbackString(trimString(params["created_by_mxid"]), s.ownerMXID),
		State:         state,
		CreatedAt:     createdAt,
	}
	if existing, ok, err := s.callByID(ctx, callID); err != nil {
		return nil, internalError(err)
	} else if ok && terminalCallState(existing.State) {
		return existing, nil
	}
	applyCallLifecycle(&call, state, params, now, s.ownerMXID)
	s.mu.Lock()
	s.calls[call.CallID] = call
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertCall(ctx, call); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.publishCallChanged(ctx, call); err != nil {
		return nil, internalError(err)
	}
	return call, nil
}

func (s *Service) callGet(ctx context.Context, params map[string]any) (any, *apiError) {
	callID := trimString(params["call_id"])
	if callID == "" {
		return nil, badRequest("call_id is required")
	}
	call, ok, err := s.callByID(ctx, callID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "call not found")
	}
	return call, nil
}

func (s *Service) callEvent(ctx context.Context, params map[string]any) (any, *apiError) {
	callID := trimString(params["call_id"])
	if callID == "" {
		return nil, badRequest("call_id is required")
	}
	event := trimString(params["event"])
	switch event {
	case "connected", "ended", "rejected", "missed", "failed":
	default:
		return nil, badRequest("event must be connected, ended, rejected, missed, or failed")
	}
	call, ok, err := s.callByID(ctx, callID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "call not found")
	}
	if terminalCallState(call.State) && call.State != event {
		return call, nil
	}
	call.State = event
	if mediaType := trimString(params["media_type"]); mediaType != "" {
		call.MediaType = mediaType
	}
	applyCallLifecycle(&call, event, params, time.Now().UTC(), s.ownerMXID)
	s.mu.Lock()
	s.calls[call.CallID] = call
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertCall(ctx, call); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.publishCallChanged(ctx, call); err != nil {
		return nil, internalError(err)
	}
	return call, nil
}

func (s *Service) publishCallChanged(ctx context.Context, call callRecord) error {
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:   "call.changed",
		RoomID: call.RoomID,
		Payload: map[string]any{
			"call": call,
		},
	})
}

func applyCallLifecycle(call *callRecord, event string, params map[string]any, now time.Time, localUserID string) {
	switch event {
	case "connected":
		if answeredAt := callTimeParam(params["answered_at"], params["answered_at_ms"]); answeredAt != "" {
			call.AnsweredAt = answeredAt
		} else if call.AnsweredAt == "" {
			call.AnsweredAt = now.Format(time.RFC3339Nano)
		}
	case "ended", "rejected", "missed", "failed":
		if endedAt := callTimeParam(params["ended_at"], params["ended_at_ms"]); endedAt != "" {
			call.EndedAt = endedAt
		} else if call.EndedAt == "" {
			call.EndedAt = now.Format(time.RFC3339Nano)
		}
		call.EndedByMXID = fallbackString(trimString(params["ended_by_mxid"]), localUserID)
		call.EndReason = trimString(params["reason"])
		if durationMS := int64Param(params["duration_ms"]); durationMS > 0 {
			call.DurationMS = durationMS
		} else if call.DurationMS <= 0 {
			call.DurationMS = callDurationMS(call.AnsweredAt, call.EndedAt)
		}
	}
}

func callDurationMS(start, end string) int64 {
	startTime, startErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(start))
	endTime, endErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(end))
	if startErr != nil || endErr != nil {
		return 0
	}
	duration := endTime.Sub(startTime)
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}

func (s *Service) callByID(ctx context.Context, callID string) (callRecord, bool, error) {
	if s.store != nil {
		calls, err := s.store.ListCalls(ctx, "", false)
		if err != nil {
			return callRecord{}, false, err
		}
		for _, call := range calls {
			if call.CallID == callID {
				return call, true, nil
			}
		}
		return callRecord{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	call, ok := s.calls[callID]
	return call, ok, nil
}

func (s *Service) callList(ctx context.Context, params map[string]any, activeOnly bool) any {
	roomID := trimString(params["room_id"])
	if s.store != nil {
		calls, err := s.store.ListCalls(ctx, roomID, activeOnly)
		if err == nil {
			return map[string]any{"calls": calls}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]callRecord, 0, len(s.calls))
	for _, call := range s.calls {
		if roomID != "" && call.RoomID != roomID {
			continue
		}
		if activeOnly && terminalCallState(call.State) {
			continue
		}
		calls = append(calls, call)
	}
	return map[string]any{"calls": calls}
}

func terminalCallState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "ended", "rejected", "missed", "failed":
		return true
	default:
		return false
	}
}

func (s *Service) ensureProductRoom(ctx context.Context, kind string, req CreateRoomRequest) (string, *apiError) {
	if s.transport != nil {
		s.mu.Lock()
		req.CreatorMXID = s.ownerMXID
		if req.CreatorDisplayName == "" {
			req.CreatorDisplayName = s.profile.DisplayName
		}
		if req.CreatorAvatarURL == "" {
			req.CreatorAvatarURL = s.profile.AvatarURL
		}
		s.mu.Unlock()
		if req.RoomType == "" {
			req.RoomType = direxioRoomType(kind)
		}
		res, err := s.transport.CreateRoom(ctx, req)
		if err != nil {
			return "", internalError(err)
		}
		return res.RoomID, nil
	}
	return "!" + kind + "-" + randomToken("room") + ":" + s.serverName, nil
}

func (s *Service) saveOwnerMember(ctx context.Context, roomID, channelID string) error {
	s.mu.Lock()
	member := memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      s.ownerMXID,
		DisplayName: s.profile.DisplayName,
		AvatarURL:   s.profile.AvatarURL,
		Domain:      s.serverName,
		Membership:  "join",
		Role:        "owner",
		JoinedAt:    time.Now().UTC().UnixMilli(),
	}
	s.mu.Unlock()
	return s.saveMember(ctx, member)
}

func (s *Service) groupResult(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	needsStatePublish := roomID != ""
	name := fallbackString(trimString(params["name"]), trimString(params["group_name"]))
	group := groupRecord{
		RoomID:       roomID,
		Name:         fallbackString(name, "Group"),
		Topic:        trimString(params["topic"]),
		AvatarURL:    trimString(params["avatar_url"]),
		MemberCount:  1,
		InvitePolicy: fallbackString(trimString(params["invite_policy"]), "member"),
	}
	if roomID == "" {
		var apiErr *apiError
		roomID, apiErr = s.ensureProductRoom(ctx, "group", CreateRoomRequest{
			Name:       fallbackString(name, "Group"),
			Topic:      trimString(params["topic"]),
			Visibility: "private",
			RoomType:   DirexioRoomTypeGroup,
			IsDirect:   false,
			InitialState: []RoomStateEvent{
				groupStateEvent(group, false),
			},
		})
		if apiErr != nil {
			return nil, apiErr
		}
	}
	group.RoomID = roomID
	if group.Name == "" || group.Name == "Group" && name == "" {
		group.Name = fallbackString(name, roomID)
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if err := s.saveOwnerMember(ctx, group.RoomID, ""); err != nil {
		return nil, internalError(err)
	}
	if needsStatePublish {
		if err := s.publishGroupState(ctx, group, false); err != nil {
			return nil, internalError(err)
		}
	}
	result, err := s.groupRecordWithConversationOperation(ctx, group, "groups.create", "ok")
	if err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) groupRecordWithConversationOperation(ctx context.Context, group groupRecord, action, status string) (groupRecord, error) {
	roomID := strings.TrimSpace(group.RoomID)
	operation := map[string]any{
		"action":  action,
		"status":  status,
		"room_id": roomID,
	}
	if roomID != "" {
		record, ok, err := s.getConversation(ctx, "", roomID)
		if err != nil {
			return groupRecord{}, err
		}
		if ok {
			view, err := s.conversationView(ctx, record)
			if err != nil {
				return groupRecord{}, err
			}
			group.Conversation = &view
			operation["conversation_id"] = view.ConversationID
		}
	}
	group.Operation = operation
	return group, nil
}

func (s *Service) groupUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	if name := fallbackString(trimString(params["name"]), trimString(params["group_name"])); name != "" {
		group.Name = name
	}
	if _, ok := params["topic"]; ok {
		group.Topic = trimString(params["topic"])
	}
	if _, ok := params["avatar_url"]; ok {
		group.AvatarURL = trimString(params["avatar_url"])
	}
	if policy := trimString(params["invite_policy"]); policy != "" {
		group.InvitePolicy = policy
	}
	if _, ok := params["muted"]; ok {
		group.Muted = boolParam(params["muted"])
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if err := s.publishGroupState(ctx, group, false); err != nil {
		return nil, internalError(err)
	}
	return group, nil
}

func (s *Service) groupList(ctx context.Context) any {
	groups, err := s.listGroups(ctx)
	if err != nil {
		return map[string]any{"groups": []groupRecord{}}
	}
	groups, err = s.joinedGroupsForOwner(ctx, groups)
	if err != nil {
		return map[string]any{"groups": []groupRecord{}}
	}
	return map[string]any{"groups": groups}
}

func (s *Service) groupPolicyMutation(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	switch action {
	case "groups.mute":
		group.Muted = true
	case "groups.unmute":
		group.Muted = false
	case "groups.invite_policy.update":
		if policy := trimString(params["invite_policy"]); policy != "" {
			group.InvitePolicy = policy
		}
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return nil, internalError(err)
	}
	if action == "groups.invite_policy.update" {
		if err := s.publishGroupState(ctx, group, false); err != nil {
			return nil, internalError(err)
		}
	}
	if action == "groups.mute" || action == "groups.unmute" {
		if err := s.setProductMemberMute(ctx, roomID, "", group.Muted); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"status": "ok", "room_id": group.RoomID, "muted": group.Muted, "group": group}, nil
	}
	return group, nil
}

func (s *Service) saveGroup(ctx context.Context, group groupRecord) error {
	s.mu.Lock()
	s.groups[group.RoomID] = group
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertGroup(ctx, group); err != nil {
			return err
		}
	}
	return s.saveConversation(ctx, conversationFromGroup(group))
}

func groupStateEvent(group groupRecord, dissolved bool) RoomStateEvent {
	return roomProfileForGroup(group, dissolved)
}

func (s *Service) publishGroupState(ctx context.Context, group groupRecord, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(group.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     group.RoomID,
		SenderMXID: senderMXID,
		Event:      groupStateEvent(group, dissolved),
	})
}

func (s *Service) deleteGroup(ctx context.Context, roomID string) error {
	s.mu.Lock()
	delete(s.groups, roomID)
	deleteConversationKindByRoomLocked(s.conversations, roomID, conversationKindGroup)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.DeleteGroup(ctx, roomID); err != nil {
			return err
		}
		return s.deleteStoredConversationKind(ctx, roomID, conversationKindGroup)
	}
	return nil
}

func (s *Service) dissolveGroup(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	group, ok, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "group not found")
	}
	if apiErr := s.requireOwnerMember(ctx, group.RoomID); apiErr != nil {
		return nil, apiErr
	}
	if err := s.publishGroupState(ctx, group, true); err != nil {
		return nil, internalError(err)
	}
	if err := s.deleteGroup(ctx, group.RoomID); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "group": group}, nil
}

func (s *Service) groupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error) {
	groups, err := s.listGroups(ctx)
	if err != nil {
		return groupRecord{}, false, err
	}
	for _, group := range groups {
		if group.RoomID == roomID {
			return group, true, nil
		}
	}
	return groupRecord{}, false, nil
}

func (s *Service) followAdd(ctx context.Context, params map[string]any) (any, *apiError) {
	domain := trimString(params["domain"])
	if domain == "" {
		return nil, badRequest("domain is required")
	}
	follow := followRecord{Domain: domain, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	s.mu.Lock()
	s.follows[domain] = follow
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertFollow(ctx, follow); err != nil {
			return nil, internalError(err)
		}
	}
	return follow, nil
}

func (s *Service) followRemove(ctx context.Context, params map[string]any) (any, *apiError) {
	domain := trimString(params["domain"])
	s.mu.Lock()
	delete(s.follows, domain)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.DeleteFollow(ctx, domain); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}

func (s *Service) followList(ctx context.Context) any {
	if s.store != nil {
		follows, err := s.store.ListFollows(ctx)
		if err == nil {
			return map[string]any{"follows": follows}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	follows := make([]followRecord, 0, len(s.follows))
	for _, follow := range s.follows {
		follows = append(follows, follow)
	}
	return map[string]any{"follows": follows}
}

func (s *Service) channelResult(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	if channelID == "" {
		channelID = "ch_" + randomToken("channel")
	}
	roomID := trimString(params["room_id"])
	channelType := fallbackString(trimString(params["channel_type"]), "chat")
	ch := channel{
		ChannelID:        channelID,
		RoomID:           roomID,
		Name:             fallbackString(trimString(params["name"]), channelID),
		Description:      trimString(params["description"]),
		AvatarURL:        trimString(params["avatar_url"]),
		Visibility:       fallbackString(trimString(params["visibility"]), "public"),
		JoinPolicy:       fallbackString(trimString(params["join_policy"]), "open"),
		ChannelType:      channelType,
		CommentsEnabled:  true,
		MemberCount:      1,
		PendingJoinCount: 0,
		IsOwned:          true,
		Role:             "owner",
		MemberStatus:     "join",
	}
	if _, ok := params["comments_enabled"]; ok {
		ch.CommentsEnabled = boolParam(params["comments_enabled"])
	}
	if roomID == "" {
		var apiErr *apiError
		roomID, apiErr = s.ensureProductRoom(ctx, "channel", CreateRoomRequest{
			Name:       fallbackString(trimString(params["name"]), channelID),
			Topic:      trimString(params["description"]),
			Visibility: fallbackString(trimString(params["visibility"]), "public"),
			RoomType:   DirexioRoomTypeChannel,
			IsDirect:   false,
			InitialState: []RoomStateEvent{
				channelStateEvent(ch, false),
			},
		})
		if apiErr != nil {
			return nil, apiErr
		}
	}
	ch.RoomID = roomID
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.saveOwnerMember(ctx, ch.RoomID, ch.ChannelID); err != nil {
		return nil, internalError(err)
	}
	return ch, nil
}

func (s *Service) channelUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if name := trimString(params["name"]); name != "" {
		ch.Name = name
	}
	if _, ok := params["description"]; ok {
		ch.Description = trimString(params["description"])
	}
	if _, ok := params["avatar_url"]; ok {
		ch.AvatarURL = trimString(params["avatar_url"])
	}
	if visibility := trimString(params["visibility"]); visibility != "" {
		ch.Visibility = visibility
	}
	if joinPolicy := trimString(params["join_policy"]); joinPolicy != "" {
		ch.JoinPolicy = joinPolicy
	}
	if channelType := trimString(params["channel_type"]); channelType != "" {
		ch.ChannelType = channelType
	}
	if _, ok := params["comments_enabled"]; ok {
		ch.CommentsEnabled = boolParam(params["comments_enabled"])
	}
	if _, ok := params["muted"]; ok {
		ch.Muted = boolParam(params["muted"])
	}
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.publishChannelState(ctx, ch, false); err != nil {
		return nil, internalError(err)
	}
	return ch, nil
}

func (s *Service) channelList(ctx context.Context) any {
	channels, err := s.listChannels(ctx)
	if err != nil {
		return map[string]any{"channels": []channel{}}
	}
	enriched, err := s.joinedChannelsForOwner(ctx, channels)
	if err != nil {
		return map[string]any{"channels": []channel{}}
	}
	return map[string]any{"channels": enriched}
}

func (s *Service) channelPublicGet(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		if remote, fetched, apiErr := s.remotePublicChannelGet(ctx, channelID, roomID, params); apiErr != nil {
			return nil, apiErr
		} else if fetched {
			ch = remote
			ok = true
		}
	}
	if !ok {
		if roomID != "" {
			s.mu.Lock()
			transport := s.transport
			s.mu.Unlock()
			if transport != nil {
				fetched, found, fetchErr := transport.GetRoomChannel(ctx, roomID)
				if fetchErr != nil {
					if roomServer := domainFromMatrixID(roomID, "!"); roomServer != "" && roomServer != s.serverName {
						return nil, statusError(404, "channel not found")
					}
					return nil, internalError(fetchErr)
				}
				if found {
					ch = fetched
					ok = true
					if err := s.saveChannel(ctx, ch); err != nil {
						return nil, internalError(err)
					}
				}
			}
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
	}
	if !strings.EqualFold(ch.Visibility, "public") {
		return nil, statusError(404, "channel not found")
	}
	return ch, nil
}

func (s *Service) channelPublicSearch(ctx context.Context, params map[string]any) (any, *apiError) {
	rawQuery := trimString(params["q"])
	query := strings.ToLower(rawQuery)
	limit := int(int64Param(params["limit"]))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if matrixRoomIDQuery(rawQuery) {
		ch, apiErr := s.channelPublicGet(ctx, map[string]any{
			"room_id":              rawQuery,
			"remote_node_base_url": remoteNodeBaseURLParam(params),
		})
		if apiErr != nil {
			if apiErr.Status == 404 {
				return map[string]any{"channels": []channel{}, "results": []channel{}}, nil
			}
			return nil, apiErr
		}
		channelResult, ok := ch.(channel)
		if !ok {
			return nil, internalError(fmt.Errorf("public get returned %T", ch))
		}
		return map[string]any{"channels": []channel{channelResult}, "results": []channel{channelResult}}, nil
	}
	channels, err := s.listChannels(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	results := make([]channel, 0, len(channels))
	for _, ch := range channels {
		if !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(ch.ChannelID+" "+ch.RoomID+" "+ch.Name+" "+ch.Description), query) {
			continue
		}
		results = append(results, ch)
		if len(results) >= limit {
			break
		}
	}
	return map[string]any{"channels": results, "results": results}, nil
}

func (s *Service) userPublicChannels(ctx context.Context, params map[string]any) (any, *apiError) {
	userID := fallbackString(trimString(params["user_id"]), trimString(params["user_mxid"]))
	userID = fallbackString(userID, trimString(params["mxid"]))
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	channels, err := s.listChannels(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	members, err := s.membersForUser(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	visibleMemberships := map[string]bool{}
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		if member.ChannelID != "" {
			visibleMemberships[member.ChannelID] = true
		}
	}
	publicChannels := make([]channel, 0, len(channels))
	for _, ch := range channels {
		if !visibleMemberships[ch.ChannelID] {
			continue
		}
		if !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		publicChannels = append(publicChannels, ch)
	}
	sort.SliceStable(publicChannels, func(i, j int) bool {
		if publicChannels[i].Name == publicChannels[j].Name {
			return publicChannels[i].ChannelID < publicChannels[j].ChannelID
		}
		return publicChannels[i].Name < publicChannels[j].Name
	})
	return map[string]any{"user_id": userID, "channels": publicChannels}, nil
}

func (s *Service) channelPolicyMutation(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	ch.Muted = action == "channels.mute"
	if err := s.saveChannel(ctx, ch); err != nil {
		return nil, internalError(err)
	}
	if err := s.setProductMemberMute(ctx, ch.RoomID, ch.ChannelID, ch.Muted); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "channel_id": ch.ChannelID, "room_id": ch.RoomID, "muted": ch.Muted, "channel": ch}, nil
}

func (s *Service) saveChannel(ctx context.Context, ch channel) error {
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	return s.saveConversation(ctx, conversationFromChannel(ch))
}

func channelStateEvent(ch channel, dissolved bool) RoomStateEvent {
	return roomProfileForChannel(ch, dissolved)
}

func (s *Service) publishChannelState(ctx context.Context, ch channel, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(ch.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     ch.RoomID,
		SenderMXID: senderMXID,
		Event:      channelStateEvent(ch, dissolved),
	})
}

func (s *Service) publishJoinRequestState(ctx context.Context, roomID, userID, status, reason string) *apiError {
	if s.transport == nil || strings.TrimSpace(roomID) == "" || strings.TrimSpace(userID) == "" {
		return nil
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "pending", "approved", "rejected":
	default:
		return badRequest("invalid join request status")
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	content := map[string]any{
		"status":     status,
		"room_id":    roomID,
		"user_id":    userID,
		"created_at": now,
		"updated_at": now,
	}
	if strings.TrimSpace(reason) != "" {
		content["reason"] = strings.TrimSpace(reason)
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     roomID,
		SenderMXID: senderMXID,
		Event: RoomStateEvent{
			Type:     DirexioJoinRequestEventType,
			StateKey: productpolicy.UserStateKey(userID),
			Content:  content,
		},
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) publishMemberPolicyState(ctx context.Context, member memberRecord) *apiError {
	if s.transport == nil || strings.TrimSpace(member.RoomID) == "" || strings.TrimSpace(member.UserID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     member.RoomID,
		SenderMXID: senderMXID,
		Event: RoomStateEvent{
			Type:     DirexioMemberPolicyEventType,
			StateKey: productpolicy.UserStateKey(member.UserID),
			Content: map[string]any{
				"role":    fallbackString(member.Role, "member"),
				"muted":   member.Muted,
				"user_id": member.UserID,
				"room_id": member.RoomID,
			},
		},
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) deleteChannel(ctx context.Context, channelID string) error {
	s.mu.Lock()
	delete(s.channels, channelID)
	s.mu.Unlock()
	if s.store != nil {
		return s.store.DeleteChannel(ctx, channelID)
	}
	return nil
}

func (s *Service) dissolveChannel(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if apiErr := s.requireOwnerMember(ctx, ch.RoomID); apiErr != nil {
		return nil, apiErr
	}
	if err := s.publishChannelState(ctx, ch, true); err != nil {
		return nil, internalError(err)
	}
	if err := s.deleteChannel(ctx, ch.ChannelID); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "ok", "channel": ch}, nil
}

func (s *Service) channelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error) {
	channels, err := s.listChannels(ctx)
	if err != nil {
		return channel{}, false, err
	}
	for _, ch := range channels {
		if channelID != "" && ch.ChannelID == channelID {
			return ch, true, nil
		}
		if roomID != "" && ch.RoomID == roomID {
			return ch, true, nil
		}
	}
	return channel{}, false, nil
}

func (s *Service) channelSnapshot(ctx context.Context, channelID string) channel {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return channel{}
	}
	if s.store != nil {
		channels, err := s.store.ListChannels(ctx)
		if err == nil {
			for _, ch := range channels {
				if ch.ChannelID == channelID {
					return ch
				}
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.channels[channelID]
}

func (s *Service) refreshStoredChannelCounts(ctx context.Context, channelID string) error {
	channelID = strings.TrimSpace(channelID)
	if s.store == nil || channelID == "" {
		return nil
	}
	channels, err := s.store.ListChannels(ctx)
	if err != nil {
		return err
	}
	var target channel
	for _, ch := range channels {
		if ch.ChannelID == channelID {
			target = ch
			break
		}
	}
	if target.ChannelID == "" {
		return nil
	}
	members, err := s.store.ListMembers(ctx, "", channelID)
	if err != nil {
		return err
	}
	target.MemberCount, target.PendingJoinCount = memberCounts(members)
	return s.store.UpsertChannel(ctx, target)
}

func (s *Service) refreshStoredGroupCounts(ctx context.Context, roomID string) error {
	roomID = strings.TrimSpace(roomID)
	if s.store == nil || roomID == "" {
		return nil
	}
	groups, err := s.store.ListGroups(ctx)
	if err != nil {
		return err
	}
	var target groupRecord
	for _, group := range groups {
		if group.RoomID == roomID {
			target = group
			break
		}
	}
	if target.RoomID == "" {
		return nil
	}
	members, err := s.store.ListMembers(ctx, roomID, "")
	if err != nil {
		return err
	}
	target.MemberCount, _ = memberCounts(members)
	return s.store.UpsertGroup(ctx, target)
}

func (s *Service) refreshChannelCountsLocked(channelID string) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return
	}
	ch, ok := s.channels[channelID]
	if !ok {
		return
	}
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.ChannelID == channelID {
			members = append(members, member)
		}
	}
	ch.MemberCount, ch.PendingJoinCount = memberCounts(members)
	s.channels[channelID] = ch
}

func (s *Service) refreshGroupCountsLocked(roomID string) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return
	}
	group, ok := s.groups[roomID]
	if !ok {
		return
	}
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.ChannelID == "" && member.RoomID == roomID {
			members = append(members, member)
		}
	}
	group.MemberCount, _ = memberCounts(members)
	s.groups[roomID] = group
}

func memberCounts(members []memberRecord) (int64, int64) {
	var joined, pending int64
	for _, member := range members {
		switch strings.ToLower(strings.TrimSpace(member.Membership)) {
		case "join", "joined":
			joined++
		case "pending":
			pending++
		}
	}
	return joined, pending
}

func (s *Service) channelPost(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	channelID := fallbackString(trimString(params["channel_id"]), "channel")
	postID := "post_" + randomToken("post")
	s.mu.Lock()
	authorMXID := s.ownerMXID
	authorName := s.profile.DisplayName
	s.mu.Unlock()
	roomID, apiErr := s.roomIDForChannel(ctx, channelID, trimString(params["room_id"]))
	if apiErr != nil {
		return nil, apiErr
	}
	body := fallbackString(trimString(params["body"]), trimString(params["content"]))
	msgType := fallbackString(trimString(params["message_type"]), "text")
	mediaJSON, media, mediaErr := mediaPayloadParam(params["media_json"])
	if mediaErr != nil {
		return nil, badRequest("media_json is invalid")
	}
	eventID := "$" + postID + ":" + s.serverName
	originServerTS := now.UnixMilli()
	if s.transport != nil && roomID != "" {
		content := channelMessageContent("channel_post", body, msgType, mediaJSON, media)
		content["channel_id"] = channelID
		content["post_id"] = postID
		res, err := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  authorMXID,
			RoomID:      roomID,
			MessageType: msgType,
			Timestamp:   now,
			Content:     content,
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		eventID = res.EventID
		originServerTS = res.OriginServerTS
	}
	post := channelPostRecord{
		PostID:         postID,
		ChannelID:      channelID,
		RoomID:         roomID,
		EventID:        eventID,
		AuthorMXID:     authorMXID,
		AuthorName:     authorName,
		Body:           body,
		MessageType:    msgType,
		MediaJSON:      mediaJSON,
		OriginServerTS: originServerTS,
		CommentCount:   0,
	}
	s.mu.Lock()
	s.posts = append(s.posts, post)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.InsertChannelPost(ctx, post); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.attachChannelPostOperation(ctx, &post, "channels.posts.create", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return post, nil
}

func (s *Service) channelPosts(ctx context.Context, params map[string]any) any {
	channelID := trimString(params["channel_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		posts, err := s.store.ListChannelPosts(ctx, channelID)
		if err != nil {
			return map[string]any{"posts": []channelPostRecord{}}
		}
		s.enrichChannelPosts(ctx, posts, ownerMXID)
		return map[string]any{"posts": posts}
	}
	s.mu.Lock()
	posts := make([]channelPostRecord, 0, len(s.posts))
	for _, post := range s.posts {
		if channelID == "" || post.ChannelID == channelID {
			posts = append(posts, post)
		}
	}
	s.mu.Unlock()
	s.enrichChannelPosts(ctx, posts, ownerMXID)
	return map[string]any{"posts": posts}
}

func (s *Service) channelComment(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	commentID := "comment_" + randomToken("comment")
	s.mu.Lock()
	authorMXID := s.ownerMXID
	authorName := s.profile.DisplayName
	s.mu.Unlock()
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	if _, ok, err := s.channelPostByID(ctx, postID, channelID); err != nil {
		return nil, internalError(err)
	} else if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	body := fallbackString(trimString(params["body"]), trimString(params["content"]))
	msgType := fallbackString(trimString(params["message_type"]), "text")
	mediaJSON, media, mediaErr := mediaPayloadParam(params["media_json"])
	if mediaErr != nil {
		return nil, badRequest("media_json is invalid")
	}
	replyToCommentID := trimString(params["reply_to_comment_id"])
	replyToAuthorMXID := trimString(params["reply_to_author_mxid"])
	mentionsJSON, err := jsonArrayStringParam(params["mentions"])
	if err != nil {
		return nil, badRequest("mentions is invalid")
	}
	if _, ok := params["mentions"]; !ok {
		mentionsJSON, err = jsonArrayStringParam(params["mentions_json"])
		if err != nil {
			return nil, badRequest("mentions_json is invalid")
		}
	}
	eventID := "$" + commentID + ":" + s.serverName
	originServerTS := now.UnixMilli()
	roomID, apiErr := s.roomIDForChannel(ctx, channelID, trimString(params["room_id"]))
	if apiErr != nil {
		return nil, apiErr
	}
	if s.transport != nil && roomID != "" {
		content := channelMessageContent("channel_comment", body, msgType, mediaJSON, media)
		content["channel_id"] = channelID
		content["post_id"] = postID
		content["comment_id"] = commentID
		content["reply_to_comment_id"] = replyToCommentID
		content["reply_to_author_mxid"] = replyToAuthorMXID
		content["mentions_json"] = mentionsJSON
		res, err := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  authorMXID,
			RoomID:      roomID,
			MessageType: msgType,
			Timestamp:   now,
			Content:     content,
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		eventID = res.EventID
		originServerTS = res.OriginServerTS
	}
	comment := channelCommentRecord{
		CommentID:         commentID,
		PostID:            postID,
		ChannelID:         channelID,
		EventID:           eventID,
		AuthorMXID:        authorMXID,
		AuthorName:        authorName,
		Body:              body,
		MessageType:       msgType,
		MediaJSON:         mediaJSON,
		ReplyToCommentID:  replyToCommentID,
		ReplyToAuthorMXID: replyToAuthorMXID,
		MentionsJSON:      mentionsJSON,
		OriginServerTS:    originServerTS,
		ReactionCount:     0,
		ReactedByMe:       false,
	}
	s.mu.Lock()
	s.comments = append(s.comments, comment)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.InsertChannelComment(ctx, comment); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.attachChannelCommentOperation(ctx, &comment, "channels.comments.create", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return comment, nil
}

func (s *Service) channelComments(ctx context.Context, params map[string]any) any {
	postID := trimString(params["post_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		comments, err := s.store.ListChannelComments(ctx, postID)
		if err != nil {
			return map[string]any{"comments": []channelCommentRecord{}}
		}
		s.enrichChannelComments(ctx, comments, ownerMXID)
		return map[string]any{"comments": comments}
	}
	s.mu.Lock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if postID == "" || comment.PostID == postID {
			comments = append(comments, comment)
		}
	}
	s.mu.Unlock()
	s.enrichChannelComments(ctx, comments, ownerMXID)
	return map[string]any{"comments": comments}
}

func (s *Service) roomIDForChannel(ctx context.Context, channelID, fallbackRoomID string) (string, *apiError) {
	if strings.TrimSpace(fallbackRoomID) != "" {
		return strings.TrimSpace(fallbackRoomID), nil
	}
	if strings.TrimSpace(channelID) == "" {
		return "", nil
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, "")
	if err != nil {
		return "", internalError(err)
	}
	if !ok {
		return "", nil
	}
	return ch.RoomID, nil
}

func mediaPayloadParam(value any) (string, map[string]any, error) {
	switch v := value.(type) {
	case nil:
		return "", nil, nil
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return "", nil, nil
		}
		var media map[string]any
		if err := json.Unmarshal([]byte(text), &media); err != nil {
			return "", nil, err
		}
		raw, err := json.Marshal(media)
		if err != nil {
			return "", nil, err
		}
		return string(raw), media, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", nil, err
		}
		var media map[string]any
		if err = json.Unmarshal(raw, &media); err != nil {
			return "", nil, err
		}
		normalized, err := json.Marshal(media)
		if err != nil {
			return "", nil, err
		}
		return string(normalized), media, nil
	}
}

func channelMessageContent(kind, body, msgType, mediaJSON string, media map[string]any) map[string]any {
	content := map[string]any{
		"msgtype":     matrixMessageType(msgType, mediaMessageType(msgType) || len(media) > 0),
		"body":        body,
		"p2p_kind":    kind,
		"client_type": msgType,
	}
	if mediaJSON != "" {
		content["media_json"] = mediaJSON
	}
	for key, value := range media {
		if key == "body" || key == "msgtype" || key == "p2p_kind" || key == "client_type" || key == "media_json" {
			continue
		}
		if key == "mxc" && content["url"] == nil {
			content["url"] = value
			continue
		}
		content[key] = value
	}
	return content
}

func mediaMessageType(messageType string) bool {
	switch strings.TrimSpace(messageType) {
	case "image", "m.image", "video", "m.video", "audio", "m.audio", "file", "m.file":
		return true
	default:
		return false
	}
}

func (s *Service) enrichChannelPosts(ctx context.Context, posts []channelPostRecord, ownerMXID string) {
	for i := range posts {
		comments, err := s.listChannelCommentsForPost(ctx, posts[i].PostID)
		if err == nil {
			posts[i].CommentCount = int64(len(comments))
		}
		count, err := s.countActiveReactions(ctx, "post", posts[i].PostID, "like")
		if err == nil {
			posts[i].ReactionCount = count
		}
		if ownerMXID != "" {
			if reaction, ok, err := s.getReaction(ctx, "post", posts[i].PostID, "like", ownerMXID); err == nil && ok {
				posts[i].ReactedByMe = reaction.Active
			}
		}
	}
}

func (s *Service) enrichChannelComments(ctx context.Context, comments []channelCommentRecord, ownerMXID string) {
	for i := range comments {
		count, err := s.countActiveReactions(ctx, "comment", comments[i].CommentID, "like")
		if err == nil {
			comments[i].ReactionCount = count
		}
		if ownerMXID != "" {
			if reaction, ok, err := s.getReaction(ctx, "comment", comments[i].CommentID, "like", ownerMXID); err == nil && ok {
				comments[i].ReactedByMe = reaction.Active
			}
		}
	}
}

func (s *Service) listChannelCommentsForPost(ctx context.Context, postID string) ([]channelCommentRecord, error) {
	if s.store != nil {
		return s.store.ListChannelComments(ctx, postID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if postID == "" || comment.PostID == postID {
			comments = append(comments, comment)
		}
	}
	return comments, nil
}

func (s *Service) myChannelComments(ctx context.Context, params map[string]any) any {
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		comments, err := s.store.ListChannelComments(ctx, postID)
		if err != nil {
			return map[string]any{"comments": []channelCommentRecord{}}
		}
		filtered := make([]channelCommentRecord, 0, len(comments))
		for _, comment := range comments {
			if comment.AuthorMXID != ownerMXID {
				continue
			}
			if channelID != "" && comment.ChannelID != channelID {
				continue
			}
			filtered = append(filtered, comment)
		}
		return map[string]any{"comments": filtered}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if comment.AuthorMXID != ownerMXID {
			continue
		}
		if channelID != "" && comment.ChannelID != channelID {
			continue
		}
		if postID != "" && comment.PostID != postID {
			continue
		}
		comments = append(comments, comment)
	}
	return map[string]any{"comments": comments}
}

func (s *Service) recallChannelContent(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	if action == "channels.posts.recall" {
		postID := trimString(params["post_id"])
		if postID == "" {
			return nil, badRequest("post_id is required")
		}
		post, ok, err := s.channelPostByID(ctx, postID, trimString(params["channel_id"]))
		if err != nil {
			return nil, transportWriteError(err)
		}
		if !ok {
			return nil, statusError(http.StatusNotFound, "post not found")
		}
		if s.transport == nil {
			if apiErr := s.authorizeChannelContentRecall(ctx, post.RoomID, post.AuthorMXID); apiErr != nil {
				return nil, apiErr
			}
		}
		eventID, roomID := post.EventID, post.RoomID
		if err := s.redactEvent(ctx, roomID, eventID, trimString(params["reason"])); err != nil {
			return nil, transportWriteError(err)
		}
		s.mu.Lock()
		filtered := s.posts[:0]
		for _, post := range s.posts {
			if post.PostID != postID {
				filtered = append(filtered, post)
			}
		}
		s.posts = filtered
		s.mu.Unlock()
		if s.store != nil {
			if err := s.store.DeleteChannelPost(ctx, postID); err != nil {
				return nil, internalError(err)
			}
		}
		result := map[string]any{"status": "ok"}
		if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
			return nil, internalError(err)
		}
		return result, nil
	}
	commentID := trimString(params["comment_id"])
	if commentID == "" {
		return nil, badRequest("comment_id is required")
	}
	postID := trimString(params["post_id"])
	comment, ok, err := s.channelCommentByID(ctx, commentID, postID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "comment not found")
	}
	roomID, err := s.roomIDForChannelComment(ctx, comment, trimString(params["room_id"]))
	if err != nil {
		return nil, internalError(err)
	}
	if s.transport == nil {
		if apiErr := s.authorizeChannelContentRecall(ctx, roomID, comment.AuthorMXID); apiErr != nil {
			return nil, apiErr
		}
	}
	eventID := comment.EventID
	if err := s.redactEvent(ctx, roomID, eventID, trimString(params["reason"])); err != nil {
		return nil, transportWriteError(err)
	}
	s.mu.Lock()
	filtered := s.comments[:0]
	for _, comment := range s.comments {
		if comment.CommentID != commentID {
			filtered = append(filtered, comment)
		}
	}
	s.comments = filtered
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.DeleteChannelComment(ctx, commentID); err != nil {
			return nil, internalError(err)
		}
	}
	result := map[string]any{"status": "ok"}
	if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) redactEvent(ctx context.Context, roomID, eventID, reason string) error {
	if s.transport == nil || roomID == "" || eventID == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	_, err := s.transport.RedactEvent(ctx, RedactEventRequest{
		RoomID:     roomID,
		EventID:    eventID,
		SenderMXID: senderMXID,
		Reason:     reason,
		Timestamp:  time.Now().UTC(),
	})
	return err
}

func (s *Service) channelPostByID(ctx context.Context, postID, channelID string) (channelPostRecord, bool, error) {
	if s.store != nil {
		posts, err := s.store.ListChannelPosts(ctx, channelID)
		if err != nil {
			return channelPostRecord{}, false, err
		}
		for _, post := range posts {
			if post.PostID == postID {
				return post, true, nil
			}
		}
		return channelPostRecord{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, post := range s.posts {
		if post.PostID == postID && (channelID == "" || post.ChannelID == channelID) {
			return post, true, nil
		}
	}
	return channelPostRecord{}, false, nil
}

func (s *Service) channelCommentByID(ctx context.Context, commentID, postID string) (channelCommentRecord, bool, error) {
	if s.store != nil {
		comments, err := s.store.ListChannelComments(ctx, postID)
		if err != nil {
			return channelCommentRecord{}, false, err
		}
		for _, comment := range comments {
			if comment.CommentID == commentID {
				return comment, true, nil
			}
		}
		return channelCommentRecord{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, comment := range s.comments {
		if comment.CommentID == commentID && (postID == "" || comment.PostID == postID) {
			return comment, true, nil
		}
	}
	return channelCommentRecord{}, false, nil
}

func (s *Service) roomIDForChannelComment(ctx context.Context, comment channelCommentRecord, fallbackRoomID string) (string, error) {
	if fallbackRoomID != "" {
		return fallbackRoomID, nil
	}
	if comment.PostID != "" {
		post, ok, err := s.channelPostByID(ctx, comment.PostID, comment.ChannelID)
		if err != nil {
			return "", err
		}
		if ok && post.RoomID != "" {
			return post.RoomID, nil
		}
	}
	if comment.ChannelID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, comment.ChannelID, "")
		if err != nil {
			return "", err
		}
		if ok {
			return ch.RoomID, nil
		}
	}
	return "", nil
}

func (s *Service) authorizeChannelContentRecall(ctx context.Context, roomID, authorMXID string) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if ownerMXID != "" && ownerMXID == authorMXID {
		return nil
	}
	if apiErr := s.requireOwnerMember(ctx, roomID); apiErr != nil {
		if apiErr.Status != http.StatusForbidden {
			return apiErr
		}
		return statusError(http.StatusForbidden, "content author or channel owner role is required")
	}
	return nil
}

func (s *Service) channelReaction(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	reactionName := fallbackString(trimString(params["reaction"]), "like")
	targetType := "post"
	targetID := trimString(params["post_id"])
	roomID := trimString(params["room_id"])
	eventID := ""
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	commentID := trimString(params["comment_id"])
	if action == "channels.comment_reaction.toggle" {
		targetType = "comment"
		targetID = commentID
	}
	if targetID == "" {
		return nil, badRequest(targetType + "_id is required")
	}
	if targetType == "post" {
		post, ok, err := s.channelPostByID(ctx, targetID, channelID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			eventID = post.EventID
			roomID = fallbackString(roomID, post.RoomID)
			channelID = fallbackString(channelID, post.ChannelID)
			postID = post.PostID
		}
	} else {
		comment, ok, err := s.channelCommentByID(ctx, targetID, postID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			eventID = comment.EventID
			channelID = fallbackString(channelID, comment.ChannelID)
			postID = fallbackString(postID, comment.PostID)
			commentID = comment.CommentID
			resolvedRoomID, err := s.roomIDForChannelComment(ctx, comment, roomID)
			if err != nil {
				return nil, internalError(err)
			}
			roomID = resolvedRoomID
		}
	}
	s.mu.Lock()
	userID := s.ownerMXID
	s.mu.Unlock()
	existing, ok, err := s.getReaction(ctx, targetType, targetID, reactionName, userID)
	if err != nil {
		return nil, internalError(err)
	}
	record := reactionRecord{
		TargetType: targetType,
		TargetID:   targetID,
		ChannelID:  channelID,
		PostID:     postID,
		CommentID:  commentID,
		Reaction:   reactionName,
		UserID:     userID,
		Active:     true,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	if ok {
		record = existing
		record.Active = !existing.Active
	}
	if record.Active && s.transport != nil && roomID != "" && eventID != "" {
		_, sendErr := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  userID,
			RoomID:      roomID,
			EventType:   "m.reaction",
			MessageType: "m.reaction",
			Timestamp:   time.Now().UTC(),
			Content: map[string]any{
				"m.relates_to": map[string]any{
					"rel_type": "m.annotation",
					"event_id": eventID,
					"key":      reactionName,
				},
				"channel_id": channelID,
				"post_id":    postID,
				"comment_id": commentID,
				"reaction":   reactionName,
			},
		})
		if sendErr != nil {
			return nil, transportWriteError(sendErr)
		}
	}
	if saveErr := s.saveReaction(ctx, record); saveErr != nil {
		return nil, internalError(saveErr)
	}
	count, countErr := s.countActiveReactions(ctx, targetType, targetID, reactionName)
	if countErr != nil {
		return nil, internalError(countErr)
	}
	result := map[string]any{
		"post_id":        record.PostID,
		"comment_id":     record.CommentID,
		"channel_id":     record.ChannelID,
		"reaction":       record.Reaction,
		"active":         record.Active,
		"reaction_count": count,
	}
	if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) attachChannelPostOperation(ctx context.Context, post *channelPostRecord, action, status, roomID string) error {
	operation, conversation, err := s.conversationOperation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	post.Operation = operation
	post.Conversation = conversation
	return nil
}

func (s *Service) attachChannelCommentOperation(ctx context.Context, comment *channelCommentRecord, action, status, roomID string) error {
	operation, conversation, err := s.conversationOperation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	comment.Operation = operation
	comment.Conversation = conversation
	return nil
}

func (s *Service) saveMember(ctx context.Context, member memberRecord) error {
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	var stored memberRecord
	var hasStored bool
	if s.store != nil && member.RoomID != "" && member.UserID != "" {
		var err error
		stored, hasStored, err = s.store.LookupMember(ctx, member.RoomID, member.UserID)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	if existing, ok := s.members[member.RoomID+"|"+member.UserID]; ok && existing.JoinedAt > 0 {
		mergeMemberPersistence(&member, existing)
	} else if hasStored {
		mergeMemberPersistence(&member, stored)
	}
	s.members[member.RoomID+"|"+member.UserID] = member
	if member.ChannelID == "" {
		s.refreshGroupCountsLocked(member.RoomID)
	} else {
		s.refreshChannelCountsLocked(member.ChannelID)
	}
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertMember(ctx, member); err != nil {
			return err
		}
		if member.ChannelID == "" {
			return s.refreshStoredGroupCounts(ctx, member.RoomID)
		}
		return s.refreshStoredChannelCounts(ctx, member.ChannelID)
	}
	return nil
}

func mergeMemberPersistence(member *memberRecord, existing memberRecord) {
	if existing.JoinedAt > 0 {
		member.JoinedAt = existing.JoinedAt
	}
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = existing.RequesterNodeBaseURL
	}
	if memberRemoved(existing.Membership) && memberLeft(member.Membership) {
		member.Membership = existing.Membership
	}
	if elevatedMemberRole(existing.Role) &&
		!elevatedMemberRole(member.Role) &&
		!memberHidden(existing.Membership) &&
		!memberHidden(member.Membership) {
		member.Role = existing.Role
	}
}

func elevatedMemberRole(role string) bool {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "owner", "admin":
		return true
	default:
		return false
	}
}

func (s *Service) repairLocalChannelOwnerRoles(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	serverName := s.serverName
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return nil
	}
	channels, err := s.store.ListChannels(ctx)
	if err != nil {
		return err
	}
	for _, ch := range channels {
		if !strings.EqualFold(domainFromMatrixID(ch.RoomID, "!"), serverName) {
			continue
		}
		member, ok, err := s.store.LookupMember(ctx, ch.RoomID, ownerMXID)
		if err != nil {
			return err
		}
		if !ok {
			if err := s.saveOwnerMember(ctx, ch.RoomID, ch.ChannelID); err != nil {
				return err
			}
			continue
		}
		if memberHidden(member.Membership) || elevatedMemberRole(member.Role) {
			continue
		}
		member.ChannelID = fallbackString(member.ChannelID, ch.ChannelID)
		member.Role = "owner"
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) setProductMemberMute(ctx context.Context, roomID, channelID string, muted bool) error {
	members, err := s.membersForProduct(ctx, roomID, channelID)
	if err != nil {
		return err
	}
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		if strings.EqualFold(member.Role, "owner") || strings.EqualFold(member.Role, "admin") {
			continue
		}
		member.Muted = muted
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
		if apiErr := s.publishMemberPolicyState(ctx, member); apiErr != nil {
			return fmt.Errorf(apiErr.Error)
		}
	}
	return nil
}

func (s *Service) membersForProduct(ctx context.Context, roomID, channelID string) ([]memberRecord, error) {
	if s.store != nil {
		return s.store.ListMembers(ctx, roomID, channelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if roomID != "" && member.RoomID != roomID {
			continue
		}
		if channelID != "" && member.ChannelID != channelID {
			continue
		}
		members = append(members, member)
	}
	sortMembersByJoinOrder(members)
	return members, nil
}

func (s *Service) inviteMembers(ctx context.Context, scope string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	users := memberIDsFromParams(params)
	if len(users) == 0 {
		return nil, badRequest("user_id is required")
	}
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, scope, roomID, channelID)
	if apiErr != nil {
		return nil, apiErr
	}
	members := make([]memberRecord, 0, len(users))
	for _, userID := range users {
		member := s.memberRecordFor(roomID, channelID, userID)
		member.Membership = "invite"
		if scope == "group" {
			member.ChannelID = ""
		}
		applyMemberProfileParams(&member, params)
		if s.transport != nil {
			s.mu.Lock()
			inviterMXID := s.ownerMXID
			s.mu.Unlock()
			if err := s.transport.InviteUser(ctx, InviteUserRequest{
				RoomID:          member.RoomID,
				InviterMXID:     inviterMXID,
				InviteeMXID:     userID,
				Reason:          trimString(params["reason"]),
				IsDirect:        boolParam(params["is_direct"]),
				InviteRoomState: inviteRoomState,
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		members = append(members, member)
	}
	result := map[string]any{"status": "ok", "members": members}
	if err := s.attachConversationOperation(ctx, result, scope+"s.invite", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

//nolint:gocyclo // Invite grants validate channel, share-room, and Matrix invite side effects together.
func (s *Service) channelInviteGrantCreate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	shareRoomID := trimString(params["share_room_id"])
	if shareRoomID == "" {
		shareRoomID = trimString(params["via_room_id"])
	}
	if shareRoomID == "" {
		return nil, badRequest("share_room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if apiErr := s.requireOwnerMember(ctx, ch.RoomID); apiErr != nil {
		return nil, apiErr
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	ownerShareMember, ok, err := s.lookupMember(ctx, shareRoomID, ownerMXID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(ownerShareMember.Membership), "join") {
		return nil, statusError(403, "owner must be joined to the share room")
	}
	grantID := trimString(params["grant_id"])
	if grantID == "" {
		grantID = "grant_" + randomToken("channel_invite")
	}
	grant := channelInviteGrant{
		GrantID:     grantID,
		ChannelID:   ch.ChannelID,
		RoomID:      ch.RoomID,
		ShareRoomID: shareRoomID,
		CreatedBy:   ownerMXID,
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	if saveErr := s.saveChannelInviteGrant(ctx, grant); saveErr != nil {
		return nil, internalError(saveErr)
	}
	shareMembers, err := s.membersForProduct(ctx, shareRoomID, "")
	if err != nil {
		return nil, internalError(err)
	}
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, "channel", ch.RoomID, ch.ChannelID)
	if apiErr != nil {
		return nil, apiErr
	}
	invited := make([]memberRecord, 0, len(shareMembers))
	for _, shareMember := range shareMembers {
		if shareMember.UserID == "" ||
			shareMember.UserID == ownerMXID ||
			!strings.EqualFold(strings.TrimSpace(shareMember.Membership), "join") {
			continue
		}
		if existing, ok, err := s.lookupMember(ctx, ch.RoomID, shareMember.UserID); err != nil {
			return nil, internalError(err)
		} else if ok && (strings.EqualFold(existing.Membership, "join") || strings.EqualFold(existing.Membership, "invite")) {
			continue
		}
		member := s.memberRecordFor(ch.RoomID, ch.ChannelID, shareMember.UserID)
		member.Membership = "invite"
		member.Role = fallbackString(member.Role, "member")
		member.DisplayName = fallbackString(shareMember.DisplayName, member.DisplayName)
		member.AvatarURL = fallbackString(shareMember.AvatarURL, member.AvatarURL)
		member.Domain = fallbackString(shareMember.Domain, member.Domain)
		if s.transport != nil {
			if err := s.transport.InviteUser(ctx, InviteUserRequest{
				RoomID:          ch.RoomID,
				InviterMXID:     ownerMXID,
				InviteeMXID:     shareMember.UserID,
				Reason:          trimString(params["reason"]),
				InviteRoomState: inviteRoomState,
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		invited = append(invited, member)
	}
	return map[string]any{
		"status":        "ok",
		"grant_id":      grant.GrantID,
		"room_id":       grant.RoomID,
		"channel_id":    grant.ChannelID,
		"share_room_id": grant.ShareRoomID,
		"grant":         grant,
		"channel":       ch,
		"members":       invited,
	}, nil
}

func (s *Service) productInviteRoomState(ctx context.Context, scope, roomID, channelID string) ([]RoomStateEvent, *apiError) {
	switch scope {
	case "group":
		group, ok, err := s.groupByRoom(ctx, roomID)
		if err != nil {
			return nil, transportWriteError(err)
		}
		if !ok {
			group = groupRecord{
				RoomID:       roomID,
				Name:         roomID,
				InvitePolicy: "member",
			}
		}
		return []RoomStateEvent{groupStateEvent(group, false)}, nil
	case "channel":
		ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
		return []RoomStateEvent{channelStateEvent(ch, false)}, nil
	default:
		return nil, nil
	}
}

//nolint:gocyclo // Join flow intentionally keeps Matrix join, invite-card, and projection refresh ordering together.
func (s *Service) joinMember(ctx context.Context, scope string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if scope == "channel" && roomID == "" && channelID == "" {
		if grant, ok, err := s.lookupChannelInviteGrantForParams(ctx, params); err != nil {
			return nil, internalError(err)
		} else if ok {
			roomID = grant.RoomID
			channelID = grant.ChannelID
			params["room_id"] = roomID
			params["channel_id"] = channelID
		}
	}
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	if scope == "channel" && channelID == "" && roomID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			channelID = ch.ChannelID
			params["channel_id"] = channelID
		}
	}
	userID := firstMemberID(params)
	if userID == "" {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
	}
	if scope == "group" {
		if apiErr := s.requireRecordedGroupInviteForCardJoin(ctx, roomID, userID, params); apiErr != nil {
			return nil, apiErr
		}
	}
	if scope == "channel" {
		if apiErr := s.requireChannelInviteGrantForJoin(ctx, roomID, channelID, userID, params); apiErr != nil {
			return nil, apiErr
		}
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok && memberRemoved(existing.Membership) && !removedMemberHasFreshInvite(scope, params) {
		return nil, statusError(403, scope+" member was removed")
	}
	member := existing
	if !ok {
		member = s.memberRecordFor(roomID, channelID, userID)
	}
	member.Membership = "join"
	if scope == "group" {
		member.ChannelID = ""
	}
	applyMemberProfileParams(&member, params)
	s.applyLocalOwnerMemberProfile(&member)
	if s.transport != nil {
		result, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
			RoomIDOrAlias: fallbackString(member.RoomID, member.ChannelID),
			UserMXID:      member.UserID,
			DisplayName:   member.DisplayName,
			AvatarURL:     member.AvatarURL,
			ServerNames:   stringSliceParam(params["server_names"]),
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		member.RoomID = fallbackString(result.RoomID, member.RoomID)
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if scope == "group" {
		if err := s.ensureJoinedGroupRecord(ctx, member, params); err != nil {
			return nil, internalError(err)
		}
	}
	if s.transport != nil {
		if refreshedChannelID, err := s.refreshRoomChannel(ctx, member.RoomID); err != nil {
			return nil, internalError(err)
		} else if refreshedChannelID != "" {
			member.ChannelID = refreshedChannelID
		}
		if err := s.refreshRoomMembers(ctx, member.RoomID, member.ChannelID); err != nil {
			return nil, internalError(err)
		}
	}
	result := map[string]any{"status": "ok", "room_id": member.RoomID, "member": member}
	if scope == "channel" {
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	if err := s.attachConversationOperation(ctx, result, scope+"s.join", "ok", member.RoomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) requireRecordedGroupInviteForCardJoin(ctx context.Context, roomID, userID string, params map[string]any) *apiError {
	if !hasGroupInviteCardParams(params) {
		return nil
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return internalError(err)
	}
	if !ok || (!strings.EqualFold(strings.TrimSpace(existing.Membership), "invite") && !removedMemberHasFreshGroupInvite(existing.Membership, params)) {
		return statusError(403, "group invite is missing or expired")
	}
	return nil
}

func hasGroupInviteCardParams(params map[string]any) bool {
	return trimString(params["invite_event_id"]) != "" || trimString(params["direct_room_id"]) != ""
}

func removedMemberHasFreshInvite(scope string, params map[string]any) bool {
	switch scope {
	case "group":
		return trimString(params["invite_event_id"]) != ""
	case "channel":
		return trimString(params["grant_id"]) != "" ||
			trimString(params["share_room_id"]) != "" ||
			trimString(params["via_room_id"]) != ""
	default:
		return false
	}
}

func removedMemberHasFreshGroupInvite(membership string, params map[string]any) bool {
	return (memberRemoved(membership) || memberLeft(membership)) && trimString(params["invite_event_id"]) != ""
}

func (s *Service) requireChannelInviteGrantForJoin(ctx context.Context, roomID, channelID, userID string, params map[string]any) *apiError {
	if trimString(params["grant_id"]) == "" && trimString(params["share_room_id"]) == "" && trimString(params["via_room_id"]) == "" {
		return nil
	}
	grant, ok, err := s.lookupChannelInviteGrantForParams(ctx, params)
	if err != nil {
		return internalError(err)
	}
	if !ok {
		if existing, memberOK, memberErr := s.lookupMember(ctx, roomID, userID); memberErr != nil {
			return internalError(memberErr)
		} else if memberOK {
			membership := strings.TrimSpace(existing.Membership)
			if strings.EqualFold(membership, "invite") ||
				((memberRemoved(membership) || memberLeft(membership)) && removedMemberHasFreshInvite("channel", params)) {
				return nil
			}
		}
		return statusError(403, "channel invite grant is missing or expired")
	}
	if roomID != "" && grant.RoomID != roomID {
		return statusError(403, "channel invite grant room mismatch")
	}
	if channelID != "" && grant.ChannelID != channelID {
		return statusError(403, "channel invite grant channel mismatch")
	}
	member, ok, err := s.lookupMember(ctx, grant.ShareRoomID, userID)
	if err != nil {
		return internalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
		return statusError(403, "user is not joined to the share room")
	}
	return nil
}

func (s *Service) ensureJoinedGroupRecord(ctx context.Context, member memberRecord, params map[string]any) error {
	if member.RoomID == "" {
		return nil
	}
	group, ok, err := s.groupByRoom(ctx, member.RoomID)
	if err != nil {
		return err
	}
	name := fallbackString(
		trimString(params["name"]),
		fallbackString(trimString(params["group_name"]), trimString(params["room_name"])),
	)
	if !ok {
		group = groupRecord{
			RoomID:       member.RoomID,
			Name:         fallbackString(name, member.RoomID),
			Topic:        trimString(params["topic"]),
			AvatarURL:    trimString(params["avatar_url"]),
			MemberCount:  1,
			InvitePolicy: fallbackString(trimString(params["invite_policy"]), "member"),
		}
		return s.saveGroup(ctx, group)
	}
	changed := false
	if name != "" && group.Name != name {
		group.Name = name
		changed = true
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" && group.AvatarURL != avatarURL {
		group.AvatarURL = avatarURL
		changed = true
	}
	if topic := trimString(params["topic"]); topic != "" && group.Topic != topic {
		group.Topic = topic
		changed = true
	}
	if group.MemberCount == 0 {
		group.MemberCount = 1
		changed = true
	}
	if !changed {
		return nil
	}
	return s.saveGroup(ctx, group)
}

func (s *Service) refreshRoomChannel(ctx context.Context, roomID string) (string, error) {
	if s.transport == nil || roomID == "" {
		return "", nil
	}
	ch, ok, err := s.transport.GetRoomChannel(ctx, roomID)
	if err != nil || !ok {
		return "", err
	}
	if existing, exists, lookupErr := s.channelByIDOrRoom(ctx, ch.ChannelID, ch.RoomID); lookupErr != nil {
		return "", lookupErr
	} else if exists {
		mergeRefreshedChannel(&ch, existing)
	}
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return "", err
		}
	}
	return ch.ChannelID, nil
}

func (s *Service) refreshRoomMembers(ctx context.Context, roomID, channelID string) error {
	if s.transport == nil || roomID == "" {
		return nil
	}
	members, err := s.transport.ListRoomMembers(ctx, roomID)
	if err != nil {
		return err
	}
	if channelID == "" {
		channelID = s.channelIDForRoom(ctx, roomID)
	}
	for _, member := range members {
		member.RoomID = roomID
		if member.ChannelID == "" {
			member.ChannelID = channelID
		}
		if member.Membership == "" {
			member.Membership = "join"
		}
		if member.Role == "" {
			member.Role = "member"
		}
		if existing, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID); lookupErr != nil {
			return lookupErr
		} else if ok {
			mergeRefreshedMember(&member, existing)
		}
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
	}
	return nil
}

func mergeRefreshedChannel(ch *channel, existing channel) {
	if ch.Name == "" {
		ch.Name = existing.Name
	}
	if ch.Description == "" {
		ch.Description = existing.Description
	}
	if ch.AvatarURL == "" {
		ch.AvatarURL = existing.AvatarURL
	}
	if ch.Visibility == "" {
		ch.Visibility = existing.Visibility
	}
	if ch.JoinPolicy == "" {
		ch.JoinPolicy = existing.JoinPolicy
	}
	if ch.ChannelType == "" {
		ch.ChannelType = existing.ChannelType
	}
	if !ch.CommentsEnabled && existing.CommentsEnabled {
		ch.CommentsEnabled = true
	}
	if !ch.Muted && existing.Muted {
		ch.Muted = true
	}
}

func mergeRefreshedMember(member *memberRecord, existing memberRecord) {
	if member.DisplayName == "" {
		member.DisplayName = existing.DisplayName
	}
	if member.AvatarURL == "" {
		member.AvatarURL = existing.AvatarURL
	}
	if member.Domain == "" {
		member.Domain = existing.Domain
	}
	if !member.Muted && existing.Muted {
		member.Muted = true
	}
}

//nolint:gocyclo // Public channel join requests branch across local and remote-node request flows.
func (s *Service) channelJoinRequest(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if userID == "" {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
		params["user_mxid"] = userID
	}
	if remote, handled, apiErr := s.remoteChannelJoinRequest(ctx, params); apiErr != nil {
		return nil, apiErr
	} else if handled {
		return remote, nil
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	roomID = ch.RoomID
	channelID = ch.ChannelID
	if !strings.EqualFold(ch.Visibility, "public") {
		return nil, statusError(403, "channel is private")
	}
	if strings.EqualFold(ch.JoinPolicy, "invite") {
		return nil, statusError(403, "channel requires invite")
	}
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok && memberRemoved(existing.Membership) {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		if apiErr := s.publishJoinRequestState(ctx, roomID, userID, "rejected", trimString(params["reason"])); apiErr != nil {
			return nil, apiErr
		}
		return map[string]any{"status": "rejected", "member": existing}, nil
	}
	member := existing
	if !ok {
		member = s.memberRecordFor(roomID, channelID, userID)
	}
	status := "pending"
	member.Membership = "pending"
	if strings.EqualFold(ch.JoinPolicy, "open") {
		status = "approved"
		member.Membership = "approved"
	}
	member.Role = fallbackString(member.Role, "member")
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = fallbackString(trimString(params["requester_node_base_url"]), trimString(params["applicant_node_base_url"]))
	}
	applyMemberProfileParams(&member, params)
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	stateStatus := "pending"
	if strings.EqualFold(ch.JoinPolicy, "open") {
		stateStatus = "approved"
	}
	if apiErr := s.publishJoinRequestState(ctx, roomID, userID, stateStatus, trimString(params["reason"])); apiErr != nil {
		return nil, apiErr
	}
	if strings.EqualFold(ch.JoinPolicy, "open") {
		result, apiErr := s.completeApprovedChannelJoin(ctx, member, params)
		if apiErr != nil {
			return nil, apiErr
		}
		result["channel"] = s.channelSnapshot(ctx, channelID)
		return result, nil
	}
	ch.MemberStatus = member.Membership
	ch.Role = fallbackString(member.Role, "member")
	ch.IsOwned = strings.EqualFold(ch.Role, "owner") || strings.EqualFold(ch.Role, "admin")
	return map[string]any{"status": status, "member": member, "channel": ch}, nil
}

func (s *Service) channelJoinResult(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	userID := fallbackString(firstMemberID(params), ownerMXID)
	if userID != ownerMXID {
		return nil, statusError(http.StatusForbidden, "join result user must be local owner")
	}
	member, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "join request not found")
	}
	switch strings.ToLower(strings.TrimSpace(member.Membership)) {
	case "pending", "approved", "joining", "join_failed":
	default:
		return nil, statusError(404, "join request not found")
	}
	if channelID != "" {
		member.ChannelID = channelID
	}
	s.applyLocalOwnerMemberProfile(&member)
	switch strings.ToLower(trimString(params["status"])) {
	case "rejected":
		member.Membership = "reject"
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		_ = s.appendP2PEvent(ctx, p2pEvent{
			Type:    "channel.join_request.changed",
			RoomID:  member.RoomID,
			Payload: map[string]any{"user_id": member.UserID, "status": "rejected", "channel_id": member.ChannelID},
		})
		return map[string]any{"status": "rejected", "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	case "approved", "joining", "joined":
		return s.completeApprovedChannelJoin(ctx, member, params)
	default:
		return nil, badRequest("status must be approved or rejected")
	}
}

func (s *Service) completeApprovedChannelJoin(ctx context.Context, member memberRecord, params map[string]any) (map[string]any, *apiError) {
	if member.UserID == "" {
		return nil, badRequest("user_id is required")
	}
	if domainFromMXID(member.UserID) != s.serverName {
		return s.notifyRemoteChannelJoinResult(ctx, member, "approved", params)
	}
	member.Membership = "joining"
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if s.transport == nil {
		member.Membership = "approved"
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"status": "approved", "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	result, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: member.RoomID,
		UserMXID:      member.UserID,
		DisplayName:   member.DisplayName,
		AvatarURL:     member.AvatarURL,
		ServerNames:   stringSliceParam(params["server_names"]),
	})
	if err != nil {
		member.Membership = "join_failed"
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": "join_failed", "member": member, "error": err.Error(), "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	if result.RoomID != "" {
		member.RoomID = result.RoomID
	}
	member.Membership = "join"
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if refreshedChannelID, err := s.refreshRoomChannel(ctx, member.RoomID); err != nil {
		return nil, internalError(err)
	} else if refreshedChannelID != "" {
		member.ChannelID = refreshedChannelID
	}
	if err := s.refreshRoomMembers(ctx, member.RoomID, member.ChannelID); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"status": "joined", "room_id": member.RoomID, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
}

func (s *Service) notifyRemoteChannelJoinResult(ctx context.Context, member memberRecord, status string, params map[string]any) (map[string]any, *apiError) {
	base := trimString(params["requester_node_base_url"])
	if base == "" {
		base = trimString(params["applicant_node_base_url"])
	}
	if base == "" {
		base = member.RequesterNodeBaseURL
	}
	if base == "" {
		switch status {
		case "approved":
			member.Membership = "approved"
		case "rejected":
			member.Membership = "reject"
		default:
			member.Membership = status
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		resultStatus := member.Membership
		if status == "rejected" {
			resultStatus = "rejected"
		}
		return map[string]any{"status": resultStatus, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	remoteParams := map[string]any{
		"room_id":              member.RoomID,
		"channel_id":           member.ChannelID,
		"user_id":              member.UserID,
		"status":               status,
		"reason":               trimString(params["reason"]),
		"request_id":           trimString(params["request_id"]),
		"server_names":         stringSliceParam(params["server_names"]),
		"remote_node_base_url": base,
	}
	var remote map[string]any
	httpStatus, err := s.remotePublicAction(ctx, domainFromMXID(member.UserID), "channels.public.join_result", remoteParams, &remote)
	if err != nil {
		if status == "approved" {
			member.Membership = "join_failed"
		} else {
			member.Membership = "reject"
		}
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": member.Membership, "member": member, "error": err.Error(), "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	if httpStatus != http.StatusOK {
		if status == "approved" {
			member.Membership = "join_failed"
		} else {
			member.Membership = "reject"
		}
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": member.Membership, "member": member, "error": "target node join result failed", "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	remoteStatus := fallbackString(trimString(remote["status"]), status)
	switch remoteStatus {
	case "joined":
		member.Membership = "join"
	case "rejected":
		member.Membership = "reject"
	default:
		member.Membership = remoteStatus
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	remote["member"] = member
	remote["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	return remote, nil
}

func (s *Service) publicP2PBaseURL() string {
	base, ok := normalizeRemoteNodeBaseURL(strings.TrimRight(s.homeserver, "/") + "/_p2p")
	if !ok {
		return ""
	}
	return base.String()
}

func cloneParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

//nolint:gocyclo // Member mutations share Matrix side effects and product state publication in one endpoint.
func (s *Service) memberMutation(ctx context.Context, scope, action string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if strings.HasSuffix(action, ".leave") || action == "groups.leave" || action == "channels.leave" || strings.Contains(action, ".invite.reject") {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
	}
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	member := s.memberRecordFor(roomID, channelID, userID)
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok {
		member = existing
		if channelID != "" {
			member.ChannelID = channelID
		}
	}
	if strings.Contains(action, ".join_request.") {
		if !ok || !joinRequestMutationAllowed(action, existing.Membership) {
			return nil, statusError(404, "join request not found")
		}
	}
	if strings.Contains(action, ".invite.reject") {
		if !ok || !strings.EqualFold(strings.TrimSpace(existing.Membership), "invite") {
			return nil, statusError(404, scope+" invite not found")
		}
	}
	if scope == "group" {
		member.ChannelID = ""
	}
	if (strings.HasSuffix(action, ".leave") || strings.Contains(action, ".remove")) && strings.EqualFold(member.Role, "owner") {
		return nil, statusError(409, scope+" owner cannot leave; dissolve the "+scope+" instead")
	}
	switch {
	case strings.Contains(action, ".remove"):
		member.Membership = "remove"
		if s.transport != nil {
			s.mu.Lock()
			senderMXID := s.ownerMXID
			s.mu.Unlock()
			if err := s.transport.KickUser(ctx, KickUserRequest{
				RoomID:     member.RoomID,
				SenderMXID: senderMXID,
				TargetMXID: member.UserID,
				Reason:     trimString(params["reason"]),
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
	case strings.HasSuffix(action, ".leave"):
		member.Membership = "leave"
		if s.transport != nil {
			if err := s.transport.LeaveRoom(ctx, LeaveRoomRequest{
				RoomID:   member.RoomID,
				UserMXID: member.UserID,
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
	case strings.Contains(action, ".unmute"):
		member.Membership = fallbackString(member.Membership, "join")
		member.Muted = false
	case strings.Contains(action, ".mute"):
		member.Membership = fallbackString(member.Membership, "join")
		member.Muted = true
	case strings.Contains(action, ".approve"):
		member.Membership = "approved"
	case strings.Contains(action, ".reject"):
		member.Membership = "reject"
	default:
		member.Membership = fallbackString(member.Membership, "join")
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if strings.Contains(action, ".mute") || strings.Contains(action, ".unmute") {
		if apiErr := s.publishMemberPolicyState(ctx, member); apiErr != nil {
			return nil, apiErr
		}
	}
	if strings.Contains(action, ".join_request.") {
		stateStatus := ""
		if strings.Contains(action, ".approve") {
			stateStatus = "approved"
		}
		if strings.Contains(action, ".reject") {
			stateStatus = "rejected"
		}
		if stateStatus != "" {
			if apiErr := s.publishJoinRequestState(ctx, member.RoomID, member.UserID, stateStatus, trimString(params["reason"])); apiErr != nil {
				return nil, apiErr
			}
		}
		if strings.Contains(action, ".approve") {
			result, apiErr := s.completeApprovedChannelJoin(ctx, member, params)
			if apiErr != nil {
				return nil, apiErr
			}
			status := fallbackString(trimString(result["status"]), "approved")
			if err := s.attachConversationOperation(ctx, result, action, status, member.RoomID); err != nil {
				return nil, internalError(err)
			}
			return result, nil
		}
		if strings.Contains(action, ".reject") && domainFromMXID(member.UserID) != s.serverName {
			result, apiErr := s.notifyRemoteChannelJoinResult(ctx, member, "rejected", params)
			if apiErr != nil {
				return nil, apiErr
			}
			result["status"] = "rejected"
			if err := s.attachConversationOperation(ctx, result, action, "rejected", member.RoomID); err != nil {
				return nil, internalError(err)
			}
			return result, nil
		}
	}
	result := map[string]any{"status": "ok", "member": member}
	if strings.Contains(action, ".invite.reject") {
		result["status"] = "rejected"
	}
	if strings.Contains(action, ".join_request.") {
		if strings.Contains(action, ".approve") {
			result["status"] = "approved"
		}
		if strings.Contains(action, ".reject") {
			result["status"] = "rejected"
		}
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	status := fallbackString(trimString(result["status"]), "ok")
	if err := s.attachConversationOperation(ctx, result, action, status, member.RoomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func joinRequestMutationAllowed(action, membership string) bool {
	membership = strings.ToLower(strings.TrimSpace(membership))
	if strings.Contains(action, ".approve") {
		switch membership {
		case "pending", "approved", "join_failed":
			return true
		default:
			return false
		}
	}
	return membership == "pending"
}

func (s *Service) lookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	if roomID == "" || userID == "" {
		return memberRecord{}, false, nil
	}
	if s.store != nil {
		return s.store.LookupMember(ctx, roomID, userID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	member, ok := s.members[roomID+"|"+userID]
	return member, ok, nil
}

func (s *Service) requireOwnerMember(ctx context.Context, roomID string) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	member, ok, err := s.lookupMember(ctx, roomID, ownerMXID)
	if err != nil {
		return internalError(err)
	}
	if !ok || !strings.EqualFold(member.Role, "owner") || memberHidden(member.Membership) {
		return statusError(403, "owner role is required")
	}
	return nil
}

func (s *Service) memberList(ctx context.Context, params map[string]any) any {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	status := fallbackString(trimString(params["status"]), trimString(params["membership"]))
	role := trimString(params["role"])
	if s.store != nil {
		members, err := s.store.ListMembers(ctx, roomID, channelID)
		if err == nil {
			return map[string]any{"members": filterMembers(members, status, role)}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if roomID != "" && member.RoomID != roomID {
			continue
		}
		if channelID != "" && member.ChannelID != channelID {
			continue
		}
		if memberHidden(member.Membership) {
			continue
		}
		members = append(members, member)
	}
	sortMembersByJoinOrder(members)
	return map[string]any{"members": filterMembers(members, status, role)}
}

func filterMembers(members []memberRecord, status, role string) []memberRecord {
	if status == "" && role == "" {
		sortMembersByJoinOrder(members)
		return members
	}
	filtered := make([]memberRecord, 0, len(members))
	for _, member := range members {
		if status != "" && !strings.EqualFold(member.Membership, status) {
			continue
		}
		if role != "" && !strings.EqualFold(member.Role, role) {
			continue
		}
		filtered = append(filtered, member)
	}
	sortMembersByJoinOrder(filtered)
	return filtered
}

func sortMembersByJoinOrder(members []memberRecord) {
	sort.SliceStable(members, func(i, j int) bool {
		left, right := members[i], members[j]
		if left.JoinedAt != right.JoinedAt {
			if left.JoinedAt == 0 {
				return false
			}
			if right.JoinedAt == 0 {
				return true
			}
			return left.JoinedAt < right.JoinedAt
		}
		return left.UserID < right.UserID
	})
}

func (s *Service) memberTarget(params map[string]any) (string, string) {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID != "" {
		s.mu.Lock()
		if ch, ok := s.channels[channelID]; ok {
			roomID = ch.RoomID
		}
		s.mu.Unlock()
	}
	if channelID == "" && roomID != "" {
		s.mu.Lock()
		for _, ch := range s.channels {
			if ch.RoomID == roomID {
				channelID = ch.ChannelID
				break
			}
		}
		s.mu.Unlock()
	}
	return roomID, channelID
}

func (s *Service) memberRecordFor(roomID, channelID, userID string) memberRecord {
	s.mu.Lock()
	if existing, ok := s.members[roomID+"|"+userID]; ok {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		s.mu.Unlock()
		return existing
	}
	s.mu.Unlock()
	return memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      userID,
		DisplayName: displayNameFromMXID(userID),
		Domain:      domainFromMXID(userID),
		Membership:  "join",
		Role:        "member",
	}
}

func applyMemberProfileParams(member *memberRecord, params map[string]any) {
	if displayName := trimString(params["display_name"]); displayName != "" {
		member.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		member.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		member.Domain = domain
	}
}

func (s *Service) applyLocalOwnerMemberProfile(member *memberRecord) {
	if member == nil {
		return
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	profile := s.profile
	serverName := s.serverName
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" || member.UserID != ownerMXID {
		return
	}
	if displayName := strings.TrimSpace(profile.DisplayName); displayName != "" {
		member.DisplayName = displayName
	}
	if avatarURL := strings.TrimSpace(profile.AvatarURL); avatarURL != "" {
		member.AvatarURL = avatarURL
	}
	if member.Domain == "" {
		member.Domain = serverName
	}
}

func (s *Service) getReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error) {
	if s.store != nil {
		return s.store.GetReaction(ctx, targetType, targetID, reaction, userID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.reactions[reactionKey(targetType, targetID, reaction, userID)]
	return record, ok, nil
}

func (s *Service) saveReaction(ctx context.Context, record reactionRecord) error {
	s.mu.Lock()
	s.reactions[reactionKey(record.TargetType, record.TargetID, record.Reaction, record.UserID)] = record
	s.mu.Unlock()
	if s.store != nil {
		return s.store.UpsertReaction(ctx, record)
	}
	return nil
}

func (s *Service) countActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error) {
	if s.store != nil {
		return s.store.CountActiveReactions(ctx, targetType, targetID, reaction)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, record := range s.reactions {
		if record.TargetType == targetType && record.TargetID == targetID && record.Reaction == reaction && record.Active {
			count++
		}
	}
	return count, nil
}

func (s *Service) myReactions(ctx context.Context) any {
	s.mu.Lock()
	userID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		reactions, err := s.store.ListReactions(ctx, userID)
		if err == nil {
			return map[string]any{"reactions": reactions}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reactions := make([]reactionRecord, 0, len(s.reactions))
	for _, record := range s.reactions {
		if record.UserID == userID && record.Active {
			reactions = append(reactions, record)
		}
	}
	return map[string]any{"reactions": reactions}
}

func (s *Service) listContacts(ctx context.Context) ([]contactRecord, error) {
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return nil, err
	}
	visible := make([]contactRecord, 0, len(contacts))
	for _, contact := range contacts {
		if contactDeleted(contact.Status) {
			continue
		}
		visible = append(visible, contact)
	}
	return dedupeContactsByPeer(visible), nil
}

func dedupeContactsByPeer(contacts []contactRecord) []contactRecord {
	if len(contacts) <= 1 {
		return contacts
	}
	byPeer := make(map[string]contactRecord, len(contacts))
	for _, contact := range contacts {
		key := strings.TrimSpace(contact.PeerMXID)
		if key == "" {
			key = strings.TrimSpace(contact.RoomID)
		}
		if key == "" {
			continue
		}
		existing, ok := byPeer[key]
		if !ok || contactStatusRank(contact.Status) > contactStatusRank(existing.Status) {
			byPeer[key] = contact
			continue
		}
		if contactStatusRank(contact.Status) == contactStatusRank(existing.Status) {
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
	result := make([]contactRecord, 0, len(byPeer))
	for _, contact := range byPeer {
		result = append(result, contact)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := strings.ToLower(result[i].DisplayName), strings.ToLower(result[j].DisplayName)
		if left == right {
			return result[i].PeerMXID < result[j].PeerMXID
		}
		return left < right
	})
	return result
}

func contactStatusRank(status string) int {
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

func (s *Service) rawContacts(ctx context.Context) ([]contactRecord, error) {
	if s.store != nil {
		return s.store.ListContacts(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	contacts := make([]contactRecord, 0, len(s.contacts))
	for _, contact := range s.contacts {
		contacts = append(contacts, contact)
	}
	return contacts, nil
}

func (s *Service) lookupContactByRoom(ctx context.Context, roomID string) (contactRecord, bool, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return contactRecord{}, false, nil
	}
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return contactRecord{}, false, err
	}
	for _, contact := range contacts {
		if contact.RoomID == roomID {
			return contact, true, nil
		}
	}
	return contactRecord{}, false, nil
}

func (s *Service) lookupContactByPeer(ctx context.Context, peerMXID string) (contactRecord, bool, error) {
	peerMXID = strings.TrimSpace(peerMXID)
	if peerMXID == "" {
		return contactRecord{}, false, nil
	}
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return contactRecord{}, false, err
	}
	var found contactRecord
	for _, contact := range contacts {
		if contact.PeerMXID == peerMXID {
			if found.PeerMXID == "" || contactStatusRank(contact.Status) > contactStatusRank(found.Status) {
				found = contact
			}
		}
	}
	if found.PeerMXID != "" {
		return found, true, nil
	}
	return contactRecord{}, false, nil
}

func (s *Service) listGroups(ctx context.Context) ([]groupRecord, error) {
	if s.store != nil {
		return s.store.ListGroups(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make([]groupRecord, 0, len(s.groups))
	for _, group := range s.groups {
		groups = append(groups, group)
	}
	return groups, nil
}

func (s *Service) listChannels(ctx context.Context) ([]channel, error) {
	if s.store != nil {
		return s.store.ListChannels(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channels := make([]channel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels, nil
}

func (s *Service) joinedGroupsForOwner(ctx context.Context, groups []groupRecord) ([]groupRecord, error) {
	if len(groups) == 0 {
		return groups, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return []groupRecord{}, nil
	}
	members, err := s.membersForUser(ctx, ownerMXID)
	if err != nil {
		return nil, err
	}
	joinedByRoom := make(map[string]bool, len(members))
	for _, member := range members {
		if member.ChannelID == "" && strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			joinedByRoom[member.RoomID] = true
		}
	}
	visible := make([]groupRecord, 0, len(groups))
	for _, group := range groups {
		if joinedByRoom[group.RoomID] {
			visible = append(visible, group)
		}
	}
	return visible, nil
}

func (s *Service) joinedChannelsForOwner(ctx context.Context, channels []channel) ([]channel, error) {
	if len(channels) == 0 {
		return channels, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return []channel{}, nil
	}
	members, err := s.membersForUser(ctx, ownerMXID)
	if err != nil {
		return nil, err
	}
	ownerByChannelID := make(map[string]memberRecord, len(members))
	ownerByRoomID := make(map[string]memberRecord, len(members))
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			continue
		}
		if member.ChannelID != "" {
			ownerByChannelID[member.ChannelID] = member
		}
		if member.RoomID != "" {
			ownerByRoomID[member.RoomID] = member
		}
	}
	visible := make([]channel, 0, len(channels))
	for _, ch := range channels {
		member, ok := ownerByChannelID[ch.ChannelID]
		if !ok {
			member, ok = ownerByRoomID[ch.RoomID]
		}
		if !ok {
			continue
		}
		role := fallbackString(member.Role, "member")
		ch.Role = role
		ch.MemberStatus = "join"
		ch.IsOwned = strings.EqualFold(role, "owner") || strings.EqualFold(role, "admin")
		visible = append(visible, ch)
	}
	return visible, nil
}

func (s *Service) sessionLocked() map[string]any {
	accountInitialized := s.accountInitializedLocked()
	return map[string]any{
		"access_token":             s.adminToken,
		"device_id":                cleanMatrixDeviceID(s.matrixDeviceID),
		"agent_token":              s.agentToken,
		"user_id":                  s.ownerMXID,
		"homeserver":               s.homeserver,
		"agent_room_id":            s.agentRoomID,
		"password":                 s.password,
		"initialized":              s.initialized,
		"password_initialized":     s.passwordInitialized,
		"profile_initialized":      s.profileInitialized,
		"account_initialized":      accountInitialized,
		"setup_completed":          accountInitialized,
		"already_initialized":      accountInitialized,
		"initialization_completed": accountInitialized,
	}
}

func (s *Service) portalStateLocked() portalState {
	return portalState{
		Initialized:         s.initialized,
		Password:            s.password,
		AdminToken:          s.adminToken,
		MatrixToken:         s.adminToken,
		MatrixDeviceID:      cleanMatrixDeviceID(s.matrixDeviceID),
		AgentToken:          s.agentToken,
		OwnerMXID:           s.ownerMXID,
		AgentRoomID:         s.agentRoomID,
		PasswordInitialized: s.passwordInitialized,
		ProfileInitialized:  s.profileInitialized,
		Profile:             s.profile,
	}
}

func (s *Service) portalProfileInitializedLocked() bool {
	return strings.TrimSpace(s.profile.DisplayName) != ""
}

func (s *Service) accountInitializedLocked() bool {
	return s.initialized && s.passwordInitialized && s.profileInitialized
}

func trimString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

func contactRequestRemark(params map[string]any) string {
	for _, key := range []string{"remark", "request_message", "message", "reason"} {
		if value := trimString(params[key]); value != "" {
			return value
		}
	}
	return ""
}

func int64Param(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	default:
		return 0
	}
}

func callTimeParam(values ...any) string {
	for _, value := range values {
		switch v := value.(type) {
		case string:
			trimmed := strings.TrimSpace(v)
			if trimmed == "" {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
				return parsed.UTC().Format(time.RFC3339Nano)
			}
		default:
			millis := int64Param(v)
			if millis > 0 {
				return time.UnixMilli(millis).UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return ""
}

func int64SliceParam(value any) []int64 {
	switch v := value.(type) {
	case []int64:
		return append([]int64{}, v...)
	case []int:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			result = append(result, int64(item))
		}
		return result
	case []float64:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			result = append(result, int64(item))
		}
		return result
	case []any:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			if n := int64Param(item); n != 0 {
				result = append(result, n)
			}
		}
		return result
	default:
		if n := int64Param(value); n != 0 {
			return []int64{n}
		}
		return nil
	}
}

func stringSliceParam(value any) []string {
	switch v := value.(type) {
	case []string:
		return append([]string{}, v...)
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if text := trimString(item); text != "" {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

func jsonArrayStringParam(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "[]", nil
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return "[]", nil
		}
		var items []any
		if err := json.Unmarshal([]byte(text), &items); err != nil {
			return "", err
		}
		raw, err := json.Marshal(items)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		var items []any
		if err = json.Unmarshal(raw, &items); err != nil {
			return "", err
		}
		normalized, err := json.Marshal(items)
		if err != nil {
			return "", err
		}
		return string(normalized), nil
	}
}

func boolParam(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}

func domainFromMXID(mxid string) string {
	return domainFromMatrixID(mxid, "@")
}

func domainFromMatrixID(id, sigil string) string {
	trimmed := strings.TrimPrefix(id, sigil)
	if idx := strings.Index(trimmed, ":"); idx >= 0 {
		idx += len(id) - len(trimmed)
		if idx+1 >= len(id) {
			return ""
		}
		return id[idx+1:]
	}
	return ""
}

func displayNameFromMXID(mxid string) string {
	localpart := strings.TrimPrefix(mxid, "@")
	if idx := strings.Index(localpart, ":"); idx >= 0 {
		localpart = localpart[:idx]
	}
	return fallbackString(localpart, mxid)
}

func firstMemberID(params map[string]any) string {
	for _, key := range []string{"user_id", "user_mxid", "peer_mxid", "mxid"} {
		if userID := trimString(params[key]); userID != "" {
			return userID
		}
	}
	for _, key := range []string{"user_ids", "user_mxids", "peer_mxids", "invitees"} {
		if values := stringSliceParam(params[key]); len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func memberIDsFromParams(params map[string]any) []string {
	seen := map[string]bool{}
	var users []string
	add := func(userID string) {
		userID = strings.TrimSpace(userID)
		if userID == "" || seen[userID] {
			return
		}
		seen[userID] = true
		users = append(users, userID)
	}
	for _, key := range []string{"user_id", "user_mxid", "peer_mxid", "mxid"} {
		add(trimString(params[key]))
	}
	for _, key := range []string{"user_ids", "user_mxids", "peer_mxids", "invitees", "invite"} {
		for _, userID := range stringSliceParam(params[key]) {
			add(userID)
		}
	}
	return users
}

func memberHidden(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "leave", "left", "remove", "removed", "reject", "rejected", "ban", "banned":
		return true
	default:
		return false
	}
}

func memberRemoved(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "remove", "removed", "ban", "banned":
		return true
	default:
		return false
	}
}

func memberLeft(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "leave", "left":
		return true
	default:
		return false
	}
}

func contactAccepted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "accepted")
}

func contactDeleted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "deleted")
}

func contactPendingInbound(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "pending_inbound")
}

func reactionKey(targetType, targetID, reaction, userID string) string {
	return targetType + "|" + targetID + "|" + reaction + "|" + userID
}

func randomToken(prefix string) string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf[:])
}

func randomNumericPassword() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%08d", time.Now().UnixNano()%100000000)
	}
	for i := range buf {
		buf[i] = '0' + (buf[i] % 10)
	}
	return string(buf[:])
}

func cleanMatrixDeviceID(deviceID string) string {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return matrixPortalDeviceID
	}
	return deviceID
}

func requestedMatrixDeviceID(params map[string]any) string {
	deviceID := strings.TrimSpace(trimString(params["device_id"]))
	if deviceID != "" {
		return cleanMatrixDeviceID(deviceID)
	}
	return "PORTALIM" + strings.TrimPrefix(randomToken("device"), "device_")
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func matrixMessageType(messageType string, media bool) string {
	if !media {
		return "m.text"
	}
	switch strings.TrimSpace(messageType) {
	case "image", "m.image":
		return "m.image"
	case "video", "m.video":
		return "m.video"
	case "audio", "m.audio":
		return "m.audio"
	default:
		return "m.file"
	}
}

func defaultPortalPassword() string {
	if password := strings.TrimSpace(os.Getenv("P2P_PORTAL_PASSWORD")); password != "" {
		return password
	}
	return randomNumericPassword()
}

func portalCredentialsFilePath() string {
	return strings.TrimSpace(os.Getenv("P2P_PORTAL_CREDENTIALS_FILE"))
}

func (s *Service) writePortalCredentialsFile() error {
	path := strings.TrimSpace(portalCredentialsFilePath())
	if path == "" {
		return nil
	}
	path = filepath.Clean(path)
	if path == "." || filepath.Base(path) == "." {
		return fmt.Errorf("portal credentials file path is required")
	}
	s.mu.Lock()
	credentials := portalCredentialsFile{
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		OwnerUserID: s.ownerMXID,
		UserID:      s.ownerMXID,
		Homeserver:  s.homeserver,
		AccessToken: s.adminToken,
		DeviceID:    matrixPortalDeviceID,
		AgentToken:  s.agentToken,
		Password:    s.password,
		AgentRoomID: s.agentRoomID,
	}
	s.mu.Unlock()

	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create portal credentials directory: %w", err)
	}
	temp, err := os.CreateTemp(parent, ".p2p-portal-*.json")
	if err != nil {
		return fmt.Errorf("create portal credentials temp file: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	_ = temp.Chmod(0o600)
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(credentials); err != nil {
		return fmt.Errorf("encode portal credentials: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("flush portal credentials: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close portal credentials: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish portal credentials: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	cleanup = false
	return nil
}
