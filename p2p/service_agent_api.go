package p2p

import (
	"context"
	"fmt"

	agentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agent"
	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentgrpc"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

// NativeAgentRunner remains the public runtime injection boundary while the
// default implementation and action ownership live in internal/agent.
type NativeAgentRunner interface {
	Apply(context.Context, string) error
	Invoke(context.Context, string, map[string]any) (map[string]any, error)
	Stream(context.Context, string, map[string]any, func(nativeagent.Event) error) error
}

type ClosableNativeAgentRunner interface {
	NativeAgentRunner
	Close() error
}

// AgentRuntimeProfileClient exposes only the independent Agent's de-secreted
// immutable catalog selection. ProductCore derives owner identity from its
// authenticated configuration and never forwards model credentials.
type AgentRuntimeProfileClient = agentmodule.RuntimeProfileClient
type AgentRuntimeProfileState = agentmodule.RuntimeProfileState
type AgentRuntimeProfile = agentmodule.RuntimeProfile
type AgentRuntimeProfileUpdate = agentmodule.RuntimeProfileUpdate

// AgentKnowledgeClient owns the complete typed, owner-bound Knowledge action
// family. ProductCore never accepts a caller owner, provider endpoint, model,
// credential, or raw backend selector through this seam.
type AgentKnowledgeClient = agentmodule.KnowledgeClient
type AgentKnowledgeState = agentmodule.KnowledgeState
type AgentKnowledgeConfigUpdate = agentmodule.KnowledgeConfigUpdate
type AgentKnowledgeSourceList = agentmodule.KnowledgeSourceList
type AgentKnowledgeSourcePage = agentmodule.KnowledgeSourcePage
type AgentKnowledgeSource = agentmodule.KnowledgeSource
type AgentKnowledgeSourceDelete = agentmodule.KnowledgeSourceDelete
type AgentKnowledgeUploadStart = agentmodule.KnowledgeUploadStart
type AgentKnowledgeUploadChunk = agentmodule.KnowledgeUploadChunk
type AgentKnowledgeUploadFinish = agentmodule.KnowledgeUploadFinish
type AgentKnowledgeUpload = agentmodule.KnowledgeUpload
type AgentKnowledgeUploadResult = agentmodule.KnowledgeUploadResult
type AgentKnowledgeMemoryCreate = agentmodule.KnowledgeMemoryCreate
type AgentKnowledgeSearch = agentmodule.KnowledgeSearch
type AgentKnowledgeSearchResult = agentmodule.KnowledgeSearchResult
type AgentKnowledgeStatus = agentmodule.KnowledgeStatus

// CloudDeployment is the existing ProductCore projection shape. The alias lets
// setup wire a generic Agent client without exposing p2p/internal packages.
type CloudDeployment = cloudmodule.Deployment
type CloudDeploymentHealthSummary = cloudmodule.DeploymentHealthSummary
type CloudDeploymentHealthProbeCount = cloudmodule.DeploymentHealthProbeCount

// CloudDeploymentReader is intentionally query-only. Service identity alone
// can never acquire approval or lifecycle mutation capability through it.
type CloudDeploymentReader interface {
	ListCloudDeployments(context.Context) ([]CloudDeployment, error)
	GetCloudDeployment(context.Context, string) (CloudDeployment, bool, error)
}

// CloudService is the existing ProductCore managed-service projection shape.
// It remains a compatibility façade; durable managed-service facts belong to
// the independent Agent.
type CloudService = cloudmodule.Service

// CloudServiceReader delegates only managed-service list/get to the
// independent Agent. It cannot execute lifecycle actions or obtain provider
// credentials.
type CloudServiceReader interface {
	ListCloudServices(context.Context) ([]CloudService, error)
	GetCloudService(context.Context, string) (CloudService, bool, error)
}

// CloudSecretBootstrapClient exposes only create and encrypted upload. The
// Message Server cannot complete, consume, or deliver a secret through it.
type CloudSecretBootstrapClient = cloudmodule.SecretBootstrapClient
type CloudSecretBootstrapSession = cloudmodule.AgentSecretBootstrapSession
type CreateCloudSecretBootstrapRequest = cloudmodule.CreateAgentSecretBootstrapRequest
type UploadCloudEncryptedSecretRequest = cloudmodule.UploadAgentEncryptedSecretRequest

// CloudIdentityPreviewClient exposes only read-only AWS caller-identity
// inspection for one already uploaded bootstrap session.
type CloudIdentityPreviewClient = cloudmodule.IdentityPreviewClient
type CloudIdentityPreviewRequest = cloudmodule.IdentityPreviewRequest
type CloudIdentityPreviewEvidence = cloudmodule.IdentityPreviewEvidence

// CloudAgentControlClient exposes typed plan approval, AWS connection
// establishment, exact Agent-owned Deployment destruction, and optional
// Agent-owned lifecycle capabilities such as managed acceptance. It does not
// expose approval-device administration, raw credentials, caller-selected
// provider resources, or arbitrary AWS operations.
type CloudAgentControlClient = cloudmodule.AgentCloudControlClient
type AgentCloudGoalCreateRequest = cloudmodule.AgentCloudGoalCreateRequest
type AgentCloudGoalResult = cloudmodule.AgentCloudGoalResult
type AgentCloudPlanRequest = cloudmodule.AgentCloudPlanRequest
type AgentCloudConnectionRequest = cloudmodule.AgentCloudConnectionRequest
type AgentCloudPlan = cloudmodule.AgentCloudPlan
type AgentCloudChallengeRequest = cloudmodule.AgentCloudChallengeRequest
type AgentCloudChallenge = cloudmodule.AgentCloudChallenge
type AgentCloudApproveRequest = cloudmodule.AgentCloudApproveRequest
type AgentCloudEstablishRequest = cloudmodule.AgentCloudEstablishRequest
type AgentCloudConnection = cloudmodule.AgentCloudConnection
type AgentCloudDeploymentDestroyChallengeRequest = cloudmodule.AgentCloudDeploymentDestroyChallengeRequest
type AgentCloudDeploymentDestroyChallenge = cloudmodule.AgentCloudDeploymentDestroyChallenge
type AgentCloudDeploymentDestroyApproveRequest = cloudmodule.AgentCloudDeploymentDestroyApproveRequest
type AgentCloudDeploymentDestroyResult = cloudmodule.AgentCloudDeploymentDestroyResult
type AgentCloudDestroyOperationRequest = cloudmodule.AgentCloudDestroyOperationRequest
type AgentCloudDestroyOperation = cloudmodule.AgentCloudDestroyOperation

type AgentEventSource = agentevents.Source
type AgentEventStream = agentevents.EventStream

// AgentEventClient is the read-only durable TaskService WatchEvents capability.
// Its source identity is stable across Service Key rotation and binds the
// persisted cursor to one Agent instance and one calling project.
type AgentEventClient interface {
	WatchEvents(context.Context, int64) (AgentEventStream, error)
	AgentEventSource() AgentEventSource
}

type AgentGRPCConfig struct {
	Target          string
	CAFile          string
	ServerName      string
	ServiceKeyFile  string
	AgentInstanceID string
	OwnerID         string
}

// NewAgentGRPCChatRunner is the public construction seam for setup. The same
// narrow transport also implements runtime-profile and Knowledge configuration, Cloud
// queries, encrypted secret bootstrap, and typed approval/connection
// establishment. Each capability is wired through a separate interface so
// setup fails closed when one is absent.
func NewAgentGRPCChatRunner(ctx context.Context, config AgentGRPCConfig) (ClosableNativeAgentRunner, error) {
	return agentgrpc.New(ctx, agentgrpc.Config{
		Target: config.Target, CAFile: config.CAFile, ServerName: config.ServerName,
		ServiceKeyFile: config.ServiceKeyFile, AgentInstanceID: config.AgentInstanceID, OwnerID: config.OwnerID,
	})
}

// serviceAgentAccountPort retains Service-owned locking, Matrix sessions and
// durable portal writes while internal/agent owns the ProductCore workflow.
type serviceAgentAccountPort struct{ service *Service }

func (p serviceAgentAccountPort) Password() string {
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	return p.service.password
}

func (p serviceAgentAccountPort) CreateMatrixSession(ctx context.Context, params map[string]any) (agentmodule.MatrixSession, *apiError) {
	p.service.matrixSessionMu.Lock()
	defer p.service.matrixSessionMu.Unlock()

	requestedDeviceID := requestedMatrixDeviceID(params)
	p.service.mu.Lock()
	issuer := p.service.sessions
	userID := p.service.agentMXIDLocked()
	displayName := p.service.agentDisplayNameLocked()
	homeserver := p.service.homeserver
	p.service.mu.Unlock()
	session := agentmodule.MatrixSession{
		DeviceID:   requestedDeviceID,
		UserID:     userID,
		Homeserver: homeserver,
	}
	if issuer == nil {
		return session, nil
	}
	token, err := issuer.EnsureMatrixSession(ctx, userID, displayName, "", requestedDeviceID, false)
	if err != nil {
		return agentmodule.MatrixSession{}, internalError(err)
	}
	session.AccessToken = &token
	return session, nil
}

func (p serviceAgentAccountPort) Config() agentConfig {
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	config := p.service.agentConfig
	config.MCPBlockedRoomIDs = append([]string(nil), config.MCPBlockedRoomIDs...)
	return config
}

func (p serviceAgentAccountPort) UpdateConfig(ctx context.Context, mutate func(agentConfig) agentConfig) (agentConfig, *apiError) {
	p.service.mu.Lock()
	p.service.agentConfig = mutate(p.service.agentConfig)
	config := p.service.agentConfig
	state := p.service.portalStateLocked()
	p.service.mu.Unlock()
	if store := p.service.portalStore(); store != nil {
		if err := store.SavePortal(ctx, state); err != nil {
			return agentConfig{}, internalError(err)
		}
	}
	return config, nil
}

func (p serviceAgentAccountPort) PublishOffline(ctx context.Context) *apiError {
	return transportWriteError(p.service.publishCurrentAgentStatusState(ctx))
}

// nativeAgentConfigStore adapts the account-scoped durable portal record to
// the runtime's narrow configuration store.
type nativeAgentConfigStore struct {
	service *Service
}

func (s nativeAgentConfigStore) Load(context.Context) (map[string]any, bool, error) {
	if s.service == nil {
		return map[string]any{}, false, nil
	}
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	return agentConfigToNativeMap(s.service.agentConfig), true, nil
}

func (s nativeAgentConfigStore) Save(ctx context.Context, config map[string]any) error {
	if s.service == nil {
		return fmt.Errorf("native agent config store is unavailable")
	}
	ctx, finishOperation := s.service.beginAccountOperation(ctx)
	defer finishOperation()
	if s.service.accountIsDeprovisioned() {
		return fmt.Errorf("account is deprovisioned")
	}
	s.service.mu.Lock()
	s.service.agentConfig = agentConfigFromNativeMap(s.service.agentConfig, config)
	state := s.service.portalStateLocked()
	s.service.mu.Unlock()
	if store := s.service.portalStore(); store != nil {
		return store.SavePortal(ctx, state)
	}
	return nil
}

// These wrappers keep the root Service construction and focused compatibility
// tests stable while Native Agent configuration ownership lives in
// internal/agent.
func agentConfigToNativeMap(cfg agentConfig) map[string]any {
	return agentmodule.ToNativeMap(cfg)
}

func agentConfigFromNativeMap(current agentConfig, config map[string]any) agentConfig {
	return agentmodule.FromNativeMap(current, config)
}

func migrateLegacyAgentPluginConfig(ctx context.Context, store Store, state *portalState) (bool, error) {
	return agentmodule.MigrateLegacyPluginConfig(ctx, store, state, agentmodule.LegacyPluginID)
}
