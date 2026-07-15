package p2p

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"

	legacygatewaymodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
)

type LegacyAgentGatewayIngress = legacygatewaymodule.Ingress

type LegacyAgentGatewaySenderResolver interface {
	QueryUserIDForSender(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error)
}

type LegacyAgentGatewayConfig struct {
	TenantID       string
	ConversationID string
	Ingress        LegacyAgentGatewayIngress
	SenderResolver LegacyAgentGatewaySenderResolver
}

type LegacyAgentGatewayClientConfig struct {
	Target         string
	TenantID       string
	ServerName     string
	RootCAFile     string
	ClientCertFile string
	ClientKeyFile  string
}

// LegacyAgentGatewayCutover is the explicit production feature gate. Merely
// configuring an ingress or constructing a module never activates a consumer.
// The Host must attest that the old dirextalk-connect Matrix consumer is
// stopped and fenced before this process may start the durable consumer.
type LegacyAgentGatewayCutover struct {
	Enabled                     bool
	LegacyConnectConsumerFenced bool
	Mode                        string
}

func (cfg LegacyAgentGatewayCutover) Validate() error {
	if !cfg.Enabled {
		return errors.New("Legacy Agent Gateway cutover is disabled")
	}
	if cfg.Mode != "vnext_gateway" {
		return errors.New("Legacy Agent Gateway cutover mode must be vnext_gateway")
	}
	if !cfg.LegacyConnectConsumerFenced {
		return errors.New("Legacy Agent Gateway requires the old Connect consumer to be fenced")
	}
	return nil
}

func NewLegacyAgentGatewayClient(
	cfg LegacyAgentGatewayClientConfig,
) (LegacyAgentGatewayIngress, io.Closer, error) {
	rootPEM, err := os.ReadFile(cfg.RootCAFile)
	if err != nil {
		return nil, nil, errors.New("read Legacy Agent Gateway root CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return nil, nil, errors.New("parse Legacy Agent Gateway root CA")
	}
	certificate, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, nil, errors.New("load Legacy Agent Gateway client identity")
	}
	client, err := legacygatewaymodule.NewGRPCIngress(legacygatewaymodule.ClientConfig{
		Target:            cfg.Target,
		TenantID:          cfg.TenantID,
		ServerName:        cfg.ServerName,
		RootCAs:           roots,
		ClientCertificate: certificate,
	})
	if err != nil {
		return nil, nil, errors.New("configure Legacy Agent Gateway client")
	}
	return client, client, nil
}

// ConfigureLegacyAgentGateway prepares the compatibility module but does not
// start its room consumer. Production callers must first prove that the old
// Connect room consumer is stopped and fenced, so one input cannot execute in
// both paths.
func (s *Service) ConfigureLegacyAgentGateway(cfg LegacyAgentGatewayConfig) error {
	var resolveSender legacygatewaymodule.SenderResolver
	if cfg.SenderResolver != nil {
		resolveSender = cfg.SenderResolver.QueryUserIDForSender
	}
	module, err := legacygatewaymodule.New(s.store, cfg.Ingress, legacygatewaymodule.Config{
		TenantID:       cfg.TenantID,
		ConversationID: cfg.ConversationID,
		Identity: func() legacygatewaymodule.Identity {
			s.mu.Lock()
			defer s.mu.Unlock()
			return legacygatewaymodule.Identity{
				AgentRoomID: s.agentRoomID,
				OwnerMXID:   s.ownerMXID,
			}
		},
		ResolveSender: resolveSender,
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.legacyAgentGatewayModule = module
	s.mu.Unlock()
	return nil
}

func (s *Service) LegacyAgentGatewayEnabled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.legacyAgentGatewayModule != nil
}

func (s *Service) ProcessLegacyAgentGatewayOutputEvent(ctx context.Context, output roomserverAPI.OutputEvent) error {
	ctx, finishOperation := s.beginAccountOperation(ctx)
	defer finishOperation()
	if s.accountIsDeprovisioned() {
		return nil
	}
	s.mu.Lock()
	module := s.legacyAgentGatewayModule
	s.mu.Unlock()
	if module == nil {
		return nil
	}
	return module.ProcessOutputEvent(ctx, output)
}

type LegacyAgentGatewayOutputRoomEventConsumer = legacygatewaymodule.OutputRoomEventConsumer

// LegacyAgentGatewayConsumerStarter is the narrow lifecycle boundary used by
// the host after it has established exclusive ownership of Matrix invokes.
type LegacyAgentGatewayConsumerStarter interface {
	Start() error
}

// StartLegacyAgentGatewayConsumer is the guarded production activation path.
// It keeps the new durable consumer fail-closed unless the host explicitly
// fences the old dirextalk-connect Matrix consumer.
func StartLegacyAgentGatewayConsumer(
	cutover LegacyAgentGatewayCutover,
	consumer LegacyAgentGatewayConsumerStarter,
) error {
	if err := cutover.Validate(); err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("Legacy Agent Gateway consumer is required")
	}
	return consumer.Start()
}

// NewLegacyAgentGatewayOutputRoomEventConsumer constructs the independent
// durable consumer. It is intentionally not wired into the production
// monolith until the exclusive-consumer cutover is implemented.
func NewLegacyAgentGatewayOutputRoomEventConsumer(
	processContext *process.ProcessContext,
	cfg *config.JetStream,
	js nats.JetStreamContext,
	service *Service,
) *LegacyAgentGatewayOutputRoomEventConsumer {
	var handler legacygatewaymodule.OutputRoomEventHandler
	if service != nil {
		handler = service.ProcessLegacyAgentGatewayOutputEvent
	}
	return legacygatewaymodule.NewOutputRoomEventConsumer(processContext, cfg, js, handler)
}
