// Package api adapts the closed Connection Stack contract to HTTP. Billable
// deployment mutation stays behind an exact, disabled-by-default runtime gate.
package api

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

const commandPath = "/v2/commands"

// KeyResolver provides only the public node key registered to one exact
// Connection Stack. It must never return a private key or any AWS credential.
type KeyResolver interface {
	Lookup(ctx context.Context, connectionID, nodeKeyID string) (NodeRegistration, bool)
}

type NodeRegistration struct {
	Generation int64
	PublicKey  ed25519.PublicKey
}

// StaticKeyResolver is intentionally narrow. It is useful for the initial
// CloudFormation handoff and test boundary; future durable registration must
// preserve the same exact lookup semantics behind an AWS-owned store.
type StaticKeyResolver struct {
	ConnectionID string
	NodeKeyID    string
	Generation   int64
	PublicKey    ed25519.PublicKey
}

func (r StaticKeyResolver) Lookup(_ context.Context, connectionID, nodeKeyID string) (NodeRegistration, bool) {
	if r.ConnectionID != connectionID || r.NodeKeyID != nodeKeyID || r.Generation < 1 || len(r.PublicKey) != ed25519.PublicKeySize {
		return NodeRegistration{}, false
	}
	return NodeRegistration{Generation: r.Generation, PublicKey: append(ed25519.PublicKey(nil), r.PublicKey...)}, true
}

// NewStaticKeyResolver decodes the PKIX/SPKI Ed25519 public key that the
// Message Server registration contract already carries. A missing value returns
// a nil resolver rather than a permissive one; the HTTP boundary will fail
// closed with broker_not_configured.
func NewStaticKeyResolver(connectionID, nodeKeyID, publicKeySPKIB64 string, generation int64) (*StaticKeyResolver, error) {
	if strings.TrimSpace(connectionID) == "" && strings.TrimSpace(nodeKeyID) == "" && strings.TrimSpace(publicKeySPKIB64) == "" && generation == 0 {
		return nil, nil
	}
	if strings.TrimSpace(connectionID) == "" || strings.TrimSpace(nodeKeyID) == "" || strings.TrimSpace(publicKeySPKIB64) == "" || generation < 1 || generation > 9007199254740991 {
		return nil, errors.New("incomplete static node registration")
	}
	if !contract.ValidConnectionID(connectionID) || !contract.ValidNodeKeyID(nodeKeyID) {
		return nil, errors.New("invalid static node registration identity")
	}
	decoded, err := base64.StdEncoding.DecodeString(publicKeySPKIB64)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != publicKeySPKIB64 {
		return nil, errors.New("invalid static node public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(decoded)
	publicKey, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok || len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("invalid static node public key")
	}
	return &StaticKeyResolver{
		ConnectionID: connectionID,
		NodeKeyID:    nodeKeyID,
		Generation:   generation,
		PublicKey:    append(ed25519.PublicKey(nil), publicKey...),
	}, nil
}

// Broker is a fail-closed HTTP command endpoint. It admits durable registration
// and quote reads, plus the complete typed deployment transaction only when
// explicitly enabled. It does not receive Worker traffic or report a service ready.
type Broker struct {
	Resolver           KeyResolver
	Store              commandstore.Repository
	Registration       RegistrationAttestor
	Quote              QuoteProvider
	DeploymentEnabled  bool
	ApprovalResolver   ApprovalKeyResolver
	DeploymentStore    commandstore.DeploymentRepository
	DeploymentProvider DeploymentProvider
	DeploymentBoundary DeploymentBoundary
	Now                func() time.Time
}

func (b Broker) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.URL.Path != commandPath || request.URL.RawQuery != "" {
		writeError(response, http.StatusNotFound, "not_found")
		return
	}
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_content_type")
		return
	}
	if b.Resolver == nil {
		writeError(response, http.StatusServiceUnavailable, "broker_not_configured")
		return
	}

	request.Body = http.MaxBytesReader(response, request.Body, contract.MaxCommandBytes)
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(response, http.StatusRequestEntityTooLarge, "request_too_large")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid_command")
		return
	}
	command, err := contract.Parse(raw)
	if err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	if command.IsDeploymentCreate() && !b.DeploymentEnabled {
		// Do not attempt a partial approval check or node-signature computation.
		// The full deterministic-CBOR proof, one-time consumption, reservation,
		// and read-back transaction must land as one provider capability.
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}

	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	registration, found := b.Resolver.Lookup(request.Context(), command.ConnectionID, command.NodeKeyID)
	if !found {
		writeError(response, http.StatusForbidden, "unknown_node_key")
		return
	}
	if err := command.VerifyNodeSignature(registration.PublicKey); err != nil {
		writeError(response, http.StatusForbidden, contract.Code(err))
		return
	}
	if command.ExpectedGeneration != registration.Generation {
		writeError(response, http.StatusConflict, "stale_generation")
		return
	}
	if command.Action == contract.ActionDeploymentCreate {
		if b.ApprovalResolver == nil || b.DeploymentStore == nil || b.DeploymentProvider == nil {
			writeError(response, http.StatusServiceUnavailable, "broker_not_configured")
			return
		}
		b.executeDeployment(response, request, command, now)
		return
	}
	if command.Action != contract.ActionRegistrationVerify && command.Action != contract.ActionQuoteRequest {
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	if b.Store == nil {
		writeError(response, http.StatusServiceUnavailable, "broker_not_configured")
		return
	}
	b.executeReadOnly(response, request, command, now)
}

func writeError(response http.ResponseWriter, status int, code string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]any{
		"error": map[string]string{"code": code},
	})
}
