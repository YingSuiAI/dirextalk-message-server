// Package api adapts the closed Connection Stack contract to HTTP. It has no
// AWS provider implementation: accepted commands are authenticated then
// rejected until their complete DynamoDB/approval/provider transaction exists.
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
)

const commandPath = "/v2/commands"

// KeyResolver provides only the public node key registered to one exact
// Connection Stack. It must never return a private key or any AWS credential.
type KeyResolver interface {
	Lookup(ctx context.Context, connectionID, nodeKeyID string) (ed25519.PublicKey, bool)
}

// StaticKeyResolver is intentionally narrow. It is useful for the initial
// CloudFormation handoff and test boundary; future durable registration must
// preserve the same exact lookup semantics behind an AWS-owned store.
type StaticKeyResolver struct {
	ConnectionID string
	NodeKeyID    string
	PublicKey    ed25519.PublicKey
}

func (r StaticKeyResolver) Lookup(_ context.Context, connectionID, nodeKeyID string) (ed25519.PublicKey, bool) {
	if r.ConnectionID != connectionID || r.NodeKeyID != nodeKeyID || len(r.PublicKey) != ed25519.PublicKeySize {
		return nil, false
	}
	return append(ed25519.PublicKey(nil), r.PublicKey...), true
}

// NewStaticKeyResolver decodes the PKIX/SPKI Ed25519 public key that the
// Message Server registration contract already carries. A missing value returns
// a nil resolver rather than a permissive one; the HTTP boundary will fail
// closed with broker_not_configured.
func NewStaticKeyResolver(connectionID, nodeKeyID, publicKeySPKIB64 string) (*StaticKeyResolver, error) {
	if strings.TrimSpace(connectionID) == "" && strings.TrimSpace(nodeKeyID) == "" && strings.TrimSpace(publicKeySPKIB64) == "" {
		return nil, nil
	}
	if strings.TrimSpace(connectionID) == "" || strings.TrimSpace(nodeKeyID) == "" || strings.TrimSpace(publicKeySPKIB64) == "" {
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
		PublicKey:    append(ed25519.PublicKey(nil), publicKey...),
	}, nil
}

// Broker is a fail-closed HTTP command endpoint. It does not persist receipts,
// execute a provider API, receive Worker traffic, or report a service ready.
// That keeps this first Go port safe while protocol/storage parity is built.
type Broker struct {
	Resolver KeyResolver
	Now      func() time.Time
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
	if command.IsDeploymentCreate() {
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
	if err := command.ValidateAt(now); err != nil {
		writeError(response, http.StatusBadRequest, contract.Code(err))
		return
	}
	publicKey, found := b.Resolver.Lookup(request.Context(), command.ConnectionID, command.NodeKeyID)
	if !found {
		writeError(response, http.StatusForbidden, "unknown_node_key")
		return
	}
	if err := command.VerifyNodeSignature(publicKey); err != nil {
		writeError(response, http.StatusForbidden, contract.Code(err))
		return
	}

	// Never return success for an action that has not atomically committed a
	// durable receipt and all required authorization/provider evidence.
	writeError(response, http.StatusNotImplemented, "operation_not_enabled")
}

func writeError(response http.ResponseWriter, status int, code string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]any{
		"error": map[string]string{"code": code},
	})
}
