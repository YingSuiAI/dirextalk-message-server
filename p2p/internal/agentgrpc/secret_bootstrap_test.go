package agentgrpc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type secretBootstrapTestService struct {
	agentv1.UnimplementedSecretBootstrapServiceServer
	mu            sync.Mutex
	createRequest *agentv1.CreateSessionRequest
	uploadRequest *agentv1.UploadEncryptedRequest
	authorization []string
	create        func(*agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error)
	upload        func(*agentv1.UploadEncryptedRequest) (*agentv1.UploadEncryptedResponse, error)
}

func (service *secretBootstrapTestService) CreateSession(ctx context.Context, request *agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
	service.capture(ctx)
	service.mu.Lock()
	service.createRequest = request
	callback := service.create
	service.mu.Unlock()
	if callback == nil {
		return nil, status.Error(codes.Unavailable, "not configured")
	}
	return callback(request)
}

func (service *secretBootstrapTestService) UploadEncrypted(ctx context.Context, request *agentv1.UploadEncryptedRequest) (*agentv1.UploadEncryptedResponse, error) {
	service.capture(ctx)
	service.mu.Lock()
	service.uploadRequest = request
	callback := service.upload
	service.mu.Unlock()
	if callback == nil {
		return nil, status.Error(codes.Unavailable, "not configured")
	}
	return callback(request)
}

func (service *secretBootstrapTestService) capture(ctx context.Context) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	value := ""
	if len(values) == 1 {
		value = values[0]
	}
	service.mu.Lock()
	service.authorization = append(service.authorization, value)
	service.mu.Unlock()
}

func TestSecretBootstrapCreateAndUploadBindOwnerAndPreserveCiphertext(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	now := time.Now().UTC().Truncate(time.Second)
	sessionID := uuid.NewString()
	targetID := "cloud_connection_remote_1"
	publicKey := append(make([]byte, 31), 1)
	uploadToken := append(make([]byte, 31), 2)
	base := &agentv1.SecretBootstrapSession{
		SessionId: sessionID, OwnerId: "owner-from-config", Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection,
		TargetId: targetID, ServerPublicKey: publicKey, CreatedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)),
		Status: agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_AWAITING_UPLOAD, Revision: 1,
		AgentInstanceId: "agent-instance-1", SessionSchemaVersion: cloudmodule.AgentSecretBootstrapSessionSchemaV1,
		EnvelopeSchemaVersion: cloudmodule.AgentSecretBootstrapEnvelopeSchemaV1,
	}
	server.secrets.create = func(*agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
		return &agentv1.CreateSessionResponse{
			SessionId: sessionID, ServerPublicKey: publicKey, UploadToken: append([]byte(nil), uploadToken...),
			ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)), Session: base,
		}, nil
	}
	createIdempotencyKey := uuid.NewString()
	created, err := runner.CreateAgentSecretBootstrap(t.Context(), cloudmodule.CreateAgentSecretBootstrapRequest{
		IdempotencyKey: createIdempotencyKey, Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection, TargetID: targetID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.SessionID != sessionID || created.OwnerID != "owner-from-config" || created.TargetID != targetID ||
		created.CreatedAt != now.Format(time.RFC3339Nano) || created.ExpiresAt != now.Add(10*time.Minute).Format(time.RFC3339Nano) ||
		created.Status != "awaiting_upload" || created.Revision != 1 || !equalBytes(created.UploadToken, uploadToken) {
		t.Fatalf("created session = %#v", created)
	}
	server.secrets.mu.Lock()
	createRequest := server.secrets.createRequest
	createAuth := server.secrets.authorization[0]
	server.secrets.mu.Unlock()
	if createRequest.GetOwnerId() != "owner-from-config" || createRequest.GetTargetId() != targetID || createAuth != authorizationScheme+" "+testServiceKey {
		t.Fatalf("create request owner=%q target=%q auth=%q", createRequest.GetOwnerId(), createRequest.GetTargetId(), createAuth)
	}

	clientPublicKey := append(make([]byte, 31), 3)
	nonce := append(make([]byte, 11), 4)
	ciphertext := append(make([]byte, 16), 5)
	uploadRevision := int64(1)
	uploadedProto := proto.Clone(base).(*agentv1.SecretBootstrapSession)
	uploadedProto.Status = agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_UPLOADED
	uploadedProto.Revision = 2
	server.secrets.upload = func(*agentv1.UploadEncryptedRequest) (*agentv1.UploadEncryptedResponse, error) {
		return &agentv1.UploadEncryptedResponse{Revision: 2, Session: uploadedProto}, nil
	}
	uploaded, err := runner.UploadAgentEncryptedSecret(t.Context(), cloudmodule.UploadAgentEncryptedSecretRequest{
		SessionID: sessionID, UploadToken: uploadToken, ClientPublicKey: clientPublicKey, Nonce: nonce,
		Ciphertext: ciphertext, IdempotencyKey: uuid.NewString(), ExpectedRevision: uploadRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploaded.Status != "uploaded" || uploaded.Revision != 2 || len(uploaded.UploadToken) != 0 {
		t.Fatalf("uploaded session = %#v", uploaded)
	}
	server.secrets.mu.Lock()
	uploadRequest := server.secrets.uploadRequest
	uploadAuth := server.secrets.authorization[1]
	server.secrets.mu.Unlock()
	if !equalBytes(uploadRequest.GetUploadToken(), uploadToken) || !equalBytes(uploadRequest.GetClientPublicKey(), clientPublicKey) ||
		!equalBytes(uploadRequest.GetNonce(), nonce) || !equalBytes(uploadRequest.GetCiphertext(), ciphertext) ||
		uploadRequest.GetExpectedRevision() != 1 || uploadAuth != authorizationScheme+" "+testServiceKey {
		t.Fatalf("encrypted upload was not forwarded exactly")
	}
	server.secrets.create = func(*agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
		return &agentv1.CreateSessionResponse{
			SessionId: sessionID, ServerPublicKey: publicKey, ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)), Session: uploadedProto,
		}, nil
	}
	replayed, err := runner.CreateAgentSecretBootstrap(t.Context(), cloudmodule.CreateAgentSecretBootstrapRequest{
		IdempotencyKey: createIdempotencyKey, Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection, TargetID: targetID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != "uploaded" || replayed.Revision != 2 || len(replayed.UploadToken) != 0 {
		t.Fatalf("uploaded replay = %#v", replayed)
	}
}

func TestSecretBootstrapRejectsMalformedSuccessAndSanitizesRPCFailures(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	targetID := "cloud_connection_remote_2"
	server.secrets.create = func(*agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
		return &agentv1.CreateSessionResponse{SessionId: uuid.NewString(), UploadToken: make([]byte, 32)}, nil
	}
	_, err := runner.CreateAgentSecretBootstrap(t.Context(), cloudmodule.CreateAgentSecretBootstrapRequest{
		IdempotencyKey: uuid.NewString(), Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection, TargetID: targetID,
	})
	if !errors.Is(err, cloudmodule.ErrAgentSecretBootstrapInvalidResponse) {
		t.Fatalf("malformed success error = %v", err)
	}
	const canary = "AKIA-SECRET-BOOTSTRAP-CANARY"
	server.secrets.create = func(*agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
		return nil, status.Error(codes.Internal, canary)
	}
	_, err = runner.CreateAgentSecretBootstrap(t.Context(), cloudmodule.CreateAgentSecretBootstrapRequest{
		IdempotencyKey: uuid.NewString(), Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection, TargetID: targetID,
	})
	if !errors.Is(err, cloudmodule.ErrAgentSecretBootstrapUnavailable) || strings.Contains(err.Error(), canary) {
		t.Fatalf("RPC failure was not sanitized: %v", err)
	}
}
