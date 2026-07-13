package legacygateway

import (
	"context"
	"crypto"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway/agentgatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	DefaultIngressDeadline = 20 * time.Second
	maxIngressDeadline     = 20 * time.Second
)

var ErrInvalidIngressConfig = errors.New("legacy gateway ingress configuration is invalid")

type IngressErrorCode string

const (
	IngressErrorUnknown            IngressErrorCode = "unknown"
	IngressErrorInvalidArgument    IngressErrorCode = "invalid_argument"
	IngressErrorUnauthenticated    IngressErrorCode = "unauthenticated"
	IngressErrorPermissionDenied   IngressErrorCode = "permission_denied"
	IngressErrorNotFound           IngressErrorCode = "not_found"
	IngressErrorAlreadyExists      IngressErrorCode = "already_exists"
	IngressErrorConflict           IngressErrorCode = "conflict"
	IngressErrorFailedPrecondition IngressErrorCode = "failed_precondition"
	IngressErrorResourceExhausted  IngressErrorCode = "resource_exhausted"
	IngressErrorCanceled           IngressErrorCode = "canceled"
	IngressErrorDeadlineExceeded   IngressErrorCode = "deadline_exceeded"
	IngressErrorUnavailable        IngressErrorCode = "unavailable"
	IngressErrorInternal           IngressErrorCode = "internal"
)

type IngressError struct {
	code IngressErrorCode
}

func (err *IngressError) Error() string {
	return "legacy gateway ingress failed: " + string(err.code)
}

func IngressErrorCodeOf(err error) IngressErrorCode {
	var ingressError *IngressError
	if errors.As(err, &ingressError) {
		return ingressError.code
	}
	return IngressErrorUnknown
}

// IsPermanentError distinguishes stable request/authentication rejection from
// retryable transport and capacity failures without exposing gRPC details.
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrInvalidIngressConfig) {
		return true
	}
	var contractError *ContractError
	if errors.As(err, &contractError) {
		return true
	}
	switch IngressErrorCodeOf(err) {
	case IngressErrorInvalidArgument,
		IngressErrorUnauthenticated,
		IngressErrorPermissionDenied,
		IngressErrorNotFound,
		IngressErrorAlreadyExists,
		IngressErrorConflict,
		IngressErrorFailedPrecondition:
		return true
	default:
		return false
	}
}

type ClientConfig struct {
	Target            string
	TenantID          string
	ServerName        string
	RootCAs           *x509.CertPool
	ClientCertificate tls.Certificate
	DefaultTimeout    time.Duration
}

type GRPCIngress struct {
	connection *grpc.ClientConn
	client     agentgatewayv1.AgentRunIngressClient
	tenantID   string
	timeout    time.Duration
}

func NewGRPCIngress(config ClientConfig) (*GRPCIngress, error) {
	if strings.TrimSpace(config.Target) == "" || strings.TrimSpace(config.ServerName) == "" ||
		config.RootCAs == nil {
		return nil, ErrInvalidIngressConfig
	}
	if err := ValidateClientCertificate(config.ClientCertificate, config.TenantID); err != nil {
		return nil, ErrInvalidIngressConfig
	}
	timeout := config.DefaultTimeout
	if timeout == 0 {
		timeout = DefaultIngressDeadline
	}
	if timeout <= 0 || timeout > maxIngressDeadline {
		return nil, ErrInvalidIngressConfig
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
		ServerName:   config.ServerName,
		RootCAs:      config.RootCAs,
		Certificates: []tls.Certificate{config.ClientCertificate},
	}
	connection, err := grpc.NewClient(
		config.Target,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(MaxAgentGatewayMessageBytes),
			grpc.MaxCallRecvMsgSize(MaxAgentGatewayMessageBytes),
		),
	)
	if err != nil {
		return nil, ErrInvalidIngressConfig
	}
	return &GRPCIngress{
		connection: connection,
		client:     agentgatewayv1.NewAgentRunIngressClient(connection),
		tenantID:   config.TenantID,
		timeout:    timeout,
	}, nil
}

func (client *GRPCIngress) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	if err := client.connection.Close(); err != nil {
		return &IngressError{code: IngressErrorInternal}
	}
	return nil
}

func (client *GRPCIngress) CreateRun(ctx context.Context, request CreateRunRequest) (CreateRunReceipt, error) {
	capabilities, err := normalizedCapabilities(request.RequiredCapabilities)
	if err != nil {
		return CreateRunReceipt{}, err
	}
	request.RequiredCapabilities = capabilities
	requestDigest, err := RequestDigest(client.tenantID, request)
	if err != nil {
		return CreateRunReceipt{}, err
	}
	dispatchMode, err := protobufDispatchMode(request.DispatchMode)
	if err != nil {
		return CreateRunReceipt{}, err
	}
	var preferredConnectorID *string
	if request.PreferredConnectorID != "" {
		preferred := request.PreferredConnectorID
		preferredConnectorID = &preferred
	}
	rpcContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	response, err := client.client.CreateAgentRun(rpcContext, &agentgatewayv1.CreateAgentRunRequest{
		RequestId:            request.RequestID,
		IdempotencyDigest:    append([]byte(nil), request.IdempotencyDigest[:]...),
		RequestDigest:        append([]byte(nil), requestDigest[:]...),
		InstallationId:       request.InstallationID,
		ConversationId:       request.ConversationID,
		RequestEventId:       request.RequestEventID,
		PreferredConnectorId: preferredConnectorID,
		RequiredCapabilities: capabilities,
		DispatchMode:         dispatchMode,
		GrantVersion:         request.GrantVersion,
	})
	if err != nil {
		return CreateRunReceipt{}, sanitizedIngressError(err)
	}
	return parseCreateRunResponse(request.RequestID, response)
}

func ValidateClientCertificate(certificate tls.Certificate, tenantID string) error {
	if _, err := parseUUIDv7(tenantID, "tenant_id"); err != nil {
		return ErrInvalidIngressConfig
	}
	if len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return ErrInvalidIngressConfig
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil || leaf.IsCA || leaf.Subject.CommonName != "" {
		return ErrInvalidIngressConfig
	}
	if len(leaf.URIs) != 1 || len(leaf.DNSNames) != 0 || len(leaf.EmailAddresses) != 0 ||
		len(leaf.IPAddresses) != 0 {
		return ErrInvalidIngressConfig
	}
	expectedIdentity, err := LegacyGatewaySPIFFEIdentity(tenantID)
	if err != nil || leaf.URIs[0].String() != expectedIdentity {
		return ErrInvalidIngressConfig
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth ||
		len(leaf.UnknownExtKeyUsage) != 0 {
		return ErrInvalidIngressConfig
	}
	signer, ok := certificate.PrivateKey.(crypto.Signer)
	if !ok {
		return ErrInvalidIngressConfig
	}
	certificatePublicKey, err := x509.MarshalPKIXPublicKey(leaf.PublicKey)
	if err != nil {
		return ErrInvalidIngressConfig
	}
	privatePublicKey, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || len(certificatePublicKey) != len(privatePublicKey) ||
		subtle.ConstantTimeCompare(certificatePublicKey, privatePublicKey) != 1 {
		return ErrInvalidIngressConfig
	}
	return nil
}

func LegacyGatewaySPIFFEIdentity(tenantID string) (string, error) {
	if _, err := parseUUIDv7(tenantID, "tenant_id"); err != nil {
		return "", ErrInvalidIngressConfig
	}
	return (&url.URL{
		Scheme: "spiffe",
		Host:   "dirextalk.internal",
		Path:   "/v1/tenants/" + tenantID + "/services/legacy-matrix-gateway",
	}).String(), nil
}

func protobufDispatchMode(mode DispatchMode) (agentgatewayv1.DispatchMode, error) {
	switch mode {
	case DispatchSingle:
		return agentgatewayv1.DispatchMode_DISPATCH_MODE_SINGLE, nil
	case DispatchFailover:
		return agentgatewayv1.DispatchMode_DISPATCH_MODE_FAILOVER, nil
	default:
		return agentgatewayv1.DispatchMode_DISPATCH_MODE_UNSPECIFIED,
			newContractError(ContractUnsupportedValue, "dispatch_mode")
	}
}

func parseCreateRunResponse(
	requestID string,
	response *agentgatewayv1.CreateAgentRunResponse,
) (CreateRunReceipt, error) {
	if response == nil || response.GetRequestId() != requestID {
		return CreateRunReceipt{}, &IngressError{code: IngressErrorInternal}
	}
	if _, err := parseUUIDv7(response.GetRequestId(), "request_id"); err != nil {
		return CreateRunReceipt{}, &IngressError{code: IngressErrorInternal}
	}
	if _, err := parseUUIDv7(response.GetRunId(), "run_id"); err != nil {
		return CreateRunReceipt{}, &IngressError{code: IngressErrorInternal}
	}
	routingState, ok := routingStateFromProtobuf(response.GetRoutingState())
	if !ok {
		return CreateRunReceipt{}, &IngressError{code: IngressErrorInternal}
	}
	return CreateRunReceipt{
		RequestID:    response.GetRequestId(),
		RunID:        response.GetRunId(),
		Inserted:     response.GetInserted(),
		RoutingState: routingState,
	}, nil
}

func routingStateFromProtobuf(value agentgatewayv1.RunRoutingState) (RoutingState, bool) {
	switch value {
	case agentgatewayv1.RunRoutingState_RUN_ROUTING_STATE_QUEUED:
		return RoutingQueued, true
	case agentgatewayv1.RunRoutingState_RUN_ROUTING_STATE_OFFERED:
		return RoutingOffered, true
	case agentgatewayv1.RunRoutingState_RUN_ROUTING_STATE_LEASED:
		return RoutingLeased, true
	case agentgatewayv1.RunRoutingState_RUN_ROUTING_STATE_RECONCILE_REQUIRED:
		return RoutingReconcileRequired, true
	case agentgatewayv1.RunRoutingState_RUN_ROUTING_STATE_EXPIRED:
		return RoutingExpired, true
	default:
		return "", false
	}
}

func sanitizedIngressError(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return &IngressError{code: IngressErrorInvalidArgument}
	case codes.Unauthenticated:
		return &IngressError{code: IngressErrorUnauthenticated}
	case codes.PermissionDenied:
		return &IngressError{code: IngressErrorPermissionDenied}
	case codes.NotFound:
		return &IngressError{code: IngressErrorNotFound}
	case codes.AlreadyExists:
		return &IngressError{code: IngressErrorAlreadyExists}
	case codes.Aborted:
		return &IngressError{code: IngressErrorConflict}
	case codes.FailedPrecondition:
		return &IngressError{code: IngressErrorFailedPrecondition}
	case codes.ResourceExhausted:
		return &IngressError{code: IngressErrorResourceExhausted}
	case codes.Canceled:
		return &IngressError{code: IngressErrorCanceled}
	case codes.DeadlineExceeded:
		return &IngressError{code: IngressErrorDeadlineExceeded}
	case codes.Unavailable:
		return &IngressError{code: IngressErrorUnavailable}
	default:
		return &IngressError{code: IngressErrorInternal}
	}
}
