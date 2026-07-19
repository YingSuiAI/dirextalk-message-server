package connectionbootstrap

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionfoundation"
)

const hkdfInfo = "dirextalk.connection-bootstrap/x25519-aes256gcm/v1"

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type CallerIdentity struct{ AccountID, ARN, UserID string }
type STSClient interface {
	GetCallerIdentity(context.Context) (CallerIdentity, error)
}
type StackClient interface {
	CreateStack(context.Context, StackRequest) (string, error)
}
type ClientFactory interface {
	Clients(Credentials) (STSClient, StackClient, error)
}
type StackRequest struct {
	StackName, ClientRequestToken string
	Region                        string
	Template                      artifactpublish.ConnectionTemplateReference
	Parameters                    map[string]string
	Tags                          map[string]string
}

const (
	stackTagManaged          = "dirextalk:managed"
	stackTagConnectionID     = "dirextalk:connection-id"
	stackTagRegion           = "dirextalk:region"
	stackTagTemplateBinding  = "dirextalk:connection-template-binding"
	stackTagParameterBinding = "dirextalk:connection-parameters"
)

// FoundationResolveRequest contains only trusted, non-secret identity data.
// It deliberately has no raw AWS credential, resource-ID, network, or generic
// parameter fields: a resolver must use its reviewed provider policy and return
// a fully validated connectionfoundation.Plan.
type FoundationResolveRequest struct {
	BootstrapID  string
	ConnectionID string
	Region       string
	AccountID    string
}

// RootBootstrapResolution is the only root-path execution input accepted by
// the Stack request builder. All values are non-secret, provider read-back
// facts: the resolver must return its Foundation Plan plus the exact immutable
// artifacts it published into that Foundation's bucket. Generic IDs, URL
// fields, arbitrary parameters, and credentials are intentionally absent.
type RootBootstrapResolution struct {
	FoundationPlan     connectionfoundation.Plan
	FoundationArtifact artifactpublish.Policy
	ConnectionTemplate artifactpublish.ConnectionTemplateReference
	BrokerArtifact     artifactpublish.BrokerArtifactReference
}

// FoundationResolver is the root-bootstrap-only hook for a reviewed provider.
// It is invoked only after STS has authenticated an owner-approved root
// credential. Credentials are passed only by stack-local value for the
// duration of ResolveFoundation and are zeroed by Service before any receipt
// returns. The returned Plan is revalidated before CreateStack; neither the
// credential nor the Plan is recorded in bootstrap receipt or session state.
type FoundationResolver interface {
	ResolveFoundation(context.Context, FoundationResolveRequest, Credentials) (RootBootstrapResolution, error)
}

type stackResolution struct {
	FoundationPlan     connectionfoundation.Plan
	ArtifactPolicy     artifactpublish.Policy
	ConnectionTemplate artifactpublish.ConnectionTemplateReference
	BrokerArtifact     artifactpublish.BrokerArtifactReference
}

type ServiceOption func(*serviceOptions) error

type serviceOptions struct{ foundationResolver FoundationResolver }

func WithFoundationResolver(resolver FoundationResolver) ServiceOption {
	return func(options *serviceOptions) error {
		if resolver == nil || options.foundationResolver != nil {
			return ErrInvalid
		}
		options.foundationResolver = resolver
		return nil
	}
}

type Service struct {
	config             Config
	factory            ClientFactory
	foundationResolver FoundationResolver
	random             io.Reader
	clock              Clock
	store              *sessionStore
}

func NewService(config Config, factory ClientFactory, randomSource io.Reader, clock Clock, options ...ServiceOption) (*Service, error) {
	if factory == nil {
		return nil, ErrInvalid
	}
	serviceOptions := serviceOptions{}
	for _, option := range options {
		if option == nil || option(&serviceOptions) != nil {
			return nil, ErrInvalid
		}
	}
	if config.validate(serviceOptions.foundationResolver != nil) != nil {
		return nil, ErrInvalid
	}
	if randomSource == nil {
		randomSource = rand.Reader
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Service{config: config, factory: factory, foundationResolver: serviceOptions.foundationResolver, random: randomSource, clock: clock, store: newSessionStore()}, nil
}
func (service *Service) CleanupExpired() {
	if service != nil {
		service.store.cleanup(service.clock.Now().UTC())
	}
}

func (service *Service) CreateSession(request CreateRequest) (CreateResponse, error) {
	if service == nil || request.Validate() != nil {
		return CreateResponse{}, ErrInvalid
	}
	now := service.clock.Now().UTC()
	plan := request.RolePlan
	roleExpires, _ := time.Parse(time.RFC3339Nano, plan.ExpiresAt)
	if !now.Before(roleExpires) || roleExpires.Sub(now) < time.Minute || plan.Region != service.config.Region || plan.SourceTreeDigest != service.config.SourceTreeDigest {
		return CreateResponse{}, ErrInvalid
	}
	if !plan.AllowRootCredentialBootstrap {
		if service.config.validateStaticBootstrapInputs() != nil || !plan.ConnectionTemplate.Equal(service.config.ConnectionTemplate) {
			return CreateResponse{}, ErrInvalid
		}
	} else if service.config.ConnectionTemplate.Mode == connectionTemplateModePublishIntent && !plan.ConnectionTemplate.Equal(service.config.ConnectionTemplate) {
		return CreateResponse{}, ErrInvalid
	}
	requestRaw, _ := json.Marshal(request)
	requestSum := sha256.Sum256(requestRaw)
	requestDigest := base64.RawURLEncoding.EncodeToString(requestSum[:])
	sessionRaw := make([]byte, 24)
	bearerRaw := make([]byte, 32)
	if _, err := io.ReadFull(service.random, sessionRaw); err != nil {
		return CreateResponse{}, err
	}
	if _, err := io.ReadFull(service.random, bearerRaw); err != nil {
		clear(sessionRaw)
		return CreateResponse{}, err
	}
	private, err := ecdh.X25519().GenerateKey(service.random)
	if err != nil {
		clear(sessionRaw)
		clear(bearerRaw)
		return CreateResponse{}, err
	}
	privateRaw := private.Bytes()
	publicRaw := private.PublicKey().Bytes()
	id := "aws-bootstrap-" + base64.RawURLEncoding.EncodeToString(sessionRaw)
	bearer := base64.RawURLEncoding.EncodeToString(bearerRaw)
	bearerHash := sha256.Sum256([]byte(bearer))
	expires := now.Add(SessionTTL)
	if roleExpires.Before(expires) {
		expires = roleExpires
	}
	identity := Identity{
		BootstrapID: plan.BootstrapID, ConnectionID: plan.ConnectionID,
		NodeKeyID: plan.NodeKeyID, NodeEd25519PublicKey: plan.NodeEd25519PublicKey,
		DeviceKeyID: plan.DeviceKeyID, DeviceEd25519PublicKey: plan.DeviceEd25519PublicKey,
		StackName: plan.StackName, AllowRootCredentialBootstrap: plan.AllowRootCredentialBootstrap,
		ConnectionTemplate: plan.ConnectionTemplate.Clone(),
		FixedParameters:    cloneStringMap(plan.FixedParameters),
	}
	expiresAt := expires.Format(time.RFC3339Nano)
	responseBase := CreateResponse{Schema: CreateResponseSchema, Status: "awaiting_upload", RequestID: request.RequestID, SessionID: id, ConnectionID: plan.ConnectionID, ServerX25519PublicKey: base64.StdEncoding.EncodeToString(publicRaw), UploadURL: strings.TrimRight(service.config.UploadBaseURL, "/") + "/v1/aws-bootstrap/sessions/" + id, ExpiresAt: expiresAt, HKDF: "HKDF-SHA256 info=" + hkdfInfo, AAD: string(EnvelopeAAD(id, plan.ConnectionID, expiresAt))}
	result, created, err := service.store.addOrReplay(request.RequestID, requestDigest, id, &session{requestID: request.RequestID, identity: identity, expiresAt: expires, privateKey: append([]byte(nil), privateRaw...), rawBearer: []byte(bearer), bearerHash: bearerHash, state: sessionActive, response: responseBase}, now)
	if err != nil {
		clear(sessionRaw)
		clear(bearerRaw)
		clear(privateRaw)
		return CreateResponse{}, err
	}
	if !created {
		clear(privateRaw)
		clear(bearerRaw)
	}
	clear(sessionRaw)
	clear(bearerRaw)
	clear(privateRaw)
	return result, nil
}

func (service *Service) Upload(ctx context.Context, sessionID, bearer string, envelope UploadEnvelope) (Receipt, error) {
	if service == nil || ctx == nil || envelope.Validate() != nil || sessionID != envelope.SessionID || bearer == "" || len(bearer) > 256 {
		return Receipt{}, ErrInvalid
	}
	fingerprint := envelopeDigest(envelope)
	bearerHash := sha256.Sum256([]byte(bearer))
	for {
		begin, err := service.store.begin(sessionID, bearerHash, fingerprint, service.clock.Now().UTC())
		if err != nil {
			return Receipt{}, err
		}
		if begin.receipt.StackID != "" {
			return begin.receipt, nil
		}
		if begin.wait != nil {
			select {
			case <-ctx.Done():
				return Receipt{}, ctx.Err()
			case <-begin.wait:
				continue
			}
		}
		if !begin.owner {
			return Receipt{}, ErrConsumed
		}
		return service.consume(ctx, sessionID, envelope, fingerprint, begin)
	}
}

func (service *Service) consume(ctx context.Context, sessionID string, envelope UploadEnvelope, fingerprint string, begin beginResult) (receipt Receipt, err error) {
	ctx, cancel := context.WithDeadline(ctx, begin.expiresAt)
	defer cancel()
	privateRaw := begin.privateKey
	defer clear(privateRaw)
	expiresAt := begin.expiresAt.UTC().Format(time.RFC3339Nano)
	if envelope.ExpiresAt != expiresAt || !service.clock.Now().Before(begin.expiresAt) {
		service.store.fail(sessionID)
		return Receipt{}, ErrExpired
	}
	plaintext, err := decryptEnvelope(privateRaw, envelope, EnvelopeAAD(sessionID, begin.identity.ConnectionID, expiresAt))
	if err != nil {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	defer clear(plaintext)
	credentials, err := ParseCredentials(plaintext)
	if err != nil {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	defer credentials.Zero()
	stsClient, stackClient, err := service.factory.Clients(credentials)
	if err != nil {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	caller, err := stsClient.GetCallerIdentity(ctx)
	// New users commonly only have an AWS root access-key export. It is accepted
	// only when the server-issued, owner-approved role plan bound to this session
	// permits it. The credential can then create only the fixed Connection Stack
	// below, and is zeroed before the receipt returns without reaching the
	// Agent, Worker, Broker, or ProductCore storage.
	if err != nil || caller.AccountID == "" || caller.ARN == "" {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	if rootARN(caller.ARN) && !begin.identity.AllowRootCredentialBootstrap {
		service.store.fail(sessionID)
		return Receipt{}, ErrUnauthorized
	}
	resolution, err := service.resolveStackResolution(ctx, begin.identity, caller, credentials)
	if err != nil {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	request, err := service.stackRequest(begin.identity, sessionID, fingerprint, resolution)
	if err != nil {
		service.store.fail(sessionID)
		return Receipt{}, err
	}
	stackID, err := stackClient.CreateStack(ctx, request)
	if err != nil || stackID == "" {
		service.store.fail(sessionID)
		return Receipt{}, ErrInvalid
	}
	credentials.Zero()
	receipt = Receipt{Schema: ReceiptSchema, Status: "accepted", StackID: stackID, ConnectionID: begin.identity.ConnectionID, AcceptedAt: service.clock.Now().UTC().Format(time.RFC3339Nano)}
	service.store.complete(sessionID, receipt)
	return receipt, nil
}

func (service *Service) resolveStackResolution(ctx context.Context, identity Identity, caller CallerIdentity, credentials Credentials) (stackResolution, error) {
	if !rootARN(caller.ARN) {
		if identity.AllowRootCredentialBootstrap {
			return stackResolution{}, ErrInvalid
		}
		return service.config.staticStackResolution()
	}
	if !identity.AllowRootCredentialBootstrap || service.foundationResolver == nil {
		return stackResolution{}, ErrInvalid
	}
	request := FoundationResolveRequest{BootstrapID: identity.BootstrapID, ConnectionID: identity.ConnectionID, Region: service.config.Region, AccountID: caller.AccountID}
	if !request.valid() {
		return stackResolution{}, ErrInvalid
	}
	rootResolution, err := service.foundationResolver.ResolveFoundation(ctx, request, credentials)
	if err != nil || rootResolution.validate(request, identity.ConnectionTemplate) != nil {
		return stackResolution{}, ErrInvalid
	}
	return stackResolution{FoundationPlan: rootResolution.FoundationPlan, ArtifactPolicy: rootResolution.FoundationArtifact, ConnectionTemplate: rootResolution.ConnectionTemplate, BrokerArtifact: rootResolution.BrokerArtifact}, nil
}

func (request FoundationResolveRequest) valid() bool {
	return identifierPattern.MatchString(request.BootstrapID) && identifierPattern.MatchString(request.ConnectionID) && regionPattern.MatchString(request.Region) && accountIDPattern.MatchString(request.AccountID)
}

func (config Config) staticStackResolution() (stackResolution, error) {
	if config.validateStaticBootstrapInputs() != nil {
		return stackResolution{}, ErrInvalid
	}
	template, err := config.ConnectionTemplate.ArtifactReference(config.ArtifactPolicy)
	if err != nil {
		return stackResolution{}, ErrInvalid
	}
	return stackResolution{FoundationPlan: config.FoundationPlan, ArtifactPolicy: config.ArtifactPolicy, ConnectionTemplate: template, BrokerArtifact: config.BrokerArtifact}, nil
}

func (resolution RootBootstrapResolution) validate(request FoundationResolveRequest, expectedTemplate ConnectionTemplateReference) error {
	if !request.valid() || expectedTemplate.ValidateForRootCredentialBootstrap(true) != nil || resolution.FoundationPlan.Region != request.Region || resolution.FoundationPlan.Validate() != nil ||
		!validFoundationArtifactPolicy(resolution.FoundationArtifact, request.Region, request.AccountID) ||
		resolution.FoundationPlan.Worker.Artifact.Bucket != resolution.FoundationArtifact.Bucket ||
		resolution.ConnectionTemplate.ValidateFor(resolution.FoundationArtifact) != nil || resolution.BrokerArtifact.ValidateFor(resolution.FoundationArtifact) != nil {
		return ErrInvalid
	}
	intent := expectedTemplate.PublishIntent
	binding := resolution.ConnectionTemplate.Binding
	if intent == nil || string(binding.Kind) != intent.Kind || binding.Version != intent.Version || binding.SHA256 != intent.SHA256 || binding.SizeBytes != intent.SizeBytes || binding.ContentType != intent.ContentType {
		return ErrInvalid
	}
	return nil
}

func validFoundationArtifactPolicy(policy artifactpublish.Policy, region, accountID string) bool {
	return policy.Validate() == nil && canonicalKMSKeyARNPattern.MatchString(policy.KMSKeyID) && strings.Contains(policy.KMSKeyID, ":kms:"+region+":"+accountID+":key/")
}

func (service *Service) stackRequest(identity Identity, sessionID, fingerprint string, resolution stackResolution) (StackRequest, error) {
	parameters := cloneStringMap(identity.FixedParameters)
	parameters["BrokerArtifactBucket"] = resolution.BrokerArtifact.Bucket
	parameters["BrokerArtifactKey"] = resolution.BrokerArtifact.Key
	parameters["BrokerArtifactVersion"] = resolution.BrokerArtifact.VersionID
	foundationParameters, err := resolution.FoundationPlan.TemplateParameters()
	if err != nil {
		return StackRequest{}, ErrInvalid
	}
	for key, value := range foundationParameters {
		parameters[key] = value
	}
	if service.config.DeploymentCreateEnabled {
		parameters["EnableDeploymentCreate"] = "true"
	} else {
		parameters["EnableDeploymentCreate"] = "false"
	}
	if service.config.DeploymentDestroyEnabled {
		parameters["EnableDeploymentDestroy"] = "true"
	} else {
		parameters["EnableDeploymentDestroy"] = "false"
	}
	if service.config.ServiceSecretsEnabled {
		parameters["EnableServiceSecrets"] = "true"
	} else {
		parameters["EnableServiceSecrets"] = "false"
	}
	if service.config.DynamicArtifactsEnabled {
		parameters["EnableDynamicArtifacts"] = "true"
	} else {
		parameters["EnableDynamicArtifacts"] = "false"
	}
	// These lifecycle controls remain closed in the first Connection Stack
	// release. Supplying their defaults explicitly lets DescribeStacks compare
	// the complete parameter set after a lost CreateStack response.
	parameters["EnableServiceBackup"] = "false"
	parameters["EnableServiceRestorePlan"] = "false"
	parameters["EnableServiceRestore"] = "false"
	tags := make(map[string]string, len(service.config.FixedTags)+5)
	for key, value := range service.config.FixedTags {
		tags[key] = value
	}
	tags[stackTagManaged] = "true"
	tags[stackTagConnectionID] = identity.ConnectionID
	tags[stackTagRegion] = service.config.Region
	tags[stackTagTemplateBinding] = connectionTemplateBindingFingerprint(resolution.ConnectionTemplate)
	tags[stackTagParameterBinding] = stackParameterBindingFingerprint(parameters)
	return StackRequest{StackName: identity.StackName, Region: service.config.Region, Template: resolution.ConnectionTemplate, ClientRequestToken: clientRequestToken(sessionID, identity.ConnectionID, fingerprint), Parameters: parameters, Tags: tags}, nil
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func sameStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func decryptEnvelope(privateRaw []byte, envelope UploadEnvelope, aad []byte) ([]byte, error) {
	private, err := ecdh.X25519().NewPrivateKey(privateRaw)
	if err != nil {
		return nil, ErrInvalid
	}
	publicRaw, err := base64.StdEncoding.DecodeString(envelope.ClientX25519PublicKey)
	if err != nil {
		return nil, ErrInvalid
	}
	public, err := ecdh.X25519().NewPublicKey(publicRaw)
	if err != nil {
		return nil, ErrInvalid
	}
	shared, err := private.ECDH(public)
	if err != nil {
		return nil, ErrInvalid
	}
	defer clear(shared)
	key, err := hkdf.Key(sha256.New, shared, []byte(envelope.SessionID), hkdfInfo, 32)
	if err != nil {
		return nil, ErrInvalid
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalid
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalid
	}
	nonce, _ := base64.StdEncoding.DecodeString(envelope.Nonce)
	ciphertext, _ := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrInvalid
	}
	return plaintext, nil
}

func rootARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) >= 6 && parts[2] == "iam" && parts[len(parts)-1] == "root"
}
