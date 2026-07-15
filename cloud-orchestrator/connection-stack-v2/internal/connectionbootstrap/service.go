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
	StackName, TemplateURL, ClientRequestToken string
	Parameters                                 map[string]string
	Tags                                       map[string]string
}
type Service struct {
	config  Config
	factory ClientFactory
	random  io.Reader
	clock   Clock
	store   *sessionStore
}

func NewService(config Config, factory ClientFactory, randomSource io.Reader, clock Clock) (*Service, error) {
	if config.Validate() != nil || factory == nil {
		return nil, ErrInvalid
	}
	if randomSource == nil {
		randomSource = rand.Reader
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Service{config: config, factory: factory, random: randomSource, clock: clock, store: newSessionStore()}, nil
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
	if !now.Before(roleExpires) || roleExpires.Sub(now) < time.Minute || plan.Region != service.config.Region || plan.TemplateURL != service.config.TemplateURL || plan.TemplateDigest != service.config.TemplateDigest || plan.SourceTreeDigest != service.config.SourceTreeDigest {
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
		FixedParameters: cloneStringMap(plan.FixedParameters),
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
	if err != nil || caller.AccountID == "" || caller.ARN == "" || (rootARN(caller.ARN) && !begin.identity.AllowRootCredentialBootstrap) {
		service.store.fail(sessionID)
		if rootARN(caller.ARN) {
			return Receipt{}, ErrUnauthorized
		}
		return Receipt{}, ErrInvalid
	}
	request := service.stackRequest(begin.identity, sessionID, fingerprint)
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

func (service *Service) stackRequest(identity Identity, sessionID, fingerprint string) StackRequest {
	parameters := cloneStringMap(identity.FixedParameters)
	parameters["BrokerArtifactBucket"] = service.config.BrokerArtifactBucket
	parameters["BrokerArtifactKey"] = service.config.BrokerArtifactKey
	parameters["BrokerArtifactVersion"] = service.config.BrokerArtifactVersion
	parameters["WorkerBaseAmiId"] = service.config.WorkerAMIID
	parameters["WorkerResourceManifestDigest"] = service.config.ArtifactManifestDigest
	parameters["WorkerVpcId"] = service.config.NetworkID
	parameters["WorkerSubnetId"] = service.config.WorkerSubnetID
	parameters["WorkerAvailabilityZone"] = service.config.WorkerAvailabilityZone
	parameters["WorkerIdentityRsaPublicKeyPem"] = service.config.WorkerIdentityRSAPublicKeyPEM
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
	tags := make(map[string]string, len(service.config.FixedTags)+2)
	for key, value := range service.config.FixedTags {
		tags[key] = value
	}
	tags["dirextalk:managed"] = "true"
	tags["dirextalk:connection-id"] = identity.ConnectionID
	return StackRequest{StackName: identity.StackName, TemplateURL: service.config.TemplateURL, ClientRequestToken: clientRequestToken(sessionID, identity.ConnectionID, fingerprint), Parameters: parameters, Tags: tags}
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
