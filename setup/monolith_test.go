package setup

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/p2p"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
)

type testAgentGRPCRunner struct{}

const testAgentInstanceID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func (*testAgentGRPCRunner) Apply(context.Context, string) error { return nil }
func (*testAgentGRPCRunner) Invoke(context.Context, string, map[string]any) (map[string]any, error) {
	return map[string]any{}, nil
}
func (*testAgentGRPCRunner) Stream(context.Context, string, map[string]any, func(nativeagent.Event) error) error {
	return nil
}
func (*testAgentGRPCRunner) Close() error { return nil }
func (*testAgentGRPCRunner) AgentEventSource() p2p.AgentEventSource {
	return p2p.AgentEventSource{AgentInstanceID: testAgentInstanceID, CallerID: "dirextalk-project:example.com"}
}
func (*testAgentGRPCRunner) WatchEvents(context.Context, int64) (p2p.AgentEventStream, error) {
	return nil, nil
}
func (*testAgentGRPCRunner) ListCloudDeployments(context.Context) ([]p2p.CloudDeployment, error) {
	return []p2p.CloudDeployment{}, nil
}
func (*testAgentGRPCRunner) GetCloudDeployment(context.Context, string) (p2p.CloudDeployment, bool, error) {
	return p2p.CloudDeployment{}, false, nil
}
func (*testAgentGRPCRunner) CreateAgentSecretBootstrap(context.Context, p2p.CreateCloudSecretBootstrapRequest) (p2p.CloudSecretBootstrapSession, error) {
	return p2p.CloudSecretBootstrapSession{}, nil
}
func (*testAgentGRPCRunner) UploadAgentEncryptedSecret(context.Context, p2p.UploadCloudEncryptedSecretRequest) (p2p.CloudSecretBootstrapSession, error) {
	return p2p.CloudSecretBootstrapSession{}, nil
}
func (*testAgentGRPCRunner) PreviewAgentAWSIdentity(context.Context, p2p.CloudIdentityPreviewRequest) (p2p.CloudIdentityPreviewEvidence, error) {
	return p2p.CloudIdentityPreviewEvidence{}, nil
}
func (*testAgentGRPCRunner) GetAgentCloudPlan(context.Context, p2p.AgentCloudPlanRequest) (p2p.AgentCloudPlan, bool, error) {
	return p2p.AgentCloudPlan{}, false, nil
}
func (*testAgentGRPCRunner) CreateAgentCloudGoal(context.Context, p2p.AgentCloudGoalCreateRequest) (p2p.AgentCloudGoalResult, error) {
	return p2p.AgentCloudGoalResult{}, nil
}
func (*testAgentGRPCRunner) ListAgentCloudPlans(context.Context) ([]p2p.AgentCloudPlan, error) {
	return nil, nil
}
func (*testAgentGRPCRunner) ListAgentCloudConnections(context.Context) ([]p2p.AgentCloudConnection, error) {
	return nil, nil
}
func (*testAgentGRPCRunner) CreateAgentCloudApprovalChallenge(context.Context, p2p.AgentCloudChallengeRequest) (p2p.AgentCloudChallenge, error) {
	return p2p.AgentCloudChallenge{}, nil
}
func (*testAgentGRPCRunner) ApproveAgentCloudPlan(context.Context, p2p.AgentCloudApproveRequest) (p2p.AgentCloudPlan, error) {
	return p2p.AgentCloudPlan{}, nil
}
func (*testAgentGRPCRunner) EstablishAgentAWSConnection(context.Context, p2p.AgentCloudEstablishRequest) (p2p.AgentCloudConnection, error) {
	return p2p.AgentCloudConnection{}, nil
}
func (*testAgentGRPCRunner) GetAgentCloudConnection(context.Context, p2p.AgentCloudConnectionRequest) (p2p.AgentCloudConnection, bool, error) {
	return p2p.AgentCloudConnection{}, false, nil
}
func (*testAgentGRPCRunner) CreateAgentCloudDeploymentDestroyChallenge(context.Context, p2p.AgentCloudDeploymentDestroyChallengeRequest) (p2p.AgentCloudDeploymentDestroyChallenge, error) {
	return p2p.AgentCloudDeploymentDestroyChallenge{}, nil
}
func (*testAgentGRPCRunner) ApproveAgentCloudDeploymentDestroy(context.Context, p2p.AgentCloudDeploymentDestroyApproveRequest) (p2p.AgentCloudDeploymentDestroyResult, error) {
	return p2p.AgentCloudDeploymentDestroyResult{}, nil
}
func (*testAgentGRPCRunner) GetAgentCloudDestroyOperation(context.Context, p2p.AgentCloudDestroyOperationRequest) (p2p.AgentCloudDestroyOperation, bool, error) {
	return p2p.AgentCloudDestroyOperation{}, false, nil
}

type chatOnlyAgentGRPCRunner struct{}

func (*chatOnlyAgentGRPCRunner) Apply(context.Context, string) error { return nil }
func (*chatOnlyAgentGRPCRunner) Invoke(context.Context, string, map[string]any) (map[string]any, error) {
	return map[string]any{}, nil
}
func (*chatOnlyAgentGRPCRunner) Stream(context.Context, string, map[string]any, func(nativeagent.Event) error) error {
	return nil
}
func (*chatOnlyAgentGRPCRunner) Close() error { return nil }

func TestP2PDatabaseOptionsUseGlobalDatabaseWhenConfigured(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.Global.DatabaseOptions.ConnectionString = "postgres://localhost/global?sslmode=disable"
	cfg.RoomServer.Database.ConnectionString = "postgres://localhost/roomserver?sslmode=disable"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "postgres://localhost/global?sslmode=disable" {
		t.Fatalf("expected global database, got %q", got.ConnectionString)
	}
}

func TestP2PDatabaseOptionsFallbackToRoomserverDatabase(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.RoomServer.Database.ConnectionString = "postgres://localhost/roomserver?sslmode=disable"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "postgres://localhost/roomserver?sslmode=disable" {
		t.Fatalf("expected roomserver database fallback, got %q", got.ConnectionString)
	}
}

func TestPersistentP2PServiceRejectsSQLiteInsteadOfFallingBackToMemory(t *testing.T) {
	dbOpts := config.DatabaseOptions{ConnectionString: "file:p2p.db"}

	service, err := newPersistentP2PService(
		context.Background(),
		p2p.Config{ServerName: "example.com"},
		sqlutil.NewConnectionManager(nil, dbOpts),
		&dbOpts,
		nil,
	)

	if err == nil || !strings.Contains(err.Error(), "SQLite") {
		t.Fatalf("expected SQLite-backed startup to fail explicitly, got service=%v err=%v", service, err)
	}
	if service != nil {
		t.Fatalf("expected no in-memory P2P service fallback")
	}
}

func TestP2PEventRetentionFromEnv(t *testing.T) {
	t.Setenv("P2P_EVENT_RETENTION_MAX_ROWS", "5000")
	t.Setenv("P2P_EVENT_RETENTION_PRUNE_ON_WRITE", "true")

	if got := p2pEventRetentionMaxRowsFromEnv(); got != 5000 {
		t.Fatalf("expected max rows 5000, got %d", got)
	}
	if !p2pEventRetentionPruneOnWriteFromEnv() {
		t.Fatalf("expected prune on write to be enabled")
	}
}

func TestP2PEventRetentionInvalidEnvDisablesPruning(t *testing.T) {
	t.Setenv("P2P_EVENT_RETENTION_MAX_ROWS", "-1")
	t.Setenv("P2P_EVENT_RETENTION_PRUNE_ON_WRITE", "not-bool")

	if got := p2pEventRetentionMaxRowsFromEnv(); got != 0 {
		t.Fatalf("expected invalid max rows to disable retention, got %d", got)
	}
	if p2pEventRetentionPruneOnWriteFromEnv() {
		t.Fatalf("expected invalid prune flag to disable pruning")
	}
}

func TestP2PAgentGRPCBackendDefaultsToLocal(t *testing.T) {
	unsetAgentGRPCEnvironment(t)
	config, err := p2pAgentGRPCBackendConfigFromEnv()
	if err != nil || config.Enabled {
		t.Fatalf("default Agent backend config=%#v err=%v", config, err)
	}
	runner, err := newP2PAgentChatRunner(context.Background(), "example.com", config, nil)
	if err != nil || runner != nil {
		t.Fatalf("default local Runner=%v err=%v", runner, err)
	}
}

func TestP2PAgentGRPCBackendFailsClosedForIncompleteOrInlineSecretConfiguration(t *testing.T) {
	for name, configure := range map[string]func(*testing.T){
		"incomplete enabled backend": func(t *testing.T) {
			t.Setenv("P2P_AGENT_GRPC_ENABLED", "true")
		},
		"invalid enabled flag": func(t *testing.T) {
			t.Setenv("P2P_AGENT_GRPC_ENABLED", "sometimes")
		},
		"inline secret": func(t *testing.T) {
			t.Setenv("P2P_AGENT_GRPC_SERVICE_KEY", "sk-"+strings.Repeat("q", 24))
		},
	} {
		t.Run(name, func(t *testing.T) {
			unsetAgentGRPCEnvironment(t)
			configure(t)
			_, err := p2pAgentGRPCBackendConfigFromEnv()
			if err == nil || strings.Contains(err.Error(), "sk-") || strings.Contains(err.Error(), strings.Repeat("q", 24)) {
				t.Fatalf("unsafe Agent backend configuration error=%v", err)
			}
		})
	}
}

func TestP2PAgentGRPCBackendBuildsChatOnlyRunnerWithTrustedOwner(t *testing.T) {
	unsetAgentGRPCEnvironment(t)
	caFile := writeAgentMountedFile(t, "agent-ca.pem", 0o644)
	serviceKeyFile := writeAgentMountedFile(t, "agent-service-key", 0o600)
	t.Setenv("P2P_AGENT_GRPC_ENABLED", "true")
	t.Setenv("P2P_AGENT_GRPC_TARGET", "dns:///agent.internal:7443")
	t.Setenv("P2P_AGENT_GRPC_CA_FILE", caFile)
	t.Setenv("P2P_AGENT_GRPC_SERVER_NAME", "agent.internal")
	t.Setenv("P2P_AGENT_GRPC_SERVICE_KEY_FILE", serviceKeyFile)
	t.Setenv("P2P_AGENT_GRPC_INSTANCE_ID", testAgentInstanceID)

	config, err := p2pAgentGRPCBackendConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var received AgentGRPCDialConfig
	wantRunner := &testAgentGRPCRunner{}
	runner, err := newP2PAgentChatRunner(context.Background(), "example.com", config, func(_ context.Context, value AgentGRPCDialConfig) (AgentGRPCRunner, error) {
		received = value
		return wantRunner, nil
	})
	if err != nil || runner != wantRunner {
		t.Fatalf("remote Chat Runner=%v err=%v", runner, err)
	}
	cloudReader, err := p2pAgentCloudDeploymentReader(config, runner)
	if err != nil || cloudReader != wantRunner {
		t.Fatalf("remote Cloud deployment reader=%v err=%v", cloudReader, err)
	}
	secretClient, err := p2pAgentSecretBootstrapClient(config, runner)
	if err != nil || secretClient != wantRunner {
		t.Fatalf("remote secret bootstrap client=%v err=%v", secretClient, err)
	}
	identityClient, err := p2pAgentIdentityPreviewClient(config, runner)
	if err != nil || identityClient != wantRunner {
		t.Fatalf("remote identity preview client=%v err=%v", identityClient, err)
	}
	cloudControlClient, err := p2pAgentCloudControlClient(config, runner)
	if err != nil || cloudControlClient != wantRunner {
		t.Fatalf("remote cloud control client=%v err=%v", cloudControlClient, err)
	}
	eventClient, err := p2pAgentEventClient(config, runner, "example.com")
	if err != nil || eventClient != wantRunner {
		t.Fatalf("remote Agent event client=%v err=%v", eventClient, err)
	}
	if received.Target != "dns:///agent.internal:7443" || received.CAFile != caFile || received.ServerName != "agent.internal" ||
		received.ServiceKeyFile != serviceKeyFile || received.AgentInstanceID != testAgentInstanceID || received.OwnerID != "dirextalk-project:example.com" {
		t.Fatalf("Agent dial config=%#v", received)
	}

	factoryCanary := "sk-" + strings.Repeat("r", 24)
	_, err = newP2PAgentChatRunner(context.Background(), "example.com", config, func(context.Context, AgentGRPCDialConfig) (AgentGRPCRunner, error) {
		return nil, errors.New("dial failed: " + factoryCanary)
	})
	if err == nil || strings.Contains(err.Error(), factoryCanary) {
		t.Fatalf("factory failure was not fail-closed and redacted: %v", err)
	}
	if _, err = p2pAgentCloudDeploymentReader(config, &chatOnlyAgentGRPCRunner{}); err == nil {
		t.Fatal("enabled Agent backend accepted a Runner without Cloud deployment query capability")
	}
	if _, err = p2pAgentSecretBootstrapClient(config, &chatOnlyAgentGRPCRunner{}); err == nil {
		t.Fatal("enabled Agent backend accepted a Runner without encrypted secret bootstrap capability")
	}
	if _, err = p2pAgentIdentityPreviewClient(config, &chatOnlyAgentGRPCRunner{}); err == nil {
		t.Fatal("enabled Agent backend accepted a Runner without AWS identity preview capability")
	}
	if _, err = p2pAgentCloudControlClient(config, &chatOnlyAgentGRPCRunner{}); err == nil {
		t.Fatal("enabled Agent backend accepted a Runner without typed cloud control capability")
	}
	if _, err = p2pAgentEventClient(config, &chatOnlyAgentGRPCRunner{}, "example.com"); err == nil {
		t.Fatal("enabled Agent backend accepted a Runner without durable events capability")
	}
}

func unsetAgentGRPCEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"P2P_AGENT_GRPC_ENABLED", "P2P_AGENT_GRPC_TARGET", "P2P_AGENT_GRPC_CA_FILE",
		"P2P_AGENT_GRPC_SERVER_NAME", "P2P_AGENT_GRPC_SERVICE_KEY_FILE", "P2P_AGENT_GRPC_INSTANCE_ID",
		"P2P_AGENT_GRPC_SERVICE_KEY",
	} {
		unsetEnvironmentVariable(t, name)
	}
}

func writeAgentMountedFile(t *testing.T, name string, mode os.FileMode) string {
	t.Helper()
	path := t.TempDir() + string(os.PathSeparator) + name
	if err := os.WriteFile(path, []byte("synthetic mounted material\n"), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestP2PCloudConnectionStackConfigFromEnv(t *testing.T) {
	unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL")
	unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST")
	t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", `{"schema":"dirextalk.connection-template-reference/v1","mode":"s3_binding","binding":{"schema":"dirextalk.immutable-artifact-binding/v1","kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","bucket":"dirextalk-artifacts","key":"releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml","version_id":"version-00000001","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml","kms_key_id":"alias/dirextalk-artifacts"}}`)
	t.Setenv("P2P_CLOUD_CONNECTION_STACK_SOURCE_TREE_DIGEST", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_KEY_ID", "node-key-1")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_PUBLIC_KEY_SPKI_BASE64", "public-key-material")
	t.Setenv("P2P_CLOUD_CONNECTION_ROLE_PLAN_TTL_SECONDS", "900")

	config := p2pCloudConnectionStackConfigFromEnv()
	if config.TemplateURL != "" || config.ConnectionTemplate.Mode != "s3_binding" ||
		config.TemplateDigest != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		config.SourceTreeDigest != "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ||
		config.NodeKeyID != "node-key-1" || config.NodePublicKeySPKIBase64 != "public-key-material" ||
		config.RolePlanTTL.Seconds() != 900 {
		t.Fatalf("connection Stack config = %#v", config)
	}

	t.Setenv("P2P_CLOUD_CONNECTION_ROLE_PLAN_TTL_SECONDS", "0")
	if config := p2pCloudConnectionStackConfigFromEnv(); config.RolePlanTTL != 0 {
		t.Fatalf("invalid role-plan TTL must fail closed: %#v", config)
	}
}

func TestP2PCloudConnectionStackConfigFromEnvRejectsLegacyOrMalformedTemplateSettings(t *testing.T) {
	validTemplate := `{"schema":"dirextalk.connection-template-reference/v1","mode":"publish_intent","publish_intent":{"kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml"}}`
	for name, configure := range map[string]func(*testing.T){
		"legacy_url": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate)
			t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL", "https://s3.us-east-1.amazonaws.com/dirextalk-artifacts/template.yaml?versionId=version-00000001")
		},
		"legacy_digest": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate)
			t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		},
		"unknown_json_field": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate[:len(validTemplate)-1]+`,"url":"https://mutable.example.invalid/template.yaml"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL")
			unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST")
			configure(t)
			if config := p2pCloudConnectionStackConfigFromEnv(); config.ConnectionTemplate.Mode != "" || config.TemplateDigest != "" || config.RolePlanTTL != 0 {
				t.Fatalf("legacy or malformed template configuration was accepted: %#v", config)
			}
		})
	}
}

func unsetEnvironmentVariable(t *testing.T, name string) {
	t.Helper()
	previous, existed := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(name, previous)
			return
		}
		_ = os.Unsetenv(name)
	})
}

func TestP2PCloudConnectionCredentialBootstrapConfigFromEnv(t *testing.T) {
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_ENDPOINT", "https://bootstrap.internal.example/v1/aws-bootstrap/sessions")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_CA_FILE", "/run/dirextalk/bootstrap-ca.pem")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_CERT_FILE", "/run/dirextalk/bootstrap-client.pem")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_KEY_FILE", "/run/dirextalk/bootstrap-client.key")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_TIMEOUT_SECONDS", "7")

	config := p2pCloudConnectionCredentialBootstrapConfigFromEnv()
	if config.Endpoint != "https://bootstrap.internal.example/v1/aws-bootstrap/sessions" || config.CAFile != "/run/dirextalk/bootstrap-ca.pem" ||
		config.CertificateFile != "/run/dirextalk/bootstrap-client.pem" || config.KeyFile != "/run/dirextalk/bootstrap-client.key" || config.Timeout != 7*time.Second {
		t.Fatalf("credential bootstrap config = %#v", config)
	}
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_TIMEOUT_SECONDS", "31")
	if config := p2pCloudConnectionCredentialBootstrapConfigFromEnv(); config.Timeout >= 0 {
		t.Fatalf("invalid timeout must fail closed: %#v", config)
	}
}
