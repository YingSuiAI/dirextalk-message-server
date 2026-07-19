package agentgrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const testServiceKey = "svc_message.AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"

const modelProfileCanary = "model-profile-api-key-canary"
const cloudDialogueConnectionID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const cloudDialogueTaskID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

func TestRunnerChatUsesTLS13MountedAuthenticationAndBoundOwner(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})

	result, err := runner.Invoke(context.Background(), "agent.chat", map[string]any{
		"owner_id":        "attacker",
		"conversation_id": "conversation-1",
		"prompt":          "hello",
		"conversation_context": map[string]any{
			"summary":  "legacy summary that must remain on the Message Server side",
			"messages": []any{map[string]any{"role": "user", "text": "legacy message"}},
		},
		"memory_disabled":                true,
		"expected_conversation_revision": 7,
		"cloud_dialogue_mode":            false,
		"knowledge_enabled":              false,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.service.mu.Lock()
	request := server.service.chatRequest
	authorization := server.service.authorization
	deadlineSet := server.service.deadlineSet
	tlsVersion := server.service.tlsVersion
	server.service.mu.Unlock()
	if request.GetOwnerId() != "owner-from-config" || request.GetConversationId() != "conversation-1" ||
		request.GetMessage() != "hello" || !request.GetMemoryDisabled() || request.GetExpectedConversationRevision() != 7 {
		t.Fatalf("unexpected request mapping: %#v", request)
	}
	if _, err := uuid.Parse(request.GetIdempotencyKey()); err != nil {
		t.Fatalf("generated idempotency key is not a UUID: %q", request.GetIdempotencyKey())
	}
	if strings.Contains(request.String(), "legacy summary") || strings.Contains(request.String(), "legacy message") {
		t.Fatal("legacy conversation context crossed the Agent service boundary")
	}
	if authorization != "DTX-Service-Key "+testServiceKey {
		t.Fatal("mounted service key was not sent as the required authorization metadata")
	}
	if !deadlineSet || tlsVersion != 0x0304 {
		t.Fatalf("deadline=%v tls_version=%#x, want TLS 1.3 with a deadline", deadlineSet, tlsVersion)
	}
	if result["text"] != "world" || result["conversation_id"] != "conversation-1" || result["conversation_revision"] != int64(8) {
		t.Fatalf("unexpected response mapping: %#v", result)
	}
	if _, present := result["cloud_task"]; present {
		t.Fatalf("ordinary chat fabricated Cloud task milestone: %#v", result)
	}
	steps, ok := result["steps"].([]map[string]any)
	if !ok || len(steps) != 1 || steps[0]["kind"] != "tool_call" || steps[0]["tool_name"] != "lookup" {
		t.Fatalf("unexpected step mapping: %#v", result["steps"])
	}
}

func TestRunnerStreamMapsEventsAndPropagatesCancellation(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{StreamTimeout: time.Second})
	var events []nativeagent.Event
	if err := runner.Stream(context.Background(), "agent.chat.stream", map[string]any{"prompt": "stream"}, func(event nativeagent.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Event != "delta" || events[1].Event != "tool" || events[2].Event != "done" {
		t.Fatalf("unexpected stream events: %#v", events)
	}
	if events[0].Data["text"] != "hel" || events[2].Data["text"] != "world" {
		t.Fatalf("unexpected stream data: %#v", events)
	}

	ctx, cancel := context.WithCancel(context.Background())
	server.service.cancelStarted = make(chan struct{})
	server.service.cancelObserved = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runner.Stream(ctx, "agent.chat.stream", map[string]any{"prompt": "cancel"}, func(nativeagent.Event) error { return nil })
	}()
	<-server.service.cancelStarted
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("cancellation error = %v", err)
	}
	select {
	case <-server.service.cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("server did not observe stream cancellation")
	}
}

func TestRunnerRedactsServiceErrorsAndSecrets(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	_, err := runner.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "fail",
	})
	if err == nil {
		t.Fatal("expected service failure")
	}
	for _, forbidden := range []string{"database-password", testServiceKey, "internal stack", modelProfileCanary} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error leaked sensitive detail %q: %v", forbidden, err)
		}
	}
	if err.Error() != "agent service request failed (internal)" {
		t.Fatalf("unexpected sanitized error: %v", err)
	}
}

func TestRunnerFailsClosedForUnrepresentableLegacyParameters(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	for _, params := range []map[string]any{
		{"prompt": "hello", "cloud_dialogue_mode": true},
		{"prompt": "hello", "knowledge_enabled": true},
		{"prompt": "hello", "embedding_profile": map[string]any{"provider": "openai"}},
		{"prompt": "hello", "attachments": []any{map[string]any{"name": "photo.png"}}},
		{"prompt": "hello", "cloud_connection_id": "connection-1"},
		{"prompt": "hello", "cloud_recipe_id": "recipe-1"},
		{"prompt": "hello", "cloud_recipe_revision": 1},
		{"messages": []any{map[string]any{"role": "user", "content": "hello"}}},
		{"prompt": "hello", "system_prompt": "override"},
		{"prompt": "hello", "enabled_tools": []any{"all"}},
		{"prompt": "hello", "model_profile_id": "deepseek:deepseek-v4-pro"},
		{"prompt": "hello", "model_profile": map[string]any{"api_key": modelProfileCanary}},
		{"prompt": "hello", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID, "model_profile": map[string]any{"api_key": modelProfileCanary}},
		{"prompt": "hello", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID, "conversation_context": map[string]any{"summary": "must not cross"}},
		{"prompt": "hello", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID, "owner_id": "attacker"},
		{"prompt": "hello", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID, "knowledge_enabled": true},
		{"prompt": "hello", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID, "knowledge_enabled": "false"},
	} {
		_, err := runner.Invoke(context.Background(), "agent.chat", params)
		if err == nil || err.Error() != "agent chat parameters cannot be represented by the remote runtime contract" {
			t.Fatalf("Invoke fail-closed error = %v", err)
		}
		err = runner.Stream(context.Background(), "agent.chat.stream", params, func(nativeagent.Event) error { return nil })
		if err == nil || err.Error() != "agent chat parameters cannot be represented by the remote runtime contract" {
			t.Fatalf("Stream fail-closed error = %v", err)
		}
	}
	server.service.mu.Lock()
	request := server.service.chatRequest
	streamRequest := server.service.streamRequest
	server.service.mu.Unlock()
	if request != nil || streamRequest != nil {
		t.Fatal("unrepresentable parameters reached the remote Agent service")
	}
}

func TestRunnerCloudDialogueValidatesOwnedConnectionAndForwardsOnlyTypedScope(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	var connectionRequests int
	server.cloud.getConnection = func(request *agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
		connectionRequests++
		if request.GetOwnerId() != "owner-from-config" || request.GetConnectionId() != cloudDialogueConnectionID {
			t.Fatalf("ownership request = %#v", request)
		}
		return &agentv1.GetCloudConnectionResponse{Connection: validAgentCloudConnectionProto(cloudDialogueConnectionID, "us-east-1")}, nil
	}
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second, StreamTimeout: time.Second})
	idempotencyKey := uuid.NewString()
	params := map[string]any{
		"prompt": "research official documentation", "conversation_id": "conversation-1",
		"idempotency_key": idempotencyKey, "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID,
		"knowledge_enabled": false,
	}
	result, err := runner.Invoke(context.Background(), "agent.chat", params)
	if err != nil {
		t.Fatal(err)
	}
	assertCloudTaskMilestone(t, result)
	if _, err := runner.Invoke(context.Background(), "agent.chat", params); err != nil {
		t.Fatal(err)
	}
	server.service.mu.Lock()
	request := server.service.chatRequest
	chatCalls := server.service.chatCalls
	server.service.mu.Unlock()
	if connectionRequests != 2 || chatCalls != 2 || request.GetIdempotencyKey() != idempotencyKey || request.GetCloudDialogueScope().GetCloudConnectionId() != cloudDialogueConnectionID {
		t.Fatalf("typed cloud request drifted: connection_reads=%d chat_calls=%d request=%#v", connectionRequests, chatCalls, request)
	}
	for _, forbidden := range []string{"owner-from-config", "api_key", "recipe", "approval"} {
		if strings.Contains(request.GetCloudDialogueScope().String(), forbidden) {
			t.Fatalf("typed Cloud Dialogue scope exposed forbidden field %q: %s", forbidden, request.GetCloudDialogueScope())
		}
	}

	var events []nativeagent.Event
	if err := runner.Stream(context.Background(), "agent.chat.stream", params, func(event nativeagent.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server.service.mu.Lock()
	streamRequest := server.service.streamRequest
	server.service.mu.Unlock()
	if streamRequest.GetCloudDialogueScope().GetCloudConnectionId() != cloudDialogueConnectionID || len(events) != 3 {
		t.Fatalf("stream Cloud Dialogue scope/events = %#v / %#v", streamRequest, events)
	}
	assertCloudTaskMilestone(t, events[2].Data)
}

func TestMapChatResponseCloudTaskMilestoneFailsClosed(t *testing.T) {
	t.Parallel()
	valid := chatResponse()
	valid.RelatedPlanIds = nil
	for name, test := range map[string]struct {
		response             *agentv1.ChatResponse
		expectedConversation string
		wantMilestone        bool
	}{
		"valid scoped task":    {response: valid, expectedConversation: "conversation-1", wantMilestone: true},
		"ordinary response":    {response: valid, expectedConversation: "", wantMilestone: false},
		"conversation drift":   {response: valid, expectedConversation: "different", wantMilestone: false},
		"invalid task ID":      {response: &agentv1.ChatResponse{ConversationId: "conversation-1", Message: valid.Message, RelatedTaskIds: []string{"task-1"}}, expectedConversation: "conversation-1", wantMilestone: false},
		"multiple task IDs":    {response: &agentv1.ChatResponse{ConversationId: "conversation-1", Message: valid.Message, RelatedTaskIds: []string{cloudDialogueTaskID, uuid.NewString()}}, expectedConversation: "conversation-1", wantMilestone: false},
		"related plan present": {response: &agentv1.ChatResponse{ConversationId: "conversation-1", Message: valid.Message, RelatedTaskIds: []string{cloudDialogueTaskID}, RelatedPlanIds: []string{uuid.NewString()}}, expectedConversation: "conversation-1", wantMilestone: false},
	} {
		t.Run(name, func(t *testing.T) {
			mapped, err := mapChatResponse(test.response, test.expectedConversation)
			if err != nil {
				t.Fatal(err)
			}
			_, present := mapped["cloud_task"]
			if present != test.wantMilestone {
				t.Fatalf("cloud task presence = %v, want %v: %#v", present, test.wantMilestone, mapped)
			}
			if test.wantMilestone {
				assertCloudTaskMilestone(t, mapped)
			}
		})
	}
}

func assertCloudTaskMilestone(t *testing.T, value map[string]any) {
	t.Helper()
	milestone, ok := value["cloud_task"].(map[string]any)
	if !ok {
		t.Fatalf("cloud task milestone = %#v", value["cloud_task"])
	}
	want := map[string]any{
		"schema": cloudTaskSchema, "task_id": cloudDialogueTaskID,
		"conversation_id": "conversation-1", "state": cloudTaskResearchQueued,
	}
	if !reflect.DeepEqual(milestone, want) {
		t.Fatalf("cloud task milestone = %#v, want %#v", milestone, want)
	}
	if _, present := milestone["plan_id"]; present {
		t.Fatalf("cloud task milestone fabricated plan id: %#v", milestone)
	}
}

func TestRunnerCloudDialogueFailsClosedForInvalidOrForeignConnection(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	base := map[string]any{"prompt": "research", "cloud_dialogue_mode": true, "cloud_connection_id": cloudDialogueConnectionID}
	for _, value := range []any{"", "connection-1", strings.ToUpper(cloudDialogueConnectionID)} {
		params := map[string]any{"prompt": base["prompt"], "cloud_dialogue_mode": true, "cloud_connection_id": value}
		if _, err := runner.Invoke(context.Background(), "agent.chat", params); err == nil {
			t.Fatalf("invalid Cloud Connection %q was accepted", value)
		}
	}
	server.cloud.getConnection = func(*agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
		connection := validAgentCloudConnectionProto(cloudDialogueConnectionID, "us-east-1")
		connection.OwnerId = "foreign-owner"
		return &agentv1.GetCloudConnectionResponse{Connection: connection}, nil
	}
	if _, err := runner.Invoke(context.Background(), "agent.chat", base); err == nil || strings.Contains(err.Error(), "foreign-owner") {
		t.Fatalf("foreign connection error = %v", err)
	}
	server.service.mu.Lock()
	chatCalls := server.service.chatCalls
	server.service.mu.Unlock()
	if chatCalls != 0 {
		t.Fatalf("invalid or foreign connection reached RuntimeService.Chat %d times", chatCalls)
	}
}

func TestRunnerStreamRequiresOneTerminalDoneEvent(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{StreamTimeout: time.Second})

	for _, test := range []struct {
		name    string
		message string
	}{
		{name: "EOF before done", message: "stream-eof-before-done"},
		{name: "delta after done", message: "stream-after-done"},
		{name: "duplicate done", message: "stream-duplicate-done"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []nativeagent.Event
			err := runner.Stream(context.Background(), "agent.chat.stream", map[string]any{"prompt": test.message}, func(event nativeagent.Event) error {
				events = append(events, event)
				return nil
			})
			if err == nil || err.Error() != "agent service returned an invalid stream sequence" {
				t.Fatalf("stream error = %v", err)
			}
			for _, event := range events {
				if event.Event == "done" {
					t.Fatalf("invalid stream emitted terminal success: %#v", events)
				}
			}
		})
	}
}

func TestRunnerEnforcesConfiguredMessageLimits(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	receiveLimited := newTestRunner(t, server, Config{MaxReceiveBytes: 128})
	if _, err := receiveLimited.Invoke(context.Background(), "agent.chat", map[string]any{"prompt": "large-response"}); err == nil || err.Error() != "agent service request failed (resourceexhausted)" {
		t.Fatalf("receive limit error = %v", err)
	}
	sendLimited := newTestRunner(t, server, Config{MaxSendBytes: 128})
	if _, err := sendLimited.Invoke(context.Background(), "agent.chat", map[string]any{"prompt": strings.Repeat("x", 1024)}); err == nil || err.Error() != "agent service request failed (resourceexhausted)" {
		t.Fatalf("send limit error = %v", err)
	}
}

func TestNewFailsClosedForInvalidSecurityConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "missing target", cfg: Config{CAFile: "ca", ServerName: "agent.test", ServiceKeyFile: "key", OwnerID: "owner"}},
		{name: "missing ca", cfg: Config{Target: "agent:443", ServerName: "agent.test", ServiceKeyFile: "key", OwnerID: "owner"}},
		{name: "missing server name", cfg: Config{Target: "agent:443", CAFile: "ca", ServiceKeyFile: "key", OwnerID: "owner"}},
		{name: "missing key", cfg: Config{Target: "agent:443", CAFile: "ca", ServerName: "agent.test", OwnerID: "owner"}},
		{name: "missing owner", cfg: Config{Target: "agent:443", CAFile: "ca", ServerName: "agent.test", ServiceKeyFile: "key"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(context.Background(), test.cfg); err == nil {
				t.Fatal("expected fail-closed configuration error")
			}
		})
	}
}

type runtimeTestService struct {
	agentv1.UnimplementedRuntimeServiceServer
	mu                   sync.Mutex
	chatRequest          *agentv1.ChatRequest
	streamRequest        *agentv1.StreamChatRequest
	runtimeConfigRequest *agentv1.GetRuntimeConfigRequest
	putRuntimeRequest    *agentv1.PutRuntimeConfigRequest
	getCapabilities      func(*agentv1.RuntimeServiceGetCapabilitiesRequest) (*agentv1.RuntimeServiceGetCapabilitiesResponse, error)
	getRuntimeConfig     func(*agentv1.GetRuntimeConfigRequest) (*agentv1.GetRuntimeConfigResponse, error)
	putRuntimeConfig     func(*agentv1.PutRuntimeConfigRequest) (*agentv1.PutRuntimeConfigResponse, error)
	chatCalls            int
	authorization        string
	deadlineSet          bool
	tlsVersion           uint16
	cancelStarted        chan struct{}
	cancelObserved       chan struct{}
}

func (service *runtimeTestService) Chat(ctx context.Context, request *agentv1.ChatRequest) (*agentv1.ChatResponse, error) {
	if request.GetMessage() == "fail" {
		return nil, status.Error(codes.Internal, "database-password internal stack")
	}
	if request.GetMessage() == "large-response" {
		response := chatResponse()
		response.Message.Content = strings.Repeat("x", 1024)
		return response, nil
	}
	service.capture(ctx, request)
	return chatResponse(), nil
}

func (service *runtimeTestService) StreamChat(request *agentv1.StreamChatRequest, stream grpc.ServerStreamingServer[agentv1.StreamChatResponse]) error {
	service.mu.Lock()
	service.streamRequest = request
	service.mu.Unlock()
	if request.GetMessage() == "cancel" {
		close(service.cancelStarted)
		<-stream.Context().Done()
		close(service.cancelObserved)
		return stream.Context().Err()
	}
	responses := []*agentv1.StreamChatResponse{
		{Event: &agentv1.StreamChatResponse_Delta{Delta: &agentv1.ChatDelta{MessageId: "message-1", Content: "hel"}}},
		{Event: &agentv1.StreamChatResponse_Tool{Tool: &agentv1.ToolExecutionSummary{ToolCallId: "call-1", ToolName: "lookup", Finished: true}}},
		{Event: &agentv1.StreamChatResponse_Done{Done: &agentv1.ChatDone{Response: chatResponse()}}},
	}
	switch request.GetMessage() {
	case "stream-eof-before-done":
		responses = responses[:1]
	case "stream-after-done":
		responses = []*agentv1.StreamChatResponse{responses[2], responses[0]}
	case "stream-duplicate-done":
		responses = []*agentv1.StreamChatResponse{responses[2], responses[2]}
	}
	for _, response := range responses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

func (service *runtimeTestService) capture(ctx context.Context, request *agentv1.ChatRequest) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	authorization := ""
	if len(values) == 1 {
		authorization = values[0]
	}
	_, deadlineSet := ctx.Deadline()
	tlsVersion := uint16(0)
	if peerInfo, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo); ok {
			tlsVersion = tlsInfo.State.Version
		}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	service.chatRequest = request
	service.chatCalls++
	service.authorization = authorization
	service.deadlineSet = deadlineSet
	service.tlsVersion = tlsVersion
}

func chatResponse() *agentv1.ChatResponse {
	return &agentv1.ChatResponse{
		ConversationId:       "conversation-1",
		Message:              &agentv1.RuntimeAssistantMessage{MessageId: "message-1", Content: "world"},
		ConversationRevision: 8,
		Steps:                []*agentv1.RuntimeStepSummary{{Kind: agentv1.RuntimeStepKind_RUNTIME_STEP_KIND_TOOL_CALL, ToolCallId: "call-1", ToolName: "lookup"}},
		RelatedTaskIds:       []string{cloudDialogueTaskID},
	}
}

type testRuntimeServer struct {
	target    string
	caFile    string
	keyFile   string
	service   *runtimeTestService
	tasks     *taskTestService
	cloud     *cloudTestService
	secrets   *secretBootstrapTestService
	knowledge *knowledgeTestService
}

func startRuntimeServer(t *testing.T) testRuntimeServer {
	t.Helper()
	certificate, caPEM := testCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &runtimeTestService{}
	tasks := &taskTestService{}
	cloud := &cloudTestService{}
	secrets := &secretBootstrapTestService{}
	knowledge := newKnowledgeTestService()
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
	})))
	agentv1.RegisterRuntimeServiceServer(server, service)
	agentv1.RegisterTaskServiceServer(server, tasks)
	agentv1.RegisterCloudControlServiceServer(server, cloud)
	agentv1.RegisterSecretBootstrapServiceServer(server, secrets)
	agentv1.RegisterKnowledgeServiceServer(server, knowledge)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	keyFile := filepath.Join(dir, "service-key")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte(testServiceKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return testRuntimeServer{target: listener.Addr().String(), caFile: caFile, keyFile: keyFile, service: service, tasks: tasks, cloud: cloud, secrets: secrets, knowledge: knowledge}
}

func newTestRunner(t *testing.T, server testRuntimeServer, override Config) *Runner {
	t.Helper()
	override.Target = server.target
	override.CAFile = server.caFile
	override.ServerName = "agent.test"
	override.ServiceKeyFile = server.keyFile
	override.AgentInstanceID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	override.OwnerID = "owner-from-config"
	runner, err := New(context.Background(), override)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	return runner
}

func testCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	now := time.Now()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "agent.test"}, DNSNames: []string{"agent.test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
