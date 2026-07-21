package nativeagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/schema"
)

func TestWebSearchUsesRequestScopedTavilyCredentials(t *testing.T) {
	var receivedAuthorization string
	var requestContainedKey bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatalf("decode Tavily request: %v", err)
		}
		receivedAuthorization = request.Header.Get("Authorization")
		_, requestContainedKey = payload["api_key"]
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"answer":"connected","results":[{"title":"Dirextalk","url":"https://example.com","content":"ok","score":0.9}]}`))
	}))
	defer server.Close()

	runtime := New(Config{
		WebSearchEndpoint: server.URL,
		HTTPClient:        server.Client(),
	})
	params := map[string]any{
		"tool_credentials": map[string]any{
			"web_search": map[string]any{
				"enabled":  true,
				"provider": "tavily",
				"api_key":  "tvly-request-only",
			},
		},
	}
	result, err := runtime.Invoke(context.Background(), "agent.web_search.test", params)
	if err != nil {
		t.Fatalf("test web search: %v", err)
	}
	if result["ok"] != true || receivedAuthorization != "Bearer tvly-request-only" {
		t.Fatalf("unexpected web search result=%#v authorization=%q", result, receivedAuthorization)
	}
	if requestContainedKey {
		t.Fatal("Tavily API key must not be included in the JSON request body")
	}
	if strings.Contains(jsonValue(result), "tvly-request-only") {
		t.Fatalf("web search response leaked the API key: %#v", result)
	}
	if tools := runtime.requestScopedWebSearchTool(map[string]any{}); len(tools) != 0 {
		t.Fatalf("web search tool must not exist without request credentials: %#v", tools)
	}
	if _, err := runtime.searchTavily(
		context.Background(),
		webSearchCredentials{Enabled: true, Provider: "tavily", APIKey: "tvly-request-only"},
		map[string]any{"query": strings.Repeat("a", 1001)},
	); err == nil || !strings.Contains(err.Error(), "1000") {
		t.Fatalf("expected oversized search query rejection, got %v", err)
	}
}

func TestWebSearchMapsProviderFailuresWithoutLeakingSecrets(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantError  string
	}{
		{name: "unauthorized", statusCode: http.StatusUnauthorized, wantError: "API key was rejected"},
		{name: "forbidden", statusCode: http.StatusForbidden, wantError: "API key was rejected"},
		{name: "rate limited", statusCode: http.StatusTooManyRequests, wantError: "rate limit"},
		{name: "provider failure", statusCode: http.StatusInternalServerError, wantError: "HTTP 500"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.WriteHeader(test.statusCode)
				_, _ = w.Write([]byte(`{"detail":"provider-body-must-not-leak"}`))
			}))
			defer server.Close()

			runtime := New(Config{
				WebSearchEndpoint: server.URL,
				HTTPClient:        server.Client(),
			})
			_, err := runtime.searchTavily(
				context.Background(),
				webSearchCredentials{Enabled: true, Provider: "tavily", APIKey: "tvly-must-not-leak"},
				map[string]any{"query": "connection test"},
			)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected error containing %q, got %v", test.wantError, err)
			}
			for _, secret := range []string{"tvly-must-not-leak", "provider-body-must-not-leak"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("web search error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestSanitizeConfigStripsNestedCredentials(t *testing.T) {
	input := map[string]any{
		"capabilities": map[string]any{
			"web_search": map[string]any{
				"provider": "tavily",
				"api_key":  "tvly-secret",
			},
			"aws": map[string]any{
				"region":            "us-east-1",
				"access_key_id":     "AKIASECRET",
				"secret_access_key": "aws-secret",
				"session_token":     "aws-session",
			},
		},
	}

	sanitized := sanitizeConfig(input)
	serialized := jsonValue(sanitized)
	for _, secret := range []string{
		"tvly-secret",
		"AKIASECRET",
		"aws-secret",
		"aws-session",
	} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("sanitized runtime config leaked %q: %#v", secret, sanitized)
		}
	}
	if !strings.Contains(serialized, "tavily") ||
		!strings.Contains(serialized, "us-east-1") {
		t.Fatalf("sanitizer removed non-secret metadata: %#v", sanitized)
	}
}

func TestAWSCreateRequiresBoundSingleUseApproval(t *testing.T) {
	fake := newFakeAWSClient()
	runtime := New(Config{
		AWSClientFactory: func(_ context.Context, credentials AWSClientCredentials, _ *http.Client) (AWSClient, error) {
			if credentials.AccessKeyID != "AKIAREQUEST" ||
				credentials.SecretAccessKey != "request-secret" ||
				credentials.Region != "us-east-1" {
				t.Fatalf("unexpected request credentials: %#v", credentials)
			}
			return fake, nil
		},
	})
	params := awsTestParams("conversation-a")
	approvalResult, err := runtime.prepareAWSCreateApproval(
		context.Background(),
		toolCredentialsFromParams(params).AWS,
		"conversation-a",
		map[string]any{
			"instance_type":  "t3.small",
			"image_alias":    defaultAWSImageAlias,
			"volume_size_gb": 20,
			"purpose":        "agent task",
		},
	)
	if err != nil {
		t.Fatalf("prepare AWS approval: %v", err)
	}
	if fake.createCalls != 0 {
		t.Fatalf("create executed before approval")
	}
	approval := nestedAnyMap(approvalResult["approval"])
	approvalID := trimString(approval["id"])
	if approvalResult["status"] != "confirmation_required" || approvalID == "" {
		t.Fatalf("unexpected approval result: %#v", approvalResult)
	}
	if strings.Contains(jsonValue(approvalResult), "request-secret") {
		t.Fatalf("approval leaked credentials: %#v", approvalResult)
	}

	wrongConversation := awsTestParams("conversation-b")
	wrongConversation["approval_id"] = approvalID
	if _, err := runtime.Invoke(context.Background(), "agent.aws.approvals.execute", wrongConversation); err == nil {
		t.Fatal("expected conversation binding to reject approval")
	}
	if fake.createCalls != 0 {
		t.Fatalf("wrong conversation executed create")
	}

	execute := awsTestParams("conversation-a")
	execute["approval_id"] = approvalID
	result, err := runtime.Invoke(context.Background(), "agent.aws.approvals.execute", execute)
	if err != nil {
		t.Fatalf("execute AWS approval: %v", err)
	}
	if result["ok"] != true || fake.createCalls != 1 || fake.lastClientToken != approvalID {
		t.Fatalf("unexpected execution result=%#v fake=%#v", result, fake)
	}
	if fake.lastCreate.InstanceType != "t3.small" ||
		fake.lastCreate.ImageID != "ami-al2023" ||
		fake.lastCreate.VolumeSizeGB != 20 {
		t.Fatalf("approved plan changed before execution: %#v", fake.lastCreate)
	}
	if _, err := runtime.Invoke(context.Background(), "agent.aws.approvals.execute", execute); err == nil {
		t.Fatal("expected consumed approval to reject replay")
	}
	if fake.createCalls != 1 {
		t.Fatalf("approval replay created another instance")
	}
}

func TestAWSTerminationRejectsUnmanagedInstanceAndSupportsCancel(t *testing.T) {
	fake := newFakeAWSClient()
	fake.instances["i-000000001"] = AWSInstance{
		InstanceID:   "i-000000001",
		InstanceType: "t3.micro",
		State:        "running",
		Managed:      false,
	}
	fake.instances["i-000000002"] = AWSInstance{
		InstanceID:   "i-000000002",
		InstanceType: "t3.micro",
		State:        "running",
		Managed:      true,
	}
	runtime := New(Config{
		AWSClientFactory: func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error) {
			return fake, nil
		},
	})
	credentials := toolCredentialsFromParams(awsTestParams("conversation-a")).AWS
	if _, err := runtime.prepareAWSTerminateApproval(
		context.Background(),
		credentials,
		"conversation-a",
		map[string]any{"instance_id": "i-000000001"},
	); err == nil || !strings.Contains(err.Error(), "not managed") {
		t.Fatalf("expected unmanaged instance rejection, got %v", err)
	}
	approvalResult, err := runtime.prepareAWSTerminateApproval(
		context.Background(),
		credentials,
		"conversation-a",
		map[string]any{"instance_id": "i-000000002"},
	)
	if err != nil {
		t.Fatalf("prepare managed termination: %v", err)
	}
	approvalID := trimString(nestedAnyMap(approvalResult["approval"])["id"])
	cancel := map[string]any{
		"approval_id":     approvalID,
		"conversation_id": "conversation-a",
	}
	result, err := runtime.Invoke(context.Background(), "agent.aws.approvals.cancel", cancel)
	if err != nil || result["status"] != "cancelled" {
		t.Fatalf("cancel AWS approval: result=%#v err=%v", result, err)
	}
	if fake.terminateCalls != 0 {
		t.Fatal("cancelled approval terminated an instance")
	}
}

func TestAWSApprovalExpiresWithoutExecuting(t *testing.T) {
	fake := newFakeAWSClient()
	runtime := New(Config{
		AWSClientFactory: func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error) {
			return fake, nil
		},
	})
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	runtime.awsApprovals.now = func() time.Time { return now }
	credentials := toolCredentialsFromParams(awsTestParams("conversation-a")).AWS
	approvalResult, err := runtime.prepareAWSCreateApproval(
		context.Background(),
		credentials,
		"conversation-a",
		map[string]any{"instance_type": "t3.small"},
	)
	if err != nil {
		t.Fatalf("prepare AWS approval: %v", err)
	}
	approvalID := trimString(nestedAnyMap(approvalResult["approval"])["id"])
	now = now.Add(awsApprovalTTL + time.Second)
	execute := awsTestParams("conversation-a")
	execute["approval_id"] = approvalID

	if _, err := runtime.Invoke(context.Background(), "agent.aws.approvals.execute", execute); err == nil ||
		!strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired approval rejection, got %v", err)
	}
	if fake.createCalls != 0 {
		t.Fatalf("expired approval created %d instances", fake.createCalls)
	}
}

func TestAWSReadOnlyListToolDoesNotRequireApprovalOrMutate(t *testing.T) {
	fake := newFakeAWSClient()
	fake.instances["i-000000001"] = AWSInstance{
		InstanceID:   "i-000000001",
		InstanceType: "t3.micro",
		State:        "running",
		Managed:      true,
	}
	runtime := New(Config{
		AWSClientFactory: func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error) {
			return fake, nil
		},
	})
	if tools := runtime.requestScopedAWSTools(map[string]any{}); len(tools) != 0 {
		t.Fatalf("AWS tools must not exist without request credentials: %#v", tools)
	}
	tools := runtime.requestScopedAWSTools(awsTestParams("conversation-a"))
	var listTool *Tool
	for index := range tools {
		if tools[index].Name == "aws_ec2_instances_list" {
			listTool = &tools[index]
			break
		}
	}
	if listTool == nil {
		t.Fatal("request-scoped AWS list tool is missing")
	}

	rawResult, err := listTool.Handler(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("list EC2 instances: %v", err)
	}
	result := rawResult.(map[string]any)
	instances := result["instances"].([]map[string]any)
	if len(instances) != 1 || instances[0]["instance_id"] != "i-000000001" {
		t.Fatalf("unexpected list result: %#v", result)
	}
	if fake.createCalls != 0 || fake.terminateCalls != 0 {
		t.Fatalf("read-only tool mutated AWS state: %#v", fake)
	}
}

func TestAWSCredentialErrorsAreRedacted(t *testing.T) {
	fake := newFakeAWSClient()
	fake.identityErr = errors.New("bad AKIAREQUEST request-secret request-token")
	runtime := New(Config{
		AWSClientFactory: func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error) {
			return fake, nil
		},
	})
	_, err := runtime.Invoke(
		context.Background(),
		"agent.aws.credentials.test",
		awsTestParams("conversation-a"),
	)
	if err == nil {
		t.Fatal("expected AWS identity failure")
	}
	for _, secret := range []string{"AKIAREQUEST", "request-secret", "request-token"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("AWS error leaked %q: %v", secret, err)
		}
	}
}

func TestAWSCredentialTestReturnsOnlyPublicIdentityFields(t *testing.T) {
	fake := newFakeAWSClient()
	runtime := New(Config{
		AWSClientFactory: func(context.Context, AWSClientCredentials, *http.Client) (AWSClient, error) {
			return fake, nil
		},
	})

	result, err := runtime.Invoke(
		context.Background(),
		"agent.aws.credentials.test",
		awsTestParams("conversation-a"),
	)
	if err != nil {
		t.Fatalf("test AWS credentials: %v", err)
	}
	identity := nestedAnyMap(result["identity"])
	if len(identity) != 2 ||
		identity["account_id"] != "123456789012" ||
		identity["arn"] != "arn:aws:iam::123456789012:user/test" {
		t.Fatalf("unexpected public identity response: %#v", result)
	}
}

func TestApprovalEventsAreExtractedFromToolResults(t *testing.T) {
	messages := []*schema.Message{{
		Role: schema.Tool,
		Content: `{"result":{"status":"confirmation_required","approval":` +
			`{"id":"aws_123","kind":"aws","title":"Create"}}}`,
	}}
	approvals := approvalsFromEinoMessages(messages)
	if len(approvals) != 1 || approvals[0]["id"] != "aws_123" {
		t.Fatalf("unexpected approvals: %#v", approvals)
	}
}

func awsTestParams(conversationID string) map[string]any {
	return map[string]any{
		"conversation_id": conversationID,
		"tool_credentials": map[string]any{
			"aws": map[string]any{
				"enabled":           true,
				"access_key_id":     "AKIAREQUEST",
				"secret_access_key": "request-secret",
				"session_token":     "request-token",
				"region":            "us-east-1",
			},
		},
	}
}

type fakeAWSClient struct {
	identity        AWSIdentity
	identityErr     error
	imageID         string
	instances       map[string]AWSInstance
	createCalls     int
	terminateCalls  int
	lastClientToken string
	lastCreate      AWSCreateInstanceInput
}

func newFakeAWSClient() *fakeAWSClient {
	return &fakeAWSClient{
		identity: AWSIdentity{
			AccountID: "123456789012",
			ARN:       "arn:aws:iam::123456789012:user/test",
			UserID:    "test",
		},
		imageID:   "ami-al2023",
		instances: map[string]AWSInstance{},
	}
}

func (f *fakeAWSClient) Identity(context.Context) (AWSIdentity, error) {
	return f.identity, f.identityErr
}

func (f *fakeAWSClient) ListInstances(context.Context) ([]AWSInstance, error) {
	result := make([]AWSInstance, 0, len(f.instances))
	for _, instance := range f.instances {
		result = append(result, instance)
	}
	return result, nil
}

func (f *fakeAWSClient) ResolveImage(context.Context, string) (string, error) {
	return f.imageID, nil
}

func (f *fakeAWSClient) DescribeInstance(_ context.Context, instanceID string) (AWSInstance, error) {
	instance, ok := f.instances[instanceID]
	if !ok {
		return AWSInstance{}, errors.New("instance not found")
	}
	return instance, nil
}

func (f *fakeAWSClient) CreateInstance(_ context.Context, input AWSCreateInstanceInput, clientToken string) (AWSInstance, error) {
	f.createCalls++
	f.lastCreate = input
	f.lastClientToken = clientToken
	return AWSInstance{
		InstanceID:   "i-000000003",
		InstanceType: input.InstanceType,
		ImageID:      input.ImageID,
		State:        "pending",
		Managed:      true,
	}, nil
}

func (f *fakeAWSClient) TerminateInstance(_ context.Context, instanceID string) (AWSInstance, error) {
	f.terminateCalls++
	return AWSInstance{InstanceID: instanceID, State: "shutting-down", Managed: true}, nil
}
