package agentgrpc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	cloudErrorCanary  = "AKIA-CLOUD-READER-SECRET-CANARY"
	testDeploymentID1 = "11111111-1111-4111-8111-111111111111"
	testPlanID1       = "22222222-2222-4222-8222-222222222222"
	testConnectionID1 = "33333333-3333-4333-8333-333333333333"
	testDeploymentID2 = "44444444-4444-4444-8444-444444444444"
	testPlanID2       = "55555555-5555-4555-8555-555555555555"
	testConnectionID2 = "66666666-6666-4666-8666-666666666666"
	missingDeployment = "77777777-7777-4777-8777-777777777777"
	failingDeployment = "88888888-8888-4888-8888-888888888888"
)

type cloudTestService struct {
	agentv1.UnimplementedCloudControlServiceServer
	mu                    sync.Mutex
	listRequests          []*agentv1.ListCloudDeploymentsRequest
	getRequests           []*agentv1.GetCloudDeploymentRequest
	previewRequest        *agentv1.PreviewAwsIdentityRequest
	auth                  []string
	list                  func(*agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error)
	get                   func(*agentv1.GetCloudDeploymentRequest) (*agentv1.GetCloudDeploymentResponse, error)
	preview               func(*agentv1.PreviewAwsIdentityRequest) (*agentv1.PreviewAwsIdentityResponse, error)
	createGoal            func(*agentv1.CreateCloudGoalRequest) (*agentv1.CreateCloudGoalResponse, error)
	getPlan               func(*agentv1.GetCloudPlanRequest) (*agentv1.GetCloudPlanResponse, error)
	createQuote           func(*agentv1.CreateCloudQuoteRequest) (*agentv1.CreateCloudQuoteResponse, error)
	getQuote              func(*agentv1.GetCloudQuoteRequest) (*agentv1.GetCloudQuoteResponse, error)
	createPlan            func(*agentv1.CreateCloudPlanRequest) (*agentv1.CreateCloudPlanResponse, error)
	createChallenge       func(*agentv1.CreateApprovalChallengeRequest) (*agentv1.CreateApprovalChallengeResponse, error)
	approvePlan           func(*agentv1.ApproveCloudPlanRequest) (*agentv1.ApproveCloudPlanResponse, error)
	establish             func(*agentv1.EstablishAwsConnectionRequest) (*agentv1.EstablishAwsConnectionResponse, error)
	getConnection         func(*agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error)
	listConnections       func(*agentv1.ListCloudConnectionsRequest) (*agentv1.ListCloudConnectionsResponse, error)
	listPlans             func(*agentv1.ListCloudPlansRequest) (*agentv1.ListCloudPlansResponse, error)
	createDestroyRequest  *agentv1.CreateCloudDeploymentDestroyChallengeRequest
	approveDestroyRequest *agentv1.ApproveCloudDeploymentDestroyRequest
	getDestroyRequest     *agentv1.GetCloudDestroyOperationRequest
	createDestroy         func(*agentv1.CreateCloudDeploymentDestroyChallengeRequest) (*agentv1.CreateCloudDeploymentDestroyChallengeResponse, error)
	approveDestroy        func(*agentv1.ApproveCloudDeploymentDestroyRequest) (*agentv1.ApproveCloudDeploymentDestroyResponse, error)
	getDestroy            func(*agentv1.GetCloudDestroyOperationRequest) (*agentv1.GetCloudDestroyOperationResponse, error)
}

func (service *cloudTestService) CreateCloudGoal(_ context.Context, request *agentv1.CreateCloudGoalRequest) (*agentv1.CreateCloudGoalResponse, error) {
	service.mu.Lock()
	callback := service.createGoal
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) CreateCloudQuote(_ context.Context, request *agentv1.CreateCloudQuoteRequest) (*agentv1.CreateCloudQuoteResponse, error) {
	service.mu.Lock()
	callback := service.createQuote
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) GetCloudQuote(_ context.Context, request *agentv1.GetCloudQuoteRequest) (*agentv1.GetCloudQuoteResponse, error) {
	service.mu.Lock()
	callback := service.getQuote
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func (service *cloudTestService) CreateCloudPlan(_ context.Context, request *agentv1.CreateCloudPlanRequest) (*agentv1.CreateCloudPlanResponse, error) {
	service.mu.Lock()
	callback := service.createPlan
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) ListCloudPlans(_ context.Context, request *agentv1.ListCloudPlansRequest) (*agentv1.ListCloudPlansResponse, error) {
	service.mu.Lock()
	callback := service.listPlans
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return &agentv1.ListCloudPlansResponse{}, nil
}

func (service *cloudTestService) ListCloudConnections(_ context.Context, request *agentv1.ListCloudConnectionsRequest) (*agentv1.ListCloudConnectionsResponse, error) {
	service.mu.Lock()
	callback := service.listConnections
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return &agentv1.ListCloudConnectionsResponse{}, nil
}

func (service *cloudTestService) GetCloudPlan(_ context.Context, request *agentv1.GetCloudPlanRequest) (*agentv1.GetCloudPlanResponse, error) {
	service.mu.Lock()
	callback := service.getPlan
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func (service *cloudTestService) CreateApprovalChallenge(_ context.Context, request *agentv1.CreateApprovalChallengeRequest) (*agentv1.CreateApprovalChallengeResponse, error) {
	service.mu.Lock()
	callback := service.createChallenge
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) ApproveCloudPlan(_ context.Context, request *agentv1.ApproveCloudPlanRequest) (*agentv1.ApproveCloudPlanResponse, error) {
	service.mu.Lock()
	callback := service.approvePlan
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) EstablishAwsConnection(_ context.Context, request *agentv1.EstablishAwsConnectionRequest) (*agentv1.EstablishAwsConnectionResponse, error) {
	service.mu.Lock()
	callback := service.establish
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) GetCloudConnection(_ context.Context, request *agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
	service.mu.Lock()
	callback := service.getConnection
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func (service *cloudTestService) CreateCloudDeploymentDestroyChallenge(ctx context.Context, request *agentv1.CreateCloudDeploymentDestroyChallengeRequest) (*agentv1.CreateCloudDeploymentDestroyChallengeResponse, error) {
	service.captureDestroy(ctx, request, nil, nil)
	service.mu.Lock()
	callback := service.createDestroy
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) ApproveCloudDeploymentDestroy(ctx context.Context, request *agentv1.ApproveCloudDeploymentDestroyRequest) (*agentv1.ApproveCloudDeploymentDestroyResponse, error) {
	service.captureDestroy(ctx, nil, request, nil)
	service.mu.Lock()
	callback := service.approveDestroy
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) GetCloudDestroyOperation(ctx context.Context, request *agentv1.GetCloudDestroyOperationRequest) (*agentv1.GetCloudDestroyOperationResponse, error) {
	service.captureDestroy(ctx, nil, nil, request)
	service.mu.Lock()
	callback := service.getDestroy
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func (service *cloudTestService) PreviewAwsIdentity(ctx context.Context, request *agentv1.PreviewAwsIdentityRequest) (*agentv1.PreviewAwsIdentityResponse, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	authorization := ""
	if len(values) == 1 {
		authorization = values[0]
	}
	service.mu.Lock()
	service.previewRequest = request
	service.auth = append(service.auth, authorization)
	callback := service.preview
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.Unavailable, "not configured")
}

func (service *cloudTestService) ListCloudDeployments(ctx context.Context, request *agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
	service.capture(ctx, request, nil)
	service.mu.Lock()
	callback := service.list
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return &agentv1.ListCloudDeploymentsResponse{}, nil
}

func (service *cloudTestService) GetCloudDeployment(ctx context.Context, request *agentv1.GetCloudDeploymentRequest) (*agentv1.GetCloudDeploymentResponse, error) {
	service.capture(ctx, nil, request)
	service.mu.Lock()
	callback := service.get
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil, status.Error(codes.NotFound, "missing")
}

func (service *cloudTestService) capture(ctx context.Context, list *agentv1.ListCloudDeploymentsRequest, get *agentv1.GetCloudDeploymentRequest) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	authorization := ""
	if len(values) == 1 {
		authorization = values[0]
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if list != nil {
		service.listRequests = append(service.listRequests, list)
	}
	if get != nil {
		service.getRequests = append(service.getRequests, get)
	}
	service.auth = append(service.auth, authorization)
}

func (service *cloudTestService) captureDestroy(ctx context.Context, create *agentv1.CreateCloudDeploymentDestroyChallengeRequest, approve *agentv1.ApproveCloudDeploymentDestroyRequest, get *agentv1.GetCloudDestroyOperationRequest) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	authorization := ""
	if len(values) == 1 {
		authorization = values[0]
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	service.createDestroyRequest = create
	service.approveDestroyRequest = approve
	service.getDestroyRequest = get
	service.auth = append(service.auth, authorization)
}

func TestCloudDeploymentReaderTraversesPagesWithBoundOwnerAndMountedAuthentication(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	first := cloudDeployment(testDeploymentID1, testPlanID1, testConnectionID1, 1)
	second := cloudDeployment(testDeploymentID2, testPlanID2, testConnectionID2, 2)
	server.cloud.list = func(request *agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
		switch request.GetPageToken() {
		case "":
			return &agentv1.ListCloudDeploymentsResponse{Deployments: []*agentv1.CloudDeployment{first}, NextPageToken: "page-2"}, nil
		case "page-2":
			return &agentv1.ListCloudDeploymentsResponse{Deployments: []*agentv1.CloudDeployment{second}}, nil
		default:
			return nil, status.Error(codes.InvalidArgument, "unexpected cursor")
		}
	}
	server.cloud.get = func(request *agentv1.GetCloudDeploymentRequest) (*agentv1.GetCloudDeploymentResponse, error) {
		if request.GetDeploymentId() != second.GetDeploymentId() {
			return nil, status.Error(codes.NotFound, "missing")
		}
		return &agentv1.GetCloudDeploymentResponse{Deployment: second}, nil
	}

	items, err := runner.ListCloudDeployments(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].DeploymentID != testDeploymentID1 || items[0].PlanID != testPlanID1 ||
		items[0].ConnectionID != testConnectionID1 || items[0].Execution != "running" || items[0].Outcome != "pending" ||
		items[0].Resource != "active" || items[0].CreatedAt != 1_000 || items[0].UpdatedAt != 2_000 {
		t.Fatalf("mapped deployments = %#v", items)
	}
	got, found, err := runner.GetCloudDeployment(t.Context(), testDeploymentID2)
	if err != nil || !found || got != items[1] {
		t.Fatalf("get deployment = %#v found=%v err=%v", got, found, err)
	}

	server.cloud.mu.Lock()
	defer server.cloud.mu.Unlock()
	if len(server.cloud.listRequests) != 2 || len(server.cloud.getRequests) != 1 {
		t.Fatalf("RPC requests list/get=%d/%d", len(server.cloud.listRequests), len(server.cloud.getRequests))
	}
	for _, request := range server.cloud.listRequests {
		if request.GetOwnerId() != "owner-from-config" || request.GetPageSize() != cloudDeploymentPageSize {
			t.Fatalf("list request = %#v", request)
		}
	}
	if server.cloud.getRequests[0].GetOwnerId() != "owner-from-config" {
		t.Fatalf("get request = %#v", server.cloud.getRequests[0])
	}
	for _, authorization := range server.cloud.auth {
		if authorization != "DTX-Service-Key "+testServiceKey {
			t.Fatalf("authorization metadata was not sourced from mounted key")
		}
	}
}

func TestCloudDeploymentReaderNotFoundAndErrorsAreSanitized(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	server.cloud.get = func(request *agentv1.GetCloudDeploymentRequest) (*agentv1.GetCloudDeploymentResponse, error) {
		if request.GetDeploymentId() == missingDeployment {
			return nil, status.Error(codes.NotFound, cloudErrorCanary)
		}
		return nil, status.Error(codes.Internal, cloudErrorCanary)
	}
	server.cloud.list = func(*agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
		return nil, status.Error(codes.Internal, cloudErrorCanary)
	}
	if _, found, err := runner.GetCloudDeployment(t.Context(), missingDeployment); err != nil || found {
		t.Fatalf("not found = found=%v err=%v", found, err)
	}
	if _, _, err := runner.GetCloudDeployment(t.Context(), failingDeployment); err == nil || err.Error() != "agent service request failed (internal)" ||
		strings.Contains(err.Error(), cloudErrorCanary) || strings.Contains(err.Error(), testServiceKey) {
		t.Fatalf("get error was not sanitized: %v", err)
	}
	if _, err := runner.ListCloudDeployments(t.Context()); err == nil || err.Error() != "agent service request failed (internal)" ||
		strings.Contains(err.Error(), cloudErrorCanary) || strings.Contains(err.Error(), testServiceKey) {
		t.Fatalf("list error was not sanitized: %v", err)
	}
}

func TestCloudDeploymentReaderRejectsInvalidStatesRelationsAndUnboundedPagination(t *testing.T) {
	t.Parallel()
	t.Run("invalid state or relation", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		base := cloudDeployment(testDeploymentID1, testPlanID1, testConnectionID1, 1)
		tests := []struct {
			name   string
			mutate func(*agentv1.CloudDeployment)
		}{
			{name: "draft execution", mutate: func(item *agentv1.CloudDeployment) {
				item.ExecutionStatus = agentv1.ExecutionStatus_EXECUTION_STATUS_DRAFT
			}},
			{name: "planning execution", mutate: func(item *agentv1.CloudDeployment) {
				item.ExecutionStatus = agentv1.ExecutionStatus_EXECUTION_STATUS_PLANNING
			}},
			{name: "awaiting approval execution", mutate: func(item *agentv1.CloudDeployment) {
				item.ExecutionStatus = agentv1.ExecutionStatus_EXECUTION_STATUS_AWAITING_APPROVAL
			}},
			{name: "unspecified outcome", mutate: func(item *agentv1.CloudDeployment) {
				item.OutcomeStatus = agentv1.OutcomeStatus_OUTCOME_STATUS_UNSPECIFIED
			}},
			{name: "unspecified resource", mutate: func(item *agentv1.CloudDeployment) {
				item.Resources.Status = agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_UNSPECIFIED
			}},
			{name: "invalid deployment id", mutate: func(item *agentv1.CloudDeployment) { item.DeploymentId = "deployment-not-uuid" }},
			{name: "missing plan id", mutate: func(item *agentv1.CloudDeployment) { item.PlanId = "" }},
			{name: "invalid connection id", mutate: func(item *agentv1.CloudDeployment) { item.ConnectionId = "connection-not-uuid" }},
			{name: "epoch timestamp", mutate: func(item *agentv1.CloudDeployment) { item.CreatedAt = timestamppb.New(time.Unix(0, 0)) }},
			{name: "negative timestamp", mutate: func(item *agentv1.CloudDeployment) { item.CreatedAt = timestamppb.New(time.Unix(-1, 0)) }},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				invalid := proto.Clone(base).(*agentv1.CloudDeployment)
				test.mutate(invalid)
				server.cloud.list = func(*agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
					return &agentv1.ListCloudDeploymentsResponse{Deployments: []*agentv1.CloudDeployment{invalid}}, nil
				}
				if _, err := runner.ListCloudDeployments(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid cloud deployment response") {
					t.Fatalf("invalid response error = %v", err)
				}
			})
		}
	})
	t.Run("repeated cursor", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		server.cloud.list = func(*agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
			return &agentv1.ListCloudDeploymentsResponse{NextPageToken: "repeat"}, nil
		}
		if _, err := runner.ListCloudDeployments(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid cloud deployment cursor") {
			t.Fatalf("cursor cycle error = %v", err)
		}
	})
	t.Run("total limit", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		tooMany := make([]*agentv1.CloudDeployment, maxCloudDeployments+1)
		server.cloud.list = func(*agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
			return &agentv1.ListCloudDeploymentsResponse{Deployments: tooMany}, nil
		}
		if _, err := runner.ListCloudDeployments(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid cloud deployment response") {
			t.Fatalf("total limit error = %v", err)
		}
	})
	t.Run("page limit", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: 2 * time.Second})
		server.cloud.list = func(request *agentv1.ListCloudDeploymentsRequest) (*agentv1.ListCloudDeploymentsResponse, error) {
			page := len(server.cloud.listRequests)
			return &agentv1.ListCloudDeploymentsResponse{NextPageToken: fmt.Sprintf("page-%d-%s", page, request.GetPageToken())}, nil
		}
		if _, err := runner.ListCloudDeployments(t.Context()); err == nil || err.Error() != "agent service returned too many cloud deployment pages" {
			t.Fatalf("page limit error = %v", err)
		}
	})
}

func cloudDeployment(deploymentID, planID, connectionID string, revision int64) *agentv1.CloudDeployment {
	return &agentv1.CloudDeployment{
		DeploymentId: deploymentID, OwnerId: "owner-from-config", PlanId: planID, ConnectionId: connectionID,
		ExecutionStatus: agentv1.ExecutionStatus_EXECUTION_STATUS_RUNNING,
		OutcomeStatus:   agentv1.OutcomeStatus_OUTCOME_STATUS_PENDING,
		Resources:       &agentv1.CloudResourceSummary{Status: agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_ACTIVE},
		Revision:        revision, CreatedAt: timestamppb.New(time.Unix(1, 0)), UpdatedAt: timestamppb.New(time.Unix(2, 0)),
	}
}
