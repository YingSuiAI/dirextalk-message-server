package productcapability

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	capv1 "github.com/YingSuiAI/dirextalk-capability-api/gen/go/dirextalk/capability/v1"
	"github.com/google/uuid"
)

func groupAgentProtocolServer() *Server {
	return &Server{config: &Config{ServiceOwnerID: "@owner:example.test", ExpectedAccountGeneration: 7, InvokeGroupAgentCapability: func(_ context.Context, operation string, _ []byte) (any, error) {
		switch operation {
		case "pull":
			return map[string]any{"requests": []any{}, "has_more": false, "next_after_request_id": ""}, nil
		case "validate":
			return map[string]any{"allowed": true}, nil
		default:
			return map[string]any{"status": "published", "replayed": true}, nil
		}
	}}, registry: NewRegistry(), readSem: make(chan struct{}, 1), mutationSem: make(chan struct{}, 1)}
}
func groupAgentProtocolContext() *capv1.CallContext {
	return &capv1.CallContext{ChainId: uuid.NewString(), RootOperationId: uuid.NewString(), Route: capv1.NodeAgent + capv1.RouteSeparator + capv1.NodeProduct, Hop: 2, DeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli()}
}

func TestPrivateGroupAgentQueryHiddenAndFenced(t *testing.T) {
	s := groupAgentProtocolServer()
	for _, op := range []string{"pull", "validate", "history"} {
		response, err := s.Query(context.Background(), &capv1.QueryRequest{CapabilityId: groupAgentCapability, OperationId: op, CallContext: groupAgentProtocolContext(), RequestJson: []byte(`{}`)})
		if err != nil || response.Error != nil || len(response.ResultJson) == 0 {
			t.Fatalf("query %s: %#v %v", op, response, err)
		}
	}
	if len(s.registry.List()) != 0 {
		t.Fatal("private group Agent capability advertised to model")
	}
	for _, name := range []string{"permission", "owner route", "parent call", "noncanonical", "mutation on query", "unconfigured generation"} {
		t.Run(name, func(t *testing.T) {
			server := groupAgentProtocolServer()
			r := &capv1.QueryRequest{CapabilityId: groupAgentCapability, OperationId: "pull", CallContext: groupAgentProtocolContext(), RequestJson: []byte(`{}`)}
			switch name {
			case "permission":
				r.Permission = &capv1.PermissionContext{AuthenticatedOwnerId: "@owner:example.test"}
			case "owner route":
				r.CallContext.Route = capv1.NodeMessage + capv1.RouteSeparator + capv1.NodeAgent + capv1.RouteSeparator + capv1.NodeProduct
				r.CallContext.Hop = 3
			case "parent call":
				r.CallContext.ParentCallId = uuid.NewString()
			case "noncanonical":
				r.RequestJson = []byte(`{ "limit": 2 }`)
			case "mutation on query":
				r.OperationId = "publish"
			case "unconfigured generation":
				server.config.ExpectedAccountGeneration = 0
			}
			res, err := server.Query(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if res.Error == nil {
				t.Fatalf("unfenced query %s", name)
			}
		})
	}
}

func TestPrivateGroupAgentMutationDigestAndReplay(t *testing.T) {
	for _, name := range []string{"valid", "digest mismatch", "query on mutation", "revision", "root mismatch", "permission"} {
		t.Run(name, func(t *testing.T) {
			s := groupAgentProtocolServer()
			call := groupAgentProtocolContext()
			body, _ := json.Marshal(map[string]any{"request_id": uuid.NewString(), "binding_revision": 1, "body": "public"})
			body, _ = capv1.CanonicalizeJSON(body)
			digest := sha256.Sum256(body)
			r := &capv1.StartOperationRequest{CapabilityId: groupAgentCapability, Operation: "publish", OperationId: call.RootOperationId, CallContext: call, RequestJson: body, RequestDigest: digest[:]}
			switch name {
			case "digest mismatch":
				r.RequestDigest = make([]byte, 32)
			case "query on mutation":
				r.Operation = "pull"
			case "revision":
				r.ExpectedRevision = 1
			case "root mismatch":
				r.OperationId = uuid.NewString()
			case "permission":
				r.Permission = &capv1.PermissionContext{}
			}
			res, err := s.StartOperation(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if name == "valid" {
				if res.Error != nil || res.State != capv1.OperationState_OPERATION_STATE_COMPLETED || !res.Replayed {
					t.Fatalf("valid response=%#v", res)
				}
			} else if res.Error == nil {
				t.Fatalf("unfenced mutation %s", name)
			}
		})
	}
}
