package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCutoverPreflightFailsClosedForLegacyCloudFacts(t *testing.T) {
	readFailure := errors.New("private database failure must not reach the owner")
	for _, test := range []struct {
		name       string
		store      *cutoverPreflightStore
		ready      bool
		reason     string
		count      int
		mustRedact []string
	}{
		{
			name:   "empty local cloud state is ready",
			store:  &cutoverPreflightStore{},
			ready:  true,
			reason: "",
			count:  0,
		},
		{
			name: "legacy local data blocks direct cutover",
			store: &cutoverPreflightStore{
				goals: []Goal{{GoalID: "PRIVATE_GOAL_ID", Prompt: "PRIVATE_GOAL_PROMPT"}},
			},
			ready:      false,
			reason:     cutoverPreflightDataReason,
			count:      1,
			mustRedact: []string{"PRIVATE_GOAL_ID", "PRIVATE_GOAL_PROMPT"},
		},
		{
			name: "legacy data and active resource block direct cutover",
			store: &cutoverPreflightStore{
				goals: []Goal{{GoalID: "PRIVATE_GOAL_ID", Prompt: "PRIVATE_GOAL_PROMPT"}},
				deployments: []Deployment{{
					DeploymentID: "PRIVATE_DEPLOYMENT_ID",
				}},
			},
			ready:      false,
			reason:     cutoverPreflightActiveResourcesReason,
			count:      2,
			mustRedact: []string{"PRIVATE_GOAL_ID", "PRIVATE_GOAL_PROMPT", "PRIVATE_DEPLOYMENT_ID"},
		},
		{
			name: "unprojected connection bootstrap blocks direct cutover",
			store: &cutoverPreflightStore{
				privateFootprint: true,
			},
			ready:  false,
			reason: cutoverPreflightDataReason,
			count:  1,
		},
		{
			name: "unknown local read failure blocks without error detail",
			store: &cutoverPreflightStore{
				readErrorAt: "services",
				readError:   readFailure,
			},
			ready:      false,
			reason:     cutoverPreflightReadFailedReason,
			count:      0,
			mustRedact: []string{readFailure.Error()},
		},
		{
			name: "unknown private footprint read failure blocks without error detail",
			store: &cutoverPreflightStore{
				readErrorAt: "private_footprint",
				readError:   readFailure,
			},
			ready:      false,
			reason:     cutoverPreflightReadFailedReason,
			count:      0,
			mustRedact: []string{readFailure.Error()},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, actionErr := New(test.store, Config{}).cutoverPreflight(context.Background(), map[string]any{})
			if actionErr != nil {
				t.Fatalf("cutoverPreflight() actionErr=%#v", actionErr)
			}
			payload, ok := result.(map[string]any)
			if !ok {
				t.Fatalf("cutoverPreflight() result=%#v", result)
			}
			if got := payload["ready"]; got != test.ready {
				t.Fatalf("ready=%#v want %v", got, test.ready)
			}
			if got := payload["blocked"]; got != !test.ready {
				t.Fatalf("blocked=%#v want %v", got, !test.ready)
			}
			if got := payload["reason"]; got != test.reason {
				t.Fatalf("reason=%#v want %q", got, test.reason)
			}
			if got := payload["count"]; got != test.count {
				t.Fatalf("count=%#v want %d", got, test.count)
			}

			encoded, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal preflight payload: %v", err)
			}
			for _, forbidden := range test.mustRedact {
				if strings.Contains(string(encoded), forbidden) {
					t.Fatalf("preflight leaked %q: %s", forbidden, encoded)
				}
			}
		})
	}
}

type cutoverPreflightStore struct {
	Store
	goals            []Goal
	plans            []Plan
	jobs             []Job
	connections      []Connection
	deployments      []Deployment
	services         []Service
	recipes          []Recipe
	alerts           []Alert
	events           []Event
	privateFootprint bool
	readErrorAt      string
	readError        error
}

func (s *cutoverPreflightStore) readErrorFor(name string) error {
	if s.readErrorAt == name {
		return s.readError
	}
	return nil
}

func (s *cutoverPreflightStore) ListCloudGoals(context.Context) ([]Goal, error) {
	return s.goals, s.readErrorFor("goals")
}

func (s *cutoverPreflightStore) ListCloudPlans(context.Context) ([]Plan, error) {
	return s.plans, s.readErrorFor("plans")
}

func (s *cutoverPreflightStore) ListCloudJobs(context.Context) ([]Job, error) {
	return s.jobs, s.readErrorFor("jobs")
}

func (s *cutoverPreflightStore) ListCloudConnections(context.Context) ([]Connection, error) {
	return s.connections, s.readErrorFor("connections")
}

func (s *cutoverPreflightStore) ListCloudDeployments(context.Context) ([]Deployment, error) {
	return s.deployments, s.readErrorFor("deployments")
}

func (s *cutoverPreflightStore) ListCloudServices(context.Context) ([]Service, error) {
	return s.services, s.readErrorFor("services")
}

func (s *cutoverPreflightStore) ListCloudRecipes(context.Context) ([]Recipe, error) {
	return s.recipes, s.readErrorFor("recipes")
}

func (s *cutoverPreflightStore) ListCloudAlerts(context.Context) ([]Alert, error) {
	return s.alerts, s.readErrorFor("alerts")
}

func (s *cutoverPreflightStore) ListCloudEvents(_ context.Context, _ int) ([]Event, error) {
	return s.events, s.readErrorFor("events")
}

func (s *cutoverPreflightStore) HasLegacyCloudPrivateFootprint(context.Context) (bool, error) {
	return s.privateFootprint, s.readErrorFor("private_footprint")
}
