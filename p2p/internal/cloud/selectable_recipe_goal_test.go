package cloud

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCreateGoalSelectedRecipePairIsOwnerOnlyAndBindsRequest(t *testing.T) {
	store := &selectableGoalStore{binding: SelectedRecipeBinding{RecipeID: "recipe-private-1", Revision: 4, Digest: "sha256:" + strings.Repeat("a", 64)}}
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	m := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-selected-1" }})
	base := map[string]any{"goal": "Deploy my selected private recipe", "cloud_connection_id": "connection-private-1", "idempotency_key": "11111111-1111-4111-8111-111111111111"}
	oneMissing := map[string]any{}
	for k, v := range base {
		oneMissing[k] = v
	}
	oneMissing["recipe_id"] = "recipe-private-1"
	if _, apiErr := m.Handlers()[actionGoalsCreate](t.Context(), oneMissing); apiErr == nil || store.createCalls != 0 {
		t.Fatalf("single selected field accepted: err=%v calls=%d", apiErr, store.createCalls)
	}
	base["recipe_id"], base["expected_recipe_revision"] = "recipe-private-1", float64(4)
	if _, apiErr := m.Handlers()[actionGoalsCreate](t.Context(), base); apiErr != nil {
		t.Fatalf("selected create: %v", apiErr)
	}
	if store.resolveCalls != 1 || store.createCalls != 1 || store.request.SelectedRecipe == nil || store.request.Goal.SelectedRecipeID != "recipe-private-1" || store.request.Plan.RecipeRevision != 4 || !strings.Contains(store.request.Outbox.PayloadJSON, `"recipe_digest"`) {
		t.Fatalf("selected request=%#v resolve=%d create=%d", store.request, store.resolveCalls, store.createCalls)
	}
	store.request = CreateGoalRequest{}
	if _, err := m.CreateResearchGoal(t.Context(), "Agent cannot select or replace a recipe", "connection-private-1", "22222222-2222-4222-8222-222222222222"); err != nil {
		t.Fatal(err)
	}
	if store.resolveCalls != 1 || store.request.SelectedRecipe != nil || store.request.Plan.RecipeID != "" {
		t.Fatalf("agent path gained recipe binding: %#v", store.request)
	}
}

type selectableGoalStore struct {
	Store
	binding                   SelectedRecipeBinding
	request                   CreateGoalRequest
	resolveCalls, createCalls int
}

func (s *selectableGoalStore) GetCloudConnection(context.Context, string) (Connection, bool, error) {
	return Connection{ConnectionID: "connection-private-1"}, true, nil
}
func (s *selectableGoalStore) ResolveCloudRecipeSelection(context.Context, string, string, string, int64) (SelectedRecipeBinding, bool, error) {
	s.resolveCalls++
	return s.binding, true, nil
}
func (s *selectableGoalStore) CreateCloudGoal(_ context.Context, request CreateGoalRequest) (CreateGoalResult, error) {
	s.createCalls++
	s.request = request
	return CreateGoalResult{Goal: request.Goal, Plan: request.Plan, Created: true}, nil
}
