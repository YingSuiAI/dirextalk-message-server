package storage

import (
	"context"
	"sort"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func (s *MemoryStore) CreateCloudGoal(_ context.Context, request cloudmodule.CreateGoalRequest) (cloudmodule.CreateGoalResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idempotencyKey := request.Goal.OwnerMXID + "\x00" + request.Goal.IdempotencyHash
	if goalID, ok := s.cloudIdem[idempotencyKey]; ok {
		goal := s.cloudGoals[goalID]
		if goal.RequestDigest != request.Goal.RequestDigest {
			return cloudmodule.CreateGoalResult{}, cloudmodule.ErrIdempotencyConflict
		}
		return cloudmodule.CreateGoalResult{Goal: goal, Plan: s.cloudPlans[goal.PlanID], Created: false}, nil
	}
	s.cloudGoals[request.Goal.GoalID] = request.Goal
	s.cloudPlans[request.Plan.PlanID] = request.Plan
	if request.Job.JobID != "" {
		s.cloudJobs[request.Job.JobID] = request.Job
	}
	s.cloudIdem[idempotencyKey] = request.Goal.GoalID
	for _, event := range request.Events {
		s.cloudEvents = append(s.cloudEvents, event)
	}
	s.cloudOutbox[request.Outbox.OutboxID] = request.Outbox
	return cloudmodule.CreateGoalResult{Goal: request.Goal, Plan: request.Plan, Created: true}, nil
}

func (s *MemoryStore) ListCloudGoals(_ context.Context) ([]cloudmodule.Goal, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Goal, 0, len(s.cloudGoals))
	for _, item := range s.cloudGoals {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].GoalID, items[j].GoalID)
	})
	return items, nil
}

func (s *MemoryStore) ListCloudPlans(_ context.Context) ([]cloudmodule.Plan, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Plan, 0, len(s.cloudPlans))
	for _, item := range s.cloudPlans {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].PlanID, items[j].PlanID)
	})
	return items, nil
}

func (s *MemoryStore) GetCloudPlan(_ context.Context, id string) (cloudmodule.Plan, bool, error) {
	s.mu.RLock()
	item, ok := s.cloudPlans[id]
	s.mu.RUnlock()
	return item, ok, nil
}

// GetCloudQuote supports focused in-process tests without introducing a second
// quote source of truth. A test may seed a Plan with its safe Quote projection;
// production startup continues to require PostgreSQL and its quote table.
func (s *MemoryStore) GetCloudQuote(_ context.Context, id string) (cloudmodule.QuoteView, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, plan := range s.cloudPlans {
		if plan.QuoteID == id && plan.Quote != nil && plan.Quote.QuoteID == id {
			return cloneCloudQuoteView(*plan.Quote), true, nil
		}
	}
	return cloudmodule.QuoteView{}, false, nil
}

func cloneCloudQuoteView(value cloudmodule.QuoteView) cloudmodule.QuoteView {
	clone := value
	clone.Candidates = make([]cloudmodule.QuoteCandidateView, len(value.Candidates))
	for index, candidate := range value.Candidates {
		clone.Candidates[index] = candidate
		clone.Candidates[index].AvailabilityZones = cloneStringSlice(candidate.AvailabilityZones)
	}
	clone.IncludedItems = cloneStringSlice(value.IncludedItems)
	clone.UnincludedItems = cloneStringSlice(value.UnincludedItems)
	return clone
}

func (s *MemoryStore) ListCloudJobs(_ context.Context) ([]cloudmodule.Job, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Job, 0, len(s.cloudJobs))
	for _, item := range s.cloudJobs {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].JobID, items[j].JobID)
	})
	return items, nil
}

func (s *MemoryStore) ListCloudConnections(_ context.Context) ([]cloudmodule.Connection, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Connection, 0, len(s.cloudConnections))
	for _, item := range s.cloudConnections {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].ConnectionID, items[j].ConnectionID)
	})
	return items, nil
}

func (s *MemoryStore) GetCloudConnection(_ context.Context, id string) (cloudmodule.Connection, bool, error) {
	s.mu.RLock()
	item, ok := s.cloudConnections[id]
	s.mu.RUnlock()
	return item, ok, nil
}

func (s *MemoryStore) ListCloudDeployments(_ context.Context) ([]cloudmodule.Deployment, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Deployment, 0, len(s.cloudDeployments))
	for _, item := range s.cloudDeployments {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].DeploymentID, items[j].DeploymentID)
	})
	return items, nil
}

func (s *MemoryStore) GetCloudDeployment(_ context.Context, id string) (cloudmodule.Deployment, bool, error) {
	s.mu.RLock()
	item, ok := s.cloudDeployments[id]
	s.mu.RUnlock()
	return item, ok, nil
}

func (s *MemoryStore) ListCloudServices(_ context.Context) ([]cloudmodule.Service, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Service, 0, len(s.cloudServices))
	for _, item := range s.cloudServices {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].ServiceID, items[j].ServiceID)
	})
	return items, nil
}

func (s *MemoryStore) GetCloudService(_ context.Context, id string) (cloudmodule.Service, bool, error) {
	s.mu.RLock()
	item, ok := s.cloudServices[id]
	s.mu.RUnlock()
	return item, ok, nil
}

func (s *MemoryStore) ListCloudRecipes(_ context.Context) ([]cloudmodule.Recipe, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Recipe, 0, len(s.cloudRecipes))
	for _, item := range s.cloudRecipes {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].RecipeID, items[j].RecipeID)
	})
	return items, nil
}

func (s *MemoryStore) GetCloudRecipe(_ context.Context, id string) (cloudmodule.Recipe, bool, error) {
	s.mu.RLock()
	item, ok := s.cloudRecipes[id]
	s.mu.RUnlock()
	return item, ok, nil
}

func (s *MemoryStore) ListCloudAlerts(_ context.Context) ([]cloudmodule.Alert, error) {
	s.mu.RLock()
	items := make([]cloudmodule.Alert, 0, len(s.cloudAlerts))
	for _, item := range s.cloudAlerts {
		items = append(items, item)
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].UpdatedAt, items[j].UpdatedAt, items[i].AlertID, items[j].AlertID)
	})
	return items, nil
}

func (s *MemoryStore) ListCloudEvents(_ context.Context, limit int) ([]cloudmodule.Event, error) {
	s.mu.RLock()
	items := append([]cloudmodule.Event(nil), s.cloudEvents...)
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return newer(items[i].CreatedAt, items[j].CreatedAt, items[i].EventID, items[j].EventID)
	})
	for index := range items {
		items[index].HydrateSummary()
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func newer(leftTime, rightTime int64, leftID, rightID string) bool {
	if leftTime != rightTime {
		return leftTime > rightTime
	}
	return leftID < rightID
}
