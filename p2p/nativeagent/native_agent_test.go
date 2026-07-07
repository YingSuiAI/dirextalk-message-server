package nativeagent

import (
	"context"
	"sync"
)

type testConfigStore struct {
	mu     sync.Mutex
	config map[string]any
}

func (s *testConfigStore) Load(context.Context) (map[string]any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAnyMap(s.config), true, nil
}

func (s *testConfigStore) Save(_ context.Context, config map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = cloneAnyMap(config)
	return nil
}
