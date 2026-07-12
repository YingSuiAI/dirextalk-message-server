package storage

import "context"

func (s *MemoryStore) LoadPortal(ctx context.Context) (portalState, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.portal == nil {
		return portalState{}, false, nil
	}
	return clonePortalState(*s.portal), true, nil
}

func (s *MemoryStore) SavePortal(ctx context.Context, state portalState) error {
	state = clonePortalState(state)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.portal != nil && s.portal.MatrixDeviceID == state.MatrixDeviceID {
		state.ClientBuild = s.portal.ClientBuild
	}
	s.portal = &state
	return nil
}

func (s *MemoryStore) SaveClientBuild(ctx context.Context, expectedDeviceID string, build clientBuild) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.portal == nil || s.portal.MatrixDeviceID != expectedDeviceID {
		return false, nil
	}
	s.portal.ClientBuild = build
	return true, nil
}

func (s *MemoryStore) SaveReadMarker(ctx context.Context, marker readMarker) error {
	s.mu.Lock()
	s.readMarks[marker.RoomID] = marker
	s.mu.Unlock()
	return nil
}
