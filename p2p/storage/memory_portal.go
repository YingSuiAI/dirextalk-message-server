package storage

import (
	"context"
	"sort"
)

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
	current, exists := s.readMarks[marker.RoomID]
	if !exists || readMarkerPositionAfter(marker, current) {
		s.readMarks[marker.RoomID] = marker
	}
	s.mu.Unlock()
	return nil
}

func readMarkerPositionAfter(candidate, current readMarker) bool {
	return candidate.TopologicalPosition > current.TopologicalPosition ||
		(candidate.TopologicalPosition == current.TopologicalPosition &&
			candidate.StreamPosition > current.StreamPosition)
}

func (s *MemoryStore) GetReadMarker(ctx context.Context, roomID string) (readMarker, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	marker, ok := s.readMarks[roomID]
	return marker, ok, nil
}

func (s *MemoryStore) ListReadMarkers(ctx context.Context) ([]readMarker, error) {
	s.mu.RLock()
	markers := make([]readMarker, 0, len(s.readMarks))
	for _, marker := range s.readMarks {
		markers = append(markers, marker)
	}
	s.mu.RUnlock()
	sort.Slice(markers, func(i, j int) bool {
		return markers[i].RoomID < markers[j].RoomID
	})
	return markers, nil
}
