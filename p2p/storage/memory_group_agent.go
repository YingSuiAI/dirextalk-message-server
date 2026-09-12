package storage

import (
	"context"
	"sort"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func (s *MemoryStore) GetGroupAgentBinding(_ context.Context, roomID string) (dirextalkdomain.GroupAgentBinding, bool, error) {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	b, ok := s.groupAgentBindings[roomID]
	return b, ok, nil
}

func (s *MemoryStore) ListEnabledGroupAgentBindings(_ context.Context, owner string, generation int64, limit int) ([]dirextalkdomain.GroupAgentBinding, error) {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	rooms := make([]string, 0, len(s.groupAgentBindings))
	for roomID := range s.groupAgentBindings {
		rooms = append(rooms, roomID)
	}
	sort.Strings(rooms)
	out := make([]dirextalkdomain.GroupAgentBinding, 0, len(rooms))
	for _, roomID := range rooms {
		b := s.groupAgentBindings[roomID]
		if !b.Enabled || b.OwnerMXID != owner || b.AccountGeneration != generation {
			continue
		}
		out = append(out, b)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *MemoryStore) MutateGroupAgentBinding(_ context.Context, roomID string, mutate func(*dirextalkdomain.GroupAgentBinding) error) error {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	b := s.groupAgentBindings[roomID]
	b.RoomID = roomID
	previous := b
	if err := mutate(&b); err != nil {
		return err
	}
	s.groupAgentBindings[roomID] = b
	if !b.Enabled || b.Revision != previous.Revision {
		for id, r := range s.groupAgentRequests {
			if r.RoomID == roomID && r.Status == "pending" && (!b.Enabled || r.BindingRevision != b.Revision) {
				r.Status = "cancelled"
				s.groupAgentRequests[id] = r
			}
		}
	}
	return nil
}

func (s *MemoryStore) EnqueueGroupAgentRequest(_ context.Context, r dirextalkdomain.GroupAgentRequest) (bool, error) {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	b := s.groupAgentBindings[r.RoomID]
	if !groupAgentRequestMatches(b, r) {
		return false, nil
	}
	if _, ok := s.groupAgentRequests[r.RequestID]; ok {
		return false, nil
	}
	count := 0
	for _, existing := range s.groupAgentRequests {
		if existing.RoomID == r.RoomID && existing.Status == "pending" {
			count++
		}
	}
	if count >= 256 {
		return false, dirextalkdomain.ErrGroupAgentQueueFull
	}
	r.Body = ""
	r.Status = "pending"
	s.groupAgentRequests[r.RequestID] = r
	return true, nil
}

func (s *MemoryStore) ListGroupAgentRequests(_ context.Context, owner string, generation int64, limit int, after string) ([]dirextalkdomain.GroupAgentRequest, error) {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	requests := make([]dirextalkdomain.GroupAgentRequest, 0)
	for _, r := range s.groupAgentRequests {
		if r.OwnerMXID == owner && r.AccountGeneration == generation && r.Status == "pending" && r.RequestID > after && groupAgentRequestMatches(s.groupAgentBindings[r.RoomID], r) {
			requests = append(requests, r)
		}
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].RequestID < requests[j].RequestID
	})
	if len(requests) > limit {
		requests = requests[:limit]
	}
	return requests, nil
}

func (s *MemoryStore) GetGroupAgentRequest(_ context.Context, id string) (dirextalkdomain.GroupAgentRequest, bool, error) {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	r, ok := s.groupAgentRequests[id]
	return r, ok, nil
}

func (s *MemoryStore) MutateGroupAgentRequest(_ context.Context, id string, mutate func(dirextalkdomain.GroupAgentBinding, *dirextalkdomain.GroupAgentRequest) error) error {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	r, ok := s.groupAgentRequests[id]
	if !ok {
		return dirextalkdomain.ErrGroupAgentConflict
	}
	if err := mutate(s.groupAgentBindings[r.RoomID], &r); err != nil {
		return err
	}
	r.Body = ""
	s.groupAgentRequests[id] = r
	return nil
}

func groupAgentRequestMatches(b dirextalkdomain.GroupAgentBinding, r dirextalkdomain.GroupAgentRequest) bool {
	return b.Enabled && b.Revision == r.BindingRevision && b.OwnerMXID == r.OwnerMXID && b.AgentMXID == r.AgentMXID && b.AccountGeneration == r.AccountGeneration
}

func (s *MemoryStore) CancelGroupAgentRequests(_ context.Context, roomID, sender, eventID string) error {
	s.groupAgentMu.Lock()
	defer s.groupAgentMu.Unlock()
	for id, r := range s.groupAgentRequests {
		if r.Status == "pending" && (roomID == "" || r.RoomID == roomID) && (sender == "" || r.SenderMXID == sender) && (eventID == "" || r.EventID == eventID) {
			r.Status = "cancelled"
			s.groupAgentRequests[id] = r
		}
	}
	return nil
}

// In-memory protocol tests supply their own PreparedMatrixMutationStore; the
// production operation ledger is PostgreSQL-only.
func (s *MemoryStore) AdmitGroupAgentPublication(context.Context, string, dirextalkdomain.GroupAgentRequest, []byte) error {
	return nil
}
