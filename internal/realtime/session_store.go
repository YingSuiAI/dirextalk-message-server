package realtime

import (
	"strings"
	"sync"
	"time"
)

var DefaultSessionStore = NewSessionStore(60 * time.Second)

type SessionState struct {
	UserID        string
	Role          string
	Foreground    bool
	Hidden        bool
	AppState      string
	FocusedRoomID string
	LastAckSeq    int64
	LastSeen      time.Time
	Flags         map[string]bool
}

type SessionStore struct {
	mu       sync.Mutex
	ttl      time.Duration
	sessions map[string]SessionState
}

func NewSessionStore(ttl time.Duration) *SessionStore {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &SessionStore{
		ttl:      ttl,
		sessions: map[string]SessionState{},
	}
}

func (s *SessionStore) Upsert(sessionID string, state SessionState) {
	if s == nil {
		return
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	if state.LastSeen.IsZero() {
		state.LastSeen = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = state
}

func (s *SessionStore) Update(sessionID string, update func(*SessionState)) {
	if s == nil || update == nil {
		return
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	update(&state)
	state.LastSeen = time.Now().UTC()
	s.sessions[sessionID] = state
}

func (s *SessionStore) Remove(sessionID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, strings.TrimSpace(sessionID))
}

func (s *SessionStore) HasFreshSession(userID string, now time.Time) bool {
	if s == nil {
		return false
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sessionID, state := range s.sessions {
		if s.expired(state, now) {
			delete(s.sessions, sessionID)
			continue
		}
		if state.UserID == userID {
			return true
		}
	}
	return false
}

func (s *SessionStore) ShouldSuppressPush(userID, roomID string, now time.Time) bool {
	if s == nil {
		return false
	}
	userID = strings.TrimSpace(userID)
	roomID = strings.TrimSpace(roomID)
	if userID == "" || roomID == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sessionID, state := range s.sessions {
		if s.expired(state, now) {
			delete(s.sessions, sessionID)
			continue
		}
		if state.UserID == userID && state.Foreground && !state.Hidden && state.FocusedRoomID == roomID {
			return true
		}
	}
	return false
}

func (s *SessionStore) expired(state SessionState, now time.Time) bool {
	return state.LastSeen.IsZero() || now.Sub(state.LastSeen) > s.ttl
}
