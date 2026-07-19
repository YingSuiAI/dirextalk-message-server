package connectionbootstrap

import (
	"crypto/subtle"
	"sync"
	"time"
)

type sessionState uint8

const (
	sessionActive sessionState = iota
	sessionProcessing
	sessionAccepted
	sessionFailed
)

type session struct {
	requestID             string
	identity              Identity
	expiresAt             time.Time
	privateKey, rawBearer []byte
	bearerHash            [32]byte
	envelopeDigest        string
	state                 sessionState
	done                  chan struct{}
	receipt               Receipt
	response              CreateResponse
}
type requestRecord struct{ digest, sessionID string }
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]*session
	requests map[string]requestRecord
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: map[string]*session{}, requests: map[string]requestRecord{}}
}
func (store *sessionStore) addOrReplay(requestID, digest, id string, value *session, now time.Time) (CreateResponse, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cleanupLocked(now)
	if record, exists := store.requests[requestID]; exists {
		if record.digest != digest {
			return CreateResponse{}, false, ErrConflict
		}
		existing := store.sessions[record.sessionID]
		if existing == nil {
			return CreateResponse{}, false, ErrExpired
		}
		return renderCreateResponse(existing), false, nil
	}
	if _, exists := store.sessions[id]; exists {
		return CreateResponse{}, false, ErrConflict
	}
	store.sessions[id] = value
	store.requests[requestID] = requestRecord{digest, id}
	return renderCreateResponse(value), true, nil
}
func renderCreateResponse(value *session) CreateResponse {
	response := value.response
	switch value.state {
	case sessionActive:
		response.Status = "awaiting_upload"
		response.UploadBearer = string(value.rawBearer)
	case sessionProcessing:
		response.Status = "processing"
		response.UploadBearer = ""
	case sessionAccepted:
		response.Status = "accepted"
		response.UploadBearer = ""
		receipt := value.receipt
		response.Receipt = &receipt
	case sessionFailed:
		response.Status = "failed"
		response.UploadBearer = ""
	}
	return response
}

type beginResult struct {
	owner      bool
	wait       <-chan struct{}
	identity   Identity
	expiresAt  time.Time
	privateKey []byte
	receipt    Receipt
}

func (store *sessionStore) begin(id string, bearerHash [32]byte, digest string, now time.Time) (beginResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cleanupLocked(now)
	value, exists := store.sessions[id]
	if !exists {
		return beginResult{}, ErrExpired
	}
	if subtle.ConstantTimeCompare(value.bearerHash[:], bearerHash[:]) != 1 {
		return beginResult{}, ErrUnauthorized
	}
	if value.envelopeDigest != "" && value.envelopeDigest != digest {
		return beginResult{}, ErrConflict
	}
	switch value.state {
	case sessionAccepted:
		return beginResult{receipt: value.receipt}, nil
	case sessionFailed:
		return beginResult{}, ErrConsumed
	case sessionProcessing:
		return beginResult{wait: value.done}, nil
	}
	value.envelopeDigest = digest
	value.state = sessionProcessing
	clear(value.rawBearer)
	value.rawBearer = nil
	value.done = make(chan struct{})
	return beginResult{owner: true, identity: value.identity, expiresAt: value.expiresAt, privateKey: append([]byte(nil), value.privateKey...)}, nil
}
func (store *sessionStore) complete(id string, receipt Receipt) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if value := store.sessions[id]; value != nil {
		clear(value.privateKey)
		clear(value.rawBearer)
		value.privateKey = nil
		value.rawBearer = nil
		value.receipt = receipt
		value.state = sessionAccepted
		if value.done != nil {
			close(value.done)
			value.done = nil
		}
	}
}
func (store *sessionStore) fail(id string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if value := store.sessions[id]; value != nil {
		clear(value.privateKey)
		clear(value.rawBearer)
		value.privateKey = nil
		value.rawBearer = nil
		value.state = sessionFailed
		if value.done != nil {
			close(value.done)
			value.done = nil
		}
	}
}
func (store *sessionStore) cleanup(now time.Time) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cleanupLocked(now)
}
func (store *sessionStore) cleanupLocked(now time.Time) {
	for id, value := range store.sessions {
		if !now.Before(value.expiresAt) {
			clear(value.privateKey)
			clear(value.rawBearer)
			delete(store.requests, value.requestID)
			delete(store.sessions, id)
			if value.done != nil {
				close(value.done)
			}
		}
	}
}
