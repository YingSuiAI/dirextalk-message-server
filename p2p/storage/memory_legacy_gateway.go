package storage

import (
	"context"
	"slices"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
)

type legacyAgentInvocationKey struct {
	matrixRoomID string
	requestID    string
}

type legacyAgentIdempotencyKey struct {
	tenantID          string
	matrixRoomID      string
	idempotencyDigest [32]byte
}

func (s *MemoryStore) ReserveInvocation(
	_ context.Context,
	candidate legacygateway.InvocationCandidate,
) (legacygateway.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate = cloneLegacyAgentInvocationCandidate(candidate)
	requestKey := legacyAgentInvocationKey{
		matrixRoomID: candidate.MatrixRoomID,
		requestID:    candidate.RequestID,
	}
	idempotencyKey := legacyAgentIdempotencyKey{
		tenantID:          candidate.TenantID,
		matrixRoomID:      candidate.MatrixRoomID,
		idempotencyDigest: candidate.IdempotencyDigest,
	}

	conflictingKeys := make([]legacyAgentInvocationKey, 0, 3)
	if _, ok := s.legacyAgentInvocations[requestKey]; ok {
		conflictingKeys = append(conflictingKeys, requestKey)
	}
	if key, ok := s.legacyAgentInvocationEvents[candidate.MatrixInvokeEventID]; ok {
		conflictingKeys = append(conflictingKeys, key)
	}
	if key, ok := s.legacyAgentInvocationIdempotency[idempotencyKey]; ok {
		conflictingKeys = append(conflictingKeys, key)
	}
	if len(conflictingKeys) == 0 {
		record := legacygateway.InvocationRecord{
			InvocationCandidate: candidate,
			State:               legacygateway.InvocationPending,
			UpdatedAt:           candidate.CreatedAt,
		}
		s.legacyAgentInvocations[requestKey] = record
		s.legacyAgentInvocationEvents[candidate.MatrixInvokeEventID] = requestKey
		s.legacyAgentInvocationIdempotency[idempotencyKey] = requestKey
		return legacygateway.Reservation{
			Status: legacygateway.ReservationInserted,
			Record: cloneLegacyAgentInvocationRecord(record),
		}, nil
	}

	firstKey := conflictingKeys[0]
	record := s.legacyAgentInvocations[firstKey]
	allSame := true
	for _, key := range conflictingKeys[1:] {
		if key != firstKey {
			allSame = false
			break
		}
	}
	status := legacygateway.ReservationConflict
	if allSame && legacyAgentInvocationMatches(record, candidate) {
		status = legacygateway.ReservationReplay
	}
	return legacygateway.Reservation{
		Status: status,
		Record: cloneLegacyAgentInvocationRecord(record),
	}, nil
}

func (s *MemoryStore) MarkAccepted(
	_ context.Context,
	matrixRoomID, requestID string,
	sourceDigest [32]byte,
	receipt legacygateway.CreateRunReceipt,
	updatedAt time.Time,
) (legacygateway.InvocationRecord, error) {
	if receipt.RequestID != requestID {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := legacyAgentInvocationKey{matrixRoomID: matrixRoomID, requestID: requestID}
	record, ok := s.legacyAgentInvocations[key]
	if !ok {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationNotFound
	}
	if record.SourceDigest != sourceDigest {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationConflict
	}
	switch record.State {
	case legacygateway.InvocationAccepted:
		if record.RunID != receipt.RunID || record.RoutingState != receipt.RoutingState ||
			record.Inserted != receipt.Inserted {
			return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
		}
		return cloneLegacyAgentInvocationRecord(record), nil
	case legacygateway.InvocationRejected:
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
	case legacygateway.InvocationPending:
	default:
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
	}

	record.State = legacygateway.InvocationAccepted
	record.RunID = receipt.RunID
	record.RoutingState = receipt.RoutingState
	record.Inserted = receipt.Inserted
	record.ErrorCode = ""
	record.UpdatedAt = normalizeInvocationTime(updatedAt)
	s.legacyAgentInvocations[key] = record
	return cloneLegacyAgentInvocationRecord(record), nil
}

func (s *MemoryStore) MarkRejected(
	_ context.Context,
	matrixRoomID, requestID string,
	sourceDigest [32]byte,
	errorCode string,
	updatedAt time.Time,
) (legacygateway.InvocationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := legacyAgentInvocationKey{matrixRoomID: matrixRoomID, requestID: requestID}
	record, ok := s.legacyAgentInvocations[key]
	if !ok {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationNotFound
	}
	if record.SourceDigest != sourceDigest {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationConflict
	}
	switch record.State {
	case legacygateway.InvocationRejected:
		if record.ErrorCode != errorCode {
			return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
		}
		return cloneLegacyAgentInvocationRecord(record), nil
	case legacygateway.InvocationAccepted:
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
	case legacygateway.InvocationPending:
	default:
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvalidInvocationTransition
	}

	record.State = legacygateway.InvocationRejected
	record.ErrorCode = errorCode
	record.UpdatedAt = normalizeInvocationTime(updatedAt)
	s.legacyAgentInvocations[key] = record
	return cloneLegacyAgentInvocationRecord(record), nil
}

func cloneLegacyAgentInvocationCandidate(
	candidate legacygateway.InvocationCandidate,
) legacygateway.InvocationCandidate {
	candidate.RequiredCapabilities = slices.Clone(candidate.RequiredCapabilities)
	candidate.CreatedAt = normalizeInvocationTime(candidate.CreatedAt)
	return candidate
}

func cloneLegacyAgentInvocationRecord(
	record legacygateway.InvocationRecord,
) legacygateway.InvocationRecord {
	record.InvocationCandidate = cloneLegacyAgentInvocationCandidate(record.InvocationCandidate)
	return record
}
