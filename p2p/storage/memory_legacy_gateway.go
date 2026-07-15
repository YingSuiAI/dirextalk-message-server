package storage

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
)

func (s *MemoryStore) LoadAcceptedInvocation(
	_ context.Context, matrixRoomID, requestID, runID string,
) (legacygateway.InvocationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.legacyAgentInvocations[legacyAgentInvocationKey{matrixRoomID: matrixRoomID, requestID: requestID}]
	if !ok || record.State != legacygateway.InvocationAccepted || record.RunID != runID {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationNotFound
	}
	return cloneLegacyAgentInvocationRecord(record), nil
}

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

func (s *MemoryStore) ReserveTerminal(
	_ context.Context, delivery legacygateway.TerminalDelivery, updatedAt time.Time,
) (legacygateway.TerminalReservation, error) {
	if err := validateTerminalDelivery(delivery); err != nil {
		return legacygateway.TerminalReservation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := legacyAgentInvocationKey{matrixRoomID: delivery.MatrixRoomID, requestID: delivery.RequestID}
	record, ok := s.legacyAgentInvocations[key]
	if !ok {
		return legacygateway.TerminalReservation{}, legacygateway.ErrInvocationNotFound
	}
	if record.State != legacygateway.InvocationAccepted || record.RunID != delivery.RunID {
		return legacygateway.TerminalReservation{}, legacygateway.ErrInvalidInvocationTransition
	}
	if record.Terminal.Phase != "" {
		if !terminalDeliveryMatches(record.Terminal, delivery) {
			return legacygateway.TerminalReservation{}, legacygateway.ErrInvocationConflict
		}
		return legacygateway.TerminalReservation{Status: legacygateway.TerminalReservationReplay, Delivery: cloneTerminalDelivery(record.Terminal)}, nil
	}
	delivery = cloneTerminalDelivery(delivery)
	record.Terminal = delivery
	record.UpdatedAt = normalizeInvocationTime(updatedAt)
	s.legacyAgentInvocations[key] = record
	return legacygateway.TerminalReservation{Status: legacygateway.TerminalReservationInserted, Delivery: cloneTerminalDelivery(delivery)}, nil
}

func (s *MemoryStore) AdvanceTerminal(
	_ context.Context, matrixRoomID, requestID string, digest [32]byte,
	from, to legacygateway.TerminalPhase, updatedAt time.Time,
) (legacygateway.TerminalDelivery, error) {
	if !validTerminalAdvance(from, to) {
		return legacygateway.TerminalDelivery{}, legacygateway.ErrInvalidInvocationTransition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := legacyAgentInvocationKey{matrixRoomID: matrixRoomID, requestID: requestID}
	record, ok := s.legacyAgentInvocations[key]
	if !ok {
		return legacygateway.TerminalDelivery{}, legacygateway.ErrInvocationNotFound
	}
	if record.Terminal.Phase == "" || !bytes.Equal(record.Terminal.Digest[:], digest[:]) {
		return legacygateway.TerminalDelivery{}, legacygateway.ErrInvocationConflict
	}
	if terminalPhaseRank(record.Terminal.Phase) >= terminalPhaseRank(to) {
		return cloneTerminalDelivery(record.Terminal), nil
	}
	if record.Terminal.Phase != from {
		return legacygateway.TerminalDelivery{}, legacygateway.ErrInvalidInvocationTransition
	}
	record.Terminal.Phase = to
	record.UpdatedAt = normalizeInvocationTime(updatedAt)
	s.legacyAgentInvocations[key] = record
	return cloneTerminalDelivery(record.Terminal), nil
}

func (s *MemoryStore) PendingTerminals(_ context.Context, limit int) ([]legacygateway.TerminalDelivery, error) {
	if limit <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type pending struct {
		updatedAt time.Time
		delivery  legacygateway.TerminalDelivery
	}
	items := make([]pending, 0)
	for _, record := range s.legacyAgentInvocations {
		if record.Terminal.Phase != "" && record.Terminal.Phase != legacygateway.TerminalSourceACK {
			items = append(items, pending{updatedAt: record.UpdatedAt, delivery: cloneTerminalDelivery(record.Terminal)})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].updatedAt.Before(items[j].updatedAt) })
	if len(items) > limit {
		items = items[:limit]
	}
	result := make([]legacygateway.TerminalDelivery, len(items))
	for i := range items {
		result[i] = items[i].delivery
	}
	return result, nil
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
	record.Terminal = cloneTerminalDelivery(record.Terminal)
	return record
}
