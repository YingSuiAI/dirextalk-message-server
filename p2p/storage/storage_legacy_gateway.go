package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
	"github.com/lib/pq"
)

const legacyAgentInvocationColumns = `
	matrix_room_id, request_id, matrix_invoke_event_id, matrix_input_event_id, tenant_id, installation_id,
	conversation_id, request_event_id, source_digest, idempotency_digest, request_digest,
	preferred_connector_id, required_capabilities, dispatch_mode, grant_version, state,
	run_id, routing_state, inserted, error_code, created_at, updated_at,
	terminal_kind, terminal_digest, terminal_cursor, terminal_event_type, terminal_content_json,
	matrix_transaction_id, matrix_terminal_event_id, terminal_phase
`

func (s *DatabaseStore) LoadAcceptedInvocation(
	ctx context.Context, matrixRoomID, requestID, runID string,
) (legacygateway.InvocationRecord, error) {
	record, err := scanLegacyAgentInvocation(s.db.QueryRowContext(ctx, `
		SELECT `+legacyAgentInvocationColumns+` FROM p2p_legacy_agent_invocations
		WHERE matrix_room_id=$1 AND request_id=$2 AND run_id=$3 AND state='accepted'
	`, matrixRoomID, requestID, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationNotFound
	}
	return record, err
}

// ReserveInvocation durably claims one Matrix invocation without ever
// overwriting an existing claim. Conflicts on the public request key, source
// event, or idempotency digest are loaded and compared after the insert so the
// result remains correct when concurrent PostgreSQL writers race.
func (s *DatabaseStore) ReserveInvocation(
	ctx context.Context,
	candidate legacygateway.InvocationCandidate,
) (legacygateway.Reservation, error) {
	var reservation legacygateway.Reservation
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		inserted, err := insertLegacyAgentInvocation(ctx, txn, candidate)
		if err == nil {
			reservation = legacygateway.Reservation{Status: legacygateway.ReservationInserted, Record: inserted}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		records, err := loadConflictingLegacyAgentInvocations(ctx, txn, candidate)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return errors.New("legacy agent invocation conflict row was not found")
		}
		reservation = legacygateway.Reservation{Status: legacygateway.ReservationConflict, Record: records[0]}
		if len(records) == 1 && legacyAgentInvocationMatches(records[0], candidate) {
			reservation.Status = legacygateway.ReservationReplay
		}
		return nil
	})
	return reservation, err
}

func insertLegacyAgentInvocation(
	ctx context.Context,
	txn *sql.Tx,
	candidate legacygateway.InvocationCandidate,
) (legacygateway.InvocationRecord, error) {
	row := txn.QueryRowContext(ctx, `
		INSERT INTO p2p_legacy_agent_invocations (
			matrix_room_id, request_id, matrix_invoke_event_id, matrix_input_event_id, tenant_id, installation_id,
			conversation_id, request_event_id, source_digest, idempotency_digest, request_digest,
			preferred_connector_id, required_capabilities, dispatch_mode, grant_version, state,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, 'reserved', $16, $16
		)
		ON CONFLICT DO NOTHING
		RETURNING `+legacyAgentInvocationColumns,
		candidate.MatrixRoomID,
		candidate.RequestID,
		candidate.MatrixInvokeEventID,
		candidate.MatrixInputEventID,
		candidate.TenantID,
		candidate.InstallationID,
		candidate.ConversationID,
		candidate.RequestEventID,
		candidate.SourceDigest[:],
		candidate.IdempotencyDigest[:],
		candidate.RequestDigest[:],
		nullableText(candidate.PreferredConnectorID),
		pq.Array(append([]string{}, candidate.RequiredCapabilities...)),
		candidate.DispatchMode,
		candidate.GrantVersion,
		normalizeInvocationTime(candidate.CreatedAt),
	)
	return scanLegacyAgentInvocation(row)
}

func loadConflictingLegacyAgentInvocations(
	ctx context.Context,
	txn *sql.Tx,
	candidate legacygateway.InvocationCandidate,
) ([]legacygateway.InvocationRecord, error) {
	rows, err := txn.QueryContext(ctx, `
		SELECT `+legacyAgentInvocationColumns+`
		FROM p2p_legacy_agent_invocations
		WHERE (matrix_room_id = $1 AND request_id = $2)
			OR matrix_invoke_event_id = $3
			OR (tenant_id = $4 AND matrix_room_id = $1 AND idempotency_digest = $5)
		ORDER BY matrix_room_id, request_id
	`, candidate.MatrixRoomID, candidate.RequestID, candidate.MatrixInvokeEventID,
		candidate.TenantID, candidate.IdempotencyDigest[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]legacygateway.InvocationRecord, 0, 2)
	for rows.Next() {
		record, err := scanLegacyAgentInvocation(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// MarkAccepted records the first accepted Router result. Both terminal states
// are immutable; an exact replay returns the already accepted record.
func (s *DatabaseStore) MarkAccepted(
	ctx context.Context,
	matrixRoomID, requestID string,
	sourceDigest [32]byte,
	receipt legacygateway.CreateRunReceipt,
	updatedAt time.Time,
) (legacygateway.InvocationRecord, error) {
	if receipt.RequestID != requestID {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationConflict
	}
	var record legacygateway.InvocationRecord
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		current, err := loadLegacyAgentInvocationForTransition(ctx, txn, matrixRoomID, requestID)
		if err != nil {
			return err
		}
		if !bytes.Equal(current.SourceDigest[:], sourceDigest[:]) {
			return legacygateway.ErrInvocationConflict
		}
		switch current.State {
		case legacygateway.InvocationAccepted:
			if current.RunID != receipt.RunID || current.RoutingState != receipt.RoutingState ||
				current.Inserted != receipt.Inserted {
				return legacygateway.ErrInvalidInvocationTransition
			}
			record = current
			return nil
		case legacygateway.InvocationRejected:
			return legacygateway.ErrInvalidInvocationTransition
		case legacygateway.InvocationPending:
		default:
			return legacygateway.ErrInvalidInvocationTransition
		}
		row := txn.QueryRowContext(ctx, `
			UPDATE p2p_legacy_agent_invocations SET
				state = 'accepted', run_id = $3, routing_state = $4,
				inserted = $5, error_code = '', updated_at = $6
			WHERE matrix_room_id = $1 AND request_id = $2
				AND source_digest = $7 AND state = 'reserved'
			RETURNING `+legacyAgentInvocationColumns+`
		`, matrixRoomID, requestID, receipt.RunID, receipt.RoutingState, receipt.Inserted,
			normalizeInvocationTime(updatedAt), sourceDigest[:])
		record, err = scanLegacyAgentInvocation(row)
		if errors.Is(err, sql.ErrNoRows) {
			return legacygateway.ErrInvalidInvocationTransition
		}
		return err
	})
	return record, err
}

// MarkRejected records the first rejection. Both terminal states are
// immutable; an exact replay returns the already rejected record.
func (s *DatabaseStore) MarkRejected(
	ctx context.Context,
	matrixRoomID, requestID string,
	sourceDigest [32]byte,
	errorCode string,
	updatedAt time.Time,
) (legacygateway.InvocationRecord, error) {
	var record legacygateway.InvocationRecord
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		current, err := loadLegacyAgentInvocationForTransition(ctx, txn, matrixRoomID, requestID)
		if err != nil {
			return err
		}
		if !bytes.Equal(current.SourceDigest[:], sourceDigest[:]) {
			return legacygateway.ErrInvocationConflict
		}
		switch current.State {
		case legacygateway.InvocationRejected:
			if current.ErrorCode != errorCode {
				return legacygateway.ErrInvalidInvocationTransition
			}
			record = current
			return nil
		case legacygateway.InvocationAccepted:
			return legacygateway.ErrInvalidInvocationTransition
		case legacygateway.InvocationPending:
		default:
			return legacygateway.ErrInvalidInvocationTransition
		}
		row := txn.QueryRowContext(ctx, `
			UPDATE p2p_legacy_agent_invocations SET
				state = 'rejected', error_code = $3, updated_at = $4
			WHERE matrix_room_id = $1 AND request_id = $2
				AND source_digest = $5 AND state = 'reserved'
			RETURNING `+legacyAgentInvocationColumns+`
		`, matrixRoomID, requestID, errorCode, normalizeInvocationTime(updatedAt), sourceDigest[:])
		record, err = scanLegacyAgentInvocation(row)
		if errors.Is(err, sql.ErrNoRows) {
			return legacygateway.ErrInvalidInvocationTransition
		}
		return err
	})
	return record, err
}

func (s *DatabaseStore) ReserveTerminal(
	ctx context.Context, delivery legacygateway.TerminalDelivery, updatedAt time.Time,
) (legacygateway.TerminalReservation, error) {
	if err := validateTerminalDelivery(delivery); err != nil {
		return legacygateway.TerminalReservation{}, err
	}
	var reservation legacygateway.TerminalReservation
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		current, err := loadLegacyAgentInvocationForTransition(ctx, txn, delivery.MatrixRoomID, delivery.RequestID)
		if err != nil {
			return err
		}
		if current.State != legacygateway.InvocationAccepted || current.RunID != delivery.RunID {
			return legacygateway.ErrInvalidInvocationTransition
		}
		if current.Terminal.Phase != "" {
			if !terminalDeliveryMatches(current.Terminal, delivery) {
				return legacygateway.ErrInvocationConflict
			}
			reservation = legacygateway.TerminalReservation{Status: legacygateway.TerminalReservationReplay, Delivery: current.Terminal}
			return nil
		}
		row := txn.QueryRowContext(ctx, `
			UPDATE p2p_legacy_agent_invocations SET terminal_kind=$3, terminal_digest=$4,
				terminal_cursor=$5, terminal_event_type=$6, terminal_content_json=$7,
				matrix_transaction_id=$8, matrix_terminal_event_id=$9, terminal_phase='send_intent', updated_at=$10
			WHERE matrix_room_id=$1 AND request_id=$2 AND state='accepted' AND terminal_phase=''
			RETURNING `+legacyAgentInvocationColumns,
			delivery.MatrixRoomID, delivery.RequestID, delivery.Kind, delivery.Digest[:], delivery.Cursor,
			delivery.EventType, delivery.ContentJSON, delivery.MatrixTransactionID, delivery.MatrixEventID,
			normalizeInvocationTime(updatedAt))
		stored, err := scanLegacyAgentInvocation(row)
		if errors.Is(err, sql.ErrNoRows) {
			return legacygateway.ErrInvalidInvocationTransition
		}
		if err != nil {
			return err
		}
		reservation = legacygateway.TerminalReservation{Status: legacygateway.TerminalReservationInserted, Delivery: stored.Terminal}
		return nil
	})
	return reservation, err
}

func (s *DatabaseStore) AdvanceTerminal(
	ctx context.Context, matrixRoomID, requestID string, digest [32]byte,
	from, to legacygateway.TerminalPhase, updatedAt time.Time,
) (legacygateway.TerminalDelivery, error) {
	if !validTerminalAdvance(from, to) {
		return legacygateway.TerminalDelivery{}, legacygateway.ErrInvalidInvocationTransition
	}
	var delivery legacygateway.TerminalDelivery
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		current, err := loadLegacyAgentInvocationForTransition(ctx, txn, matrixRoomID, requestID)
		if err != nil {
			return err
		}
		if current.Terminal.Phase == "" || !bytes.Equal(current.Terminal.Digest[:], digest[:]) {
			return legacygateway.ErrInvocationConflict
		}
		if terminalPhaseRank(current.Terminal.Phase) >= terminalPhaseRank(to) {
			delivery = current.Terminal
			return nil
		}
		if current.Terminal.Phase != from {
			return legacygateway.ErrInvalidInvocationTransition
		}
		row := txn.QueryRowContext(ctx, `
			UPDATE p2p_legacy_agent_invocations SET terminal_phase=$3, updated_at=$4
			WHERE matrix_room_id=$1 AND request_id=$2 AND terminal_digest=$5 AND terminal_phase=$6
			RETURNING `+legacyAgentInvocationColumns,
			matrixRoomID, requestID, to, normalizeInvocationTime(updatedAt), digest[:], from)
		stored, err := scanLegacyAgentInvocation(row)
		if errors.Is(err, sql.ErrNoRows) {
			return legacygateway.ErrInvalidInvocationTransition
		}
		if err == nil {
			delivery = stored.Terminal
		}
		return err
	})
	return delivery, err
}

func (s *DatabaseStore) PendingTerminals(ctx context.Context, limit int) ([]legacygateway.TerminalDelivery, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+legacyAgentInvocationColumns+` FROM p2p_legacy_agent_invocations
		WHERE terminal_phase IN ('send_intent','sent','committed') ORDER BY updated_at, matrix_room_id, request_id LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]legacygateway.TerminalDelivery, 0, limit)
	for rows.Next() {
		record, err := scanLegacyAgentInvocation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record.Terminal)
	}
	return result, rows.Err()
}

func loadLegacyAgentInvocationForTransition(
	ctx context.Context,
	txn *sql.Tx,
	matrixRoomID, requestID string,
) (legacygateway.InvocationRecord, error) {
	record, err := scanLegacyAgentInvocation(txn.QueryRowContext(ctx, `
		SELECT `+legacyAgentInvocationColumns+`
		FROM p2p_legacy_agent_invocations
		WHERE matrix_room_id = $1 AND request_id = $2
		FOR UPDATE
	`, matrixRoomID, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return legacygateway.InvocationRecord{}, legacygateway.ErrInvocationNotFound
	}
	return record, err
}

type legacyAgentInvocationScanner interface {
	Scan(dest ...any) error
}

func scanLegacyAgentInvocation(scanner legacyAgentInvocationScanner) (legacygateway.InvocationRecord, error) {
	var record legacygateway.InvocationRecord
	var sourceDigest, idempotencyDigest, requestDigest, terminalDigest, terminalContent []byte
	var preferredConnectorID, runID sql.NullString
	var inserted sql.NullBool
	var persistedState, terminalKind, terminalCursor, terminalEventType, matrixTransactionID, matrixEventID, terminalPhase string
	err := scanner.Scan(
		&record.MatrixRoomID,
		&record.RequestID,
		&record.MatrixInvokeEventID,
		&record.MatrixInputEventID,
		&record.TenantID,
		&record.InstallationID,
		&record.ConversationID,
		&record.RequestEventID,
		&sourceDigest,
		&idempotencyDigest,
		&requestDigest,
		&preferredConnectorID,
		pq.Array(&record.RequiredCapabilities),
		&record.DispatchMode,
		&record.GrantVersion,
		&persistedState,
		&runID,
		&record.RoutingState,
		&inserted,
		&record.ErrorCode,
		&record.CreatedAt,
		&record.UpdatedAt,
		&terminalKind,
		&terminalDigest,
		&terminalCursor,
		&terminalEventType,
		&terminalContent,
		&matrixTransactionID,
		&matrixEventID,
		&terminalPhase,
	)
	if err != nil {
		return legacygateway.InvocationRecord{}, err
	}
	if len(sourceDigest) != len(record.SourceDigest) ||
		len(idempotencyDigest) != len(record.IdempotencyDigest) ||
		len(requestDigest) != len(record.RequestDigest) {
		return legacygateway.InvocationRecord{}, errors.New("legacy agent invocation has an invalid digest length")
	}
	copy(record.SourceDigest[:], sourceDigest)
	copy(record.IdempotencyDigest[:], idempotencyDigest)
	copy(record.RequestDigest[:], requestDigest)
	record.PreferredConnectorID = preferredConnectorID.String
	record.RunID = runID.String
	record.Inserted = inserted.Valid && inserted.Bool
	record.RequiredCapabilities = slices.Clone(record.RequiredCapabilities)
	if terminalPhase != "" {
		if len(terminalDigest) != len(record.Terminal.Digest) {
			return legacygateway.InvocationRecord{}, errors.New("legacy agent terminal has an invalid digest length")
		}
		record.Terminal = legacygateway.TerminalDelivery{
			MatrixRoomID: record.MatrixRoomID, RequestID: record.RequestID, RunID: record.RunID,
			Cursor: terminalCursor, Kind: legacygateway.TerminalKind(terminalKind), EventType: terminalEventType,
			ContentJSON: bytes.Clone(terminalContent), MatrixTransactionID: matrixTransactionID,
			MatrixEventID: matrixEventID, Phase: legacygateway.TerminalPhase(terminalPhase),
		}
		copy(record.Terminal.Digest[:], terminalDigest)
	}
	switch persistedState {
	case "reserved":
		record.State = legacygateway.InvocationPending
	case "accepted":
		record.State = legacygateway.InvocationAccepted
	case "rejected":
		record.State = legacygateway.InvocationRejected
	default:
		return legacygateway.InvocationRecord{}, errors.New("legacy agent invocation has an invalid state")
	}
	return record, nil
}

func legacyAgentInvocationMatches(
	record legacygateway.InvocationRecord,
	candidate legacygateway.InvocationCandidate,
) bool {
	return record.MatrixRoomID == candidate.MatrixRoomID &&
		record.RequestID == candidate.RequestID &&
		record.MatrixInvokeEventID == candidate.MatrixInvokeEventID &&
		record.MatrixInputEventID == candidate.MatrixInputEventID &&
		record.TenantID == candidate.TenantID &&
		record.InstallationID == candidate.InstallationID &&
		record.ConversationID == candidate.ConversationID &&
		bytes.Equal(record.SourceDigest[:], candidate.SourceDigest[:]) &&
		bytes.Equal(record.IdempotencyDigest[:], candidate.IdempotencyDigest[:]) &&
		record.PreferredConnectorID == candidate.PreferredConnectorID &&
		slices.Equal(record.RequiredCapabilities, candidate.RequiredCapabilities) &&
		record.DispatchMode == candidate.DispatchMode &&
		record.GrantVersion == candidate.GrantVersion
}

func normalizeInvocationTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func validateTerminalDelivery(delivery legacygateway.TerminalDelivery) error {
	if delivery.MatrixRoomID == "" || delivery.RequestID == "" || delivery.RunID == "" || delivery.Cursor == "" ||
		delivery.EventType == "" || len(delivery.ContentJSON) == 0 || delivery.MatrixTransactionID == "" ||
		delivery.MatrixEventID == "" || delivery.Phase != legacygateway.TerminalSendIntent ||
		(delivery.Kind != legacygateway.TerminalResult && delivery.Kind != legacygateway.TerminalError) {
		return legacygateway.ErrInvalidInvocationTransition
	}
	return nil
}

func terminalDeliveryMatches(stored, candidate legacygateway.TerminalDelivery) bool {
	return stored.MatrixRoomID == candidate.MatrixRoomID && stored.RequestID == candidate.RequestID &&
		stored.RunID == candidate.RunID && stored.Cursor == candidate.Cursor && stored.Kind == candidate.Kind &&
		stored.Digest == candidate.Digest && stored.EventType == candidate.EventType &&
		bytes.Equal(stored.ContentJSON, candidate.ContentJSON) &&
		stored.MatrixTransactionID == candidate.MatrixTransactionID && stored.MatrixEventID == candidate.MatrixEventID
}

func terminalPhaseRank(phase legacygateway.TerminalPhase) int {
	switch phase {
	case legacygateway.TerminalSendIntent:
		return 1
	case legacygateway.TerminalSent:
		return 2
	case legacygateway.TerminalCommitted:
		return 3
	case legacygateway.TerminalSourceACK:
		return 4
	default:
		return 0
	}
}

func validTerminalAdvance(from, to legacygateway.TerminalPhase) bool {
	return terminalPhaseRank(from) > 0 && terminalPhaseRank(to) == terminalPhaseRank(from)+1
}

func cloneTerminalDelivery(delivery legacygateway.TerminalDelivery) legacygateway.TerminalDelivery {
	delivery.ContentJSON = bytes.Clone(delivery.ContentJSON)
	return delivery
}
