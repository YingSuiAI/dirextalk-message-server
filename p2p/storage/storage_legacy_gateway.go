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
	run_id, routing_state, inserted, error_code, created_at, updated_at
`

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
	var sourceDigest, idempotencyDigest, requestDigest []byte
	var preferredConnectorID, runID sql.NullString
	var inserted sql.NullBool
	var persistedState string
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
