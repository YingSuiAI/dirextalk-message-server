package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
	"github.com/google/uuid"
)

// Cursor returns the durable Agent fact sequence last processed for one
// independent Agent instance and one stable calling project.
func (s *DatabaseStore) Cursor(ctx context.Context, source agentevents.Source) (int64, error) {
	if s == nil || s.db == nil || !validAgentEventSource(source) {
		return 0, errors.New("Agent event cursor source is invalid")
	}
	var cursor int64
	err := s.db.QueryRowContext(ctx, `
		SELECT after_seq
		FROM p2p_agent_event_cursors
		WHERE agent_instance_id = $1 AND caller_id = $2
	`, source.AgentInstanceID, source.CallerID).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil || cursor < 0 {
		return 0, errors.New("load Agent event cursor failed")
	}
	return cursor, nil
}

// Commit atomically inserts one ProductCore projection, advances its entity
// revision watermark, and advances the Agent fact cursor. Non-projectable and
// stale-revision events advance only the cursor. No cursor can cross a failed
// ProductCore event insert.
func (s *DatabaseStore) Commit(ctx context.Context, request agentevents.CommitRequest) (agentevents.CommitResult, error) {
	if s == nil || s.db == nil || s.writer == nil || !validAgentEventCommit(request) {
		return agentevents.CommitResult{}, errors.New("Agent event projection request is invalid")
	}
	var payloadJSON []byte
	var err error
	if request.Projection != nil {
		payloadJSON, err = json.Marshal(request.Projection.Payload)
		if err != nil {
			return agentevents.CommitResult{}, errors.New("encode Agent event projection failed")
		}
	}

	result := agentevents.CommitResult{}
	err = s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		now := time.Now().UTC()
		if _, insertErr := txn.ExecContext(ctx, `
			INSERT INTO p2p_agent_event_cursors (agent_instance_id, caller_id, after_seq, updated_at)
			VALUES ($1, $2, 0, $3)
			ON CONFLICT (agent_instance_id, caller_id) DO NOTHING
		`, request.Source.AgentInstanceID, request.Source.CallerID, now.UnixMilli()); insertErr != nil {
			return insertErr
		}
		var cursor int64
		if lockErr := txn.QueryRowContext(ctx, `
			SELECT after_seq
			FROM p2p_agent_event_cursors
			WHERE agent_instance_id = $1 AND caller_id = $2
			FOR UPDATE
		`, request.Source.AgentInstanceID, request.Source.CallerID).Scan(&cursor); lockErr != nil {
			return lockErr
		}
		result.Cursor = cursor
		if request.Event.Seq <= cursor {
			return nil
		}

		if request.Projection != nil {
			var currentRevision int64
			revisionErr := txn.QueryRowContext(ctx, `
				SELECT revision
				FROM p2p_agent_projection_revisions
				WHERE agent_instance_id = $1 AND caller_id = $2 AND event_type = $3 AND aggregate_id = $4
			`, request.Source.AgentInstanceID, request.Source.CallerID, request.Event.EventType, request.Event.AggregateID).Scan(&currentRevision)
			if revisionErr != nil && !errors.Is(revisionErr, sql.ErrNoRows) {
				return revisionErr
			}
			if errors.Is(revisionErr, sql.ErrNoRows) || request.Event.Revision > currentRevision {
				messageSeq, seqErr := nextProductEventSequence(ctx, txn, now.UnixNano())
				if seqErr != nil {
					return seqErr
				}
				inserted, insertErr := insertAgentProjectionEvent(ctx, txn, messageSeq, request, payloadJSON)
				if insertErr != nil {
					return insertErr
				}
				if !inserted {
					return errors.New("Agent projection event conflicted with existing ProductCore state")
				}
				if _, revisionWriteErr := txn.ExecContext(ctx, `
					INSERT INTO p2p_agent_projection_revisions (
						agent_instance_id, caller_id, event_type, aggregate_type, aggregate_id,
						revision, source_event_seq, projected_event_seq, updated_at
					) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
					ON CONFLICT (agent_instance_id, caller_id, event_type, aggregate_id) DO UPDATE SET
						aggregate_type = EXCLUDED.aggregate_type,
						revision = EXCLUDED.revision,
						source_event_seq = EXCLUDED.source_event_seq,
						projected_event_seq = EXCLUDED.projected_event_seq,
						updated_at = EXCLUDED.updated_at
					WHERE p2p_agent_projection_revisions.revision < EXCLUDED.revision
				`, request.Source.AgentInstanceID, request.Source.CallerID, request.Event.EventType,
					request.Event.AggregateType, request.Event.AggregateID, request.Event.Revision,
					request.Event.Seq, messageSeq, now.UnixMilli()); revisionWriteErr != nil {
					return revisionWriteErr
				}
				result.Inserted = true
			}
		}

		if _, updateErr := txn.ExecContext(ctx, `
			UPDATE p2p_agent_event_cursors
			SET after_seq = $3, updated_at = $4
			WHERE agent_instance_id = $1 AND caller_id = $2 AND after_seq < $3
		`, request.Source.AgentInstanceID, request.Source.CallerID, request.Event.Seq, now.UnixMilli()); updateErr != nil {
			return updateErr
		}
		result.Cursor = request.Event.Seq
		return nil
	})
	if err != nil {
		return agentevents.CommitResult{}, errors.New("commit Agent event projection failed")
	}
	return result, nil
}

func nextProductEventSequence(ctx context.Context, txn *sql.Tx, candidate int64) (int64, error) {
	var sequence int64
	if err := txn.QueryRowContext(ctx, `
		SELECT GREATEST($1::BIGINT, COALESCE(MAX(seq) + 1, $1::BIGINT))
		FROM p2p_events
	`, candidate).Scan(&sequence); err != nil || sequence <= 0 {
		return 0, fmt.Errorf("allocate ProductCore event sequence: %w", err)
	}
	return sequence, nil
}

func insertAgentProjectionEvent(ctx context.Context, txn *sql.Tx, sequence int64, request agentevents.CommitRequest, payload []byte) (bool, error) {
	projection := request.Projection
	result, err := txn.ExecContext(ctx, `
		INSERT INTO p2p_events (seq, type, room_id, event_id, dedupe_key, payload_json, created_at)
		VALUES ($1,$2,'',$3,$4,$5,$6)
		ON CONFLICT DO NOTHING
	`, sequence, projection.Type, projection.EventID, projection.DedupeKey, string(payload), projection.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func validAgentEventSource(source agentevents.Source) bool {
	parsed, err := uuid.Parse(source.AgentInstanceID)
	caller := strings.TrimSpace(source.CallerID)
	return err == nil && parsed != uuid.Nil && parsed.String() == source.AgentInstanceID && caller == source.CallerID &&
		len(caller) >= 3 && len(caller) <= 256 && !strings.ContainsAny(caller, "\x00\r\n\t")
}

func validAgentEventCommit(request agentevents.CommitRequest) bool {
	if !validAgentEventSource(request.Source) || request.Event.Seq <= 0 || request.Event.Revision <= 0 ||
		!canonicalAgentEventUUID(request.Event.EventID) || !canonicalAgentEventUUID(request.Event.AggregateID) {
		return false
	}
	if request.Projection == nil {
		return true
	}
	projection := request.Projection
	return projection.SourceEventSeq == request.Event.Seq && projection.Type == request.Event.EventType &&
		projection.EventID == request.Event.EventID && strings.TrimSpace(projection.DedupeKey) == projection.DedupeKey &&
		projection.DedupeKey != "" && projection.Payload != nil && !projection.CreatedAt.IsZero() && projection.CreatedAt.Location() == time.UTC
}

func canonicalAgentEventUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}
