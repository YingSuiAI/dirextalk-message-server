package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

const groupAgentBindingColumns = `room_id,enabled,owner_mxid,agent_mxid,revision,account_generation,enabled_at`
const groupAgentRequestColumns = `request_id,room_id,event_id,sender_mxid,owner_mxid,agent_mxid,binding_revision,account_generation,origin_server_ts,status,reply_event_id,reply_digest,body,scheduled_by`

// groupAgentRequestQueueMax bounds one room's pending requests so a runaway
// producer cannot grow the outbox without limit.
const groupAgentRequestQueueMax = 256

type groupAgentScanner interface{ Scan(...any) error }

func scanGroupAgentBinding(row groupAgentScanner) (b dirextalkdomain.GroupAgentBinding, err error) {
	err = row.Scan(&b.RoomID, &b.Enabled, &b.OwnerMXID, &b.AgentMXID, &b.Revision, &b.AccountGeneration, &b.EnabledAt)
	b.DisplayName, b.MemberPolicy, b.Status = "Ying", "all_joined", "disabled"
	if b.Enabled {
		b.Status = "enabled"
	}
	return
}
func scanGroupAgentRequest(row groupAgentScanner) (r dirextalkdomain.GroupAgentRequest, err error) {
	err = row.Scan(&r.RequestID, &r.RoomID, &r.EventID, &r.SenderMXID, &r.OwnerMXID, &r.AgentMXID, &r.BindingRevision, &r.AccountGeneration, &r.OriginServerTS, &r.Status, &r.ReplyEventID, &r.ReplyDigest, &r.Body, &r.ScheduledBy)
	return
}

func (s *DatabaseStore) GetGroupAgentBinding(ctx context.Context, roomID string) (dirextalkdomain.GroupAgentBinding, bool, error) {
	b, err := scanGroupAgentBinding(s.db.QueryRowContext(ctx, `SELECT `+groupAgentBindingColumns+` FROM p2p_group_agent_bindings WHERE room_id=$1`, roomID))
	if errors.Is(err, sql.ErrNoRows) {
		return b, false, nil
	}
	return b, err == nil, err
}

// ListEnabledGroupAgentBindings returns this owner's enabled groups in a stable
// order so the Agent's summary sweep is restart-safe.
func (s *DatabaseStore) ListEnabledGroupAgentBindings(ctx context.Context, owner string, generation int64, limit int) ([]dirextalkdomain.GroupAgentBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+groupAgentBindingColumns+` FROM p2p_group_agent_bindings
	 WHERE enabled AND owner_mxid=$1 AND account_generation=$2 ORDER BY room_id LIMIT $3`, owner, generation, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dirextalkdomain.GroupAgentBinding, 0)
	for rows.Next() {
		b, scanErr := scanGroupAgentBinding(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *DatabaseStore) MutateGroupAgentBinding(ctx context.Context, roomID string, mutate func(*dirextalkdomain.GroupAgentBinding) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_group_agent_bindings(room_id) VALUES($1) ON CONFLICT DO NOTHING`, roomID); err != nil {
		return err
	}
	b, err := scanGroupAgentBinding(tx.QueryRowContext(ctx, `SELECT `+groupAgentBindingColumns+` FROM p2p_group_agent_bindings WHERE room_id=$1 FOR UPDATE`, roomID))
	if err != nil {
		return err
	}
	if err = mutate(&b); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_group_agent_bindings SET enabled=$2,owner_mxid=$3,agent_mxid=$4,revision=$5,account_generation=$6,enabled_at=$7 WHERE room_id=$1`, roomID, b.Enabled, b.OwnerMXID, b.AgentMXID, b.Revision, b.AccountGeneration, b.EnabledAt); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_group_agent_requests SET status='cancelled' WHERE room_id=$1 AND status='pending' AND (NOT $2 OR binding_revision<>$3 OR account_generation<>$4)`, roomID, b.Enabled, b.Revision, b.AccountGeneration); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *DatabaseStore) EnqueueGroupAgentRequest(ctx context.Context, r dirextalkdomain.GroupAgentRequest) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	b, err := scanGroupAgentBinding(tx.QueryRowContext(ctx, `SELECT `+groupAgentBindingColumns+` FROM p2p_group_agent_bindings WHERE room_id=$1 FOR UPDATE`, r.RoomID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !groupAgentRequestMatches(b, r) {
		return false, nil
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM p2p_group_agent_requests WHERE request_id=$1 OR (room_id=$2 AND event_id=$3))`, r.RequestID, r.RoomID, r.EventID).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM p2p_group_agent_requests WHERE room_id=$1 AND status='pending'`, r.RoomID).Scan(&count); err != nil {
		return false, err
	}
	if count >= groupAgentRequestQueueMax {
		return false, dirextalkdomain.ErrGroupAgentQueueFull
	}
	// A member request's body always comes from the room transcript, so the
	// enqueue caller may never supply one. A due schedule has no member message
	// behind it and is the single case where Product stores the body itself.
	if r.ScheduledBy == "" {
		r.Body = ""
	} else if strings.TrimSpace(r.Body) == "" {
		return false, nil
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO p2p_group_agent_requests (`+groupAgentRequestColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending','','',$10,$11) ON CONFLICT DO NOTHING`, r.RequestID, r.RoomID, r.EventID, r.SenderMXID, r.OwnerMXID, r.AgentMXID, r.BindingRevision, r.AccountGeneration, r.OriginServerTS, r.Body, r.ScheduledBy)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, tx.Commit()
}

func (s *DatabaseStore) ListGroupAgentRequests(ctx context.Context, owner string, generation int64, limit int, after string) ([]dirextalkdomain.GroupAgentRequest, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+groupAgentRequestColumns+` FROM p2p_group_agent_requests r
	 WHERE r.owner_mxid=$1 AND r.account_generation=$2 AND r.status='pending' AND r.request_id::text>$4
	 AND EXISTS(SELECT 1 FROM p2p_group_agent_bindings b WHERE b.room_id=r.room_id AND b.enabled AND b.revision=r.binding_revision AND b.owner_mxid=r.owner_mxid AND b.account_generation=r.account_generation)
	 ORDER BY r.request_id LIMIT $3`, owner, generation, limit, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dirextalkdomain.GroupAgentRequest, 0)
	for rows.Next() {
		r, scanErr := scanGroupAgentRequest(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *DatabaseStore) GetGroupAgentRequest(ctx context.Context, id string) (dirextalkdomain.GroupAgentRequest, bool, error) {
	r, err := scanGroupAgentRequest(s.db.QueryRowContext(ctx, `SELECT `+groupAgentRequestColumns+` FROM p2p_group_agent_requests WHERE request_id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}

func (s *DatabaseStore) MutateGroupAgentRequest(ctx context.Context, id string, mutate func(dirextalkdomain.GroupAgentBinding, *dirextalkdomain.GroupAgentRequest) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var roomID string
	if err = tx.QueryRowContext(ctx, `SELECT room_id FROM p2p_group_agent_requests WHERE request_id=$1`, id).Scan(&roomID); errors.Is(err, sql.ErrNoRows) {
		return dirextalkdomain.ErrGroupAgentConflict
	}
	if err != nil {
		return err
	}
	b, err := scanGroupAgentBinding(tx.QueryRowContext(ctx, `SELECT `+groupAgentBindingColumns+` FROM p2p_group_agent_bindings WHERE room_id=$1 FOR UPDATE`, roomID))
	if err != nil {
		return err
	}
	r, err := scanGroupAgentRequest(tx.QueryRowContext(ctx, `SELECT `+groupAgentRequestColumns+` FROM p2p_group_agent_requests WHERE request_id=$1 FOR UPDATE`, id))
	if err != nil {
		return err
	}
	if err = mutate(b, &r); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_group_agent_requests SET status=$2,reply_event_id=$3,reply_digest=$4 WHERE request_id=$1`, id, r.Status, r.ReplyEventID, r.ReplyDigest); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_capability_operations o SET state='completed',updated_at=NOW() WHERE o.capability_id='product.group_agent.v1' AND EXISTS(SELECT 1 FROM p2p_capability_matrix_prepared_events p WHERE p.operation_id=o.operation_id AND p.logical_id=$1 AND p.state='sent')`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// The private publisher uses the shared prepared-PDU algorithm. It creates a
// minimal hidden operation ledger row, with identity derived solely from the
// already-authorized request; no owner/model grant is minted or exposed.
func (s *DatabaseStore) AdmitGroupAgentPublication(ctx context.Context, id string, r dirextalkdomain.GroupAgentRequest, digest []byte) error {
	if len(digest) != 32 {
		return dirextalkdomain.ErrGroupAgentConflict
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO p2p_capability_operations(operation_id,capability_id,operation,owner_id,account_generation,root_request_digest,request_digest,state) VALUES($1,'product.group_agent.v1','publish',$2,$3,$4,$4,'running') ON CONFLICT DO NOTHING`, id, r.OwnerMXID, r.AccountGeneration, digest)
	if err != nil {
		return err
	}
	var capability, operation, owner string
	var generation int64
	var existing []byte
	if err = s.db.QueryRowContext(ctx, `SELECT capability_id,operation,owner_id,account_generation,root_request_digest FROM p2p_capability_operations WHERE operation_id=$1`, id).Scan(&capability, &operation, &owner, &generation, &existing); err != nil {
		return err
	}
	if capability != "product.group_agent.v1" || operation != "publish" || owner != r.OwnerMXID || generation != r.AccountGeneration || !bytes.Equal(existing, digest) {
		return dirextalkdomain.ErrGroupAgentConflict
	}
	return nil
}

func (s *DatabaseStore) CancelGroupAgentRequests(ctx context.Context, roomID, sender, eventID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE p2p_group_agent_requests SET status='cancelled' WHERE status='pending' AND ($1='' OR room_id=$1) AND ($2='' OR sender_mxid=$2) AND ($3='' OR event_id=$3)`, roomID, sender, eventID)
	return err
}
