// Package blocks owns ProductCore contact-block actions and block lookups.
package blocks

import (
	"context"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionList   = "blocks.list"
	actionAdd    = "blocks.add"
	actionRemove = "blocks.remove"
)

// Store is the sole block-state repository used by Module.
type Store interface {
	UpsertBlock(ctx context.Context, block dirextalkdomain.BlockRecord) error
	DeleteBlock(ctx context.Context, targetType, targetID string) (bool, error)
	ListBlocks(ctx context.Context) ([]dirextalkdomain.BlockRecord, error)
}

// ContactLookup resolves a durable contact snapshot for a peer MXID.
type ContactLookup func(context.Context, string) (dirextalkdomain.ContactRecord, bool, error)

type Config struct {
	Now           func() time.Time
	LookupContact ContactLookup
}

type Module struct {
	store         Store
	now           func() time.Time
	lookupContact ContactLookup
}

func New(store Store, cfg Config) *Module {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Module{store: store, now: now, lookupContact: cfg.LookupContact}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionList:   m.handleList,
		actionAdd:    m.handleAdd,
		actionRemove: m.handleRemove,
	}
}

func (m *Module) handleAdd(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	block, apiErr := m.recordFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	if block.CreatedAt == 0 {
		block.CreatedAt = m.now().UTC().UnixMilli()
	}
	if err := m.store.UpsertBlock(ctx, block); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"status": "blocked", "block": block}, nil
}

func (m *Module) handleRemove(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	block, apiErr := m.recordFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	removed, err := m.store.DeleteBlock(ctx, block.TargetType, block.TargetID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{
		"status":      "ok",
		"removed":     removed,
		"target_type": block.TargetType,
		"target_id":   block.TargetID,
	}, nil
}

func (m *Module) handleList(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	blocks, err := m.store.ListBlocks(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	contacts := make([]dirextalkdomain.BlockRecord, 0)
	for _, block := range blocks {
		if block.TargetType == "contact" {
			contacts = append(contacts, block)
		}
	}
	return map[string]any{"contacts": contacts}, nil
}

func (m *Module) recordFromParams(ctx context.Context, raw map[string]any) (dirextalkdomain.BlockRecord, *actionbase.Error) {
	params := actionbase.Params(raw)
	rawTargetType := dirextalkdomain.FallbackString(params.String("target_type"), params.String("type"))
	if rawTargetType == "" {
		return dirextalkdomain.BlockRecord{}, actionbase.BadRequest("target_type is required")
	}
	targetType := dirextalkdomain.NormalizeBlockTargetType(rawTargetType)
	if targetType == "" {
		return dirextalkdomain.BlockRecord{}, actionbase.BadRequest("target_type must be contact")
	}
	block := dirextalkdomain.BlockRecord{
		TargetType:  targetType,
		TargetID:    params.String("target_id"),
		RoomID:      params.String("room_id"),
		ChannelID:   params.String("channel_id"),
		PeerMXID:    dirextalkdomain.FallbackString(params.String("peer_mxid"), params.String("mxid")),
		DisplayName: dirextalkdomain.FallbackString(params.String("display_name"), params.String("name")),
		AvatarURL:   params.String("avatar_url"),
	}
	memberID := params.FirstString("user_id", "user_mxid", "peer_mxid", "mxid")
	if memberID == "" {
		memberID = params.FirstListString("user_ids", "user_mxids", "peer_mxids", "invitees")
	}
	block.PeerMXID = dirextalkdomain.FallbackString(block.PeerMXID,
		dirextalkdomain.FallbackString(memberID, block.TargetID))
	if block.PeerMXID == "" {
		return dirextalkdomain.BlockRecord{}, actionbase.BadRequest("peer_mxid is required")
	}
	if m.lookupContact != nil {
		contact, ok, err := m.lookupContact(ctx, block.PeerMXID)
		if err != nil {
			return dirextalkdomain.BlockRecord{}, actionbase.InternalError(err)
		}
		if ok {
			block.RoomID = dirextalkdomain.FallbackString(block.RoomID, contact.RoomID)
			block.DisplayName = dirextalkdomain.FallbackString(block.DisplayName, contact.DisplayName)
			block.AvatarURL = dirextalkdomain.FallbackString(block.AvatarURL, contact.AvatarURL)
		}
	}
	block.TargetID = block.PeerMXID
	block.DisplayName = strings.TrimSpace(block.DisplayName)
	if block.DisplayName == "" {
		block.DisplayName = dirextalkdomain.DisplayNameFromMXID(block.PeerMXID)
	}
	return block, nil
}

// Exists reports whether any identifier matches a stored block snapshot.
func (m *Module) Exists(ctx context.Context, targetType string, identifiers ...string) (bool, error) {
	targetType = dirextalkdomain.NormalizeBlockTargetType(targetType)
	if targetType == "" {
		return false, nil
	}
	ids := make(map[string]struct{}, len(identifiers))
	for _, identifier := range identifiers {
		identifier = strings.TrimSpace(identifier)
		if identifier != "" {
			ids[identifier] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	blocks, err := m.store.ListBlocks(ctx)
	if err != nil {
		return false, err
	}
	for _, block := range blocks {
		if block.TargetType != targetType {
			continue
		}
		for _, candidate := range []string{block.TargetID, block.RoomID, block.ChannelID, block.PeerMXID} {
			if _, ok := ids[strings.TrimSpace(candidate)]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}
