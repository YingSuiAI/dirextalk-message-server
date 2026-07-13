// Package profile owns the owner profile ProductCore workflow and public
// well-known projection.
package profile

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionGet    = "profile.get"
	actionUpdate = "profile.update"
)

type Port interface {
	Current() dirextalkdomain.OwnerProfile
	Commit(context.Context, func(*dirextalkdomain.OwnerProfile)) (dirextalkdomain.OwnerProfile, error)
	UpdateMatrix(context.Context, dirextalkdomain.OwnerProfile) error
	UpdateMembers(context.Context, dirextalkdomain.OwnerProfile) error
}

type Module struct {
	port Port
}

func New(port Port) *Module {
	return &Module{port: port}
}

func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionGet:    m.get,
		actionUpdate: m.update,
	}
}

func (m *Module) get(context.Context, map[string]any) (any, *actionbase.Error) {
	return m.port.Current(), nil
}

func (m *Module) update(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	values := actionbase.Params(params)
	current, err := m.port.Commit(ctx, func(current *dirextalkdomain.OwnerProfile) {
		if value := values.String("display_name"); value != "" {
			current.DisplayName = value
		}
		current.AvatarURL = values.String("avatar_url")
		current.Gender = values.String("gender")
		current.Birthday = values.String("birthday")
		current.Phone = values.String("phone")
		current.Email = values.String("email")
	})
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.port.UpdateMatrix(ctx, current); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.port.UpdateMembers(ctx, current); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return current, nil
}

func (m *Module) WellKnown() map[string]any {
	current := m.port.Current()
	return map[string]any{
		"matrix_user_id": current.UserID,
		"mxid":           current.UserID,
		"user_id":        current.UserID,
		"display_name":   current.DisplayName,
		"avatar_url":     current.AvatarURL,
	}
}
