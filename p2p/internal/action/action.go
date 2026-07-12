// Package action owns the shared ProductCore action boundary primitives.
package action

import "context"

// Error is the transport-neutral ProductCore error returned by action handlers.
// Status is consumed by HTTP and realtime adapters and is intentionally omitted
// from the JSON error body.
type Error struct {
	Status int    `json:"-"`
	Error  string `json:"error"`
	Code   string `json:"code,omitempty"`
}

// Handler implements one ProductCore action.
type Handler func(context.Context, map[string]any) (any, *Error)
