// Package action owns the shared ProductCore action boundary primitives.
package action

import (
	"context"
	"fmt"
	"net/http"
)

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

// StatusError creates a ProductCore error with an explicit transport status.
func StatusError(status int, message string) *Error {
	return &Error{Status: status, Error: message}
}

// BadRequest creates a 400 ProductCore error.
func BadRequest(message string) *Error {
	return StatusError(http.StatusBadRequest, message)
}

// CodedError creates a ProductCore error with a stable machine-readable code.
func CodedError(status int, code, message string) *Error {
	return &Error{Status: status, Error: message, Code: code}
}

// InternalError preserves the existing ProductCore internal-error envelope.
func InternalError(err error) *Error {
	return StatusError(http.StatusInternalServerError, fmt.Sprintf("internal error: %s", err.Error()))
}
