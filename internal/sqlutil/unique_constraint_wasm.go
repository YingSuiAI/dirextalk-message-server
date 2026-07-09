// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build wasm
// +build wasm

package sqlutil

// IsUniqueConstraintViolationErr returns true if the error is an unique_violation error
func IsUniqueConstraintViolationErr(err error) bool {
	return false
}
