package p2p

import "fmt"

func internalError(err error) *apiError {
	return statusError(500, fmt.Sprintf("internal error: %s", err.Error()))
}
