package recipeexec

import "errors"

type permanentExecutionError struct{ cause error }

func (e permanentExecutionError) Error() string { return "permanent recipe execution failure" }
func (e permanentExecutionError) Unwrap() error { return e.cause }

// PermanentExecutionFailure marks a typed driver rejection that retrying the
// same sealed task cannot repair. The wrapper deliberately does not expose the
// underlying host error in transport payloads.
func PermanentExecutionFailure(cause error) error {
	return permanentExecutionError{cause: cause}
}

// IsPermanentExecutionFailure distinguishes invalid immutable scope from a
// retryable host/service failure. Checkpoint CAS conflicts and context errors
// are intentionally not permanent.
func IsPermanentExecutionFailure(err error) bool {
	var marked permanentExecutionError
	if errors.As(err, &marked) {
		return true
	}
	for _, target := range []error{
		ErrExecutorConfiguration,
		ErrArtifactDigestMismatch,
		ErrActionUnsupported,
		ErrCheckpointBinding,
		ErrCheckpointState,
		ErrCheckpointOutOfOrder,
		ErrExecutionIncomplete,
		ErrTaskInvalid,
		ErrTaskManifestBinding,
		ErrTaskCheckpointBinding,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
