package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestRecipeManifestRegistrationRunnerDelegatesOneAtomicRegistration(t *testing.T) {
	store := &fakeRecipeManifestRegistrationStore{created: true}
	runner := NewRecipeManifestRegistrationRunner(store)
	created, err := runner.RunOnce(t.Context())
	if err != nil || !created || store.calls != 1 {
		t.Fatalf("registration created=%v calls=%d err=%v", created, store.calls, err)
	}

	want := errors.New("registration failed")
	store.created, store.err = false, want
	created, err = runner.RunOnce(t.Context())
	if created || !errors.Is(err, want) || store.calls != 2 {
		t.Fatalf("failed registration created=%v calls=%d err=%v", created, store.calls, err)
	}
}

type fakeRecipeManifestRegistrationStore struct {
	created bool
	err     error
	calls   int
}

func (store *fakeRecipeManifestRegistrationStore) RegisterNextTrustedRecipeExecutionManifest(context.Context) (bool, error) {
	store.calls++
	return store.created, store.err
}
