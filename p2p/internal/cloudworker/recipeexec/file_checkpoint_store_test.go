package recipeexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFileCheckpointStorePersistsAndResumesWithoutExposingBinding(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "trusted-checkpoints")
	store, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	binding := Binding{ExecutionID: "execution-private-0001", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
	initial := InitialCheckpointState(binding)
	if got, loadErr := store.Load(context.Background(), binding); loadErr != nil || got != initial {
		t.Fatalf("fresh Load() = %#v, error = %v, want %#v", got, loadErr, initial)
	}
	first := CheckpointState{Binding: binding, Checkpoint: "artifact_verified", Index: 0}
	if err := store.Advance(context.Background(), initial, first); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	next := CheckpointState{Binding: binding, Checkpoint: "health_verified", Index: 1, Completed: true}
	if err := store.Advance(context.Background(), first, next); err != nil {
		t.Fatalf("second Advance() error = %v", err)
	}

	restarted, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatalf("restart NewFileCheckpointStore() error = %v", err)
	}
	got, err := restarted.Load(context.Background(), binding)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != next {
		t.Fatalf("Load() = %#v, want %#v", got, next)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("checkpoint files = %d, want 1", len(entries))
	}
	name := entries[0].Name()
	if strings.Contains(name, binding.ExecutionID) || strings.Contains(name, binding.ManifestDigest) || len(name) != 64+len(".json") {
		t.Fatalf("checkpoint filename exposes or does not hash the binding: %q", name)
	}
	if runtime.GOOS != "windows" {
		if info, statErr := os.Stat(directory); statErr != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("checkpoint directory mode = %v, error = %v, want 0700", info.Mode().Perm(), statErr)
		}
		if info, statErr := entries[0].Info(); statErr != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("checkpoint file mode = %v, error = %v, want 0600", info.Mode().Perm(), statErr)
		}
	}
}

func TestFileCheckpointStoreCASIsSerializedAcrossStoreInstances(t *testing.T) {
	directory := t.TempDir()
	first, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{ExecutionID: "execution-cas-0001", ManifestDigest: "sha256:" + strings.Repeat("b", 64)}
	previous := InitialCheckpointState(binding)
	next := CheckpointState{Binding: binding, Checkpoint: "artifact_verified", Index: 0}

	var successes atomic.Int32
	var conflicts atomic.Int32
	var wait sync.WaitGroup
	for _, store := range []*FileCheckpointStore{first, second} {
		wait.Add(1)
		go func(store *FileCheckpointStore) {
			defer wait.Done()
			switch advanceErr := store.Advance(context.Background(), previous, next); {
			case advanceErr == nil:
				successes.Add(1)
			case errors.Is(advanceErr, ErrCheckpointConflict):
				conflicts.Add(1)
			default:
				t.Errorf("Advance() error = %v", advanceErr)
			}
		}(store)
	}
	wait.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("successes = %d, conflicts = %d, want 1/1", successes.Load(), conflicts.Load())
	}
}

func TestFileCheckpointStoreRejectsCorruptAndUnknownJSON(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{ExecutionID: "execution-corrupt-0001", ManifestDigest: "sha256:" + strings.Repeat("c", 64)}
	path := filepath.Join(directory, checkpointFilename(binding))
	contents := `{"schema":"dirextalk.recipe-checkpoint/v1","execution_id":"execution-corrupt-0001","manifest_digest":"` + binding.ManifestDigest + `","checkpoint":"artifact_verified","index":0,"completed":false,"unexpected":true}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), binding); !errors.Is(err, ErrCheckpointState) {
		t.Fatalf("Load() error = %v, want ErrCheckpointState", err)
	}
}

func TestFileCheckpointStoreHonorsCanceledContextWithoutPersistence(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileCheckpointStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{ExecutionID: "execution-canceled-0001", ManifestDigest: "sha256:" + strings.Repeat("d", 64)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = store.Advance(ctx, InitialCheckpointState(binding), CheckpointState{Binding: binding, Checkpoint: "artifact_verified", Index: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Advance() error = %v, want context.Canceled", err)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled Advance() persisted %d files", len(entries))
	}
}
