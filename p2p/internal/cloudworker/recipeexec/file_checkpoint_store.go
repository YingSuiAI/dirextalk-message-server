package recipeexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const (
	fileCheckpointSchema = "dirextalk.recipe-checkpoint/v1"
	maxCheckpointBytes   = 64 << 10
)

var checkpointDirectoryLocks = struct {
	sync.Mutex
	locks map[string]*sync.Mutex
}{locks: make(map[string]*sync.Mutex)}

// FileCheckpointStore persists Recipe checkpoints below a directory selected
// by the trusted Worker process. A task can supply neither the directory nor a
// filename: the latter is a domain-separated hash of the sealed Binding.
//
// Its compare-and-swap guarantee covers all FileCheckpointStore instances in
// this process that refer to the same canonical directory. Cloud Worker runs
// one process per deployment; cross-process coordination is deliberately not
// claimed by this local store.
type FileCheckpointStore struct {
	directory string
	mu        *sync.Mutex
}

type fileCheckpointV1 struct {
	Schema         string `json:"schema"`
	ExecutionID    string `json:"execution_id"`
	ManifestDigest string `json:"manifest_digest"`
	Checkpoint     string `json:"checkpoint"`
	Index          int    `json:"index"`
	Completed      bool   `json:"completed"`
}

// NewFileCheckpointStore creates or opens a trusted checkpoint directory.
func NewFileCheckpointStore(directory string) (*FileCheckpointStore, error) {
	if directory == "" {
		return nil, fmt.Errorf("checkpoint directory is empty: %w", ErrCheckpointState)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve checkpoint directory: %w", err)
	}
	absolute = filepath.Clean(absolute)
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, fmt.Errorf("inspect checkpoint directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("checkpoint directory is not a real directory: %w", ErrCheckpointState)
	}
	if err := os.Chmod(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("secure checkpoint directory: %w", err)
	}

	checkpointDirectoryLocks.Lock()
	directoryLock := checkpointDirectoryLocks.locks[absolute]
	if directoryLock == nil {
		directoryLock = &sync.Mutex{}
		checkpointDirectoryLocks.locks[absolute] = directoryLock
	}
	checkpointDirectoryLocks.Unlock()
	return &FileCheckpointStore{directory: absolute, mu: directoryLock}, nil
}

func (store *FileCheckpointStore) Load(ctx context.Context, binding Binding) (CheckpointState, error) {
	if err := checkpointContext(ctx); err != nil {
		return CheckpointState{}, err
	}
	if store == nil || store.mu == nil || store.directory == "" {
		return CheckpointState{}, fmt.Errorf("file checkpoint store is not configured: %w", ErrCheckpointState)
	}
	if err := validateCheckpointBinding(binding); err != nil {
		return CheckpointState{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.loadLocked(ctx, binding)
}

func (store *FileCheckpointStore) Advance(ctx context.Context, previous, next CheckpointState) error {
	if err := checkpointContext(ctx); err != nil {
		return err
	}
	if store == nil || store.mu == nil || store.directory == "" {
		return fmt.Errorf("file checkpoint store is not configured: %w", ErrCheckpointState)
	}
	if err := validateCheckpointTransition(previous, next); err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.loadLocked(ctx, previous.Binding)
	if err != nil {
		return err
	}
	if current != previous {
		return ErrCheckpointConflict
	}
	if err := checkpointContext(ctx); err != nil {
		return err
	}
	return store.persistLocked(ctx, next)
}

func (store *FileCheckpointStore) loadLocked(ctx context.Context, binding Binding) (CheckpointState, error) {
	if err := checkpointContext(ctx); err != nil {
		return CheckpointState{}, err
	}
	path := filepath.Join(store.directory, checkpointFilename(binding))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return InitialCheckpointState(binding), nil
	}
	if err != nil {
		return CheckpointState{}, fmt.Errorf("inspect recipe checkpoint: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxCheckpointBytes {
		return CheckpointState{}, fmt.Errorf("recipe checkpoint file is invalid: %w", ErrCheckpointState)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return CheckpointState{}, fmt.Errorf("secure recipe checkpoint: %w", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return CheckpointState{}, fmt.Errorf("read recipe checkpoint: %w", err)
	}
	if err := checkpointContext(ctx); err != nil {
		return CheckpointState{}, err
	}
	record, err := decodeFileCheckpoint(contents)
	if err != nil {
		return CheckpointState{}, err
	}
	state := CheckpointState{
		Binding: Binding{
			ExecutionID:    record.ExecutionID,
			ManifestDigest: record.ManifestDigest,
		},
		Checkpoint: record.Checkpoint,
		Index:      record.Index,
		Completed:  record.Completed,
	}
	if state.Binding != binding {
		return CheckpointState{}, ErrCheckpointBinding
	}
	if err := validateStoredCheckpointState(state); err != nil {
		return CheckpointState{}, err
	}
	return state, nil
}

func (store *FileCheckpointStore) persistLocked(ctx context.Context, state CheckpointState) error {
	record := fileCheckpointV1{
		Schema:         fileCheckpointSchema,
		ExecutionID:    state.Binding.ExecutionID,
		ManifestDigest: state.Binding.ManifestDigest,
		Checkpoint:     state.Checkpoint,
		Index:          state.Index,
		Completed:      state.Completed,
	}
	contents, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode recipe checkpoint: %w", err)
	}
	contents = append(contents, '\n')
	temporary, err := os.CreateTemp(store.directory, ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create recipe checkpoint temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	closeTemporary := func() {
		_ = temporary.Close()
	}
	if err := temporary.Chmod(0o600); err != nil {
		closeTemporary()
		return fmt.Errorf("secure recipe checkpoint temp file: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		closeTemporary()
		return fmt.Errorf("write recipe checkpoint temp file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		closeTemporary()
		return fmt.Errorf("sync recipe checkpoint temp file: %w", err)
	}
	if err := checkpointContext(ctx); err != nil {
		closeTemporary()
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close recipe checkpoint temp file: %w", err)
	}
	destination := filepath.Join(store.directory, checkpointFilename(state.Binding))
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("replace recipe checkpoint: %w", err)
	}
	if err := syncCheckpointDirectory(store.directory); err != nil {
		return fmt.Errorf("sync recipe checkpoint directory: %w", err)
	}
	return nil
}

func checkpointFilename(binding Binding) string {
	digest := sha256.Sum256([]byte("dirextalk.recipe-checkpoint-binding/v1\x00" + binding.ExecutionID + "\x00" + binding.ManifestDigest))
	return hex.EncodeToString(digest[:]) + ".json"
}

func decodeFileCheckpoint(contents []byte) (fileCheckpointV1, error) {
	if len(contents) == 0 || len(contents) > maxCheckpointBytes {
		return fileCheckpointV1{}, fmt.Errorf("recipe checkpoint JSON size is invalid: %w", ErrCheckpointState)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var record fileCheckpointV1
	if err := decoder.Decode(&record); err != nil {
		return fileCheckpointV1{}, fmt.Errorf("decode recipe checkpoint: %w: %v", ErrCheckpointState, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fileCheckpointV1{}, fmt.Errorf("recipe checkpoint has trailing JSON: %w", ErrCheckpointState)
	}
	if record.Schema != fileCheckpointSchema {
		return fileCheckpointV1{}, fmt.Errorf("recipe checkpoint schema is invalid: %w", ErrCheckpointState)
	}
	return record, nil
}

func validateCheckpointBinding(binding Binding) error {
	if !validBindingIdentifier(binding.ExecutionID) || !validTaskDigest(binding.ManifestDigest) {
		return ErrCheckpointBinding
	}
	return nil
}

func validateStoredCheckpointState(state CheckpointState) error {
	if err := validateCheckpointBinding(state.Binding); err != nil {
		return err
	}
	if state.Index < -1 || (state.Index == -1 && (state.Checkpoint != "" || state.Completed)) ||
		(state.Index >= 0 && !validBindingIdentifier(state.Checkpoint)) {
		return ErrCheckpointState
	}
	return nil
}

func validateCheckpointTransition(previous, next CheckpointState) error {
	if err := validateStoredCheckpointState(previous); err != nil {
		return err
	}
	if err := validateStoredCheckpointState(next); err != nil {
		return err
	}
	if previous.Binding != next.Binding {
		return ErrCheckpointBinding
	}
	if previous.Completed || next.Index != previous.Index+1 || next.Checkpoint == "" {
		return ErrCheckpointState
	}
	return nil
}

func checkpointContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func syncCheckpointDirectory(directory string) error {
	// Windows does not expose a portable directory handle that os.File.Sync can
	// flush. Rename is still atomic there; the file itself was synced first.
	if runtime.GOOS == "windows" {
		return nil
	}
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
