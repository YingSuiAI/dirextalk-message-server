//go:build linux

package ociservice

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateStorageDirectoryRejectsModeAndSymlinkDrift(t *testing.T) {
	owner := uint32(os.Geteuid())
	directory := filepath.Join(t.TempDir(), "storage")
	if err := ensurePrivateDirectoryOwnedBy(directory, owner); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(directory); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory=%#v err=%v", info, err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateDirectoryOwnedBy(directory, owner); !errors.Is(err, ErrProductionHost) {
		t.Fatalf("mode drift error=%v", err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, directory); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateDirectoryOwnedBy(directory, owner); !errors.Is(err, ErrProductionHost) {
		t.Fatalf("symlink drift error=%v", err)
	}
}

func TestServiceSecretAtomicCopyCreates0400AndRejectsDestinationDrift(t *testing.T) {
	owner := uint32(os.Geteuid())
	group := uint32(os.Getegid())
	directory := t.TempDir()
	source := filepath.Join(directory, "staged")
	destination := filepath.Join(directory, "stable")
	value := []byte("test-secret-value")
	if err := os.WriteFile(source, value, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicCopyServiceSecretWithOwnership(source, destination, owner, group, 0o440); err != nil {
		t.Fatal(err)
	}
	installed, err := os.ReadFile(destination)
	info, statErr := os.Lstat(destination)
	if err != nil || statErr != nil || !bytes.Equal(installed, value) || info.Mode().Perm() != 0o440 {
		t.Fatalf("installed info=%v readErr=%v statErr=%v", info, err, statErr)
	}
	if err := os.Chmod(destination, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := atomicCopyServiceSecretWithOwnership(source, destination, owner, group, 0o440); !errors.Is(err, ErrProductionHost) {
		t.Fatalf("mode drift error=%v", err)
	}
	if err := os.Remove(destination); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(source, destination); err != nil {
		t.Fatal(err)
	}
	if err := atomicCopyServiceSecretWithOwnership(source, destination, owner, group, 0o440); !errors.Is(err, ErrProductionHost) {
		t.Fatalf("symlink drift error=%v", err)
	}
}
