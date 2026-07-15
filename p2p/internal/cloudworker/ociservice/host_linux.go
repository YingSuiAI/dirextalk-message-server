//go:build linux

package ociservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"golang.org/x/sys/unix"
)

const maxServiceSecretBytes = 64 << 10

type execCommandRunner struct{}

func (execCommandRunner) Run(ctx context.Context, executable string, arguments []string) ([]byte, error) {
	if executable != podmanPath {
		return nil, ErrProductionHost
	}
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = []string{"HOME=/nonexistent", "LANG=C", "PATH=/usr/bin:/bin"}
	command.Dir = "/"
	command.Stdin = nil
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	err := command.Run()
	if output.Len() > 64*1024 {
		return nil, ErrProductionHost
	}
	return output.Bytes(), err
}

func NewProductionDriver(resolver DescriptorResolver) (*Driver, error) {
	containerInitSource, initErr := productionContainerInitSource()
	if resolver == nil || os.Geteuid() != 0 || validatePodmanBinary() != nil || initErr != nil {
		return nil, ErrProductionHost
	}
	host := newPodmanHost(os.Geteuid(), execCommandRunner{})
	host.containerInitSource = containerInitSource
	return NewDriver(resolver, host), nil
}

func productionContainerInitSource() (string, error) {
	executable, err := os.Executable()
	if err != nil || !validContainerInitSourcePath(filepath.Clean(executable)) {
		return "", ErrProductionHost
	}
	info, err := os.Lstat(executable)
	owner, ownerOK := fileRootOwner(info)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 || !ownerOK || owner.Uid != 0 {
		return "", ErrProductionHost
	}
	return filepath.Clean(executable), nil
}

func validatePodmanBinary() error {
	info, err := os.Lstat(podmanPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return ErrProductionHost
	}
	state, ok := info.Sys().(*syscall.Stat_t)
	if !ok || state.Uid != 0 {
		return ErrProductionHost
	}
	return nil
}

func validateProductionSecretFiles(spec ContainerSpec) error {
	paths := make([]string, 0, len(spec.SecretMounts))
	for _, mount := range spec.SecretMounts {
		paths = append(paths, mount.StagedSource)
	}
	if len(paths) == 0 {
		return nil
	}
	var state unix.Statfs_t
	if err := unix.Statfs(SecretStagingRoot, &state); err != nil || uint64(state.Type) != uint64(unix.TMPFS_MAGIC) {
		return ErrProductionHost
	}
	root, err := os.Lstat(SecretStagingRoot)
	rootOwner, rootOwnerOK := fileRootOwner(root)
	if err != nil || !root.IsDir() || root.Mode()&fs.ModeSymlink != 0 || !rootOwnerOK || rootOwner.Uid != 0 || root.Mode().Perm()&0o022 != 0 {
		return ErrProductionHost
	}
	for _, value := range paths {
		parent, err := os.Lstat(filepath.Dir(value))
		parentOwner, parentOwnerOK := fileRootOwner(parent)
		if err != nil || !parent.IsDir() || parent.Mode()&fs.ModeSymlink != 0 || parent.Mode().Perm() != 0o700 || !parentOwnerOK || parentOwner.Uid != 0 {
			return ErrProductionHost
		}
		info, err := os.Lstat(value)
		owner, ownerOK := fileRootOwner(info)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || !ownerOK || owner.Uid != 0 || info.Size() <= 0 || info.Size() > maxServiceSecretBytes {
			return ErrProductionHost
		}
	}
	return nil
}

func refreshProductionServiceSecrets(spec ContainerSpec) error {
	if len(spec.SecretMounts) == 0 {
		return nil
	}
	if err := validateProductionSecretFiles(spec); err != nil {
		return err
	}
	var state unix.Statfs_t
	if err := unix.Statfs("/run", &state); err != nil || uint64(state.Type) != uint64(unix.TMPFS_MAGIC) {
		return ErrProductionHost
	}
	run, err := os.Lstat("/run")
	runOwner, runOwnerOK := fileRootOwner(run)
	if err != nil || !run.IsDir() || run.Mode()&fs.ModeSymlink != 0 || !runOwnerOK || runOwner.Uid != 0 || run.Mode().Perm()&0o022 != 0 {
		return ErrProductionHost
	}
	for _, directory := range []string{"/run/dirextalk", "/run/dirextalk/cloud-worker", ServiceSecretRoot} {
		if err := ensureRootOnlyDirectory(directory); err != nil {
			return err
		}
	}
	stableDirectory := filepath.Dir(spec.SecretMounts[0].StableSource)
	if filepath.Dir(stableDirectory) != ServiceSecretRoot {
		return ErrProductionHost
	}
	secretReadGID, stableDirectoryMode, secretFileMode := uint32(0), uint32(0o700), uint32(0o400)
	if spec.RuntimeProfile != nil && spec.RuntimeProfile.SecretReadGID != 0 {
		secretReadGID, stableDirectoryMode, secretFileMode = spec.RuntimeProfile.SecretReadGID, 0o750, 0o440
	}
	if err := ensureDirectoryOwnedBy(stableDirectory, 0, secretReadGID, stableDirectoryMode); err != nil {
		return ErrProductionHost
	}
	expected := make(map[string]struct{}, len(spec.SecretMounts))
	for _, mount := range spec.SecretMounts {
		if filepath.Dir(mount.StableSource) != stableDirectory {
			return ErrProductionHost
		}
		expected[filepath.Base(mount.StableSource)] = struct{}{}
	}
	if err := rejectUnexpectedServiceSecretFiles(stableDirectory, expected, secretReadGID, secretFileMode); err != nil {
		return err
	}
	for _, mount := range spec.SecretMounts {
		if err := atomicCopyServiceSecretWithOwnership(mount.StagedSource, mount.StableSource, 0, secretReadGID, secretFileMode); err != nil {
			return err
		}
	}
	return rejectUnexpectedServiceSecretFiles(stableDirectory, expected, secretReadGID, secretFileMode)
}

func rejectUnexpectedServiceSecretFiles(directory string, expected map[string]struct{}, expectedGID, expectedMode uint32) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return ErrProductionHost
	}
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		info, statErr := os.Lstat(path)
		owner, ownerOK := fileRootOwner(info)
		if statErr != nil || info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() || !ownerOK || owner.Uid != 0 || owner.Gid != expectedGID || uint32(info.Mode().Perm()) != expectedMode {
			return ErrProductionHost
		}
		if strings.HasPrefix(entry.Name(), ".secret-") {
			if err := os.Remove(path); err != nil {
				return ErrProductionHost
			}
			continue
		}
		if _, ok := expected[entry.Name()]; !ok {
			return ErrProductionHost
		}
	}
	return nil
}

func atomicCopyServiceSecret(sourcePath, destinationPath string) error {
	return atomicCopyServiceSecretWithOwnership(sourcePath, destinationPath, 0, 0, 0o400)
}

func atomicCopyServiceSecretWithOwnership(sourcePath, destinationPath string, expectedUID, expectedGID, destinationMode uint32) error {
	before, err := os.Lstat(sourcePath)
	beforeOwner, beforeOwnerOK := fileRootOwner(before)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&fs.ModeSymlink != 0 || before.Mode().Perm() != 0o600 || !beforeOwnerOK || beforeOwner.Uid != expectedUID || before.Size() <= 0 || before.Size() > maxServiceSecretBytes {
		return ErrProductionHost
	}
	if current, statErr := os.Lstat(destinationPath); statErr == nil {
		owner, ownerOK := fileRootOwner(current)
		if !current.Mode().IsRegular() || current.Mode()&fs.ModeSymlink != 0 || uint32(current.Mode().Perm()) != destinationMode || !ownerOK || owner.Uid != expectedUID || owner.Gid != expectedGID {
			return ErrProductionHost
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return ErrProductionHost
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return ErrProductionHost
	}
	defer source.Close()
	after, err := source.Stat()
	if err != nil || !os.SameFile(before, after) {
		return ErrProductionHost
	}
	temporary, err := os.CreateTemp(filepath.Dir(destinationPath), ".secret-*")
	if err != nil {
		return ErrProductionHost
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chown(int(expectedUID), int(expectedGID)); err != nil {
		temporary.Close()
		return ErrProductionHost
	}
	if err := temporary.Chmod(fs.FileMode(destinationMode)); err != nil {
		temporary.Close()
		return ErrProductionHost
	}
	written, copyErr := io.Copy(temporary, io.LimitReader(source, maxServiceSecretBytes+1))
	if copyErr != nil || written != before.Size() {
		temporary.Close()
		return ErrProductionHost
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return ErrProductionHost
	}
	if err := temporary.Close(); err != nil {
		return ErrProductionHost
	}
	if err := os.Rename(temporaryPath, destinationPath); err != nil {
		return ErrProductionHost
	}
	installed, err := os.Lstat(destinationPath)
	owner, ownerOK := fileRootOwner(installed)
	if err != nil || !installed.Mode().IsRegular() || installed.Mode()&fs.ModeSymlink != 0 || uint32(installed.Mode().Perm()) != destinationMode || installed.Size() != before.Size() || !ownerOK || owner.Uid != expectedUID || owner.Gid != expectedGID {
		return ErrProductionHost
	}
	return nil
}

func ensureProductionStorageDirectories(spec ContainerSpec) error {
	if len(spec.StorageMounts) == 0 {
		return nil
	}
	for _, base := range []string{"/var", "/var/lib"} {
		info, err := os.Lstat(base)
		state, ok := fileRootOwner(info)
		if err != nil || info == nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 || !ok || state.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
			return ErrProductionHost
		}
	}
	for _, directory := range []string{"/var/lib/dirextalk", "/var/lib/dirextalk/cloud-worker", StorageRoot} {
		if err := ensureRootOnlyDirectory(directory); err != nil {
			return err
		}
	}
	for _, mount := range spec.StorageMounts {
		if !storageSourcePattern.MatchString(mount.Source) || filepath.Clean(mount.Source) != mount.Source {
			return ErrProductionHost
		}
		kindDirectory := filepath.Dir(mount.Source)
		deploymentDirectory := filepath.Dir(kindDirectory)
		if filepath.Dir(deploymentDirectory) != StorageRoot || filepath.Base(kindDirectory) != "volumes" && filepath.Base(kindDirectory) != "data" {
			return ErrProductionHost
		}
		for _, directory := range []string{deploymentDirectory, kindDirectory} {
			if err := ensureRootOnlyDirectory(directory); err != nil {
				return err
			}
		}
		mode, err := cloudorchestrator.NormalizeOCIServiceStorageDirectoryMode(mount.DirectoryMode)
		if err != nil || ensureDirectoryOwnedBy(mount.Source, mount.OwnerUID, mount.OwnerGID, mode) != nil {
			return ErrProductionHost
		}
	}
	return nil
}

func ensureRootOnlyDirectory(directory string) error {
	return ensureDirectoryOwnedBy(directory, 0, 0, 0o700)
}

func ensureDirectoryOwnedBy(directory string, expectedUID, expectedGID, mode uint32) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(directory, 0o700); err != nil || os.Chown(directory, int(expectedUID), int(expectedGID)) != nil || os.Chmod(directory, fs.FileMode(mode)) != nil {
			return ErrProductionHost
		}
		info, err = os.Lstat(directory)
	}
	state, ownerOK := fileRootOwner(info)
	if err != nil || info == nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 || uint32(info.Mode().Perm()) != mode ||
		info.Mode()&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 || !ownerOK || state.Uid != expectedUID || state.Gid != expectedGID {
		return ErrProductionHost
	}
	return nil
}

func ensurePrivateDirectoryOwnedBy(directory string, expectedUID uint32) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(directory, 0o700); err != nil {
			return ErrProductionHost
		}
		info, err = os.Lstat(directory)
	}
	state, ownerOK := fileRootOwner(info)
	if err != nil || info == nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm() != 0o700 ||
		info.Mode()&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 || !ownerOK || state.Uid != expectedUID {
		return ErrProductionHost
	}
	return nil
}

func fileRootOwner(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	state, ok := info.Sys().(*syscall.Stat_t)
	return state, ok
}
