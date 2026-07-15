//go:build linux

package ociservice

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

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
	if resolver == nil || os.Geteuid() != 0 || validatePodmanBinary() != nil {
		return nil, ErrProductionHost
	}
	return NewDriver(resolver, newPodmanHost(os.Geteuid(), execCommandRunner{})), nil
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
		paths = append(paths, mount.Source)
	}
	if len(paths) == 0 {
		return nil
	}
	var state unix.Statfs_t
	if err := unix.Statfs(SecretStagingRoot, &state); err != nil || uint64(state.Type) != uint64(unix.TMPFS_MAGIC) {
		return ErrProductionHost
	}
	root, err := os.Lstat(SecretStagingRoot)
	if err != nil || !root.IsDir() || root.Mode()&fs.ModeSymlink != 0 {
		return ErrProductionHost
	}
	for _, value := range paths {
		parent, err := os.Lstat(filepath.Dir(value))
		if err != nil || !parent.IsDir() || parent.Mode()&fs.ModeSymlink != 0 || parent.Mode().Perm() != 0o700 {
			return ErrProductionHost
		}
		info, err := os.Lstat(value)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
			return ErrProductionHost
		}
	}
	return nil
}
