//go:build linux

package containerinit

import (
	"io"
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const maxSecretBytes = 64 * 1024

// Run is the measured cloud-worker binary's only container-side mode. It
// materializes file-backed secrets directly into the child process environment,
// drops identity, and execs one compiler-owned absolute entrypoint.
func Run(arguments []string) error {
	parsed, err := parse(arguments)
	if err != nil || os.Geteuid() != 0 {
		return ErrContainerInit
	}
	environment, secretValues, err := materializeEnvironment(parsed, os.Environ())
	if err != nil {
		return ErrContainerInit
	}
	defer erase(secretValues)
	if unix.Setgroups([]int{int(parsed.gid)}) != nil || unix.Setgid(int(parsed.gid)) != nil || unix.Setuid(int(parsed.uid)) != nil {
		return ErrContainerInit
	}
	argv := append([]string{parsed.entrypoint}, parsed.arguments...)
	if unix.Exec(parsed.entrypoint, argv, environment) != nil {
		return ErrContainerInit
	}
	return nil
}

func materializeEnvironment(parsed config, inherited []string) ([]string, [][]byte, error) {
	existing := make(map[string]struct{}, len(inherited))
	for _, variable := range inherited {
		key, _, ok := splitEnvironment(variable)
		if !ok {
			return nil, nil, ErrContainerInit
		}
		existing[key] = struct{}{}
	}
	environment := append([]string(nil), inherited...)
	values := make([][]byte, 0, len(parsed.secrets))
	for _, binding := range parsed.secrets {
		if _, found := existing[binding.key]; found {
			erase(values)
			return nil, nil, ErrContainerInit
		}
		value, err := readSecret(binding.file, parsed.gid)
		if err != nil {
			erase(values)
			return nil, nil, ErrContainerInit
		}
		values = append(values, value)
		environment = append(environment, binding.key+"="+string(value))
		existing[binding.key] = struct{}{}
	}
	return environment, values, nil
}

func readSecret(file string, expectedGID uint32) ([]byte, error) {
	before, err := os.Lstat(file)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&fs.ModeSymlink != 0 || before.Mode().Perm() != 0o440 || before.Size() <= 0 || before.Size() > maxSecretBytes {
		return nil, ErrContainerInit
	}
	owner, ok := before.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || owner.Gid != expectedGID {
		return nil, ErrContainerInit
	}
	handle, err := os.Open(file)
	if err != nil {
		return nil, ErrContainerInit
	}
	defer handle.Close()
	after, err := handle.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, ErrContainerInit
	}
	value, err := io.ReadAll(io.LimitReader(handle, maxSecretBytes+1))
	if err != nil || len(value) == 0 || len(value) > maxSecretBytes {
		erase([][]byte{value})
		return nil, ErrContainerInit
	}
	for _, character := range value {
		if character == 0 {
			erase([][]byte{value})
			return nil, ErrContainerInit
		}
	}
	return value, nil
}

func splitEnvironment(variable string) (string, string, bool) {
	for index := 0; index < len(variable); index++ {
		if variable[index] == '=' {
			return variable[:index], variable[index+1:], index > 0
		}
	}
	return "", "", false
}

func erase(values [][]byte) {
	for _, value := range values {
		for index := range value {
			value[index] = 0
		}
	}
}
