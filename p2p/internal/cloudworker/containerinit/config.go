package containerinit

import (
	"errors"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxSecretEnvironment = 32
	maxProcessArguments  = 32
	maxArgumentBytes     = 256
)

var (
	ErrContainerInit         = errors.New("container init rejected")
	environmentKeyPattern    = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,63}$`)
	serviceSecretPathPattern = regexp.MustCompile(`^/run/secrets/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

type secretEnvironment struct {
	key  string
	file string
}

type config struct {
	uid        uint32
	gid        uint32
	secrets    []secretEnvironment
	entrypoint string
	arguments  []string
}

func parse(arguments []string) (config, error) {
	if len(arguments) < 5 || arguments[0] != "container-init" {
		return config{}, ErrContainerInit
	}
	var result config
	seenRunAs := false
	index := 1
	for ; index < len(arguments) && arguments[index] != "--"; index++ {
		argument := arguments[index]
		switch {
		case strings.HasPrefix(argument, "--run-as="):
			if seenRunAs {
				return config{}, ErrContainerInit
			}
			uid, gid, err := parseRunAs(strings.TrimPrefix(argument, "--run-as="))
			if err != nil {
				return config{}, ErrContainerInit
			}
			result.uid, result.gid, seenRunAs = uid, gid, true
		case strings.HasPrefix(argument, "--secret-env="):
			binding, err := parseSecretEnvironment(strings.TrimPrefix(argument, "--secret-env="))
			if err != nil || len(result.secrets) >= maxSecretEnvironment {
				return config{}, ErrContainerInit
			}
			result.secrets = append(result.secrets, binding)
		default:
			return config{}, ErrContainerInit
		}
	}
	if !seenRunAs || len(result.secrets) == 0 || index >= len(arguments) || arguments[index] != "--" || index+1 >= len(arguments) {
		return config{}, ErrContainerInit
	}
	result.entrypoint = arguments[index+1]
	result.arguments = append([]string(nil), arguments[index+2:]...)
	if !validEntrypoint(result.entrypoint) || len(result.arguments) > maxProcessArguments {
		return config{}, ErrContainerInit
	}
	for _, argument := range result.arguments {
		if !validArgument(argument) {
			return config{}, ErrContainerInit
		}
	}
	if !sort.SliceIsSorted(result.secrets, func(i, j int) bool { return result.secrets[i].key < result.secrets[j].key }) {
		return config{}, ErrContainerInit
	}
	for i := 1; i < len(result.secrets); i++ {
		if result.secrets[i-1].key == result.secrets[i].key || result.secrets[i-1].file == result.secrets[i].file {
			return config{}, ErrContainerInit
		}
	}
	return result, nil
}

func parseRunAs(value string) (uint32, uint32, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, 0, ErrContainerInit
	}
	uid, uidErr := strconv.ParseUint(parts[0], 10, 16)
	gid, gidErr := strconv.ParseUint(parts[1], 10, 16)
	if uidErr != nil || gidErr != nil || uid == 0 || gid == 0 {
		return 0, 0, ErrContainerInit
	}
	return uint32(uid), uint32(gid), nil
}

func parseSecretEnvironment(value string) (secretEnvironment, error) {
	key, file, ok := strings.Cut(value, "=")
	if !ok || !environmentKeyPattern.MatchString(key) || strings.HasSuffix(key, "_FILE") || !serviceSecretPathPattern.MatchString(file) {
		return secretEnvironment{}, ErrContainerInit
	}
	return secretEnvironment{key: key, file: file}, nil
}

func validEntrypoint(value string) bool {
	if !path.IsAbs(value) || path.Clean(value) != value || value == "/" || !validArgument(value) {
		return false
	}
	switch path.Base(value) {
	case "sh", "bash", "dash", "ash", "zsh", "fish", "busybox", "env", "sudo":
		return false
	default:
		return true
	}
}

func validArgument(value string) bool {
	if len(value) > maxArgumentBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
