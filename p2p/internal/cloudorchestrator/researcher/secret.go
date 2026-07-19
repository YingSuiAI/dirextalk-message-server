package researcher

import (
	"errors"
	"os"
	"strings"
)

const maxModelSecretBytes = 8192

// ReadModelAPIKeyFile reads one mounted model credential into the private
// researcher process. Callers must not log, persist, return, or place the
// returned value in an event or error.
func ReadModelAPIKeyFile(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.ContainsAny(path, "\r\n\x00") {
		return "", errors.New("cloud researcher model secret file is invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxModelSecretBytes {
		return "", errors.New("cloud researcher model secret file is invalid")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("cloud researcher model secret file is invalid")
	}
	apiKey := strings.TrimSpace(string(content))
	if !validModelSecret(apiKey) {
		return "", errors.New("cloud researcher model secret file is invalid")
	}
	return apiKey, nil
}
