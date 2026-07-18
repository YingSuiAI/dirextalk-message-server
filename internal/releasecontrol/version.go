package releasecontrol

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Masterminds/semver/v3"
)

var canonicalVersionPattern = regexp.MustCompile(`^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$`)

func parseCanonicalVersion(field, value string) (*semver.Version, error) {
	value = strings.TrimSpace(value)
	if !canonicalVersionPattern.MatchString(value) {
		return nil, fmt.Errorf("%s must be a canonical stable version such as v1.0.0", field)
	}
	parsed, err := semver.NewVersion(value)
	if err != nil {
		return nil, fmt.Errorf("%s is invalid: %w", field, err)
	}
	return parsed, nil
}
