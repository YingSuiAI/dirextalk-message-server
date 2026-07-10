package releasecontrol

import (
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
)

func (manifest Manifest) AllowsUpgradeFrom(currentVersion string) (bool, error) {
	current, err := parseCanonicalVersion("current_version", currentVersion)
	if err != nil {
		return false, err
	}
	target, err := parseCanonicalVersion("version", manifest.Version)
	if err != nil {
		return false, err
	}
	if !current.LessThan(target) {
		return false, nil
	}
	for index, value := range manifest.UpgradeFrom {
		constraint, err := semver.NewConstraint(strings.TrimSpace(value))
		if err != nil {
			return false, fmt.Errorf("upgrade_from[%d] is invalid: %w", index, err)
		}
		if constraint.Check(current) {
			return true, nil
		}
	}
	return false, nil
}

func (manifest Manifest) SupportsClient(clientVersion string) (bool, error) {
	client, err := parseClientVersion(clientVersion)
	if err != nil {
		return false, err
	}
	minimum, err := parseCanonicalVersion("minimum_client_version", manifest.MinimumClientVersion)
	if err != nil {
		return false, err
	}
	maximum, err := parseCanonicalVersion("maximum_client_version_exclusive", manifest.MaximumClientVersionExclusive)
	if err != nil {
		return false, err
	}
	return !client.LessThan(minimum) && client.LessThan(maximum), nil
}

func parseClientVersion(value string) (*semver.Version, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "v") {
		value = "v" + value
	}
	return parseCanonicalVersion("client_version", value)
}
