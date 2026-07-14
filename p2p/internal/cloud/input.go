package cloud

import (
	"regexp"
	"strings"
)

var (
	awsAccessKeyIDPattern = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	githubTokenPattern    = regexp.MustCompile(`\b(?:ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{22,})\b`)
	modelTokenPattern     = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`)
	secretAssignment      = regexp.MustCompile(`(?i)\b(?:aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key|session[_-]?token)|github[_-]?token|(?:api|model)[_-]?(?:key|token)|access[_-]?token|authorization)\s*[:=]\s*([^\s,;]+)`)
)

// ContainsSensitiveGoalMaterial recognizes credential-shaped material that
// must never be copied into a durable cloud goal. It intentionally accepts
// only a secret_ref placeholder for an assignment; real secret upload belongs
// to the later client-encrypted Connection Stack bootstrap channel.
func ContainsSensitiveGoalMaterial(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	upper := strings.ToUpper(value)
	if strings.Contains(upper, "-----BEGIN") && strings.Contains(upper, "PRIVATE KEY-----") {
		return true
	}
	if awsAccessKeyIDPattern.MatchString(value) || githubTokenPattern.MatchString(value) || modelTokenPattern.MatchString(value) {
		return true
	}
	for _, match := range secretAssignment.FindAllStringSubmatch(value, -1) {
		if len(match) == 2 && !isSecretReference(match[1]) {
			return true
		}
	}
	return false
}

func isSecretReference(value string) bool {
	value = strings.Trim(strings.TrimSpace(value), "'\"<>[](){}.,")
	value = strings.ToLower(value)
	return value == "secret_ref" || strings.HasPrefix(value, "secret_ref:") ||
		strings.HasPrefix(value, "${secret") || strings.HasPrefix(value, "{{secret")
}
