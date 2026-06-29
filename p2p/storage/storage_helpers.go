package storage

import "strings"

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func normalizeStoredProductMemberRole(role string) string {
	if strings.EqualFold(strings.TrimSpace(role), "owner") {
		return "owner"
	}
	return "member"
}
