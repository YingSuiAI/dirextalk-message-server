package action

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Params provides compatible typed reads over a ProductCore params object.
// Invalid or absent values retain the historic zero-value behavior.
type Params map[string]any

func (p Params) Raw(key string) any                 { return p[key] }
func (p Params) String(key string) string           { return String(p[key]) }
func (p Params) Strings(key string) []string        { return Strings(p[key]) }
func (p Params) Int64(key string) int64             { return Int64(p[key]) }
func (p Params) Int64s(key string) []int64          { return Int64s(p[key]) }
func (p Params) Bool(key string) bool               { return Bool(p[key]) }
func (p Params) BoolMap(key string) map[string]bool { return BoolMap(p[key]) }

// String trims string and fmt.Stringer values. Other values return an empty
// string, matching the existing ProductCore parameter behavior.
func String(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

// Strings reads a []string or []any, trims entries, removes empty values, and
// de-duplicates while preserving first-seen order.
func Strings(value any) []string {
	switch v := value.(type) {
	case []string:
		return normalizedStrings(v)
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			values = append(values, String(item))
		}
		return normalizedStrings(values)
	default:
		return nil
	}
}

func normalizedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// Int64 accepts the numeric forms produced by JSON decoding and existing
// in-process callers. Invalid values return zero.
func Int64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	default:
		return 0
	}
}

// Int64s preserves the legacy collection behavior: typed slices are copied as
// given, while loosely typed slices and scalar inputs omit zero values.
func Int64s(value any) []int64 {
	switch v := value.(type) {
	case []int64:
		return append([]int64{}, v...)
	case []int:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			result = append(result, int64(item))
		}
		return result
	case []float64:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			result = append(result, int64(item))
		}
		return result
	case []any:
		result := make([]int64, 0, len(v))
		for _, item := range v {
			if n := Int64(item); n != 0 {
				result = append(result, n)
			}
		}
		return result
	default:
		if n := Int64(value); n != 0 {
			return []int64{n}
		}
		return nil
	}
}

// Bool accepts booleans, the strings true/1, and non-zero numeric values.
func Bool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}

// BoolMap reads a JSON object of feature flags, trimming keys and ignoring
// empty keys.
func BoolMap(value any) map[string]bool {
	raw, ok := value.(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	flags := make(map[string]bool, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		flags[key] = Bool(value)
	}
	if len(flags) == 0 {
		return nil
	}
	return flags
}
