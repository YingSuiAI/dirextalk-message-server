package storage

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
