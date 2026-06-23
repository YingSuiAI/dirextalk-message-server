package test

import "testing"

func TestWithAllDatabasesRunsOnlyPostgres(t *testing.T) {
	WithAllDatabases(t, func(t *testing.T, dbType DBType) {
		if dbType != DBTypePostgres {
			t.Fatalf("WithAllDatabases returned db type %d, want PostgreSQL only", dbType)
		}
	})
}
