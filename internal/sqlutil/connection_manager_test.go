package sqlutil_test

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestConnectionManager(t *testing.T) {

	t.Run("component defined connection string", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
			conStr, close := test.PrepareDBConnectionString(t, dbType)
			t.Cleanup(close)
			cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})

			dbProps := &config.DatabaseOptions{ConnectionString: config.DataSource(conStr)}
			db, writer, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}

			_, ok := writer.(*sqlutil.DummyWriter)
			if !ok {
				t.Fatalf("expected dummy writer")
			}

			// reuse existing connection
			db2, writer2, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}
			if db != db2 {
				t.Fatalf("expected database connection to be reused")
			}
			if writer != writer2 {
				t.Fatalf("expected database writer to be reused")
			}
		})
	})

	t.Run("global connection pool", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
			conStr, close := test.PrepareDBConnectionString(t, dbType)
			t.Cleanup(close)
			cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{ConnectionString: config.DataSource(conStr)})

			dbProps := &config.DatabaseOptions{}
			db, writer, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}

			_, ok := writer.(*sqlutil.DummyWriter)
			if !ok {
				t.Fatalf("expected dummy writer")
			}

			// reuse existing connection
			db2, writer2, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}
			if db != db2 {
				t.Fatalf("expected database connection to be reused")
			}
			if writer != writer2 {
				t.Fatalf("expected database writer to be reused")
			}
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
			conStr, close := test.PrepareDBConnectionString(t, dbType)
			t.Cleanup(close)

			processCtx := process.NewProcessContext()
			cm := sqlutil.NewConnectionManager(processCtx, config.DatabaseOptions{ConnectionString: config.DataSource(conStr)})

			dbProps := &config.DatabaseOptions{}
			_, _, err := cm.Connection(dbProps)
			if err != nil {
				t.Fatal(err)
			}

			processCtx.ShutdownDendrite()
			processCtx.WaitForComponentsToFinish()
		})
	})

	// test invalid connection string configured
	cm2 := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	_, _, err := cm2.Connection(&config.DatabaseOptions{ConnectionString: "http://"})
	if err == nil {
		t.Fatal("expected an error but got none")
	}

	// empty connection string is not allowed
	_, _, err = cm2.Connection(&config.DatabaseOptions{})
	if err == nil {
		t.Fatal("expected an error but got none")
	}
}
