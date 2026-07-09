package tables_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/storage/postgres"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/storage/tables"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func mustCreatePublishedTable(t *testing.T, dbType test.DBType) (tab tables.Published, close func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	switch dbType {
	case test.DBTypePostgres:
		err = postgres.CreatePublishedTable(db)
		assert.NoError(t, err)
		tab, err = postgres.PreparePublishedTable(db)
	}
	assert.NoError(t, err)

	return tab, close
}

func TestPublishedTable(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, close := mustCreatePublishedTable(t, dbType)
		defer close()

		// Publish some rooms
		publishedRooms := []string{}
		asID := ""
		nwID := ""
		for i := 0; i < 10; i++ {
			room := test.NewRoom(t, alice)
			published := i%2 == 0
			err := tab.UpsertRoomPublished(ctx, nil, room.ID, asID, nwID, published)
			assert.NoError(t, err)
			if published {
				publishedRooms = append(publishedRooms, room.ID)
			}
			publishedRes, err := tab.SelectPublishedFromRoomID(ctx, nil, room.ID)
			assert.NoError(t, err)
			assert.Equal(t, published, publishedRes)
		}
		sort.Strings(publishedRooms)

		// check that we get the expected published rooms
		roomIDs, err := tab.SelectAllPublishedRooms(ctx, nil, "", true, true)
		assert.NoError(t, err)
		assert.Equal(t, publishedRooms, roomIDs)

		// test an actual upsert
		room := test.NewRoom(t, alice)
		err = tab.UpsertRoomPublished(ctx, nil, room.ID, asID, nwID, true)
		assert.NoError(t, err)
		err = tab.UpsertRoomPublished(ctx, nil, room.ID, asID, nwID, false)
		assert.NoError(t, err)
		// should now be false, due to the upsert
		publishedRes, err := tab.SelectPublishedFromRoomID(ctx, nil, room.ID)
		assert.NoError(t, err)
		assert.False(t, publishedRes, fmt.Sprintf("expected room %s to be unpublished", room.ID))

		// network specific test
		nwID = "irc"
		room = test.NewRoom(t, alice)
		err = tab.UpsertRoomPublished(ctx, nil, room.ID, asID, nwID, true)
		assert.NoError(t, err)
		publishedRooms = append(publishedRooms, room.ID)
		sort.Strings(publishedRooms)
		// should only return the room for network "irc"
		allNWPublished, err := tab.SelectAllPublishedRooms(ctx, nil, nwID, true, true)
		assert.NoError(t, err)
		assert.Equal(t, []string{room.ID}, allNWPublished)

		// check that we still get all published rooms regardless networkID
		roomIDs, err = tab.SelectAllPublishedRooms(ctx, nil, "", true, true)
		assert.NoError(t, err)
		assert.Equal(t, publishedRooms, roomIDs)
	})
}
