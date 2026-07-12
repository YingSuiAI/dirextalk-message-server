package storage_test

import (
	"github.com/YingSuiAI/dirextalk-message-server/p2p"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

var _ p2p.Store = (*storage.MemoryStore)(nil)
