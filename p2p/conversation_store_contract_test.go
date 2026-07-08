package p2p

import "context"

type conversationOnlyStore struct{}

func (conversationOnlyStore) UpsertConversation(context.Context, conversationRecord) error {
	return nil
}

func (conversationOnlyStore) GetConversationByID(context.Context, string) (conversationRecord, bool, error) {
	return conversationRecord{}, false, nil
}

func (conversationOnlyStore) GetConversationByRoomID(context.Context, string) (conversationRecord, bool, error) {
	return conversationRecord{}, false, nil
}

func (conversationOnlyStore) ListConversations(context.Context) ([]conversationRecord, error) {
	return nil, nil
}

func (conversationOnlyStore) DeleteConversationByRoomID(context.Context, string) error {
	return nil
}

var _ conversationStore = conversationOnlyStore{}
