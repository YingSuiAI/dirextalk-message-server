package domain_test

import (
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/domain"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

func TestContactRecordRemainsAnAliasToModuleView(t *testing.T) {
	if reflect.TypeOf(domain.ContactRecord{}) != reflect.TypeOf(contactsmodule.View{}) {
		t.Fatalf("domain.ContactRecord must remain a type alias to contacts.View")
	}
	var legacy domain.ContactRecord = contactsmodule.View{PeerMXID: "@alice:example.com"}
	var module contactsmodule.View = legacy
	if module.PeerMXID != "@alice:example.com" {
		t.Fatalf("contact alias conversion lost fields: %#v", module)
	}
}
