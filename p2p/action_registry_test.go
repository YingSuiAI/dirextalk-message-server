package p2p

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
)

func TestActionRegistryCoversPublicAndAgentActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	for _, action := range serviceapi.PublicActions() {
		if _, ok := service.actions[action]; !ok {
			t.Errorf("public action %q has no registered handler", action)
		}
	}
	for _, action := range serviceapi.AgentActions() {
		if _, ok := service.actions[action]; !ok {
			t.Errorf("agent action %q has no registered handler", action)
		}
	}
}
