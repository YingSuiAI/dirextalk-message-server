package brokertransport

import (
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

func TestRecipeInstallBrokerErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "expired", err: &broker.Error{Code: "expired_command", StatusCode: 401}, want: "recipe_install_command_expired"},
		{name: "unavailable", err: errors.New("connection reset"), want: "recipe_install_broker_unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyRecipeInstallBrokerError(test.err).Error(); !strings.Contains(got, test.want) {
				t.Fatalf("classification=%q, want %q", got, test.want)
			}
		})
	}
}
