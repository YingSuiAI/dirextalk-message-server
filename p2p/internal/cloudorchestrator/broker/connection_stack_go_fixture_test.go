package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStandaloneGoStackSuccessFixturesMatchOrchestratorContract(t *testing.T) {
	quoteCommand := testCommand(t)
	var quoteResult QuoteResult
	readGoStackFixture(t, "quote-success-v1.json", &quoteResult)
	if err := ValidateQuoteResult(quoteCommand, quoteResult); err != nil {
		t.Fatalf("Go Stack quote fixture: %v", err)
	}

	registrationCommand := testRegistrationCommand(t)
	var registrationResult RegistrationResult
	readGoStackFixture(t, "registration-success-v1.json", &registrationResult)
	if err := ValidateRegistrationResult(registrationCommand, registrationResult); err != nil {
		t.Fatalf("Go Stack registration fixture: %v", err)
	}
}

func readGoStackFixture(t *testing.T, name string, target any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "cloud-orchestrator", "connection-stack-v2", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}
