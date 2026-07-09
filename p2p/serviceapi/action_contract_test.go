package serviceapi

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestActionContractArtifactMatchesActionSpecs(t *testing.T) {
	data, err := os.ReadFile("../../docs/product-action-contract.json")
	if err != nil {
		t.Fatalf("read generated contract: %v", err)
	}
	var artifact ActionContractDocument
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("parse generated contract: %v", err)
	}
	expected := ActionContract()
	if !reflect.DeepEqual(artifact, expected) {
		t.Fatalf("generated action contract is stale; run go run ./cmd/dirextalk-action-contract > docs/product-action-contract.json")
	}
}
