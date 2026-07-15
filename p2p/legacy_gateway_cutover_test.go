package p2p

import "testing"

func TestLegacyAgentGatewayCutoverFailsClosedUntilOldConsumerIsFenced(t *testing.T) {
	for _, test := range []struct {
		name  string
		gate  LegacyAgentGatewayCutover
		valid bool
	}{
		{name: "disabled", gate: LegacyAgentGatewayCutover{Mode: "vnext_gateway", LegacyConnectConsumerFenced: true}},
		{name: "wrong mode", gate: LegacyAgentGatewayCutover{Enabled: true, Mode: "legacy", LegacyConnectConsumerFenced: true}},
		{name: "old consumer active", gate: LegacyAgentGatewayCutover{Enabled: true, Mode: "vnext_gateway"}},
		{name: "exclusive vnext consumer", gate: LegacyAgentGatewayCutover{Enabled: true, Mode: "vnext_gateway", LegacyConnectConsumerFenced: true}, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.gate.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("Validate() error=%v valid=%v", err, test.valid)
			}
		})
	}
}

func TestStartLegacyAgentGatewayConsumerRequiresExclusiveCutover(t *testing.T) {
	consumer := &recordingLegacyGatewayConsumer{}
	if err := StartLegacyAgentGatewayConsumer(LegacyAgentGatewayCutover{}, consumer); err == nil {
		t.Fatal("disabled cutover started consumer")
	}
	if consumer.starts != 0 {
		t.Fatalf("disabled cutover started consumer %d times", consumer.starts)
	}

	cutover := LegacyAgentGatewayCutover{
		Enabled:                     true,
		Mode:                        "vnext_gateway",
		LegacyConnectConsumerFenced: true,
	}
	if err := StartLegacyAgentGatewayConsumer(cutover, consumer); err != nil {
		t.Fatal(err)
	}
	if consumer.starts != 1 {
		t.Fatalf("exclusive cutover started consumer %d times", consumer.starts)
	}
}

type recordingLegacyGatewayConsumer struct{ starts int }

func (consumer *recordingLegacyGatewayConsumer) Start() error {
	consumer.starts++
	return nil
}
