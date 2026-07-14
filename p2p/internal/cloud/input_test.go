package cloud

import "testing"

func TestContainsSensitiveGoalMaterial(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "aws secret assignment", value: "AWS_SECRET_ACCESS_KEY=not-a-real-secret-value", want: true},
		{name: "model token assignment", value: "MODEL_TOKEN=not-a-real-model-token", want: true},
		{name: "model API key assignment", value: "MODEL_API_KEY=not-a-real-model-key", want: true},
		{name: "private key", value: "-----BEGIN PRIVATE KEY-----", want: true},
		{name: "secret reference", value: "MODEL_TOKEN=secret_ref:cloud_secret_123", want: false},
		{name: "ordinary requirement", value: "The service requires a model token supplied through the secure channel.", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ContainsSensitiveGoalMaterial(test.value); got != test.want {
				t.Fatalf("ContainsSensitiveGoalMaterial(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
