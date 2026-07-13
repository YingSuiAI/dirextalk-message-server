package legacygateway

import (
	"testing"
	"time"
)

func TestRetryDelayIsBoundedExponentialAndDeterministic(t *testing.T) {
	testCases := []struct {
		name     string
		delivery uint64
		minimum  time.Duration
		maximum  time.Duration
	}{
		{name: "missing metadata", delivery: 0, minimum: time.Second, maximum: 2 * time.Second},
		{name: "first delivery", delivery: 1, minimum: time.Second, maximum: 2 * time.Second},
		{name: "second delivery", delivery: 2, minimum: 2 * time.Second, maximum: 4 * time.Second},
		{name: "third delivery", delivery: 3, minimum: 4 * time.Second, maximum: 8 * time.Second},
		{name: "capped delivery", delivery: 6, minimum: 30 * time.Second, maximum: time.Minute},
		{name: "far beyond cap", delivery: 100, minimum: 30 * time.Second, maximum: time.Minute},
	}

	const seed = uint64(0x5eed)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := legacyGatewayRetryDelay(testCase.delivery, seed)
			if got < testCase.minimum || got > testCase.maximum {
				t.Fatalf("retry delay = %v, want range [%v, %v]", got, testCase.minimum, testCase.maximum)
			}
			if repeated := legacyGatewayRetryDelay(testCase.delivery, seed); repeated != got {
				t.Fatalf("retry delay is not deterministic: first %v, repeated %v", got, repeated)
			}
		})
	}
}
