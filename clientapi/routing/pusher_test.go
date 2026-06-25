package routing

import "testing"

func TestIsDirexioHTTPPusherAppID(t *testing.T) {
	tests := []struct {
		name  string
		appID string
		want  bool
	}{
		{name: "android package", appID: "com.direxio.ai", want: true},
		{name: "ios bundle", appID: "com.direxio.app", want: true},
		{name: "old android pusher id", appID: "io.direxio.app.android", want: false},
		{name: "old ios pusher id", appID: "io.direxio.app.ios", want: false},
		{name: "unknown", appID: "io.direxio.mobile", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDirexioHTTPPusherAppID(tt.appID); got != tt.want {
				t.Fatalf("isDirexioHTTPPusherAppID(%q) = %t, want %t", tt.appID, got, tt.want)
			}
		})
	}
}
