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

func TestRequiresDirexioHTTPPusherAppID(t *testing.T) {
	tests := []struct {
		name  string
		appID string
		data  map[string]interface{}
		want  bool
	}{
		{
			name:  "current app id to production gateway",
			appID: "com.direxio.ai",
			data:  map[string]interface{}{"url": "https://push.direxio.ai/_matrix/push/v1/notify"},
			want:  true,
		},
		{
			name:  "retired app id to any gateway",
			appID: "io.direxio.app.android",
			data:  map[string]interface{}{"url": "https://example.com/_matrix/push/v1/notify"},
			want:  true,
		},
		{
			name:  "regional Direxio gateway",
			appID: "custom",
			data:  map[string]interface{}{"url": "https://push-eu.direxio.ai/_matrix/push/v1/notify"},
			want:  true,
		},
		{
			name:  "non-Direxio gateway",
			appID: "custom",
			data:  map[string]interface{}{"url": "https://push.example.com/_matrix/push/v1/notify"},
			want:  false,
		},
		{
			name:  "Direxio host wrong path",
			appID: "custom",
			data:  map[string]interface{}{"url": "https://push.direxio.ai/notify"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresDirexioHTTPPusherAppID(tt.appID, tt.data); got != tt.want {
				t.Fatalf("requiresDirexioHTTPPusherAppID(%q, %#v) = %t, want %t", tt.appID, tt.data, got, tt.want)
			}
		})
	}
}
