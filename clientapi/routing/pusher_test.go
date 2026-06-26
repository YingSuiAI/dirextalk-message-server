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
		{name: "unknown", appID: "custom.web", want: false},
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
		name string
		data map[string]interface{}
		want bool
	}{
		{
			name: "production gateway",
			data: map[string]interface{}{"url": "https://push.direxio.ai/_matrix/push/v1/notify"},
			want: true,
		},
		{
			name: "regional Direxio gateway",
			data: map[string]interface{}{"url": "https://push-eu.direxio.ai/_matrix/push/v1/notify"},
			want: true,
		},
		{
			name: "non-Direxio gateway",
			data: map[string]interface{}{"url": "https://push.example.com/_matrix/push/v1/notify"},
			want: false,
		},
		{
			name: "Direxio host wrong path",
			data: map[string]interface{}{"url": "https://push.direxio.ai/notify"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresDirexioHTTPPusherAppID(tt.data); got != tt.want {
				t.Fatalf("requiresDirexioHTTPPusherAppID(%#v) = %t, want %t", tt.data, got, tt.want)
			}
		})
	}
}
