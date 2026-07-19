package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeHandlerIsFixedAndLoopbackSafe(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18080/ready", nil)
	response := httptest.NewRecorder()
	probeHandler().ServeHTTP(response, request)
	result := response.Result()
	defer result.Body.Close()
	body, _ := io.ReadAll(result.Body)
	if probeListenAddress != "127.0.0.1:18080" || result.StatusCode != http.StatusOK || string(body) != probeReadyBody || result.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("probe response address=%q status=%d body=%q", probeListenAddress, result.StatusCode, body)
	}
	for _, target := range []string{"/ready?token=value", "/", "/ready/"} {
		request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18080"+target, nil)
		response = httptest.NewRecorder()
		probeHandler().ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("target %q status=%d", target, response.Code)
		}
	}
	request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18080/ready", nil)
	response = httptest.NewRecorder()
	probeHandler().ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("method status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}
}
