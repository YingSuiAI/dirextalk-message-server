package routing

import "testing"

func TestVersionsAdvertisesDirextalkResumableUpload(t *testing.T) {
	body := clientVersionsResponse(nil)
	if !body.UnstableFeatures["io.dirextalk.media.resumable_upload"] {
		t.Fatalf("unstable features = %#v, want io.dirextalk.media.resumable_upload=true", body.UnstableFeatures)
	}
}
