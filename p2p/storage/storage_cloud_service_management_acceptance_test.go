package storage

import (
	"context"
	"testing"
	"time"
)

func TestGetCloudManagedAcceptanceCompatibilityIsOwnerScoped(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 2, 0, 0, 0, time.UTC)
	_, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())

	compatibility, found, err := store.GetCloudManagedAcceptanceCompatibility(ctx, "@owner:example.com", "service-destroy-0001")
	if err != nil || !found || compatibility.DeploymentID != "deployment-destroy-0001" ||
		compatibility.DeploymentRevision <= 0 || compatibility.SignerKeyID == "" {
		t.Fatalf("compatibility=%#v found=%v err=%v", compatibility, found, err)
	}
	if _, found, err = store.GetCloudManagedAcceptanceCompatibility(ctx, "@other:example.com", "service-destroy-0001"); err != nil || found {
		t.Fatalf("cross-owner compatibility found=%v err=%v", found, err)
	}
}
