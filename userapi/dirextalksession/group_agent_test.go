package dirextalksession

import (
	"context"
	"testing"

	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
)

type yingIdentityUserAPI struct {
	fakeUserAPI
	account *userapi.Account
	created *userapi.PerformAccountCreationRequest
}

func (f *yingIdentityUserAPI) QueryAccountByLocalpart(_ context.Context, _ *userapi.QueryAccountByLocalpartRequest, res *userapi.QueryAccountByLocalpartResponse) error {
	res.Account = f.account
	return nil
}
func (f *yingIdentityUserAPI) PerformAccountCreation(_ context.Context, req *userapi.PerformAccountCreationRequest, res *userapi.PerformAccountCreationResponse) error {
	copy := *req
	f.created = &copy
	f.account = &userapi.Account{UserID: "@ying:example.test", Localpart: "ying", ServerName: "example.test", AccountType: req.AccountType, AppServiceID: req.AppServiceID}
	res.Account = f.account
	res.AccountCreated = true
	return nil
}

func TestNativeYingIdentityIsPasswordlessAndNeverIssuesDevice(t *testing.T) {
	api := &yingIdentityUserAPI{}
	issuer := NewIssuer(api, "example.test", "DEVICE", "test")
	for i := 0; i < 2; i++ {
		if err := issuer.EnsureNativeGroupAgentIdentity(context.Background(), "@ying:example.test"); err != nil {
			t.Fatal(err)
		}
	}
	if api.created == nil || api.created.Password != "" || api.created.AccountType != userapi.AccountTypeAppService || api.created.AppServiceID != nativeYingAppServiceID || api.createdDeviceAccessToken != "" {
		t.Fatalf("unsafe Native identity provision: %#v", api)
	}
}

func TestNativeYingIdentityRejectsExistingUserAndForeignIdentity(t *testing.T) {
	for _, account := range []*userapi.Account{{AccountType: userapi.AccountTypeUser}, {AccountType: userapi.AccountTypeAppService, AppServiceID: "external"}} {
		api := &yingIdentityUserAPI{account: account}
		issuer := NewIssuer(api, "example.test", "DEVICE", "test")
		if err := issuer.EnsureNativeGroupAgentIdentity(context.Background(), "@ying:example.test"); err == nil {
			t.Fatal("existing account was hijacked")
		}
		if api.created != nil || api.createdDeviceAccessToken != "" {
			t.Fatal("existing account was mutated")
		}
	}
	issuer := NewIssuer(&yingIdentityUserAPI{}, "example.test", "DEVICE", "test")
	for _, id := range []string{"@agent:example.test", "@ying:remote.test", "bad"} {
		if err := issuer.EnsureNativeGroupAgentIdentity(context.Background(), id); err == nil {
			t.Fatalf("accepted %s", id)
		}
	}
}
