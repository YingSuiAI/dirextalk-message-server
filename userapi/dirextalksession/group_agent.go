package dirextalksession

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
)

const nativeYingAppServiceID = "dirextalk-native-ying"

// EnsureNativeGroupAgentIdentity creates a passwordless service account, not
// a user session. Never repurpose an existing user or another appservice's
// account: old devices or passwords could otherwise impersonate Native Ying.
func (i *Issuer) EnsureNativeGroupAgentIdentity(ctx context.Context, mxid string) error {
	local, server, err := gomatrixserverlib.SplitID('@', mxid)
	if err != nil || local != "ying" || server != i.serverName {
		return fmt.Errorf("invalid Native Ying identity")
	}
	var query userapi.QueryAccountByLocalpartResponse
	err = i.userAPI.QueryAccountByLocalpart(ctx, &userapi.QueryAccountByLocalpartRequest{Localpart: local, ServerName: server}, &query)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if query.Account == nil {
		var created userapi.PerformAccountCreationResponse
		err = i.userAPI.PerformAccountCreation(ctx, &userapi.PerformAccountCreationRequest{AccountType: userapi.AccountTypeAppService, Localpart: local, ServerName: server, AppServiceID: nativeYingAppServiceID, OnConflict: userapi.ConflictAbort}, &created)
		if err != nil {
			return err
		}
		query.Account = created.Account
	}
	if query.Account == nil || query.Account.AccountType != userapi.AccountTypeAppService || query.Account.AppServiceID != nativeYingAppServiceID {
		return errors.New("reserved Native Ying identity is already owned by another account")
	}
	return i.updateMatrixProfile(ctx, local, server, "Ying", "")
}
