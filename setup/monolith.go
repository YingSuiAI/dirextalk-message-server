// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package setup

import (
	"os"
	"strconv"
	"strings"

	appserviceAPI "github.com/YingSuiAI/direxio-message-server/appservice/api"
	"github.com/YingSuiAI/direxio-message-server/clientapi"
	"github.com/YingSuiAI/direxio-message-server/clientapi/api"
	"github.com/YingSuiAI/direxio-message-server/federationapi"
	federationAPI "github.com/YingSuiAI/direxio-message-server/federationapi/api"
	"github.com/YingSuiAI/direxio-message-server/internal/caching"
	"github.com/YingSuiAI/direxio-message-server/internal/httputil"
	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/internal/transactions"
	"github.com/YingSuiAI/direxio-message-server/mediaapi"
	"github.com/YingSuiAI/direxio-message-server/p2p"
	"github.com/YingSuiAI/direxio-message-server/relayapi"
	relayAPI "github.com/YingSuiAI/direxio-message-server/relayapi/api"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/setup/jetstream"
	"github.com/YingSuiAI/direxio-message-server/setup/process"
	"github.com/YingSuiAI/direxio-message-server/syncapi"
	userapi "github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/sirupsen/logrus"
)

// Monolith represents an instantiation of all dependencies required to build
// all components of Dendrite, for use in monolith mode.
type Monolith struct {
	Config    *config.Dendrite
	KeyRing   *gomatrixserverlib.KeyRing
	Client    *fclient.Client
	FedClient fclient.FederationClient

	AppserviceAPI appserviceAPI.AppServiceInternalAPI
	FederationAPI federationAPI.FederationInternalAPI
	RoomserverAPI roomserverAPI.RoomserverInternalAPI
	UserAPI       userapi.UserInternalAPI
	RelayAPI      relayAPI.RelayInternalAPI

	// Optional
	ExtPublicRoomsProvider   api.ExtraPublicRoomsProvider
	ExtUserDirectoryProvider userapi.QuerySearchProfilesAPI
}

// AddAllPublicRoutes attaches all public paths to the given router
func (m *Monolith) AddAllPublicRoutes(
	processCtx *process.ProcessContext,
	cfg *config.Dendrite,
	routers httputil.Routers,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	caches *caching.Caches,
	enableMetrics bool,
) {
	userDirectoryProvider := m.ExtUserDirectoryProvider
	if userDirectoryProvider == nil {
		userDirectoryProvider = m.UserAPI
	}
	clientapi.AddPublicRoutes(
		processCtx, routers, cfg, natsInstance, m.FedClient, m.RoomserverAPI, m.AppserviceAPI, transactions.New(),
		m.FederationAPI, m.UserAPI, userDirectoryProvider,
		m.ExtPublicRoomsProvider, enableMetrics,
	)
	federationapi.AddPublicRoutes(
		processCtx, routers, cfg, natsInstance, m.UserAPI, m.FedClient, m.KeyRing, m.RoomserverAPI, m.FederationAPI, enableMetrics,
	)
	mediaapi.AddPublicRoutes(routers, cm, cfg, m.UserAPI, m.Client, m.FedClient, m.KeyRing)
	syncapi.AddPublicRoutes(processCtx, routers, cfg, cm, natsInstance, m.UserAPI, m.RoomserverAPI, caches, enableMetrics)
	remoteNodeInsecureSkipTLSVerify := p2pRemoteNodeInsecureSkipTLSVerifyFromEnv()
	p2pConfig := p2p.Config{
		ServerName:                      string(cfg.Global.ServerName),
		Homeserver:                      cfg.Global.WellKnownClientName,
		RemoteNodeInsecureSkipTLSVerify: remoteNodeInsecureSkipTLSVerify,
		RemoteNodeAllowPrivateBaseURLs:  remoteNodeInsecureSkipTLSVerify,
	}
	matrixHistoryBaseURL := matrixHistoryReaderBaseURL(p2pConfig.Homeserver)
	p2pTransport := p2p.NewDendriteTransport(cfg.Global.ServerName, cfg.Global.KeyID, cfg.Global.PrivateKey, m.RoomserverAPI)
	p2pService := p2p.NewServiceWithTransport(p2pConfig, p2pTransport)
	p2pService.SetMatrixSessionIssuer(p2p.NewDendriteMatrixSessionIssuer(m.UserAPI, cfg.Global.ServerName))
	p2pService.SetMatrixMessageReader(p2p.NewHTTPMatrixHistoryReader(matrixHistoryBaseURL, p2pService.MatrixHistoryAccessToken, nil))
	if store, err := p2p.NewDatabaseStore(processCtx.Context(), cm, p2pDatabaseOptions(cfg)); err != nil {
		logrus.WithError(err).Warn("P2P integrated AS store unavailable; falling back to in-memory business state")
	} else if service, err := p2p.NewServiceWithStoreAndTransport(processCtx.Context(), p2pConfig, store, p2pTransport); err != nil {
		logrus.WithError(err).Warn("P2P integrated AS state load failed; falling back to in-memory business state")
	} else {
		service.SetMatrixSessionIssuer(p2p.NewDendriteMatrixSessionIssuer(m.UserAPI, cfg.Global.ServerName))
		service.SetMatrixMessageReader(p2p.NewHTTPMatrixHistoryReader(matrixHistoryBaseURL, service.MatrixHistoryAccessToken, nil))
		p2pService = service
	}
	if natsInstance != nil {
		js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		if err := p2p.NewOutputRoomEventConsumer(processCtx, &cfg.Global.JetStream, js, p2pService).Start(); err != nil {
			logrus.WithError(err).Warn("P2P integrated AS projector unavailable")
		} else {
			p2pService.SetProjectorStarted(true)
		}
	}
	p2p.Register(routers.P2P, p2pService)
	p2p.RegisterWellKnown(routers.PortalWellKnown, p2pService)

	if m.RelayAPI != nil {
		relayapi.AddPublicRoutes(routers, cfg, m.KeyRing, m.RelayAPI)
	}
}

func p2pDatabaseOptions(cfg *config.Dendrite) *config.DatabaseOptions {
	if cfg.Global.DatabaseOptions.ConnectionString != "" {
		return &cfg.Global.DatabaseOptions
	}
	return &cfg.RoomServer.Database
}

func matrixHistoryReaderBaseURL(configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" ||
		strings.EqualFold(configured, "auto") ||
		strings.EqualFold(configured, "http://auto") ||
		strings.EqualFold(configured, "https://auto") {
		return "http://127.0.0.1:8008"
	}
	return configured
}

func p2pRemoteNodeInsecureSkipTLSVerifyFromEnv() bool {
	value := strings.TrimSpace(os.Getenv("P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY"))
	if value == "" {
		return false
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		logrus.WithField("value", value).Warn("Ignoring invalid P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY value")
		return false
	}
	return parsed
}
