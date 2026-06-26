#
# base installs required dependencies and runs go mod download to cache dependencies
#

# NOTE:
# If you update this Dockerfile, ensure to sync your changes to the other
# Dockerfiles in this repo (search *Dockerfile).
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.26.4-alpine AS base
RUN apk --update --no-cache add bash build-base curl git

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    USERARCH=`go env GOARCH` \
    GOARCH="$TARGETARCH" \
    GOOS="linux" \
    CGO_ENABLED=$([ "$TARGETARCH" = "$USERARCH" ] && echo "1" || echo "0") \
    go build -v -trimpath -o /out/ ./cmd/...


#
# Builds the Direxio Message Server image containing all required binaries
#
FROM alpine:latest
RUN apk --update --no-cache add curl
LABEL org.opencontainers.image.title="Direxio Message Server"
LABEL org.opencontainers.image.description="Direxio Matrix homeserver and P2P product API server"
LABEL org.opencontainers.image.source="https://github.com/YingSuiAI/direxio-message-server"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only OR LicenseRef-Element-Commercial"
LABEL org.opencontainers.image.documentation="https://github.com/YingSuiAI/direxio-message-server"
LABEL org.opencontainers.image.vendor="YingSuiAI"

COPY --from=build /out/create-account /usr/bin/create-account
COPY --from=build /out/generate-config /usr/bin/generate-config
COPY --from=build /out/generate-keys /usr/bin/generate-keys
COPY --from=build /out/direxio-message-server /usr/bin/direxio-message-server
COPY --from=build /out/dendrite /usr/bin/dendrite

VOLUME /etc/direxio-message-server
WORKDIR /etc/direxio-message-server

ENTRYPOINT ["/usr/bin/direxio-message-server"]
EXPOSE 8008 8448
