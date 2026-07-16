#
# base installs required dependencies and runs go mod download to cache dependencies
#

# NOTE:
# If you update this Dockerfile, ensure to sync your changes to the other
# Dockerfiles in this repo (search *Dockerfile).
FROM --platform=${BUILDPLATFORM} docker.io/golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS base
RUN apk --update --no-cache add bash build-base git

#
# build creates all needed binaries
#
FROM --platform=${BUILDPLATFORM} base AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=v0.0.0-dev+local
ARG COMMIT=uncommitted
ARG BUILD_TIME=
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    USERARCH=`go env GOARCH` \
    GOARCH="$TARGETARCH" \
    GOOS="linux" \
    CGO_ENABLED=$([ "$TARGETARCH" = "$USERARCH" ] && echo "1" || echo "0") \
    go build -v -trimpath \
    -ldflags="-s -w -X github.com/YingSuiAI/dirextalk-message-server/internal.version=${VERSION} -X github.com/YingSuiAI/dirextalk-message-server/internal.commit=${COMMIT} -X github.com/YingSuiAI/dirextalk-message-server/internal.buildTime=${BUILD_TIME}" \
    -o /out/ \
      ./cmd/dirextalk-message-server \
      ./cmd/generate-config \
      ./cmd/generate-keys


#
# Builds the Dirextalk Message Server image containing the runtime binary and
# per-instance initialization tools.
#
FROM docker.io/alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
ARG VERSION=v0.0.0-dev+local
ARG COMMIT=uncommitted
ARG BUILD_TIME=

RUN apk --update --no-cache add bash ca-certificates
LABEL org.opencontainers.image.title="Dirextalk Message Server"
LABEL org.opencontainers.image.description="Dirextalk Matrix homeserver and P2P product API server"
LABEL org.opencontainers.image.source="https://github.com/YingSuiAI/dirextalk-message-server"
LABEL org.opencontainers.image.licenses="AGPL-3.0-only OR LicenseRef-Element-Commercial"
LABEL org.opencontainers.image.documentation="https://github.com/YingSuiAI/dirextalk-message-server"
LABEL org.opencontainers.image.vendor="YingSuiAI"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${COMMIT}"
LABEL org.opencontainers.image.created="${BUILD_TIME}"

COPY --from=build /out/generate-config /usr/bin/generate-config
COPY --from=build /out/generate-keys /usr/bin/generate-keys
COPY --from=build /out/dirextalk-message-server /usr/bin/dirextalk-message-server

VOLUME /etc/dirextalk-message-server
WORKDIR /etc/dirextalk-message-server

ENTRYPOINT ["/usr/bin/dirextalk-message-server"]
EXPOSE 8008 8448
