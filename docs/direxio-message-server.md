# Direxio Message Server Image

This backend image packages the merged Matrix and P2P AS service.

## Image

Docker Hub repository:

```bash
direxio/message-server:latest
```

Release tags:

```bash
direxio/message-server:latest
direxio/message-server:<git-sha>
```

After pushing, inspect the immutable digest with:

```bash
docker buildx imagetools inspect direxio/message-server:<git-sha>
```

## Build And Push

Run these commands from the backend repository root:

```bash
docker build -t direxio/message-server:latest -t direxio/message-server:<git-sha> .
docker push direxio/message-server:latest
docker push direxio/message-server:<git-sha>
```

The compose files in this repository also tag the backend service as `direxio/message-server:latest`, so this is equivalent for a local rebuild:

```bash
docker compose -f docker-compose.p2p.yml build message-server
docker push direxio/message-server:latest
```

Before pushing a new `latest`, run the WSL2 multi-node regression against the rebuilt image:

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

## Runtime Notes

- PostgreSQL is expected to run as `postgres:18`.
- The default compose path for local credentials is `/var/direxio-message-server/p2p/bootstrap.json`.
- The credentials file contains `password`, unified `access_token`, `agent_token`, and `device_id`.
- If `P2P_PORTAL_PASSWORD` is not set, a new portal initializes with an 8 digit numeric password and writes it to the credentials file.
- Remote public channel lookup requires the client request to include `remote_node_base_url`, for example `https://dendrite-b:8448/_p2p`.
- `P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY=true` is intended only for trusted local self-signed test nodes.
