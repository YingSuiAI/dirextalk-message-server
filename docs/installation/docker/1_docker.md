---
title: Installation
parent: Docker
grand_parent: Installation
has_toc: true
nav_order: 1
permalink: /installation/docker/install
---

# Installing Dirextalk Message Server using Docker Compose

Dirextalk Message Server provides an [example](https://github.com/YingSuiAI/dirextalk-message-server/blob/main/build/docker/docker-compose.yml)
Docker compose file, which needs some preparation to start successfully.
Please note that this compose file only has Postgres as a dependency, and you need to configure
a [reverse proxy](../planning#reverse-proxy).

## Preparations

### Generate a private key

First we'll generate private key, which is used to sign events, the following will create one in `./config`:

```bash
mkdir -p ./config
docker run --rm --entrypoint="/usr/bin/generate-keys" \
  -v $(pwd)/config:/mnt \
  ghcr.io/yingsuiai/dirextalk-message-server:latest \
  -private-key /mnt/matrix_key.pem

# Windows equivalent: docker run --rm --entrypoint="/usr/bin/generate-keys" -v %cd%/config:/mnt ghcr.io/yingsuiai/dirextalk-message-server:latest -private-key /mnt/matrix_key.pem
```
(**NOTE**: This only needs to be executed **once**, as you otherwise overwrite the key)

### Generate a config

Similar to the command above, we can generate a config to be used, which will use the correct paths
as specified in the example docker-compose file. Change `server` to your domain and `db` according to your changes
to the docker-compose file (`services.postgres.environment` values):

```bash
mkdir -p ./config
docker run --rm --entrypoint="/bin/sh" \
  -v $(pwd)/config:/mnt \
  ghcr.io/yingsuiai/dirextalk-message-server:latest \
  -c "/usr/bin/generate-config \
    -dir /var/dirextalk-message-server/ \
    -db postgres://dirextalk_message_server:itsasecret@postgres/dirextalk_message_server?sslmode=disable \
    -server YourDomainHere > /mnt/message-server.yaml"

# Windows equivalent: docker run --rm --entrypoint="/bin/sh" -v %cd%/config:/mnt ghcr.io/yingsuiai/dirextalk-message-server:latest -c "/usr/bin/generate-config -dir /var/dirextalk-message-server/ -db postgres://dirextalk_message_server:itsasecret@postgres/dirextalk_message_server?sslmode=disable -server YourDomainHere > /mnt/message-server.yaml"
```

You can then change `config/message-server.yaml` to your liking.

## Starting Dirextalk Message Server

Once you're done changing the config, you can now start up Dirextalk Message Server with

```bash
docker-compose -f docker-compose.yml up
```
