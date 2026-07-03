---
title: Starting Dirextalk Message Server
parent: Manual
grand_parent: Installation
nav_order: 5
permalink: /installation/manual/start
---

# Starting Dirextalk Message Server

Once you have completed all preparation and installation steps,
you can start your Dirextalk Message Server deployment by executing the `dendrite` binary:

```bash
./dendrite -config /path/to/message-server.yaml
```

By default, Dirextalk Message Server will listen HTTP on port 8008. If you want to change the addresses
or ports that Dirextalk Message Server listens on, you can use the `-http-bind-address` and
`-https-bind-address` command line arguments:

```bash
./dendrite -config /path/to/message-server.yaml \
    -http-bind-address 1.2.3.4:12345 \
    -https-bind-address 1.2.3.4:54321
```
