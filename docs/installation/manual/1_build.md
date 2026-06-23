---
title: Building/Installing Direxio Message Server
parent: Manual
grand_parent: Installation
has_toc: true
nav_order: 1
permalink: /installation/manual/build
---

# Build all Direxio Message Server commands

Direxio Message Server has numerous utility commands in addition to the actual server binaries.
Build them all from the root of the source repo with:

```sh
go build -o bin/ ./cmd/...
```

The resulting binaries will be placed in the `bin` subfolder.

# Installing Direxio Message Server

You can install the Direxio Message Server binary into `$GOPATH/bin` by using `go install`:

```sh
go install ./cmd/dendrite
```

Alternatively, you can specify a custom path for the binary to be written to using `go build`:

```sh
go build -o /usr/local/bin/ ./cmd/dendrite
```
