# Cloud Orchestrator domain contracts

This package is the provider-neutral, in-process contract library for the
separately deployed Cloud Orchestrator. It deliberately does **not** implement
ProductCore actions, PostgreSQL storage, an AWS SDK client, AWS CLI execution,
credential bootstrap, direct Worker session control, a Worker executor, or an
MCP server.

## V1 contracts

- `RecipeV1` records de-secretsed provenance, minimum requirements, install
  checkpoints, health checks, and lifecycle action identifiers.
- `QuoteV1` records one to three estimates in integer minor currency units,
  with a maximum 15-minute validity window. It remains an estimate, not a
  billing hard stop.
- `PlanV1` binds one exact cloud connection, recipe digest, quote digest,
  resource scope, network scope, opaque secret references, and integrations.
- `ApprovalV1` repeats that binding in a canonical, device-signable payload
  with challenge, signer key id, revision, and expiry. A Broker must verify the
  signature and call `ValidateAgainstPlan` against its current plan before any
  typed mutation.
- `ExecutionProbeManifestV1` and `NoInputV1` are sealed, private artifacts for
  the sole pre-executor Worker task. They bind only immutable deployment/plan
  references and opaque digests; they cannot carry a command, URL, secret,
  image, or AWS action.

No type exposes a secret value. A secret field is accepted only as an opaque
`secret_ref:<identifier>` reference. The validators reject common credential
shapes, inline URL credentials, and credential-bearing URL query parameters;
this is a guardrail, not a replacement for the client-encrypted bootstrap
channel or AWS KMS/Secrets Manager policy.

## Hash format

The current V1 hash is **RFC 8949 Core Deterministic CBOR plus SHA-256**,
identified by `deterministic-cbor-sha256` and rendered as
`sha256:<lowercase-hex>`. It uses the official
`github.com/fxamacker/cbor/v2` `CoreDetEncOptions()` implementation.

Before CBOR encoding, the contract is marshalled through a JSON-compatible
tree using its JSON tags. This makes `snake_case` field names the wire names
for Go and Dart alike; JSON numbers are retained as signed/unsigned integers,
never converted through `float64`. CBOR is the only input to any digest or
approval signature. JSON, if produced by a future display API, is not a hash
or signing format.

`PlanV1.Hash` excludes mutable status/execution projection fields and covers
the immutable approval surface instead. Set-like scope lists are copied and
sorted before encoding, so equivalent ordering cannot change a hash or
signature. `RecipeV1.Digest`, `QuoteV1.Digest`, and the fixed execution-probe
artifacts use the same deterministic CBOR format for their respective full
content.

`golden_test.go` pins representative V1 CBOR (base64), hash, and approval
payload vectors to make a Go/Dart implementation mismatch visible before a
wire contract is enabled.
