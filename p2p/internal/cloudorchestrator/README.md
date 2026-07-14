# Cloud Orchestrator domain contracts

This package is the provider-neutral, in-process contract library for the
separately deployed Cloud Orchestrator. It deliberately does **not** implement
ProductCore actions, PostgreSQL storage, an AWS SDK client, AWS CLI execution,
credential bootstrap, Worker control, or an MCP server.

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

No type exposes a secret value. A secret field is accepted only as an opaque
`secret_ref:<identifier>` reference. The validators reject common credential
shapes, inline URL credentials, and credential-bearing URL query parameters;
this is a guardrail, not a replacement for the client-encrypted bootstrap
channel or AWS KMS/Secrets Manager policy.

## Hash format

The current V1 hash is **canonical JSON plus SHA-256**, identified by
`canonical-json-sha256` and rendered as `sha256:<lowercase-hex>`. It reuses the
already pinned Matrix canonical-JSON implementation in this repository. It is
not deterministic CBOR and must not be advertised as one.

`PlanV1.Hash` excludes mutable status/execution projection fields and covers
the immutable approval surface instead. Set-like scope lists are copied and
sorted before encoding, so equivalent ordering cannot change a hash or
signature. `RecipeV1.Digest` and `QuoteV1.Digest` use the same canonical JSON
format for their respective full content.

If a later release adopts deterministic CBOR, it must introduce a new schema
version and hash algorithm identifier; a CBOR digest must never be compared to
or accepted as this V1 hash.

`golden_test.go` pins a representative V1 plan JSON, hash, and approval
payload to make a Go/Dart implementation mismatch visible before a wire
contract is enabled.
