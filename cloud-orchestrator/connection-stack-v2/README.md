# Dirextalk Connection Stack V2 (Go)

This directory is the user-owned AWS Connection Stack boundary. It is a
standalone nested Go module and is deliberately not imported by the Message
Server or the Cloud Orchestrator process.

It replaces the historical Node/SAM bundle that was removed from
`dirextalk-deployer`. There is no `package.json`, npm lockfile, JavaScript,
Node runtime, SAM source, or shell deployment script here.

## Current safety state

The Lambda accepts only `POST /v2/commands` and validates the closed
`dirextalk.aws.command/v2` outer envelope:

- exact fields, no duplicate JSON keys, canonical base64, payload SHA-256,
  canonical millisecond timestamps, command lifetime, and Ed25519 signature;
- an exact `(connection_id, node_key_id)` PKIX/SPKI Ed25519 public-key lookup;
- the existing V2 signature base, including the four empty approval lines for
  non-deployment commands; and
- safe, no-store error responses only.

Every operation currently returns `operation_not_enabled` after that boundary.
`deployment.create` is rejected before signature verification because its
deterministic-CBOR `ApprovalV1` verifier, one-time approval consumption,
receipt/counter transaction, EC2 read-back, and provider mutation must be
ported and reviewed as one capability. No EC2, DynamoDB, EBS, VPC, IAM
mutation, Worker session, root command, secret delivery, public service, or
cost operation is implemented by this stage.

This is intentional. A partial mutation path must fail closed rather than make
an untracked or billable resource and claim feature parity.

## Build the Lambda artifact

The Go custom runtime executable must be named `bootstrap` at the root of a
zip archive. Build it from this directory with Go; no Node/npm installation is
needed:

```powershell
$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
$env:GOARCH = "amd64"
go build -trimpath -buildvcs=false -o bootstrap ./cmd/broker
Compress-Archive -Path bootstrap -DestinationPath broker-<immutable-build-id>.zip
Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH
```

Store the resulting immutable zip in a reviewed artifact bucket. The
CloudFormation template consumes its bucket, key, and optional object version
as parameters; this module intentionally does not contain an AWS CLI or shell
deployment entrypoint. The owner may deploy the reviewed template through the
AWS console or an approved release pipeline.

The CloudFormation template takes the exact `ConnectionID`, `NodeKeyID`, and
base64 PKIX/SPKI Ed25519 public key already bound by the Message Server
Connection registration flow. That public key is not an AWS credential or a
private signing key.

## Verify locally

```powershell
go test ./...
$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
$env:GOARCH = "amd64"
go build -trimpath -buildvcs=false ./cmd/broker
Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH
```

No local or real AWS account test is authorized by this module. The template's
Lambda execution role has only CloudWatch Logs permissions; it has no EC2,
EBS, IAM pass-role, DynamoDB, Secrets Manager, S3, or network-management
permissions.

## Next parity boundary

The next implementation stage must add, together, the durable receipt/counter
store and action-specific read-only registration/quote flow with contract
fixtures. A later mutation stage must separately add deterministic-CBOR
ApprovalV1 verification, one-time approval consumption, deployment
reservation, fixed Worker artifact/network validation, and AWS read-back.
Until then, this module remains a safe Go-only protocol gateway rather than a
cloud executor.
