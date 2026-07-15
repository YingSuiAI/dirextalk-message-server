# Dirextalk Connection Stack V2 (Go)

This directory is the user-owned AWS Connection Stack boundary. It is a
standalone nested Go module and is deliberately not imported by the Message
Server or the Cloud Orchestrator process.

It replaces the historical Node/SAM bundle that was removed from
`dirextalk-deployer`. There is no `package.json`, npm lockfile, JavaScript,
Node runtime, SAM source, or shell deployment script here.

## Current read-only capability

The Lambda accepts only `POST /v2/commands` and validates the closed
`dirextalk.aws.command/v2` outer envelope:

- exact fields, no duplicate JSON keys, canonical base64, payload SHA-256,
  canonical millisecond timestamps, command lifetime, and Ed25519 signature;
- an exact `(connection_id, node_key_id)` PKIX/SPKI Ed25519 public-key lookup;
- the existing V2 signature base, including the four empty approval lines for
  non-deployment commands; and
- safe, no-store error responses only.

After node authentication and the generation fence, only two typed actions are
enabled:

- `connection.registration.verify` attests the exact Stack identity, explicit
  `prod` Broker URL, and fixed Worker AMI/network/manifest bindings;
- `quote.request` reads EC2 instance offerings/capacity and the AWS Price List
  to issue a 15-minute On-Demand estimate in USD.

Both actions atomically commit the per-Connection node counter, exact command
receipt, and (for quotes) issued quote in encrypted, deletion-protected DynamoDB
tables. Exact retries return the stored result as `idempotent`; command-id and
stale-counter conflicts fail closed. Stored results are validated again before
they are returned.

All other operations return `operation_not_enabled`. `deployment.create` is
rejected before signature verification because its
deterministic-CBOR `ApprovalV1` verifier, one-time approval consumption,
receipt/counter transaction, EC2 read-back, and provider mutation must be
ported and reviewed as one capability. Apart from the bounded receipt-table
writes above, no EC2, EBS, VPC, or IAM mutation, Worker session, root command,
secret delivery, public service, or billable resource creation is implemented
by this stage.

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

The CloudFormation template takes the exact `ConnectionId`,
`ConnectionGeneration`, `NodeKeyId`, public-key and `StageName` parameters
already emitted by the Message Server Role Plan, plus the immutable Broker
artifact and fixed Worker attestation parameters. Public keys are not AWS
credentials or private signing keys.

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
Lambda execution role can write its own logs and receipt tables and call only
`DescribeInstanceTypeOfferings`, `DescribeInstanceTypes`, and
`pricing:GetProducts`. It has no EC2/EBS/VPC mutation, IAM pass-role, Secrets
Manager, Worker, secret-bootstrap, or network-management permission.

## Next parity boundary

A later mutation stage must separately add deterministic-CBOR ApprovalV1
verification, one-time approval consumption, deployment reservation, fixed
Worker artifact/network enforcement, and AWS read-back before any provider
mutation is enabled. Until that whole boundary lands, this module remains a
safe Go-only registration and quote Broker rather than a cloud executor.
