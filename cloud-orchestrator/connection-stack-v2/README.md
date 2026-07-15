# Dirextalk Connection Stack V2 (Go)

This directory is the user-owned AWS Connection Stack boundary. It is a
standalone nested Go module and is deliberately not imported by the Message
Server or the Cloud Orchestrator process.

It replaces the historical Node/SAM bundle that was removed from
`dirextalk-deployer`. There is no `package.json`, npm lockfile, JavaScript,
Node runtime, SAM source, or shell deployment script here.

## Current capability

The Lambda accepts only `POST /v2/commands` and validates the closed
`dirextalk.aws.command/v2` outer envelope:

- exact fields, no duplicate JSON keys, canonical base64, payload SHA-256,
  canonical millisecond timestamps, command lifetime, and Ed25519 signature;
- an exact `(connection_id, node_key_id)` PKIX/SPKI Ed25519 public-key lookup;
- the existing V2 signature base, including the four empty approval lines for
  non-deployment commands; and
- safe, no-store error responses only.

After node authentication and the generation fence, two typed actions are
always enabled:

- `connection.registration.verify` attests the exact Stack identity, explicit
  `prod` Broker URL, and fixed Worker AMI/network/manifest bindings;
- `quote.request` reads EC2 instance offerings/capacity and the AWS Price List
  to issue a 15-minute On-Demand estimate in USD.

Both actions atomically commit the per-Connection node counter, exact command
receipt, and (for quotes) issued quote in encrypted, deletion-protected DynamoDB
tables. Exact retries return the stored result as `idempotent`; command-id and
stale-counter conflicts fail closed. Stored results are validated again before
they are returned.

`deployment.create` is a third complete typed action, but is disabled by
default. When `EnableDeploymentCreate=true` is explicitly selected, it:

- verifies the registered Flutter device ApprovalV1 and recomputes the exact
  deterministic-CBOR QuoteV1 digest from the persisted issued quote;
- atomically consumes the approval/challenge, advances the node counter and
  writes a deployment reservation before provider mutation;
- creates one fixed-AMI, fixed-subnet, no-public-IP EC2 instance using a
  deterministic ClientToken, encrypted retained gp3 root volume, retained ENI,
  IMDSv2 and a Stack-owned no-ingress security group; and
- returns success only after EC2/EBS/ENI read-back matches the approved scope,
  then atomically commits the resource evidence and command receipt.

Exact retries resume the reservation with the same ClientToken or return the
validated receipt. Concurrent approval/challenge reuse, quote drift, stale
generation/counter and read-back mismatch fail closed. The same deployment
gate also protects the IID-attested Worker session, fixed digest-only task,
sealed Recipe install and independent readiness challenge routes. These routes
do not accept arbitrary commands, paths, URLs, ports or secret values.

`deployment.destroy` is a separate complete typed action and has its own
default-off `EnableDeploymentDestroy` gate. It verifies a fresh device proof
bound to the exact Service/Deployment revisions and the original persisted
EC2/EBS/ENI receipt, atomically consumes the approval/challenge and reserves
the request before provider mutation, then terminates the instance and deletes
the exact interfaces and volumes. It returns a committed receipt only after
individual AWS read-back proves every identifier absent. Transition states
return `deployment_destroy_in_progress`; unavailable or denied provider calls
never become success.

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

No local or real AWS account test is authorized by this module yet. With the
default gate, the Lambda execution role can write only its own logs/receipt
tables and call the quote read APIs. The conditional mutation statements exist
only when their explicit create or destroy gate is true; they permit the fixed
RunInstances shape and tags or exact tagged-resource termination/deletion plus
EC2/EBS/ENI read-back. They never grant IAM pass-role, Secrets Manager,
secret-bootstrap, ingress or arbitrary AWS permissions.

## Next lifecycle boundary

Add separately approved start/stop/restart and backup/restore contracts, then
management acceptance. Do not add arbitrary AWS passthrough, credentials,
public ingress or user-selected root commands to those boundaries.
