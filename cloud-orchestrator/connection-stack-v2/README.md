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

`service.backup` is another separately gated typed action. With
`EnableServiceBackup=true`, it verifies a fresh device proof bound to the
exact Service/Deployment revisions, Connection, Recipe digest, original
tracked instance and complete EBS volume set. The Stack consumes the
approval/challenge and reserves the request before calling EC2. Because
`CreateImage` has no ClientToken, the provider uses a deterministic unique AMI
name derived from Connection and backup IDs as its mutation fence; exact
retries read back the same image instead of creating another backup.

The provider requests `NoReboot=true`, so this is a crash-consistent backup,
not an application-consistent backup. It returns a committed receipt only
after the AMI is available and every approved volume has exactly one completed
encrypted snapshot with the expected tags. Both the AMI and snapshots are
retained and reported as manually managed resources. Service stop, failure,
destruction, or backup Job completion does not delete them.

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

## One-time credential bootstrap

`cmd/connection-bootstrap` is a separate Go controller for the optional
advanced CSV path. Its controller listener accepts only mTLS
`POST /v1/aws-bootstrap/sessions` requests from the trusted control plane; its
public upload listener accepts only the returned one-time HTTPS bearer session.
The browser encrypts the CSV to the returned X25519 public key with AES-GCM,
and the session expires after ten minutes or one accepted upload. Session state
is intentionally in memory, so expiry or controller restart requires a new
owner action.

The controller accepts AWS root access-key material only when the
server-issued Role Plan explicitly has `allow_root_credential_bootstrap=true`.
That non-secret capability is bound to the exact bootstrap, Connection, public
keys, expiration and immutable Role Plan; a default/stale plan fails closed.
Credentials, caller identity, session bearer and plaintext never reach the
Message Server, Agent, MCP, Worker, persistent store or logs. The controller
can use the decrypted credentials only for one fixed `CreateStack` request and
zeroes the buffers after it receives the provider result.

Neither the Role Plan nor the uploader can pass arbitrary CloudFormation
parameters. The plan contributes exactly the reviewed connection/public-key
parameters. The controller maps only its own
`deployment_create_enabled`, `deployment_destroy_enabled`,
`service_secrets_enabled` and `dynamic_artifacts_enabled` configuration to the
corresponding fixed template flags; no uploaded value can enable an AWS action.

The controller configuration also requires one typed `foundation_plan`. Its
region must exactly equal the controller region and it is the sole source of
the seven Worker template parameters: immutable AMI and resource-manifest
digest, VPC, private subnet, Worker security group, Availability Zone, and IID
verifier public key.
Legacy loose `worker_ami_id`, VPC/subnet/AZ, manifest-digest, and IID-PEM
configuration fields are rejected rather than merged into the request.

## Disposable Connection Stack teardown

`cmd/connection-stack-teardown` is a separate Go-only owner cleanup controller.
It accepts only a Connection ID and Region when it creates a plan; it has no
credential, ARN, table, bucket, image, snapshot, or generic AWS API flags.
It uses the standard AWS SDK credential chain of the owner-operated process.

```powershell
go run ./cmd/connection-stack-teardown plan --connection-id <connection-id> --region <region> > teardown-plan.json
go run ./cmd/connection-stack-teardown execute --plan teardown-plan.json
go run ./cmd/connection-stack-teardown readback --plan teardown-plan.json
```

The plan is derived from the deterministic Connection Stack name and the
Stack's own CloudFormation resource inventory. Before `execute` makes a
mutation, it re-reads that inventory and rejects a stale or redirected plan.
It does not pass `RetainResources` to CloudFormation: normal stack deletion is
requested first, then only the reviewed retained resources are cleaned.

The current closed cleanup set is the retained DynamoDB tables, optional
dynamic-artifact S3 bucket and table, optional service-secret table/key/alias,
optional dynamic-artifact key, plus manual-retention backup AMIs and snapshots
that have the exact Connection ownership tags. DynamoDB deletion protection is
disabled and read back before each table deletion; versioned S3 objects and
delete markers are emptied before bucket deletion; every AMI/snapshot is read
back after deregistration/deletion. KMS aliases are removed first and keys are
scheduled with the fixed seven-day deletion window. A KMS key is reported as
`pending_key_deletion`, never as `verified_destroyed`, until AWS actually
removes it. Provider denial, lost plan provenance, a new untracked retained
resource, or incomplete read-back fails closed.

## Verify locally

```powershell
go test ./...
$env:CGO_ENABLED = "0"
$env:GOOS = "linux"
$env:GOARCH = "amd64"
go build -trimpath -buildvcs=false ./cmd/broker
Remove-Item Env:CGO_ENABLED, Env:GOOS, Env:GOARCH
```

These local commands do not contact an AWS account. With the default gates,
the Lambda execution role can write only its own logs/receipt
tables and call the quote read APIs. The conditional mutation statements exist
only when their explicit create, destroy or backup gate is true; they permit
the fixed RunInstances shape and tags, exact tagged-resource
termination/deletion, or tagged CreateImage plus AMI/snapshot read-back. They
never grant IAM pass-role, Secrets Manager,
secret-bootstrap, ingress or arbitrary AWS permissions.

## Next lifecycle boundary

Add a separately approved restore/rollback contract for one retained backup,
then management acceptance. Do not add arbitrary AWS passthrough, credentials,
public ingress or user-selected root commands to those boundaries.
