# Redacted draft contracts

Use these shapes as planning contracts, not provider requests. Omit unknown optional fields instead of inventing values. Use opaque references and SHA-256 digests; never include credentials, account IDs, role ARNs, IAM policies, provider actions, shell commands, user-data, or secret-bearing URLs.

- [CloudPlanV1](#cloudplanv1)
- [RecipeDraftV1](#recipedraftv1)
- [Capability vocabulary](#capability-vocabulary)
- [Confirmation record](#confirmation-record)

## CloudPlanV1

```yaml
schema_version: dirextalk.cloud-plan/v1
plan_id: draft-<uuid>
intent: <short non-secret summary>
provider: aws
connection_ref: <opaque existing connection id>
region: <region selected by user>
workload:
  kind: job | service_candidate
compute:
  topology: single_vm
  instance_count: 1
  class: cpu | gpu
  profile: <non-provider capability profile>
  market: on_demand | spot
  max_attempts: <user-selected planning bound>
isolation:
  mode: dedicated_vm
  root_requested: <boolean>
recipe:
  draft_id: <recipe draft id>
  digest_sha256: <digest or pending>
capabilities:
  - id: <product capability id>
    scope: <redacted structured scope>
    reason: <why it is required>
network:
  public_https: <boolean>
  hostname_ref: <opaque reference or omitted>
  audit_required: <same value as public_https>
lifecycle:
  destroy_mode: manual
  budget_mode: alert_only
  budget_alerts: [<non-binding thresholds>]
service:
  promotion: not_applicable | contract_required
  dirextalk_integration: none | optional_after_verification
sources: [<source evidence ids>]
cost_estimate:
  currency: USD
  amount: <estimate or range>
  basis: <assumptions and timestamp>
  binding: false
warnings: [<redacted warning>]
approval_status: external_confirmation_required
plan_digest_sha256: <digest or pending>
```

## RecipeDraftV1

```yaml
schema_version: dirextalk.recipe-draft/v1
draft_id: draft-<uuid>
name: <private display name>
visibility: private
workload_kind: job | service_candidate
installer_contract: dirextalk.generic-installer/v1
inputs_schema: <secret-free JSON Schema>
sources:
  - evidence_id: <id>
    authority: official | community | user_supplied
    url: <public documentation or immutable artifact URL>
    resolved_version: <pinned version>
    sha256: <required artifact digest>
    community_confirmation_required: <boolean>
capabilities_requested:
  - id: <capability id>
    scope: <structured least-privilege scope>
    reason: <reason>
steps:
  - id: <stable step id>
    action: package.install | archive.fetch | git.checkout | file.render | process.configure | health.verify
    args: <structured, secret-free values>
checkpoint:
  supported: <boolean>
  contract_version: <dirextalk.checkpoint/v1 or omitted>
  durable_artifact_ref: <opaque reference or omitted>
  restore_step_id: <step id or omitted>
service_output:
  contract_version: <dirextalk.service/v1 or omitted>
  internal_port: <non-public application port or omitted>
  health_path: <path or omitted>
  promotion_required: <boolean>
digest_sha256: <digest or pending>
```

## Capability vocabulary

- `compute.cpu`: one CPU VM.
- `compute.gpu`: one GPU VM.
- `host.root`: root inside a dedicated VM only.
- `network.egress`: scoped outbound access needed by pinned sources.
- `network.public_https`: audited public TLS on port 443 only.
- `storage.persistent`: scoped persistent storage request.
- `checkpoint.write`: durable checkpoint and restore contract.
- `dirextalk.integration`: optional post-verification integration request.

Reject unknown capability IDs from the draft. In particular, never use an `aws.*` capability or pass through an AWS/provider action.

## Confirmation record

Confirmation is informational and never authorizes execution:

```yaml
confirmation:
  recipe_digest_sha256: <exact digest>
  plan_digest_sha256: <exact digest>
  capability_digest_sha256: <exact digest>
  user_response: pending | confirmed_for_productcore_review
  approval_status: external_confirmation_required
```
