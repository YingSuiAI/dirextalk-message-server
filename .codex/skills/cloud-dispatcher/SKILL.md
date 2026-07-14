---
name: cloud-dispatcher
description: Research cloud workload requirements and prepare redacted, non-executable Cloud Plan and private RecipeDraft proposals for user review. Use when planning a single-VM CPU/GPU workload, generic software installation, public HTTPS exposure, checkpoint-safe Spot execution, or optional Dirextalk service integration without provisioning or changing cloud resources.
---

# Cloud Dispatcher

Produce research-backed planning artifacts only. Keep every artifact non-executable and require an external ProductCore confirmation boundary before any implementation.

## Enforce the boundary

- Remain read-only. Research requirements, repository contracts, and documentation; generate only redacted drafts.
- Never request, open, inspect, echo, transform, transmit, or save AWS AK/SK (access key ID or secret access key), a session token, private key, password, bearer token, environment secret, or secret file.
- Never call AWS CLI, an AWS SDK, an AWS API, Terraform, CloudFormation, Pulumi, CDK, or any deployer command. Do not make credential-validation or identity calls.
- Never approve, create, start, stop, modify, retry, resume, or destroy a cloud resource or workload.
- Never emit IAM policies, AWS action lists, instance user-data, presigned URLs, or arbitrary provider API payloads.
- Never treat user confirmation as execution authority. Record it only as input for a future ProductCore approval flow.
- If a request crosses this boundary, stop at the draft and explain that a separate authorized control plane must perform the action.
- If secret material appears in the conversation, do not repeat it. Ask the user to remove or rotate it and continue only with non-secret identifiers such as an opaque connection reference.

## Research safely

1. Inspect only the repository files and public documentation needed to resolve the workload.
2. Prefer official vendor or project documentation. Label every source as `official`, `community`, or `user_supplied`.
3. Treat a community source as unapproved until the user separately confirms its exact URL, version, and digest. Never convert community instructions into an executable action.
4. State unresolved assumptions and their impact. Ask only for choices that materially change cost, exposure, isolation, or recoverability.
5. Do not inspect local credential files, shell history, environment variables, cloud state, or live cloud accounts.

## Build the RecipeDraft

Read [references/contracts.md](references/contracts.md) before drafting.

- Make the recipe owner-private and immutable by digest once handed off.
- Describe a generic installer workflow; do not select a built-in service template.
- Declare all requested capabilities and the reason for each one.
- Keep execution to one dedicated VM. Mark every root request as `host.root` and `dedicated_vm`.
- Permit Spot only for a `job` with a declared, durable checkpoint and restore contract. Keep services and non-checkpoint jobs on On-Demand.
- Describe software steps with structured installer actions. Do not include shell commands, provider commands, or cloud API operations.
- Pin source versions and content digests. Keep credentials and secret values out of inputs, steps, examples, and evidence.
- For an unknown service, emit a `service_candidate` contract. Require later health and HTTPS verification before it can be treated as a managed service.

## Build the Cloud Plan

- Reference an existing cloud connection only by opaque `connection_ref`; never include or derive credentials, account IDs, or role ARNs.
- Fix `instance_count` to `1` and choose only `cpu` or `gpu` compute class.
- Set resource retention to manual destruction. Treat budgets as alerts only; never imply a budget threshold will stop or destroy resources.
- Represent public exposure only as the `network.public_https` capability. Require port 443, a hostname reference, TLS verification, ingress review, and continuous audit. Do not propose SSH or arbitrary public ports.
- Keep Dirextalk integration optional and separate from deployment. Require a verified service contract and a later ProductCore confirmation.
- Include cost assumptions and uncertainty, but label estimates as non-binding.
- Compute no executable approval token. Leave approval status as `external_confirmation_required`.

## Request confirmation

Return these sections in order:

1. `Research`: sources, facts, and unresolved uncertainty.
2. `RecipeDraft`: a redacted draft following the reference contract.
3. `CloudPlan`: a redacted draft following the reference contract.
4. `Capability review`: each capability, scope, risk, and reason.
5. `Confirmation request`: ask the user to confirm the exact recipe digest, capability digest, compute class, market, public HTTPS choice, community sources, manual-destroy policy, and alert-only budget policy.
6. `Safety statement`: state that no credential was read or stored, no AWS/provider API was called, no approval was granted, and no resource was changed.

After confirmation, update only the non-executable draft metadata and keep `approval_status` external. Hand off to ProductCore; do not continue into execution.
