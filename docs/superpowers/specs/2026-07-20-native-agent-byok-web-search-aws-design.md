# Native Agent BYOK Web Search and AWS Design

Date: 2026-07-20
Status: Accepted
Branches: `codex/agent-main-upgrade`

## Problem

Dirextalk Native Agent can call a user-selected model and built-in Dirextalk,
runtime, Skill, and MCP tools, but its Agent Settings page does not let an
owner configure public web search or AWS credentials. Product Agent has
server-managed search and burst-compute paths, but those credentials and
services must not become an implicit dependency of the user-owned Native
Agent.

The new settings must make both capabilities usable immediately after an owner
provides their own credentials. AWS create and terminate operations are
allowed, but a model must never be able to approve its own cloud mutation.

## Scope

This feature changes only Agent-owned code in:

- `YingSuiAI/dirextalk-message-server`
- `YingSuiAI/direxio-flutter`

It does not change Product Agent, Message Server non-Agent behavior, deployment
defaults, or either repository's `main` branch.

## Decisions

### Request-scoped BYOK

Web-search and AWS secrets are stored per signed-in Matrix account in Flutter
secure storage. When a capability is enabled, its credentials are sent over
the existing authenticated TLS Native Agent request for that turn or direct
capability action.

Message Server:

- does not persist or return these credentials;
- does not include them in Agent config, traces, errors, logs, subprocess
  environments, MCP environments, or tool schemas;
- rebuilds credential-bound tools for each request; and
- discards the request credential object after the action or turn completes.

The transient request shape is:

```json
{
  "tool_credentials": {
    "web_search": {
      "enabled": true,
      "provider": "tavily",
      "api_key": "<request-scoped>"
    },
    "aws": {
      "enabled": true,
      "access_key_id": "<request-scoped>",
      "secret_access_key": "<request-scoped>",
      "session_token": "<optional request-scoped>",
      "region": "us-east-1"
    }
  }
}
```

Persisted Native Agent config may contain non-secret capability preferences,
but all credential fields and the complete `tool_credentials` object are
removed by the backend config sanitizer.

### Web Search

The first provider is Tavily behind a provider interface. V1 uses the fixed
`https://api.tavily.com/search` endpoint; the client cannot supply an arbitrary
endpoint.

When enabled and configured, the turn receives one `web_search` tool. It:

- accepts one focused query;
- caps query length and result count;
- applies a bounded HTTP timeout and response-body limit;
- returns concise title, URL, and content snippets; and
- never exposes the provider key in model-visible arguments or results.

Agent Settings includes an enable switch, provider field, obscured API-key
field, configured/not-configured state, and Test Connection action. The test
uses a request-scoped `agent.web_search.test` action and does not update server
config.

### AWS Tools

AWS integration uses the AWS SDK rather than an inherited AWS CLI environment.
Request-scoped static credentials and region construct a client only for the
current action or turn.

Configured turns expose:

- `aws_account_identity`
- `aws_ec2_instances_list`
- `aws_ec2_instance_create`
- `aws_ec2_instance_terminate`

Read tools execute immediately. Create and terminate tools only create an
approval plan on their first call.

V1 creates one EC2 instance per approval. The plan contains explicit region,
instance type, image selection, storage and networking inputs, and purpose.
Amazon Linux 2023 may be selected through its public AWS image alias; other
images require an explicit AMI ID. Missing or invalid prerequisites produce a
model-visible validation result without creating an approval.

Instances created through this feature are tagged as Dirextalk Native
Agent-managed resources. Termination is restricted to instances with that
management tag. Existing untagged customer infrastructure cannot be terminated
through these tools.

Agent Settings includes an enable switch, obscured Access Key ID and Secret
Access Key fields, optional Session Token, region, configured/not-configured
state, and Test Credentials action. Testing calls STS `GetCallerIdentity` and
returns only the account ID and ARN.

## Approval State Machine

AWS mutations use a server-owned two-phase workflow:

```text
model proposes mutation
  -> Message Server validates and stores a non-secret pending plan
  -> stream emits approval_required
  -> Flutter renders Confirm / Cancel card
  -> owner taps Confirm
  -> Flutter calls agent.aws.approvals.execute with credentials again
  -> Message Server verifies owner, conversation, plan digest, expiry, and use
  -> AWS SDK executes exactly the approved plan
  -> stream/UI receives the sanitized execution result
```

Pending plans:

- contain no AWS credentials;
- live in the Native Agent runtime's in-memory approval store;
- expire after ten minutes;
- are bound to the current Native Agent conversation;
- have a random opaque ID and canonical plan digest; and
- become terminal after execute, cancel, or expiry.

A server restart invalidates pending plans and performs no mutation. This is a
safe failure mode.

The model never receives an `approved` boolean it can set. Only the
owner-authenticated Flutter action can execute an approval. Flutter must not
infer approval from text such as "yes" or "confirm".

EC2 creation uses the approval identity as the AWS `ClientToken` so transport
retries cannot create duplicate instances. Termination revalidates the
Dirextalk management tag immediately before execution.

## Flutter Data Flow

A dedicated `NativeAgentToolCredentialsStore` uses
`FlutterSecureStorage`. Storage keys are scoped by normalized homeserver and
Matrix user ID so two accounts on one device cannot share credentials.

The Agent Settings save action writes secrets only to that store. Non-secret
enable/provider/region preferences may be represented in the same encrypted
payload to keep the complete capability profile account-scoped.

Native Agent chat loads the local profile before each turn and adds
`tool_credentials` to `agent.chat.stream`. Test and approval actions load the
same profile and send only the credential subset required for that action.

An `approval_required` stream event becomes a stable, typed chat card. Confirm
and Cancel buttons call dedicated Native Agent actions directly; they do not
send another natural-language model turn.

## Backend Boundaries

The Native Agent runtime owns provider clients, request parsing, tool
registration, pending approvals, and sanitized results. Existing ProductCore
and MCP tools remain unchanged.

New direct Native Agent actions are:

- `agent.web_search.test`
- `agent.aws.credentials.test`
- `agent.aws.approvals.execute`
- `agent.aws.approvals.cancel`

All remain owner-only through the existing `agent.*` action boundary.

The existing Agent config sanitizers are extended defensively to remove:

- `tool_credentials`
- web-search API keys and key references
- AWS access key IDs, secret keys, session tokens, and credential references
- nested variants of those fields in capability profiles

HTTP/WebSocket diagnostic redaction recognizes the same key names.

## Error Handling

- Missing credentials omit the corresponding model tool and produce a clear
  setup-needed result for explicit test actions.
- Invalid Tavily or AWS credentials produce sanitized provider errors.
- Provider responses are size-limited and time-bounded.
- Expired, cancelled, consumed, mismatched, or unknown approval IDs fail
  closed without calling AWS.
- AWS throttling and transient failures may be retried only with the same
  idempotency identity.
- No error includes request headers, raw provider bodies that may echo
  credentials, or AWS secret material.

## Verification

Message Server tests use fake provider and AWS client interfaces; they never
call real cloud services. Coverage proves:

- Tavily results become a `web_search` tool result;
- unconfigured tools are absent;
- config, traces, errors, and subprocess environments contain no secrets;
- read-only AWS tools may execute without approval;
- create and terminate cannot execute before owner approval;
- approval binding, expiry, cancellation, single use, and idempotency work;
- only Dirextalk-managed instances may be terminated; and
- Native Agent actions remain owner-only.

Flutter tests use an in-memory credential store and recorded AsClient. Coverage
proves:

- Agent Settings saves and reloads account-scoped profiles;
- secrets are obscured and never included in `agent.config.update`;
- chat, test, and approval requests include only the required transient
  credential fields;
- approval cards render concise operation details; and
- Confirm and Cancel invoke direct approval actions exactly once.

Focused verification commands:

```text
go test ./p2p/nativeagent ./p2p/internal/agent ./p2p -count=1
go build ./cmd/dirextalk-message-server
flutter test --no-pub test/agent_pages_test.dart
flutter analyze --no-pub
git diff --check
```

## Deferred Work

- Additional search providers
- Server-side encrypted credential synchronization
- Arbitrary AWS CLI access
- Mutating untagged AWS resources
- Multi-instance launch and Auto Scaling Group administration
- Cost estimation and budget enforcement
