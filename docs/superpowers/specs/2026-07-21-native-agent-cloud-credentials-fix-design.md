# Native Agent Cloud Credentials Fix Design

## Problem

The Native Agent settings UI can submit a valid Tavily API key but report a
failed connection. The server currently places the key in the JSON body while
the current Tavily Search API authenticates with an `Authorization: Bearer`
header. The AWS credential form also renders four mostly blank fields because
the shared form control hides labels and the first three fields have no hint
text.

## Scope

This repair is limited to Native Agent cloud-tool configuration:

- update the server-side Tavily HTTP adapter;
- improve safe, actionable Tavily connection errors;
- add concise AWS field hints and a root-key warning in Flutter;
- add focused regression tests; and
- rebuild and deploy only after the local checks pass.

It does not change Agent chat routing, LangGraph/Eino orchestration, credential
storage, AWS approval behavior, or any non-Agent product surface.

## Tavily Request

The message server remains the only component that contacts Tavily. Flutter
sends request-scoped tool credentials to the user's authenticated message
server. The server sends:

```text
POST https://api.tavily.com/search
Authorization: Bearer <TAVILY_API_KEY>
Content-Type: application/json
```

The JSON body contains the query and search options, but never the API key.
The key must not appear in returned errors, logs, results, or persisted Agent
configuration.

Provider errors are mapped to useful categories without exposing response
bodies: invalid/rejected credentials, exhausted or rate-limited quota, and a
generic provider failure. Transport failures remain distinguishable from
provider rejections.

## AWS Form Guidance

The four existing fields keep their current storage and request behavior but
gain visible hints:

- Access Key ID: `AKIA...` or `ASIA...`;
- Secret Access Key: the matching secret;
- Session Token: required only for temporary credentials; and
- Region: for example `us-east-1`.

A short warning tells users to use least-privilege IAM or temporary STS
credentials and never an AWS root access key. The existing rule remains:
read-only actions may run directly, while instance creation and termination
require explicit approval.

## Verification

Server tests must prove that the Tavily adapter uses the Bearer header, omits
the key from the JSON body and returned data, and maps common HTTP failures.
Flutter widget tests must prove that all four AWS fields have meaningful
visible hints and the root-key warning is shown. Existing credential-store,
Agent request-forwarding, AWS approval, static-analysis, build, and diff checks
must continue to pass.

The updated Flutter client alone is insufficient: the deployed message server
must also contain the new `agent.web_search.test` action and Tavily adapter.
