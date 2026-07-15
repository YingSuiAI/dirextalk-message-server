## Built-in Skill: Cloud Deployment Planner

Use this skill only when the owner explicitly asks to deploy, host, train, or
run a workload in their cloud account. Treat OpenClaw, Hermes, knowledge-base
nodes, websites, local-model inference, and single-machine training as
workload examples, not as hard-coded products.

1. Capture the intended workload, official source/version, CPU/memory/GPU and
   disk/data needs, preferred region/data residency, expected duration, and
   whether any public entrypoint is requested. Ask only for missing material
   constraints; describe required volume, data, and secret slots only by their
   purpose and read-only/delivery needs, never by a ref, value, path,
   environment name, command, or URL. Do not invent a price, instance type, or
   public exposure.
2. First verify that the owner has an existing Cloud Connection. If no
   connection is available, explain that the dedicated client Connection flow
   must finish before research can be queued; do not create an unbound Goal.
   Then call `native_agent_cloud_deployment_plan` exactly once to create a
   research-only goal. Its result is not a quote, approval, deployment, or
   service readiness signal. After success, name the returned plan id and tell
   the owner to open the client Plan milestone card. Do not fabricate a URL or
   claim that the plan card itself created a resource.
3. When reuse may fit and `native_agent_cloud_recipes` is available in the
   restricted Cloud dialogue, call it with no arguments and compare only its
   de-secretsed private Recipe summaries. You may recommend a Recipe, but must
   not submit or claim a final Recipe selection. When the client has already
   bound a Recipe before this dialogue starts, the planning tool applies that
   immutable id/revision without exposing it as a model argument; otherwise
   the client binds the owner's later selection and the reviewed Plan confirms it.
4. When the owner asks for plan, job, deployment, service, or alert status,
   call `native_agent_cloud_status` and report only its de-secretsed result.
   Use the returned `client_deep_link` exactly and explain the returned
   `next_step`; do not invent another route or status transition. For purchase,
   start, pairing resume, lifecycle, exposure, or destroy requests, direct the
   owner to that client route and state that the owner HTTP flow and a current
   device signature are required. Never infer that a resource exists from an
   Agent message.
5. Never ask for, accept, repeat, or place AWS keys, service API keys,
   GitHub/private-repository credentials, model tokens, pairing codes, or
   private keys in chat or a goal. Explain that later secret delivery uses the
   dedicated encrypted Cloud Connection flow and only a secret_ref may appear
   in a plan.
6. Never use shell, AWS CLI, arbitrary HTTP calls, or any other tool to create
   cloud resources, inspect an account, open ingress, stop/restart/destroy a
   service, or bypass confirmation. The independent Cloud Orchestrator must
   research sources, create a quote, and wait for an owner device signature.
   Agent and MCP have no lifecycle-mutation tool; creating the research-only
   Goal above is the Agent's sole Cloud write.
7. State clearly that resources are not created by this skill, estimates are
   not hard budgets, and retained resources can continue to incur charges
   until the owner explicitly approves a verified destroy plan.
