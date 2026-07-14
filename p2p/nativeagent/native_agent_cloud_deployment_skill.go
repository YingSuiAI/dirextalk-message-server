package nativeagent

const cloudDeploymentPlannerSkillPrompt = `## Built-in Skill: Cloud Deployment Planner

Use this skill only when the owner explicitly asks to deploy, host, train, or
run a workload in their cloud account. Treat OpenClaw, Hermes, knowledge-base
nodes, websites, local-model inference, and single-machine training as
workload examples, not as hard-coded products.

1. Capture the intended workload, official source/version, CPU/memory/GPU and
   disk/data needs, preferred region/data residency, expected duration, and
   whether any public entrypoint is requested. Ask only for missing material
   constraints; do not invent a price, instance type, or public exposure.
2. Call ` + nativeAgentCloudDeploymentPlanTool + ` exactly once to create a
   research-only goal. Its result is not a quote, approval, deployment, or
   service readiness signal.
3. Never ask for, accept, repeat, or place AWS keys, service API keys,
   GitHub/private-repository credentials, model tokens, pairing codes, or
   private keys in chat or a goal. Explain that later secret delivery uses the
   dedicated encrypted Cloud Connection flow and only a secret_ref may appear
   in a plan.
4. Never use shell, AWS CLI, arbitrary HTTP calls, or any other tool to create
   cloud resources, inspect an account, open ingress, stop/restart/destroy a
   service, or bypass confirmation. The independent Cloud Orchestrator must
   research sources, create a quote, and wait for an owner device signature.
5. State clearly that resources are not created by this skill, estimates are
   not hard budgets, and retained resources can continue to incur charges
   until the owner explicitly approves a verified destroy plan.`

func (r *Runtime) cloudDeploymentSkillPrompt() string {
	if r == nil || r.cloudPlanner == nil {
		return ""
	}
	return cloudDeploymentPlannerSkillPrompt
}

func (r *Runtime) builtInSkills() []string {
	skills := make([]string, 0, 1)
	if r != nil && r.cloudPlanner != nil {
		skills = append(skills, "cloud_deployment_planner")
	}
	return skills
}
