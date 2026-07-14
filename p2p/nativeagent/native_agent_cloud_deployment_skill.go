package nativeagent

import _ "embed"

// cloudDeploymentPlannerSkillPrompt is a source-controlled, server-side Eino
// asset. It is deliberately separate from user-managed native_agent_skills_*
// records and from every deployer/release script.
//
//go:embed skills/cloud_deployment_planner/SKILL.md
var cloudDeploymentPlannerSkillPrompt string

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
