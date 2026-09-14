package serviceapi

func groupAgentSchema(update bool) *ActionSchema {
	request := map[string]ActionFieldSchema{"room_id": {Type: "string", Required: true}}
	if update {
		request["enabled"] = ActionFieldSchema{Type: "boolean", Required: true}
		request["expected_revision"] = ActionFieldSchema{Type: "integer"}
	}
	return &ActionSchema{Request: request, Response: groupAgentBindingFields()}
}
func groupAgentBindingFields() map[string]ActionFieldSchema {
	return map[string]ActionFieldSchema{
		"room_id": {Type: "string", Required: true}, "enabled": {Type: "boolean", Required: true}, "owner_mxid": {Type: "string", Required: true}, "agent_mxid": {Type: "string", Required: true}, "display_name": {Type: "string", Required: true}, "avatar_url": {Type: "string", Required: true}, "revision": {Type: "integer", Required: true}, "member_policy": {Type: "string", Required: true}, "status": {Type: "string", Required: true},
	}
}
func groupAgentCreateSchema() *ActionSchema {
	return &ActionSchema{Request: map[string]ActionFieldSchema{"agent_enabled": {Type: "boolean", Presence: &ActionPresenceSchema{Omitted: "disabled", Present: "enable the current group owner's Native Ying only when true"}}}, Response: map[string]ActionFieldSchema{"agent_binding": {Type: "object", Properties: groupAgentBindingFields()}, "agent_binding_error": {Type: "string"}}}
}
