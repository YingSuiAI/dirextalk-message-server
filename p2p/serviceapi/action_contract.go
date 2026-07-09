package serviceapi

const ActionContractSchemaVersion = 1

type ActionContractDocument struct {
	SchemaVersion int                  `json:"schema_version"`
	GeneratedFrom string               `json:"generated_from"`
	Actions       []ActionContractSpec `json:"actions"`
}

type ActionContractSpec struct {
	Name      string `json:"name"`
	Auth      string `json:"auth"`
	Transport string `json:"transport"`
}

func ActionContract() ActionContractDocument {
	specs := ActionSpecs()
	actions := make([]ActionContractSpec, 0, len(specs))
	for _, spec := range specs {
		actions = append(actions, ActionContractSpec{
			Name:      spec.Name,
			Auth:      string(spec.Auth),
			Transport: string(spec.Transport),
		})
	}
	return ActionContractDocument{
		SchemaVersion: ActionContractSchemaVersion,
		GeneratedFrom: "p2p/serviceapi.ActionSpecs",
		Actions:       actions,
	}
}
