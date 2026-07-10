package dirextalkdomain

type ClientBuild struct {
	Version     string `json:"client_version"`
	BuildNumber string `json:"build_number,omitempty"`
	Platform    string `json:"platform,omitempty"`
	ReportedAt  string `json:"reported_at,omitempty"`
}
