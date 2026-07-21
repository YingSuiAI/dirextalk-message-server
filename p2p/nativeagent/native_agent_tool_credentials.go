package nativeagent

import (
	"fmt"
	"strings"
)

type webSearchCredentials struct {
	Enabled  bool
	Provider string
	APIKey   string
}

type awsCredentials struct {
	Enabled         bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
}

type requestToolCredentials struct {
	WebSearch webSearchCredentials
	AWS       awsCredentials
}

func toolCredentialsFromParams(params map[string]any) requestToolCredentials {
	raw := nestedAnyMap(params["tool_credentials"])
	web := nestedAnyMap(raw["web_search"])
	aws := nestedAnyMap(raw["aws"])
	return requestToolCredentials{
		WebSearch: webSearchCredentials{
			Enabled:  boolParam(web["enabled"]),
			Provider: strings.ToLower(fallbackString(trimString(web["provider"]), "tavily")),
			APIKey:   strings.TrimSpace(trimString(web["api_key"])),
		},
		AWS: awsCredentials{
			Enabled:         boolParam(aws["enabled"]),
			AccessKeyID:     strings.TrimSpace(trimString(aws["access_key_id"])),
			SecretAccessKey: strings.TrimSpace(trimString(aws["secret_access_key"])),
			SessionToken:    strings.TrimSpace(trimString(aws["session_token"])),
			Region:          strings.ToLower(strings.TrimSpace(trimString(aws["region"]))),
		},
	}
}

func (c webSearchCredentials) validate() error {
	if !c.Enabled {
		return fmt.Errorf("web search is disabled")
	}
	if c.Provider != "tavily" {
		return fmt.Errorf("web search provider %q is not supported", c.Provider)
	}
	if c.APIKey == "" {
		return fmt.Errorf("web search API key is required")
	}
	return nil
}

func (c awsCredentials) validate() error {
	if !c.Enabled {
		return fmt.Errorf("AWS tools are disabled")
	}
	if c.AccessKeyID == "" {
		return fmt.Errorf("AWS access key ID is required")
	}
	if c.SecretAccessKey == "" {
		return fmt.Errorf("AWS secret access key is required")
	}
	if c.Region == "" {
		return fmt.Errorf("AWS region is required")
	}
	if !validAWSRegion(c.Region) {
		return fmt.Errorf("AWS region is invalid")
	}
	return nil
}

func validAWSRegion(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 9 || len(value) > 32 {
		return false
	}
	for _, ch := range value {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			continue
		}
		return false
	}
	return strings.Count(value, "-") >= 2
}

func nestedAnyMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	default:
		return map[string]any{}
	}
}
