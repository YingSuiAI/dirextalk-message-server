package main

import "testing"

func TestRuntimeConfigKeepsDeploymentCreateBehindExactExplicitGate(t *testing.T) {
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", "false")
	config, err := runtimeConfigFromEnvironment()
	if err != nil || config.deploymentEnabled {
		t.Fatalf("false gate config=(%#v,%v)", config, err)
	}

	t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", "true")
	config, err = runtimeConfigFromEnvironment()
	if err != nil || !config.deploymentEnabled {
		t.Fatalf("true gate config=(%#v,%v)", config, err)
	}

	for _, invalid := range []string{"", "TRUE", "1"} {
		t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("gate %q unexpectedly accepted", invalid)
		}
	}
}

func setValidRuntimeEnvironment(t *testing.T) {
	t.Helper()
	values := map[string]string{
		"DIREXTALK_CONNECTION_ID":                       "connection-create-0001",
		"DIREXTALK_CONNECTION_GENERATION":               "2",
		"DIREXTALK_NODE_KEY_ID":                         "node-key-1",
		"DIREXTALK_NODE_PUBLIC_KEY_SPKI_B64":            "unused-in-config-parse",
		"DIREXTALK_DEVICE_APPROVAL_KEY_ID":              "device-key-1",
		"DIREXTALK_DEVICE_APPROVAL_PUBLIC_KEY_SPKI_B64": "unused-in-config-parse",
		"DIREXTALK_STACK_ACCOUNT_ID":                    "123456789012",
		"DIREXTALK_STACK_REGION":                        "us-east-1",
		"DIREXTALK_STACK_ARN":                           "arn:aws:cloudformation:us-east-1:123456789012:stack/test/id",
		"DIREXTALK_AWS_URL_SUFFIX":                      "amazonaws.com",
		"DIREXTALK_BROKER_STAGE_NAME":                   "prod",
		"DIREXTALK_WORKER_BASE_AMI_ID":                  "ami-0123456789abcdef0",
		"DIREXTALK_WORKER_RESOURCE_MANIFEST_DIGEST":     "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"DIREXTALK_WORKER_VPC_ID":                       "vpc-0123456789abcdef0",
		"DIREXTALK_WORKER_SUBNET_ID":                    "subnet-0123456789abcdef0",
		"DIREXTALK_WORKER_AVAILABILITY_ZONE":            "us-east-1a",
		"DIREXTALK_WORKER_SECURITY_GROUP_ID":            "sg-0123456789abcdef0",
		"DIREXTALK_COMMAND_RECEIPTS_TABLE":              "receipts",
		"DIREXTALK_CONNECTION_COUNTERS_TABLE":           "counters",
		"DIREXTALK_ISSUED_QUOTES_TABLE":                 "quotes",
		"DIREXTALK_DEPLOYMENT_RESERVATIONS_TABLE":       "deployments",
		"DIREXTALK_APPROVAL_USES_TABLE":                 "approval-uses",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
