package main

import "testing"

func TestRuntimeConfigKeepsDeploymentCreateBehindExactExplicitGate(t *testing.T) {
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_SECRETS_ENABLED", "true")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("service secrets enabled without isolated AWS resources")
	}
	t.Setenv("DIREXTALK_SERVICE_SECRET_SESSIONS_TABLE", "service-secret-sessions")
	t.Setenv("DIREXTALK_SERVICE_SECRET_KMS_KEY_ID", "alias/dirextalk-service-secrets")
	if config, err := runtimeConfigFromEnvironment(); err != nil || !config.serviceSecretsEnabled {
		t.Fatalf("service secret gate=%#v err=%v", config, err)
	}
	for _, invalid := range []string{"", "TRUE", "1"} {
		setValidRuntimeEnvironment(t)
		t.Setenv("DIREXTALK_SERVICE_SECRETS_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("service secret gate %q accepted", invalid)
		}
	}

	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_DEPLOYMENT_DESTROY_ENABLED", "true")
	config, err := runtimeConfigFromEnvironment()
	if err != nil || !config.deploymentDestroyEnabled {
		t.Fatalf("destroy true gate config=(%#v,%v)", config, err)
	}
	for _, invalid := range []string{"", "TRUE", "1"} {
		setValidRuntimeEnvironment(t)
		t.Setenv("DIREXTALK_DEPLOYMENT_DESTROY_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("destroy gate %q unexpectedly accepted", invalid)
		}
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_RESTORE_ENABLED", "true")
	config, err = runtimeConfigFromEnvironment()
	if err != nil || !config.serviceRestoreEnabled {
		t.Fatalf("restore true gate config=(%#v,%v)", config, err)
	}
	for _, invalid := range []string{"", "TRUE", "1"} {
		setValidRuntimeEnvironment(t)
		t.Setenv("DIREXTALK_SERVICE_RESTORE_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("restore gate %q unexpectedly accepted", invalid)
		}
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_RESTORES_TABLE", "receipts")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("service restore table unexpectedly shared the receipt table")
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_DEPLOYMENT_DESTROY_TABLE", "receipts")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("deployment destroy table unexpectedly shared the receipt table")
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_BACKUP_ENABLED", "true")
	config, err = runtimeConfigFromEnvironment()
	if err != nil || !config.serviceBackupEnabled {
		t.Fatalf("backup true gate config=(%#v,%v)", config, err)
	}
	for _, invalid := range []string{"", "TRUE", "1"} {
		setValidRuntimeEnvironment(t)
		t.Setenv("DIREXTALK_SERVICE_BACKUP_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("backup gate %q unexpectedly accepted", invalid)
		}
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_BACKUPS_TABLE", "receipts")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("service backup table unexpectedly shared the receipt table")
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_RESTORE_PLAN_ENABLED", "true")
	config, err = runtimeConfigFromEnvironment()
	if err != nil || !config.serviceRestorePlanEnabled {
		t.Fatalf("restore plan true gate config=(%#v,%v)", config, err)
	}
	for _, invalid := range []string{"", "TRUE", "1"} {
		setValidRuntimeEnvironment(t)
		t.Setenv("DIREXTALK_SERVICE_RESTORE_PLAN_ENABLED", invalid)
		if _, err := runtimeConfigFromEnvironment(); err == nil {
			t.Fatalf("restore plan gate %q unexpectedly accepted", invalid)
		}
	}

	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", "false")
	config, err = runtimeConfigFromEnvironment()
	if err != nil || config.deploymentEnabled {
		t.Fatalf("false gate config=(%#v,%v)", config, err)
	}

	t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", "true")
	t.Setenv("DIREXTALK_WORKER_IDENTITY_RSA_PUBLIC_KEY_PEM", "test-public-key")
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

	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_DEPLOYMENT_CREATE_ENABLED", "false")
	t.Setenv("DIREXTALK_WORKER_TASKS_TABLE", "receipts")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("worker task table unexpectedly shared the command receipt table")
	}
	setValidRuntimeEnvironment(t)
	t.Setenv("DIREXTALK_SERVICE_READINESS_TASKS_TABLE", "worker-tasks")
	if _, err := runtimeConfigFromEnvironment(); err == nil {
		t.Fatal("service readiness task table unexpectedly shared the worker task table")
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
		"DIREXTALK_WORKER_BOOTSTRAP_ENDPOINT":           "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/worker-sessions",
		"DIREXTALK_WORKER_IDENTITY_RSA_PUBLIC_KEY_PEM":  "",
		"DIREXTALK_COMMAND_RECEIPTS_TABLE":              "receipts",
		"DIREXTALK_CONNECTION_COUNTERS_TABLE":           "counters",
		"DIREXTALK_ISSUED_QUOTES_TABLE":                 "quotes",
		"DIREXTALK_DEPLOYMENT_RESERVATIONS_TABLE":       "deployments",
		"DIREXTALK_DEPLOYMENT_DESTROY_TABLE":            "deployment-destroys",
		"DIREXTALK_SERVICE_BACKUPS_TABLE":               "service-backups",
		"DIREXTALK_SERVICE_RESTORES_TABLE":              "service-restores",
		"DIREXTALK_APPROVAL_USES_TABLE":                 "approval-uses",
		"DIREXTALK_WORKER_SESSIONS_TABLE":               "worker-sessions",
		"DIREXTALK_WORKER_TASKS_TABLE":                  "worker-tasks",
		"DIREXTALK_SERVICE_READINESS_TASKS_TABLE":       "service-readiness-tasks",
		"DIREXTALK_DEPLOYMENT_CREATE_ENABLED":           "false",
		"DIREXTALK_DEPLOYMENT_DESTROY_ENABLED":          "false",
		"DIREXTALK_SERVICE_BACKUP_ENABLED":              "false",
		"DIREXTALK_SERVICE_RESTORE_PLAN_ENABLED":        "false",
		"DIREXTALK_SERVICE_RESTORE_ENABLED":             "false",
		"DIREXTALK_SERVICE_SECRETS_ENABLED":             "false",
		"DIREXTALK_SERVICE_SECRET_SESSIONS_TABLE":       "",
		"DIREXTALK_SERVICE_SECRET_KMS_KEY_ID":           "",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
