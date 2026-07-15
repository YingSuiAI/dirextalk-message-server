package infra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemplateEnablesOnlyReadOnlyQuoteAndDurableCommandFences(t *testing.T) {
	path := filepath.Join("..", "..", "infra", "template.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	template := string(raw)
	for _, required := range []string{
		"Runtime: provided.al2023",
		"Handler: bootstrap",
		"POST /v2/commands",
		"ConnectionId:",
		"ConnectionGeneration:",
		"NodeKeyId:",
		"NodePublicKeySpkiBase64:",
		"DeviceApprovalKeyId:",
		"DeviceApprovalPublicKeySpkiBase64:",
		"AllowedValues:\n      - prod",
		"StageName: !Ref StageName",
		"BrokerCommandUrl:",
		"DIREXTALK_NODE_PUBLIC_KEY_SPKI_B64",
		"BrokerArtifactBucket",
		"AWS::DynamoDB::Table",
		"dynamodb:GetItem",
		"dynamodb:TransactWriteItems",
		"ec2:DescribeInstanceTypeOfferings",
		"ec2:DescribeInstanceTypes",
		"pricing:GetProducts",
		"DeletionProtectionEnabled: true",
		"PointInTimeRecoveryEnabled: true",
	} {
		if !strings.Contains(template, required) {
			t.Fatalf("template is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"AWS::Serverless::",
		"nodejs",
		"iam:PassRole",
		"secretsmanager:",
		"ec2:RunInstances",
		"ec2:CreateSecurityGroup",
		"ec2:AuthorizeSecurityGroupIngress",
		"ec2:TerminateInstances",
		"ec2:StartInstances",
		"ec2:StopInstances",
		"dynamodb:Scan",
		"dynamodb:Query",
		"/v2/worker-sessions",
		"StageName: \"$default\"",
	} {
		if strings.Contains(strings.ToLower(template), strings.ToLower(forbidden)) {
			t.Fatalf("template unexpectedly grants or depends on %q", forbidden)
		}
	}
}
