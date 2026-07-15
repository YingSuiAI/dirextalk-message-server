package infra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemplateKeepsTypedMutationBehindDisabledByDefaultGate(t *testing.T) {
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
		"DeploymentReservationsTable:",
		"ApprovalUsesTable:",
		"WorkerSessionsTable:",
		"DIREXTALK_DEPLOYMENT_RESERVATIONS_TABLE",
		"DIREXTALK_APPROVAL_USES_TABLE",
		"DIREXTALK_WORKER_SESSIONS_TABLE",
		"DIREXTALK_WORKER_BOOTSTRAP_ENDPOINT",
		"DIREXTALK_WORKER_IDENTITY_RSA_PUBLIC_KEY_PEM",
		"WorkerIdentityRsaPublicKeyPem:",
		"DeploymentCreateRequiresWorkerIdentityKey:",
		"Worker bootstrap identity verification key must be configured when",
		"TimeToLiveSpecification:",
		"AttributeName: ttl_epoch_seconds",
		"POST /v2/worker-sessions/{session_id}/claim",
		"/POST/v2/worker-sessions/*/claim",
		"WorkerBootstrapEndpoint:",
		"WorkerSecurityGroup:",
		"DIREXTALK_WORKER_SECURITY_GROUP_ID",
		"SecurityGroupEgress:",
		"EnableDeploymentCreate:",
		"Default: \"false\"",
		"DeploymentCreateEnabled:",
		"DIREXTALK_DEPLOYMENT_CREATE_ENABLED",
		"ec2:RunInstances",
		"ec2:CreateTags",
		"ec2:DescribeInstances",
		"ec2:DescribeVolumes",
		"aws:RequestTag/dirextalk:managed",
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
		"ec2:CreateSecurityGroup",
		"ec2:AuthorizeSecurityGroupIngress",
		"SecurityGroupIngress:",
		"ec2:TerminateInstances",
		"ec2:StartInstances",
		"ec2:StopInstances",
		"dynamodb:Scan",
		"dynamodb:Query",
		"StageName: \"$default\"",
	} {
		if strings.Contains(strings.ToLower(template), strings.ToLower(forbidden)) {
			t.Fatalf("template unexpectedly grants or depends on %q", forbidden)
		}
	}
	for _, guardedBoundary := range []string{
		"BrokerWorkerClaimRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerClaimInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"WorkerSessionsTable:\n    Type: AWS::DynamoDB::Table\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
	} {
		if !strings.Contains(template, guardedBoundary) {
			t.Fatalf("template boundary is not fail closed: missing %q", guardedBoundary)
		}
	}
}
