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
		"DeploymentDestroyTable:",
		"ServiceBackupsTable:",
		"ServiceRestoresTable:",
		"ApprovalUsesTable:",
		"WorkerSessionsTable:",
		"WorkerTasksTable:",
		"ServiceReadinessTasksTable:",
		"DIREXTALK_DEPLOYMENT_RESERVATIONS_TABLE",
		"DIREXTALK_DEPLOYMENT_DESTROY_TABLE",
		"DIREXTALK_SERVICE_BACKUPS_TABLE",
		"DIREXTALK_SERVICE_RESTORES_TABLE",
		"DIREXTALK_APPROVAL_USES_TABLE",
		"DIREXTALK_WORKER_SESSIONS_TABLE",
		"DIREXTALK_WORKER_TASKS_TABLE",
		"DIREXTALK_SERVICE_READINESS_TASKS_TABLE",
		"DIREXTALK_WORKER_BOOTSTRAP_ENDPOINT",
		"DIREXTALK_WORKER_IDENTITY_RSA_PUBLIC_KEY_PEM",
		"WorkerIdentityRsaPublicKeyPem:",
		"DeploymentCreateRequiresWorkerIdentityKey:",
		"Worker bootstrap identity verification key must be configured when",
		"TimeToLiveSpecification:",
		"AttributeName: ttl_epoch_seconds",
		"POST /v2/worker-sessions/{session_id}/claim",
		"/POST/v2/worker-sessions/*/claim",
		"POST /v2/worker-sessions/{session_id}/tasks/claim",
		"POST /v2/worker-sessions/{session_id}/tasks/{task_id}/events",
		"POST /v2/worker-sessions/{session_id}/events",
		"/POST/v2/worker-sessions/*/tasks/claim",
		"/POST/v2/worker-sessions/*/tasks/*/events",
		"/POST/v2/worker-sessions/*/events",
		"POST /v2/worker-sessions/{session_id}/recipe-tasks/claim",
		"POST /v2/worker-sessions/{session_id}/recipe-tasks/{task_id}/events",
		"/POST/v2/worker-sessions/*/recipe-tasks/claim",
		"/POST/v2/worker-sessions/*/recipe-tasks/*/events",
		"AccessTypedWorkerTasksOnly",
		"!If [DeploymentCreateEnabled, !GetAtt WorkerSessionsTable.Arn, !Ref \"AWS::NoValue\"]",
		"WorkerBootstrapEndpoint:",
		"WorkerSecurityGroup:",
		"DIREXTALK_WORKER_SECURITY_GROUP_ID",
		"SecurityGroupEgress:",
		"EnableDeploymentCreate:",
		"Default: \"false\"",
		"DeploymentCreateEnabled:",
		"DIREXTALK_DEPLOYMENT_CREATE_ENABLED",
		"EnableDeploymentDestroy:",
		"DeploymentDestroyEnabled:",
		"DIREXTALK_DEPLOYMENT_DESTROY_ENABLED",
		"EnableServiceBackup:",
		"ServiceBackupEnabled:",
		"DIREXTALK_SERVICE_BACKUP_ENABLED",
		"EnableServiceRestorePlan:",
		"ServiceRestorePlanEnabled:",
		"DIREXTALK_SERVICE_RESTORE_PLAN_ENABLED",
		"EnableServiceRestore:",
		"ServiceRestoreEnabled:",
		"DIREXTALK_SERVICE_RESTORE_ENABLED",
		"EnableServiceSecrets:",
		"ServiceSecretsEnabled:",
		"ServiceSecretDestroyEnabled:",
		"DIREXTALK_SERVICE_SECRETS_ENABLED",
		"ServiceSecretSessionsTable:",
		"ServiceSecretSealingKey:",
		"ServiceSecretSealingKeyAlias:",
		"DIREXTALK_SERVICE_SECRET_SESSIONS_TABLE",
		"DIREXTALK_SERVICE_SECRET_KMS_KEY_ID",
		"POST /v2/service-secret-sessions",
		"PUT /v2/service-secret-sessions/{session_id}/encrypted-upload",
		"POST /v2/service-secret-sessions/{session_id}/complete",
		"PlanInPlaceServiceRestoreReadOnly",
		"CreateReplacementFromTrackedBackupSnapshotsOnly",
		"CreateTaggedReplacementVolumesOnly",
		"SwapTrackedDeploymentVolumesOnly",
		"MarkReplacementFallbackOnly",
		"ec2:CreateImage",
		"ec2:DescribeImages",
		"ec2:DescribeSnapshots",
		"ec2:RunInstances",
		"ec2:CreateTags",
		"ec2:DescribeInstances",
		"ec2:DescribeVolumes",
		"ec2:DescribeNetworkInterfaces",
		"ec2:TerminateInstances",
		"ec2:DeleteNetworkInterface",
		"ec2:DeleteVolume",
		"ec2:ResourceTag/dirextalk:managed",
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
		"ec2:CreateSecurityGroup",
		"ec2:AuthorizeSecurityGroupIngress",
		"SecurityGroupIngress:",
		"dynamodb:Scan",
		"WorkerTaskEventsTable",
		"DIREXTALK_WORKER_TASK_EVENTS_TABLE",
		"RecipeTasksTable",
		"DIREXTALK_RECIPE_TASKS_TABLE",
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
		`WorkerTasksTable:
    Type: AWS::DynamoDB::Table
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      BillingMode: PAY_PER_REQUEST
      DeletionProtectionEnabled: true
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      SSESpecification:
        SSEEnabled: true
      AttributeDefinitions:
        - AttributeName: deployment_id
          AttributeType: S
        - AttributeName: task_id
          AttributeType: S
      KeySchema:
        - AttributeName: deployment_id
          KeyType: HASH
        - AttributeName: task_id
          KeyType: RANGE`,
		"BrokerWorkerTaskClaimRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerTaskEventRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerSessionEventRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerTaskClaimInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerTaskEventInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerWorkerSessionEventInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerRecipeTaskClaimRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerRecipeTaskEventRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerRecipeTaskClaimInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerRecipeTaskEventInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerServiceReadinessClaimRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerServiceReadinessEventRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: DeploymentCreateEnabled",
		"BrokerServiceReadinessClaimInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"BrokerServiceReadinessEventInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: DeploymentCreateEnabled",
		"DeploymentDestroyTable:\n    Type: AWS::DynamoDB::Table\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"- DeploymentDestroyEnabled\n                - Sid: DestroyTrackedWorkersOnly",
		"- DeploymentDestroyEnabled\n                - Sid: ObserveDestroyedWorkersOnly",
		"ServiceBackupsTable:\n    Type: AWS::DynamoDB::Table\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"- ServiceBackupEnabled\n                - Sid: ImageTrackedWorkerInstanceOnly",
		"- ServiceBackupEnabled\n                - Sid: CreateTaggedServiceBackupImageAndSnapshotsOnly",
		"- ServiceBackupEnabled\n                - Sid: ObserveServiceBackupImageAndSnapshotsOnly",
		"- ServiceRestorePlanEnabled\n                - Sid: PlanInPlaceServiceRestoreReadOnly",
		"ServiceRestoresTable:\n    Type: AWS::DynamoDB::Table\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"- ServiceRestoreEnabled\n                - Sid: CreateReplacementFromTrackedBackupSnapshotsOnly",
		"- ServiceRestoreEnabled\n                - Sid: CreateTaggedReplacementVolumesOnly",
		"- ServiceRestoreEnabled\n                - Sid: SwapTrackedDeploymentVolumesOnly",
		"- ServiceRestoreEnabled\n                - Sid: MarkReplacementFallbackOnly",
		"ServiceSecretSessionsTable:\n    Type: AWS::DynamoDB::Table\n    Condition: ServiceSecretsEnabled\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"ServiceSecretSealingKey:\n    Type: AWS::KMS::Key\n    Condition: ServiceSecretsEnabled\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"ServiceSecretSealingKeyAlias:\n    Type: AWS::KMS::Alias\n    Condition: ServiceSecretsEnabled\n    DeletionPolicy: Retain\n    UpdateReplacePolicy: Retain",
		"- ServiceSecretsEnabled\n                - Sid: AccessServiceSecretSessionsOnly",
		"- ServiceSecretsEnabled\n                - Sid: QueryCompletedServiceSecretMaterializationOnly",
		"- ServiceSecretsEnabled\n                - Sid: SealServiceSecretSessionMaterialOnly",
		"- ServiceSecretsEnabled\n                - Sid: WriteScopedServiceSecretVersionsOnly",
		"- ServiceSecretDestroyEnabled\n                - Sid: DeleteTrackedDeploymentServiceSecretsOnly",
		"- ServiceSecretDestroyEnabled\n                - Sid: DeleteCompletedServiceSecretBindingsOnly",
		"- ServiceSecretDestroyEnabled\n                - Sid: QueryDeploymentServiceSecretBindingsOnly",
		"BrokerServiceSecretCreateRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: ServiceSecretsEnabled",
		"BrokerServiceSecretUploadRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: ServiceSecretsEnabled",
		"BrokerServiceSecretCompleteRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: ServiceSecretsEnabled",
		"BrokerWorkerServiceSecretRoute:\n    Type: AWS::ApiGatewayV2::Route\n    Condition: ServiceSecretsEnabled",
		"BrokerServiceSecretCreateInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: ServiceSecretsEnabled",
		"BrokerServiceSecretUploadInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: ServiceSecretsEnabled",
		"BrokerServiceSecretCompleteInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: ServiceSecretsEnabled",
		"BrokerWorkerServiceSecretInvokePermission:\n    Type: AWS::Lambda::Permission\n    Condition: ServiceSecretsEnabled",
		"aws:RequestTag/DirextalkBackupId: \"false\"",
		"aws:RequestTag/DirextalkServiceId: \"false\"",
		"aws:RequestTag/DirextalkDeploymentId: \"false\"",
	} {
		if !strings.Contains(template, guardedBoundary) {
			t.Fatalf("template boundary is not fail closed: missing %q", guardedBoundary)
		}
	}
	if strings.Count(template, "- dynamodb:Query") != 3 || !strings.Contains(template, `- !If
                - DeploymentCreateEnabled
                - Sid: AccessTypedWorkerTasksOnly
                  Effect: Allow
                  Action:
                    - dynamodb:GetItem
                    - dynamodb:Query
                    - dynamodb:TransactWriteItems
                  Resource:
                    - !GetAtt WorkerTasksTable.Arn
                    - !GetAtt ServiceReadinessTasksTable.Arn
                - !Ref "AWS::NoValue"`) {
		t.Fatal("template does not scope Query and task mutation permissions to typed task tables")
	}
	if !strings.Contains(template, `ServiceReadinessTasksTable:
    Type: AWS::DynamoDB::Table
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      BillingMode: PAY_PER_REQUEST
      DeletionProtectionEnabled: true
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      SSESpecification:
        SSEEnabled: true`) {
		t.Fatal("service readiness tasks are not retained, encrypted, deletion-protected, and point-in-time recoverable")
	}
	if !strings.Contains(template, `EnableServiceSecrets:
    Type: String
    Default: "false"
    AllowedValues:
      - "false"
      - "true"`) {
		t.Fatal("service secrets must remain disabled by default")
	}
	if strings.Count(template, "- secretsmanager:") != 5 ||
		!strings.Contains(template, `Resource: !Sub "arn:${AWS::Partition}:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:dirextalk/${ConnectionId}/*"`) {
		t.Fatal("service-secret provider IAM is not limited to the connection-scoped Secrets Manager namespace")
	}
	if !strings.Contains(template, `ServiceSecretSessionsTable:
    Type: AWS::DynamoDB::Table
    Condition: ServiceSecretsEnabled
    DeletionPolicy: Retain
    UpdateReplacePolicy: Retain
    Properties:
      BillingMode: PAY_PER_REQUEST
      DeletionProtectionEnabled: true
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      SSESpecification:
        SSEEnabled: true
        SSEType: KMS
        KMSMasterKeyId: !Ref ServiceSecretSealingKey
      TimeToLiveSpecification:
        AttributeName: ttl_epoch_seconds
        Enabled: true`) {
		t.Fatal("service-secret sessions are not retained, CMK-encrypted, deletion-protected, point-in-time recoverable, and TTL-enabled")
	}
	if !strings.Contains(template, `IndexName: materialization-key-index`) || !strings.Contains(template, `ProjectionType: KEYS_ONLY`) || !strings.Contains(template, "POST /v2/worker-sessions/{session_id}/service-secrets/materialize") || !strings.Contains(template, "- secretsmanager:GetSecretValue") {
		t.Fatal("worker secret materialization route/index/read permission missing")
	}
	if !strings.Contains(template, `ServiceSecretDestroyEnabled: !And [!Condition DeploymentDestroyEnabled, !Condition ServiceSecretsEnabled]`) || !strings.Contains(template, `IndexName: deployment-key-index`) || !strings.Contains(template, `Resource: !Sub "${ServiceSecretSessionsTable.Arn}/index/deployment-key-index"`) || !strings.Contains(template, "- secretsmanager:DeleteSecret") || !strings.Contains(template, "- secretsmanager:DescribeSecret") || !strings.Contains(template, "- dynamodb:DeleteItem") {
		t.Fatal("service-secret destroy is not gated by both flags or lacks bounded read-back/delete IAM")
	}
	if !strings.Contains(template, `Resource: !Sub "${ServiceSecretSessionsTable.Arn}/index/materialization-key-index"`) {
		t.Fatal("materialization Query IAM does not target the exact GSI ARN")
	}
	if !strings.Contains(template, `KeySpec: SYMMETRIC_DEFAULT
      KeyUsage: ENCRYPT_DECRYPT`) || strings.Count(template, "- kms:Encrypt") != 1 || strings.Count(template, "- kms:Decrypt") != 1 || strings.Count(template, "- kms:GenerateDataKey") != 1 {
		t.Fatal("service-secret sealing and Secrets Manager encryption must use one symmetric key with minimum Broker data-plane access")
	}
	if strings.Count(template, "Type: AWS::IAM::Role") != 1 || strings.Contains(template, "WorkerRole:") || strings.Contains(template, "WorkerInstanceProfile:") {
		t.Fatal("service-secret infrastructure must not introduce a Worker IAM role or instance profile")
	}
}
