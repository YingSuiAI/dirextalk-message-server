package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/pricing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/lambdaadapter"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/provider"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func main() {
	broker, err := productionBroker(context.Background())
	if err != nil {
		// Configuration and AWS errors are deliberately not logged: Lambda still
		// starts behind a fail-closed endpoint without exposing environment data.
		log.Printf("connection stack broker configuration is unavailable")
		broker = api.Broker{}
	}
	lambda.Start(lambdaadapter.New(broker).Handle)
}

func productionBroker(ctx context.Context) (api.Broker, error) {
	config, err := runtimeConfigFromEnvironment()
	if err != nil {
		return api.Broker{}, err
	}
	approvalResolver, err := api.NewStaticApprovalKeyResolver(config.connectionID, config.deviceApprovalKeyID, config.deviceApprovalPublicKeySPKIBase64)
	if err != nil {
		return api.Broker{}, err
	}
	resolver, err := api.NewStaticKeyResolver(
		config.connectionID, config.nodeKeyID, config.nodePublicKeySPKIBase64, config.connectionGeneration,
	)
	if err != nil || resolver == nil {
		return api.Broker{}, errors.New("invalid node registration")
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.region))
	if err != nil {
		return api.Broker{}, errors.New("AWS runtime unavailable")
	}
	dynamoClient := dynamodb.NewFromConfig(awsConfig)
	repository, err := commandstore.NewDynamoRepository(commandstore.DynamoConfig{
		Client: dynamoClient, ReceiptsTable: config.receiptsTable,
		CountersTable: config.countersTable, IssuedQuotesTable: config.issuedQuotesTable, DeploymentReservationsTable: config.deploymentReservationsTable, ApprovalUsesTable: config.approvalUsesTable, WorkerSessionsTable: config.workerSessionsTable,
	})
	if err != nil {
		return api.Broker{}, err
	}
	workerTasks, err := commandstore.NewDynamoWorkerTaskStore(dynamoClient, config.receiptsTable, config.countersTable,
		config.workerSessionsTable, config.workerTasksTable)
	if err != nil {
		return api.Broker{}, err
	}
	serviceReadiness, err := commandstore.NewDynamoServiceReadinessStore(dynamoClient, config.receiptsTable, config.countersTable,
		config.workerSessionsTable, config.serviceReadinessTasksTable)
	if err != nil {
		return api.Broker{}, err
	}
	registration, err := provider.NewRegistrationAttestor(provider.RegistrationConfig{
		ConnectionID: config.connectionID, ConnectionGeneration: config.connectionGeneration, NodeKeyID: config.nodeKeyID,
		AccountID: config.accountID, Region: config.region, StackARN: config.stackARN, URLSuffix: config.urlSuffix,
		StageName: config.stageName, WorkerArtifact: contract.WorkerArtifactReference{Kind: "fixed_ami", AMIID: config.workerAMIID},
		WorkerNetwork:                contract.WorkerNetworkReference{VPCID: config.workerVPCID, SubnetID: config.workerSubnetID, AvailabilityZone: config.workerAvailabilityZone},
		WorkerResourceManifestDigest: config.workerResourceManifestDigest,
	})
	if err != nil {
		return api.Broker{}, err
	}
	pricingConfig := awsConfig.Copy()
	pricingConfig.Region = pricingRegion(config.region)
	quote, err := provider.NewOnDemandQuoteProvider(ec2.NewFromConfig(awsConfig), pricing.NewFromConfig(pricingConfig))
	if err != nil {
		return api.Broker{}, err
	}
	var deploymentProvider api.DeploymentProvider
	var workerIdentity api.WorkerIdentityVerifier
	if config.deploymentEnabled {
		deploymentProvider, err = provider.NewEC2DeploymentProvider(ec2.NewFromConfig(awsConfig))
		if err != nil {
			return api.Broker{}, err
		}
		workerIdentity, err = provider.NewAWSWorkerIdentityVerifier(ec2.NewFromConfig(awsConfig), config.accountID, config.region, []byte(config.workerIdentityRSAPublicKeyPEM))
		if err != nil {
			return api.Broker{}, err
		}
	}
	return api.Broker{
		Resolver: resolver, Store: repository, Registration: registration, Quote: quote,
		DeploymentEnabled: config.deploymentEnabled, ApprovalResolver: approvalResolver,
		DeploymentStore: repository, DeploymentProvider: deploymentProvider,
		DeploymentBoundary: api.DeploymentBoundary{
			WorkerArtifact: contract.WorkerArtifactReference{Kind: "fixed_ami", AMIID: config.workerAMIID},
			WorkerNetwork: contract.WorkerNetworkReference{
				VPCID: config.workerVPCID, SubnetID: config.workerSubnetID, AvailabilityZone: config.workerAvailabilityZone,
			},
			WorkerResourceManifestDigest: config.workerResourceManifestDigest,
			WorkerSecurityGroupID:        config.workerSecurityGroupID,
			WorkerBootstrapEndpoint:      config.workerBootstrapEndpoint,
		},
		WorkerIdentity: workerIdentity, WorkerTokens: api.CryptoWorkerTokenGenerator{},
		WorkerTasks: workerTasks, RecipeTasks: workerTasks, ServiceReadiness: serviceReadiness, WorkerSessionEvents: repository,
	}, nil
}

type runtimeConfig struct {
	connectionID, nodeKeyID, nodePublicKeySPKIBase64                                                                  string
	deviceApprovalKeyID, deviceApprovalPublicKeySPKIBase64                                                            string
	accountID, region, stackARN, urlSuffix, stageName                                                                 string
	workerAMIID, workerVPCID, workerSubnetID, workerAvailabilityZone, workerSecurityGroupID                           string
	workerResourceManifestDigest, workerBootstrapEndpoint, workerIdentityRSAPublicKeyPEM                              string
	receiptsTable, countersTable, issuedQuotesTable                                                                   string
	deploymentReservationsTable, approvalUsesTable, workerSessionsTable, workerTasksTable, serviceReadinessTasksTable string
	connectionGeneration                                                                                              int64
	deploymentEnabled                                                                                                 bool
}

func runtimeConfigFromEnvironment() (runtimeConfig, error) {
	config := runtimeConfig{
		connectionID: requiredEnvironment("DIREXTALK_CONNECTION_ID"), nodeKeyID: requiredEnvironment("DIREXTALK_NODE_KEY_ID"),
		nodePublicKeySPKIBase64: requiredEnvironment("DIREXTALK_NODE_PUBLIC_KEY_SPKI_B64"),
		deviceApprovalKeyID:     requiredEnvironment("DIREXTALK_DEVICE_APPROVAL_KEY_ID"), deviceApprovalPublicKeySPKIBase64: requiredEnvironment("DIREXTALK_DEVICE_APPROVAL_PUBLIC_KEY_SPKI_B64"),
		accountID: requiredEnvironment("DIREXTALK_STACK_ACCOUNT_ID"), region: requiredEnvironment("DIREXTALK_STACK_REGION"),
		stackARN: requiredEnvironment("DIREXTALK_STACK_ARN"), urlSuffix: requiredEnvironment("DIREXTALK_AWS_URL_SUFFIX"),
		stageName: requiredEnvironment("DIREXTALK_BROKER_STAGE_NAME"), workerAMIID: requiredEnvironment("DIREXTALK_WORKER_BASE_AMI_ID"),
		workerVPCID: requiredEnvironment("DIREXTALK_WORKER_VPC_ID"), workerSubnetID: requiredEnvironment("DIREXTALK_WORKER_SUBNET_ID"),
		workerAvailabilityZone:        requiredEnvironment("DIREXTALK_WORKER_AVAILABILITY_ZONE"),
		workerSecurityGroupID:         requiredEnvironment("DIREXTALK_WORKER_SECURITY_GROUP_ID"),
		workerResourceManifestDigest:  requiredEnvironment("DIREXTALK_WORKER_RESOURCE_MANIFEST_DIGEST"),
		workerBootstrapEndpoint:       requiredEnvironment("DIREXTALK_WORKER_BOOTSTRAP_ENDPOINT"),
		workerIdentityRSAPublicKeyPEM: requiredEnvironment("DIREXTALK_WORKER_IDENTITY_RSA_PUBLIC_KEY_PEM"),
		receiptsTable:                 requiredEnvironment("DIREXTALK_COMMAND_RECEIPTS_TABLE"), countersTable: requiredEnvironment("DIREXTALK_CONNECTION_COUNTERS_TABLE"),
		issuedQuotesTable:           requiredEnvironment("DIREXTALK_ISSUED_QUOTES_TABLE"),
		deploymentReservationsTable: requiredEnvironment("DIREXTALK_DEPLOYMENT_RESERVATIONS_TABLE"), approvalUsesTable: requiredEnvironment("DIREXTALK_APPROVAL_USES_TABLE"),
		workerSessionsTable:        requiredEnvironment("DIREXTALK_WORKER_SESSIONS_TABLE"),
		workerTasksTable:           requiredEnvironment("DIREXTALK_WORKER_TASKS_TABLE"),
		serviceReadinessTasksTable: requiredEnvironment("DIREXTALK_SERVICE_READINESS_TASKS_TABLE"),
	}
	generation, err := strconv.ParseInt(requiredEnvironment("DIREXTALK_CONNECTION_GENERATION"), 10, 64)
	if err != nil || generation < 1 || generation > 9007199254740991 {
		return runtimeConfig{}, errors.New("invalid connection generation")
	}
	config.connectionGeneration = generation
	switch requiredEnvironment("DIREXTALK_DEPLOYMENT_CREATE_ENABLED") {
	case "true":
		config.deploymentEnabled = true
		if config.workerIdentityRSAPublicKeyPEM == "" {
			return runtimeConfig{}, errors.New("worker identity verifier is required when deployment create is enabled")
		}
	case "false":
		config.deploymentEnabled = false
	default:
		return runtimeConfig{}, errors.New("invalid deployment create gate")
	}
	if config.connectionID == "" || config.nodeKeyID == "" || config.nodePublicKeySPKIBase64 == "" || config.deviceApprovalKeyID == "" || config.deviceApprovalPublicKeySPKIBase64 == "" || config.accountID == "" || config.region == "" || config.stackARN == "" || config.urlSuffix == "" || config.stageName == "" || config.workerAMIID == "" || config.workerVPCID == "" || config.workerSubnetID == "" || config.workerAvailabilityZone == "" || config.workerSecurityGroupID == "" || config.workerResourceManifestDigest == "" || config.workerBootstrapEndpoint == "" || config.receiptsTable == "" || config.countersTable == "" || config.issuedQuotesTable == "" || config.deploymentReservationsTable == "" || config.approvalUsesTable == "" || config.workerSessionsTable == "" || config.workerTasksTable == "" || config.serviceReadinessTasksTable == "" {
		return runtimeConfig{}, errors.New("incomplete broker configuration")
	}
	for _, existing := range []string{config.receiptsTable, config.countersTable, config.issuedQuotesTable,
		config.deploymentReservationsTable, config.approvalUsesTable, config.workerSessionsTable, config.serviceReadinessTasksTable} {
		if config.workerTasksTable == existing {
			return runtimeConfig{}, errors.New("worker task table must be isolated")
		}
	}
	for _, existing := range []string{config.receiptsTable, config.countersTable, config.issuedQuotesTable, config.deploymentReservationsTable, config.approvalUsesTable, config.workerSessionsTable, config.workerTasksTable} {
		if config.serviceReadinessTasksTable == existing {
			return runtimeConfig{}, errors.New("service readiness task table must be isolated")
		}
	}
	return config, nil
}

func requiredEnvironment(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func pricingRegion(region string) string {
	if strings.HasPrefix(region, "cn-") {
		return "cn-northwest-1"
	}
	if strings.HasPrefix(region, "us-gov-") {
		return "us-gov-west-1"
	}
	return "us-east-1"
}
