package connectionbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cloudformationtypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type stsAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}
type cloudFormationAPI interface {
	CreateStack(context.Context, *cloudformation.CreateStackInput, ...func(*cloudformation.Options)) (*cloudformation.CreateStackOutput, error)
	DescribeStacks(context.Context, *cloudformation.DescribeStacksInput, ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error)
	GetTemplate(context.Context, *cloudformation.GetTemplateInput, ...func(*cloudformation.Options)) (*cloudformation.GetTemplateOutput, error)
}
type AWSClientFactory struct {
	region string
	newSTS func(aws.Config) stsAPI
	newCFN func(aws.Config) cloudFormationAPI
}

func NewAWSClientFactory(region string) (*AWSClientFactory, error) {
	if !regionPattern.MatchString(region) {
		return nil, ErrInvalid
	}
	return &AWSClientFactory{region: region, newSTS: func(config aws.Config) stsAPI { return sts.NewFromConfig(config) }, newCFN: func(config aws.Config) cloudFormationAPI { return cloudformation.NewFromConfig(config) }}, nil
}

// AWSConfigForCredentials builds an SDK configuration from exactly the
// short-lived credentials uploaded to this bootstrap session. It deliberately
// does not load the ambient environment, shared credentials files, or a
// default profile: root-bootstrap callers must never accidentally inherit the
// host's AWS identity.
func AWSConfigForCredentials(region string, value Credentials) (aws.Config, error) {
	if !regionPattern.MatchString(region) || len(value.AccessKeyID) == 0 || len(value.SecretAccessKey) == 0 {
		return aws.Config{}, ErrInvalid
	}
	provider := credentials.NewStaticCredentialsProvider(string(value.AccessKeyID), string(value.SecretAccessKey), string(value.SessionToken))
	return aws.Config{Region: region, Credentials: aws.NewCredentialsCache(provider), RetryMaxAttempts: 3, RetryMode: aws.RetryModeStandard}, nil
}

func (factory *AWSClientFactory) Clients(value Credentials) (STSClient, StackClient, error) {
	if factory == nil {
		return nil, nil, ErrInvalid
	}
	config, err := AWSConfigForCredentials(factory.region, value)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	return &awsSTSClient{client: factory.newSTS(config)}, &awsStackClient{client: factory.newCFN(config), region: factory.region}, nil
}

type awsSTSClient struct{ client stsAPI }

func (client *awsSTSClient) GetCallerIdentity(ctx context.Context) (CallerIdentity, error) {
	output, err := client.client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return CallerIdentity{}, err
	}
	return CallerIdentity{AccountID: aws.ToString(output.Account), ARN: aws.ToString(output.Arn), UserID: aws.ToString(output.UserId)}, nil
}

type awsStackClient struct {
	client cloudFormationAPI
	region string
}

func (client *awsStackClient) CreateStack(ctx context.Context, request StackRequest) (string, error) {
	if client == nil || client.client == nil || !strings.HasPrefix(request.StackName, "dirextalk-connection-") || !regionPattern.MatchString(request.Region) || request.Region != client.region || !strings.HasPrefix(request.ClientRequestToken, "dtx-") {
		return "", ErrInvalid
	}
	templateURL, err := request.Template.CloudFormationURL(request.Region)
	if err != nil {
		return "", ErrInvalid
	}
	parameterKeys := sortedMapKeys(request.Parameters)
	parameters := make([]cloudformationtypes.Parameter, 0, len(parameterKeys))
	for _, key := range parameterKeys {
		parameters = append(parameters, cloudformationtypes.Parameter{ParameterKey: aws.String(key), ParameterValue: aws.String(request.Parameters[key])})
	}
	tagKeys := sortedMapKeys(request.Tags)
	tags := make([]cloudformationtypes.Tag, 0, len(tagKeys))
	for _, key := range tagKeys {
		tags = append(tags, cloudformationtypes.Tag{Key: aws.String(key), Value: aws.String(request.Tags[key])})
	}
	output, err := client.client.CreateStack(ctx, &cloudformation.CreateStackInput{StackName: aws.String(request.StackName), TemplateURL: aws.String(templateURL), ClientRequestToken: aws.String(request.ClientRequestToken), Capabilities: []cloudformationtypes.Capability{cloudformationtypes.CapabilityCapabilityNamedIam}, OnFailure: cloudformationtypes.OnFailureDelete, EnableTerminationProtection: aws.Bool(false), Parameters: parameters, Tags: tags})
	if err == nil && output != nil && aws.ToString(output.StackId) != "" {
		return aws.ToString(output.StackId), nil
	}

	// The CreateStack request can have reached AWS even when its response was
	// lost. Never retry a mutation blindly: recover only the deterministic name
	// after its complete identity, parameter, tag, and original template have
	// been read back from CloudFormation.
	stackID, recoveryErr := client.recoverStackID(ctx, request)
	if recoveryErr != nil || stackID == "" {
		return "", ErrInvalid
	}
	return stackID, nil
}

func (client *awsStackClient) recoverStackID(ctx context.Context, request StackRequest) (string, error) {
	output, err := client.client.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(request.StackName)})
	if err != nil || output == nil || len(output.Stacks) != 1 {
		return "", ErrInvalid
	}
	stack := output.Stacks[0]
	stackID := aws.ToString(stack.StackId)
	if !stackIdentityMatches(request, stack) || !stackParametersMatch(request.Parameters, stack.Parameters) || !stackTagsMatch(request.Tags, stack.Tags) {
		return "", ErrInvalid
	}
	template, err := client.client.GetTemplate(ctx, &cloudformation.GetTemplateInput{StackName: aws.String(stackID), TemplateStage: cloudformationtypes.TemplateStageOriginal})
	if err != nil || template == nil || template.TemplateBody == nil || !templateBodyMatches(request.Template.SHA256, *template.TemplateBody) {
		return "", ErrInvalid
	}
	return stackID, nil
}

func stackIdentityMatches(request StackRequest, stack cloudformationtypes.Stack) bool {
	stackID := aws.ToString(stack.StackId)
	parsed, err := arn.Parse(stackID)
	if err != nil || parsed.Partition == "" || parsed.Service != "cloudformation" || parsed.Region != request.Region || !accountIDPattern.MatchString(parsed.AccountID) || aws.ToString(stack.StackName) != request.StackName {
		return false
	}
	resource := strings.Split(parsed.Resource, "/")
	return len(resource) == 3 && resource[0] == "stack" && resource[1] == request.StackName && resource[2] != ""
}

func stackParametersMatch(expected map[string]string, observed []cloudformationtypes.Parameter) bool {
	if len(expected) != len(observed) {
		return false
	}
	values := make(map[string]string, len(observed))
	for _, parameter := range observed {
		key, value := aws.ToString(parameter.ParameterKey), aws.ToString(parameter.ParameterValue)
		if key == "" || parameter.ParameterValue == nil || (parameter.UsePreviousValue != nil && *parameter.UsePreviousValue) {
			return false
		}
		if _, duplicate := values[key]; duplicate {
			return false
		}
		values[key] = value
	}
	for key, want := range expected {
		got, exists := values[key]
		if !exists || (got != want && !(redactedStackParameter(key) && got == "****")) {
			return false
		}
	}
	return true
}

func redactedStackParameter(key string) bool {
	switch key {
	case "NodePublicKeySpkiBase64", "DeviceApprovalPublicKeySpkiBase64", "WorkerIdentityRsaPublicKeyPem":
		return true
	default:
		return false
	}
}

func stackTagsMatch(expected map[string]string, observed []cloudformationtypes.Tag) bool {
	values := make(map[string]string, len(observed))
	for _, tag := range observed {
		key, value := aws.ToString(tag.Key), aws.ToString(tag.Value)
		if key == "" || tag.Value == nil {
			return false
		}
		if strings.HasPrefix(strings.ToLower(key), "aws:") {
			continue
		}
		if _, duplicate := values[key]; duplicate {
			return false
		}
		values[key] = value
	}
	return sameStringMap(expected, values)
}

func templateBodyMatches(expectedDigest, body string) bool {
	if !namedDigest(expectedDigest) {
		return false
	}
	sum := sha256.Sum256([]byte(body))
	return expectedDigest == "sha256:"+hex.EncodeToString(sum[:])
}
