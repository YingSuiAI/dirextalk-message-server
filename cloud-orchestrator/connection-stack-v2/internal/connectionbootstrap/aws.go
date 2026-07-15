package connectionbootstrap

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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
func (factory *AWSClientFactory) Clients(value Credentials) (STSClient, StackClient, error) {
	if factory == nil || !regionPattern.MatchString(factory.region) || len(value.AccessKeyID) == 0 || len(value.SecretAccessKey) == 0 {
		return nil, nil, ErrInvalid
	}
	provider := credentials.NewStaticCredentialsProvider(string(value.AccessKeyID), string(value.SecretAccessKey), string(value.SessionToken))
	config := aws.Config{Region: factory.region, Credentials: aws.NewCredentialsCache(provider), RetryMaxAttempts: 3, RetryMode: aws.RetryModeStandard}
	return &awsSTSClient{client: factory.newSTS(config)}, &awsStackClient{client: factory.newCFN(config)}, nil
}

type awsSTSClient struct{ client stsAPI }

func (client *awsSTSClient) GetCallerIdentity(ctx context.Context) (CallerIdentity, error) {
	output, err := client.client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return CallerIdentity{}, err
	}
	return CallerIdentity{AccountID: aws.ToString(output.Account), ARN: aws.ToString(output.Arn), UserID: aws.ToString(output.UserId)}, nil
}

type awsStackClient struct{ client cloudFormationAPI }

func (client *awsStackClient) CreateStack(ctx context.Context, request StackRequest) (string, error) {
	if client == nil || client.client == nil || !strings.HasPrefix(request.StackName, "dirextalk-connection-") || !validHTTPSURL(request.TemplateURL) || !strings.HasPrefix(request.ClientRequestToken, "dtx-") {
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
	output, err := client.client.CreateStack(ctx, &cloudformation.CreateStackInput{StackName: aws.String(request.StackName), TemplateURL: aws.String(request.TemplateURL), ClientRequestToken: aws.String(request.ClientRequestToken), Capabilities: []cloudformationtypes.Capability{cloudformationtypes.CapabilityCapabilityNamedIam}, OnFailure: cloudformationtypes.OnFailureDelete, EnableTerminationProtection: aws.Bool(false), Parameters: parameters, Tags: tags})
	if err != nil {
		return "", err
	}
	return aws.ToString(output.StackId), nil
}
