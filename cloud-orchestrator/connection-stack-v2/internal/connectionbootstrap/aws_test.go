package connectionbootstrap

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cloudformationtypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func TestAWSAdapterUsesUploadedCredentialsAndClosedCreateStackShape(t *testing.T) {
	stsFake := &fakeSTSAPI{}
	cfnFake := &fakeCFNAPI{}
	factory := &AWSClientFactory{region: "us-east-1", newSTS: func(config aws.Config) stsAPI {
		credentials, err := config.Credentials.Retrieve(context.Background())
		if err != nil || credentials.AccessKeyID != "AKIAABCDEFGHIJKLMNOP" || credentials.SecretAccessKey != "secret-access-key-value-1234567890" || credentials.SessionToken != "session-token" {
			t.Fatalf("independent credentials=%#v err=%v", credentials, err)
		}
		return stsFake
	}, newCFN: func(config aws.Config) cloudFormationAPI { return cfnFake }}
	identityClient, stackClient, err := factory.Clients(Credentials{AccessKeyID: []byte("AKIAABCDEFGHIJKLMNOP"), SecretAccessKey: []byte("secret-access-key-value-1234567890"), SessionToken: []byte("session-token")})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := identityClient.GetCallerIdentity(context.Background())
	if err != nil || identity.ARN == "" {
		t.Fatalf("identity=%#v err=%v", identity, err)
	}
	configFixture := configFixture()
	template, err := configFixture.ConnectionTemplate.ArtifactReference(configFixture.ArtifactPolicy)
	if err != nil {
		t.Fatal(err)
	}
	request := StackRequest{StackName: "dirextalk-connection-0123456789abcdef01234567", Region: "us-east-1", Template: template, ClientRequestToken: "dtx-0123456789abcdef", Parameters: map[string]string{"ConnectionId": "connection-0001"}, Tags: map[string]string{"dirextalk:managed": "true"}}
	templateURL, err := request.Template.CloudFormationURL(request.Region)
	if err != nil {
		t.Fatal(err)
	}
	stackID, err := stackClient.CreateStack(context.Background(), request)
	if err != nil || stackID == "" {
		t.Fatalf("CreateStack=%q err=%v", stackID, err)
	}
	input := cfnFake.input
	if input == nil || len(input.Capabilities) != 1 || input.Capabilities[0] != cloudformationtypes.CapabilityCapabilityNamedIam || input.OnFailure != cloudformationtypes.OnFailureDelete || aws.ToString(input.TemplateURL) != templateURL || aws.ToString(input.ClientRequestToken) != request.ClientRequestToken || len(input.Parameters) != 1 || len(input.Tags) != 1 {
		t.Fatalf("unsafe CreateStack input: %#v", input)
	}
}

func TestAWSConfigForCredentialsUsesOnlyProvidedEphemeralValues(t *testing.T) {
	config, err := AWSConfigForCredentials("us-east-1", Credentials{
		AccessKeyID:     []byte("AKIAABCDEFGHIJKLMNOP"),
		SecretAccessKey: []byte("secret-access-key-value-1234567890"),
		SessionToken:    []byte("session-token"),
	})
	if err != nil || config.Region != "us-east-1" || config.Credentials == nil {
		t.Fatalf("AWSConfigForCredentials() config=%#v err=%v", config, err)
	}
	value, err := config.Credentials.Retrieve(context.Background())
	if err != nil || value.AccessKeyID != "AKIAABCDEFGHIJKLMNOP" || value.SecretAccessKey != "secret-access-key-value-1234567890" || value.SessionToken != "session-token" {
		t.Fatalf("ephemeral credential provider=%#v err=%v", value, err)
	}
	if config.RetryMaxAttempts != 3 || config.RetryMode != aws.RetryModeStandard {
		t.Fatalf("unexpected retry config: %#v", config)
	}
}

func TestAWSConfigForCredentialsRejectsIncompleteOrInvalidInputs(t *testing.T) {
	for name, test := range map[string]struct {
		region string
		value  Credentials
	}{
		"invalid_region": {region: "not-a-region", value: Credentials{AccessKeyID: []byte("AKIAABCDEFGHIJKLMNOP"), SecretAccessKey: []byte("secret")}},
		"missing_key":    {region: "us-east-1", value: Credentials{SecretAccessKey: []byte("secret")}},
		"missing_secret": {region: "us-east-1", value: Credentials{AccessKeyID: []byte("AKIAABCDEFGHIJKLMNOP")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := AWSConfigForCredentials(test.region, test.value); err == nil {
				t.Fatal("AWSConfigForCredentials() accepted incomplete or invalid input")
			}
		})
	}
}

func TestAWSAdapterRecoversCreateStackResponseLossOnlyAfterStrictReadBack(t *testing.T) {
	for name, createErr := range map[string]error{
		"response_lost":  errors.New("connection reset after accepted request"),
		"already_exists": errors.New("AlreadyExistsException: stack already exists"),
	} {
		t.Run(name, func(t *testing.T) {
			request, templateBody := recoveryStackRequest(t)
			fake := &fakeCFNAPI{
				createErr: createErr,
				describeOutput: &cloudformation.DescribeStacksOutput{Stacks: []cloudformationtypes.Stack{
					observedRecoveryStack(request),
				}},
				getTemplateOutput: &cloudformation.GetTemplateOutput{TemplateBody: aws.String(templateBody)},
			}
			client := &awsStackClient{client: fake, region: request.Region}

			stackID, err := client.CreateStack(context.Background(), request)
			if err != nil || stackID != aws.ToString(fake.describeOutput.Stacks[0].StackId) {
				t.Fatalf("CreateStack() stackID=%q err=%v", stackID, err)
			}
			if aws.ToString(fake.describeInput.StackName) != request.StackName || aws.ToString(fake.getTemplateInput.StackName) != stackID || fake.getTemplateInput.TemplateStage != cloudformationtypes.TemplateStageOriginal {
				t.Fatalf("recovery did not use deterministic stack read-back: describe=%#v template=%#v", fake.describeInput, fake.getTemplateInput)
			}
		})
	}
}

func TestAWSAdapterRefusesCreateStackRecoveryWhenObservedStackDoesNotExactlyMatch(t *testing.T) {
	for name, mutate := range map[string]func(*cloudformationtypes.Stack, *cloudformation.GetTemplateOutput){
		"worker_security_group_parameter": func(stack *cloudformationtypes.Stack, _ *cloudformation.GetTemplateOutput) {
			stack.Parameters[1].ParameterValue = aws.String("sg-99999999999999999")
		},
		"connection_template_tag": func(stack *cloudformationtypes.Stack, _ *cloudformation.GetTemplateOutput) {
			for index := range stack.Tags {
				if aws.ToString(stack.Tags[index].Key) == stackTagTemplateBinding {
					value := aws.ToString(stack.Tags[index].Value)
					replacement := byte('0')
					if value[7] == replacement {
						replacement = '1'
					}
					stack.Tags[index].Value = aws.String("sha256:" + string(replacement) + value[8:])
					return
				}
			}
			t.Fatal("fixture missing template binding tag")
		},
		"stack_region": func(stack *cloudformationtypes.Stack, _ *cloudformation.GetTemplateOutput) {
			stack.StackId = aws.String("arn:aws:cloudformation:us-west-2:123456789012:stack/" + aws.ToString(stack.StackName) + "/stack-id")
		},
		"template_body": func(_ *cloudformationtypes.Stack, output *cloudformation.GetTemplateOutput) {
			output.TemplateBody = aws.String("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n  Other: {}\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			request, templateBody := recoveryStackRequest(t)
			stack := observedRecoveryStack(request)
			output := &cloudformation.GetTemplateOutput{TemplateBody: aws.String(templateBody)}
			mutate(&stack, output)
			fake := &fakeCFNAPI{createErr: errors.New("connection reset after accepted request"), describeOutput: &cloudformation.DescribeStacksOutput{Stacks: []cloudformationtypes.Stack{stack}}, getTemplateOutput: output}

			stackID, err := (&awsStackClient{client: fake, region: request.Region}).CreateStack(context.Background(), request)
			if err == nil || stackID != "" {
				t.Fatalf("CreateStack() recovered mismatched stackID=%q err=%v", stackID, err)
			}
		})
	}
}

func recoveryStackRequest(t *testing.T) (StackRequest, string) {
	t.Helper()
	templateBody := "AWSTemplateFormatVersion: '2010-09-09'\nResources: {}\n"
	digest := sha256.Sum256([]byte(templateBody))
	policy := configFixture().ArtifactPolicy
	template, err := artifactpublish.NewConnectionTemplateReference(policy, "v1.1.0-cloud-mvp.20260716.1", fmt.Sprintf("sha256:%x", digest), int64(len(templateBody)), "version-0001")
	if err != nil {
		t.Fatal(err)
	}
	request := StackRequest{
		StackName:          "dirextalk-connection-0123456789abcdef01234567",
		Region:             "us-east-1",
		Template:           template,
		ClientRequestToken: "dtx-0123456789abcdef",
		Parameters: map[string]string{
			"ConnectionId":          "connection-recovery-0001",
			"WorkerSecurityGroupId": "sg-0123456789abcdef0",
			"WorkerSubnetId":        "subnet-0123456789abcdef0",
			"WorkerBaseAmiId":       "ami-0123456789abcdef0",
		},
		Tags: map[string]string{
			stackTagManaged:         "true",
			stackTagConnectionID:    "connection-recovery-0001",
			stackTagRegion:          "us-east-1",
			stackTagTemplateBinding: connectionTemplateBindingFingerprint(template),
			stackTagParameterBinding: stackParameterBindingFingerprint(map[string]string{
				"ConnectionId":          "connection-recovery-0001",
				"WorkerSecurityGroupId": "sg-0123456789abcdef0",
				"WorkerSubnetId":        "subnet-0123456789abcdef0",
				"WorkerBaseAmiId":       "ami-0123456789abcdef0",
			}),
		},
	}
	return request, templateBody
}

func observedRecoveryStack(request StackRequest) cloudformationtypes.Stack {
	parameters := make([]cloudformationtypes.Parameter, 0, len(request.Parameters))
	for _, key := range sortedMapKeys(request.Parameters) {
		parameters = append(parameters, cloudformationtypes.Parameter{ParameterKey: aws.String(key), ParameterValue: aws.String(request.Parameters[key])})
	}
	tags := make([]cloudformationtypes.Tag, 0, len(request.Tags))
	for _, key := range sortedMapKeys(request.Tags) {
		tags = append(tags, cloudformationtypes.Tag{Key: aws.String(key), Value: aws.String(request.Tags[key])})
	}
	return cloudformationtypes.Stack{
		StackName:  aws.String(request.StackName),
		StackId:    aws.String("arn:aws:cloudformation:" + request.Region + ":123456789012:stack/" + request.StackName + "/stack-id"),
		Parameters: parameters,
		Tags:       tags,
	}
}

type fakeSTSAPI struct{}

func (*fakeSTSAPI) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Account: aws.String("123456789012"), Arn: aws.String("arn:aws:iam::123456789012:role/bootstrap"), UserId: aws.String("role")}, nil
}

type fakeCFNAPI struct {
	input             *cloudformation.CreateStackInput
	createErr         error
	describeInput     *cloudformation.DescribeStacksInput
	describeOutput    *cloudformation.DescribeStacksOutput
	describeErr       error
	getTemplateInput  *cloudformation.GetTemplateInput
	getTemplateOutput *cloudformation.GetTemplateOutput
	getTemplateErr    error
}

func (fake *fakeCFNAPI) CreateStack(_ context.Context, input *cloudformation.CreateStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.CreateStackOutput, error) {
	fake.input = input
	if fake.createErr != nil {
		return nil, fake.createErr
	}
	return &cloudformation.CreateStackOutput{StackId: aws.String("arn:aws:cloudformation:us-east-1:123456789012:stack/test/id")}, nil
}

func (fake *fakeCFNAPI) DescribeStacks(_ context.Context, input *cloudformation.DescribeStacksInput, _ ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error) {
	fake.describeInput = input
	return fake.describeOutput, fake.describeErr
}

func (fake *fakeCFNAPI) GetTemplate(_ context.Context, input *cloudformation.GetTemplateInput, _ ...func(*cloudformation.Options)) (*cloudformation.GetTemplateOutput, error) {
	fake.getTemplateInput = input
	return fake.getTemplateOutput, fake.getTemplateErr
}
