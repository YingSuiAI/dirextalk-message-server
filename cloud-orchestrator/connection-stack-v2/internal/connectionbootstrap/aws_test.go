package connectionbootstrap

import (
	"context"
	"testing"

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
	request := StackRequest{StackName: "dirextalk-connection-0123456789abcdef01234567", TemplateURL: "https://artifacts.example.invalid/stack.yaml", ClientRequestToken: "dtx-0123456789abcdef", Parameters: map[string]string{"ConnectionId": "connection-0001"}, Tags: map[string]string{"dirextalk:managed": "true"}}
	stackID, err := stackClient.CreateStack(context.Background(), request)
	if err != nil || stackID == "" {
		t.Fatalf("CreateStack=%q err=%v", stackID, err)
	}
	input := cfnFake.input
	if input == nil || len(input.Capabilities) != 1 || input.Capabilities[0] != cloudformationtypes.CapabilityCapabilityNamedIam || input.OnFailure != cloudformationtypes.OnFailureDelete || aws.ToString(input.TemplateURL) != request.TemplateURL || aws.ToString(input.ClientRequestToken) != request.ClientRequestToken || len(input.Parameters) != 1 || len(input.Tags) != 1 {
		t.Fatalf("unsafe CreateStack input: %#v", input)
	}
}

type fakeSTSAPI struct{}

func (*fakeSTSAPI) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Account: aws.String("123456789012"), Arn: aws.String("arn:aws:iam::123456789012:role/bootstrap"), UserId: aws.String("role")}, nil
}

type fakeCFNAPI struct {
	input *cloudformation.CreateStackInput
}

func (fake *fakeCFNAPI) CreateStack(_ context.Context, input *cloudformation.CreateStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.CreateStackOutput, error) {
	fake.input = input
	return &cloudformation.CreateStackOutput{StackId: aws.String("arn:aws:cloudformation:us-east-1:123456789012:stack/test/id")}, nil
}
