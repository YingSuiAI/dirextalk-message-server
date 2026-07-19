package stackteardown

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cloudformationtypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestDeleteStackDoesNotOverrideTemplateRetentionPolicy(t *testing.T) {
	t.Parallel()
	fake := &deleteStackFake{}
	provider := &awsProvider{cloudFormation: fake}
	stackID := "arn:aws:cloudformation:us-east-1:123456789012:stack/dirextalk-connection-0123456789abcdef01234567/01234567-89ab-cdef-0123-456789abcdef"
	if err := provider.DeleteStack(context.Background(), stackID); err != nil {
		t.Fatal(err)
	}
	if fake.input == nil || aws.ToString(fake.input.StackName) != stackID || len(fake.input.RetainResources) != 0 || fake.input.RoleARN != nil || fake.input.ClientRequestToken != nil {
		t.Fatalf("DeleteStack must use the closed Stack ID and template retention policy: %#v", fake.input)
	}
}

func TestVersionObjectsRejectsIncompleteObjectIdentity(t *testing.T) {
	t.Parallel()
	if _, err := versionObjects([]types.ObjectVersion{{Key: aws.String("object-without-version")}}, nil); err != ErrProviderUnavailable {
		t.Fatalf("incomplete version err=%v", err)
	}
}

func TestStackResourcesPaginatesInsteadOfSilentlyTruncatingInventory(t *testing.T) {
	t.Parallel()
	stackID := "arn:aws:cloudformation:us-east-1:123456789012:stack/dirextalk-connection-0123456789abcdef01234567/01234567-89ab-cdef-0123-456789abcdef"
	fake := &deleteStackFake{resourcePages: []*cloudformation.ListStackResourcesOutput{
		{NextToken: aws.String("page-2"), StackResourceSummaries: []cloudformationtypes.StackResourceSummary{{LogicalResourceId: aws.String("First"), ResourceType: aws.String("AWS::DynamoDB::Table"), PhysicalResourceId: aws.String("first")}}},
		{StackResourceSummaries: []cloudformationtypes.StackResourceSummary{{LogicalResourceId: aws.String("Second"), ResourceType: aws.String("AWS::DynamoDB::Table"), PhysicalResourceId: aws.String("second")}}},
	}}
	resources, err := (&awsProvider{cloudFormation: fake}).StackResources(context.Background(), stackID)
	if err != nil || len(resources) != 2 || len(fake.resourceInputs) != 2 || aws.ToString(fake.resourceInputs[0].StackName) != stackID || fake.resourceInputs[0].NextToken != nil || aws.ToString(fake.resourceInputs[1].NextToken) != "page-2" {
		t.Fatalf("resources=%#v inputs=%#v err=%v", resources, fake.resourceInputs, err)
	}
}

type deleteStackFake struct {
	input          *cloudformation.DeleteStackInput
	resourcePages  []*cloudformation.ListStackResourcesOutput
	resourceInputs []*cloudformation.ListStackResourcesInput
}

func (*deleteStackFake) DescribeStacks(context.Context, *cloudformation.DescribeStacksInput, ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error) {
	return nil, nil
}
func (fake *deleteStackFake) ListStackResources(_ context.Context, input *cloudformation.ListStackResourcesInput, _ ...func(*cloudformation.Options)) (*cloudformation.ListStackResourcesOutput, error) {
	fake.resourceInputs = append(fake.resourceInputs, input)
	if len(fake.resourcePages) == 0 {
		return &cloudformation.ListStackResourcesOutput{}, nil
	}
	result := fake.resourcePages[0]
	fake.resourcePages = fake.resourcePages[1:]
	return result, nil
}
func (fake *deleteStackFake) DeleteStack(_ context.Context, input *cloudformation.DeleteStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.DeleteStackOutput, error) {
	fake.input = input
	return &cloudformation.DeleteStackOutput{}, nil
}
