package nativeagent

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type awsSDKClient struct {
	ec2 *ec2.Client
	sts *sts.Client
}

func newAWSClient(_ context.Context, credentialsValue AWSClientCredentials, httpClient *http.Client) (AWSClient, error) {
	config := aws.Config{
		Region: credentialsValue.Region,
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				credentialsValue.AccessKeyID,
				credentialsValue.SecretAccessKey,
				credentialsValue.SessionToken,
			),
		),
		Retryer: func() aws.Retryer {
			return retry.NewStandard()
		},
	}
	if httpClient != nil {
		config.HTTPClient = httpClient
	}
	return &awsSDKClient{
		ec2: ec2.NewFromConfig(config),
		sts: sts.NewFromConfig(config),
	}, nil
}

func (c *awsSDKClient) Identity(ctx context.Context) (AWSIdentity, error) {
	output, err := c.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return AWSIdentity{}, err
	}
	return AWSIdentity{
		AccountID: aws.ToString(output.Account),
		ARN:       aws.ToString(output.Arn),
		UserID:    aws.ToString(output.UserId),
	}, nil
}

func (c *awsSDKClient) ListInstances(ctx context.Context) ([]AWSInstance, error) {
	output, err := c.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		MaxResults: aws.Int32(200),
	})
	if err != nil {
		return nil, err
	}
	instances := make([]AWSInstance, 0)
	for _, reservation := range output.Reservations {
		for _, instance := range reservation.Instances {
			instances = append(instances, awsInstanceFromSDK(instance))
		}
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].LaunchTime > instances[j].LaunchTime
	})
	return instances, nil
}

func (c *awsSDKClient) ResolveImage(ctx context.Context, alias string) (string, error) {
	if strings.TrimSpace(alias) != defaultAWSImageAlias {
		return "", fmt.Errorf("AWS image alias %q is not supported", alias)
	}
	output, err := c.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{"amazon"},
		Filters: []ec2types.Filter{
			{Name: aws.String("name"), Values: []string{"al2023-ami-2023.*-x86_64"}},
			{Name: aws.String("architecture"), Values: []string{"x86_64"}},
			{Name: aws.String("root-device-type"), Values: []string{"ebs"}},
			{Name: aws.String("state"), Values: []string{"available"}},
			{Name: aws.String("virtualization-type"), Values: []string{"hvm"}},
		},
	})
	if err != nil {
		return "", err
	}
	if len(output.Images) == 0 {
		return "", fmt.Errorf("no Amazon Linux 2023 image is available in this region")
	}
	sort.Slice(output.Images, func(i, j int) bool {
		return aws.ToString(output.Images[i].CreationDate) > aws.ToString(output.Images[j].CreationDate)
	})
	imageID := strings.TrimSpace(aws.ToString(output.Images[0].ImageId))
	if imageID == "" {
		return "", fmt.Errorf("resolved Amazon Linux image has no image ID")
	}
	return imageID, nil
}

func (c *awsSDKClient) DescribeInstance(ctx context.Context, instanceID string) (AWSInstance, error) {
	output, err := c.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return AWSInstance{}, err
	}
	for _, reservation := range output.Reservations {
		for _, instance := range reservation.Instances {
			return awsInstanceFromSDK(instance), nil
		}
	}
	return AWSInstance{}, fmt.Errorf("EC2 instance %s was not found", instanceID)
}

func (c *awsSDKClient) CreateInstance(ctx context.Context, input AWSCreateInstanceInput, clientToken string) (AWSInstance, error) {
	tags := []ec2types.Tag{
		{Key: aws.String(awsManagedTagKey), Value: aws.String(awsManagedTagValue)},
		{Key: aws.String(awsAgentTagKey), Value: aws.String(awsAgentTagValue)},
		{Key: aws.String(awsApprovalTagKey), Value: aws.String(clientToken)},
	}
	if strings.TrimSpace(input.Purpose) != "" {
		tags = append(tags,
			ec2types.Tag{Key: aws.String("Name"), Value: aws.String(input.Purpose)},
			ec2types.Tag{Key: aws.String("dirextalk-purpose"), Value: aws.String(input.Purpose)},
		)
	}
	request := &ec2.RunInstancesInput{
		ClientToken:  aws.String(clientToken),
		ImageId:      aws.String(input.ImageID),
		InstanceType: ec2types.InstanceType(input.InstanceType),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeInstance,
			Tags:         tags,
		}},
	}
	if input.SubnetID != "" {
		request.SubnetId = aws.String(input.SubnetID)
	}
	if len(input.SecurityGroupIDs) > 0 {
		request.SecurityGroupIds = append([]string(nil), input.SecurityGroupIDs...)
	}
	if input.KeyName != "" {
		request.KeyName = aws.String(input.KeyName)
	}
	if input.VolumeSizeGB > 0 {
		request.BlockDeviceMappings = []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/xvda"),
			Ebs: &ec2types.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int32(input.VolumeSizeGB),
				VolumeType:          ec2types.VolumeTypeGp3,
			},
		}}
	}
	output, err := c.ec2.RunInstances(ctx, request)
	if err != nil {
		return AWSInstance{}, err
	}
	if len(output.Instances) == 0 {
		return AWSInstance{}, fmt.Errorf("AWS returned no EC2 instance")
	}
	return awsInstanceFromSDK(output.Instances[0]), nil
}

func (c *awsSDKClient) TerminateInstance(ctx context.Context, instanceID string) (AWSInstance, error) {
	output, err := c.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return AWSInstance{}, err
	}
	state := "shutting-down"
	if len(output.TerminatingInstances) > 0 && output.TerminatingInstances[0].CurrentState != nil {
		state = string(output.TerminatingInstances[0].CurrentState.Name)
	}
	return AWSInstance{
		InstanceID: instanceID,
		State:      state,
		Managed:    true,
	}, nil
}

func awsInstanceFromSDK(instance ec2types.Instance) AWSInstance {
	tags := make(map[string]string, len(instance.Tags))
	for _, tag := range instance.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	launchTime := ""
	if instance.LaunchTime != nil {
		launchTime = instance.LaunchTime.UTC().Format("2006-01-02T15:04:05Z")
	}
	availabilityZone := ""
	if instance.Placement != nil {
		availabilityZone = aws.ToString(instance.Placement.AvailabilityZone)
	}
	state := ""
	if instance.State != nil {
		state = string(instance.State.Name)
	}
	return AWSInstance{
		InstanceID:       aws.ToString(instance.InstanceId),
		InstanceType:     string(instance.InstanceType),
		ImageID:          aws.ToString(instance.ImageId),
		State:            state,
		AvailabilityZone: availabilityZone,
		PublicIPAddress:  aws.ToString(instance.PublicIpAddress),
		PrivateIPAddress: aws.ToString(instance.PrivateIpAddress),
		LaunchTime:       launchTime,
		Name:             tags["Name"],
		Managed:          tags[awsManagedTagKey] == awsManagedTagValue,
	}
}
