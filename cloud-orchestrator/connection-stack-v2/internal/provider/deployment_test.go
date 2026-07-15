package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

func TestEC2DeploymentProviderUsesClosedCreateSpecAndIndependentReadBack(t *testing.T) {
	client := &fakeEC2Deployment{}
	provider, err := NewEC2DeploymentProvider(client)
	if err != nil {
		t.Fatal(err)
	}
	spec := validEC2DeploymentSpec()
	instanceID, err := provider.EnsureCreated(t.Context(), spec)
	if err != nil || instanceID != "i-0123456789abcdef0" {
		t.Fatalf("EnsureCreated()=(%s,%v)", instanceID, err)
	}
	input := client.runInput
	if input == nil || aws.ToString(input.ClientToken) != spec.ClientToken || input.MinCount == nil || *input.MinCount != 1 || input.MaxCount == nil || *input.MaxCount != 1 || len(input.NetworkInterfaces) != 1 || len(input.NetworkInterfaces[0].Groups) != 1 || input.NetworkInterfaces[0].Groups[0] != spec.SecurityGroupID || aws.ToBool(input.NetworkInterfaces[0].AssociatePublicIpAddress) || aws.ToBool(input.NetworkInterfaces[0].DeleteOnTermination) || input.IamInstanceProfile != nil || input.KeyName != nil || input.UserData != nil || input.InstanceInitiatedShutdownBehavior != ec2types.ShutdownBehaviorStop || input.MetadataOptions == nil || input.MetadataOptions.HttpTokens != ec2types.HttpTokensStateRequired || len(input.BlockDeviceMappings) != 1 || input.BlockDeviceMappings[0].Ebs == nil || !aws.ToBool(input.BlockDeviceMappings[0].Ebs.Encrypted) || aws.ToBool(input.BlockDeviceMappings[0].Ebs.DeleteOnTermination) || len(input.TagSpecifications) != 2 {
		t.Fatalf("unsafe RunInstances input=%#v", input)
	}
	evidence, err := provider.ReadBack(t.Context(), spec, instanceID)
	if err != nil || evidence.InstanceID != instanceID || len(evidence.VolumeIDs) != 1 || len(evidence.NetworkInterfaceIDs) != 1 {
		t.Fatalf("ReadBack()=(%#v,%v)", evidence, err)
	}
}

func TestEC2DeploymentProviderRejectsReadBackOutsideFixedSecurityGroup(t *testing.T) {
	client := &fakeEC2Deployment{securityGroupID: "sg-fffffffffffffffff"}
	provider, err := NewEC2DeploymentProvider(client)
	if err != nil {
		t.Fatal(err)
	}
	spec := validEC2DeploymentSpec()
	if _, err := provider.ReadBack(t.Context(), spec, "i-0123456789abcdef0"); err == nil {
		t.Fatal("ReadBack() unexpectedly succeeded")
	} else {
		var apiError *api.Error
		if !errors.As(err, &apiError) || apiError.Code != "provider_readback_invalid" {
			t.Fatalf("ReadBack() error=%v", err)
		}
	}
}

type fakeEC2Deployment struct {
	runInput        *ec2.RunInstancesInput
	securityGroupID string
}

func (f *fakeEC2Deployment) RunInstances(_ context.Context, input *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	f.runInput = input
	return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{{InstanceId: aws.String("i-0123456789abcdef0")}}}, nil
}
func (f *fakeEC2Deployment) DescribeInstances(_ context.Context, _ *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	spec := validEC2DeploymentSpec()
	securityGroupID := f.securityGroupID
	if securityGroupID == "" {
		securityGroupID = spec.SecurityGroupID
	}
	return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{InstanceId: aws.String("i-0123456789abcdef0"), ImageId: aws.String(spec.AMIId), InstanceType: ec2types.InstanceType(spec.InstanceType), SubnetId: aws.String(spec.SubnetID), VpcId: aws.String(spec.VPCID), Placement: &ec2types.Placement{AvailabilityZone: aws.String(spec.AvailabilityZone)}, SecurityGroups: []ec2types.GroupIdentifier{{GroupId: aws.String(securityGroupID)}}, Tags: deploymentTags(spec), BlockDeviceMappings: []ec2types.InstanceBlockDeviceMapping{{Ebs: &ec2types.EbsInstanceBlockDevice{VolumeId: aws.String("vol-0123456789abcdef0")}}}, NetworkInterfaces: []ec2types.InstanceNetworkInterface{{NetworkInterfaceId: aws.String("eni-0123456789abcdef0")}}}}}}}, nil
}
func (f *fakeEC2Deployment) DescribeVolumes(_ context.Context, _ *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	spec := validEC2DeploymentSpec()
	return &ec2.DescribeVolumesOutput{Volumes: []ec2types.Volume{{VolumeId: aws.String("vol-0123456789abcdef0"), Encrypted: aws.Bool(true), Size: aws.Int32(80), VolumeType: ec2types.VolumeTypeGp3, Tags: deploymentTags(spec)}}}, nil
}
func validEC2DeploymentSpec() api.DeploymentSpec {
	return api.DeploymentSpec{ConnectionID: "connection-create-0001", DeploymentID: "deployment-create-0001", ClientToken: "dtx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", AMIId: "ami-0123456789abcdef0", InstanceType: "m7i.xlarge", Architecture: "amd64", DiskGiB: 80, VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "us-east-1a", SecurityGroupID: "sg-0123456789abcdef0"}
}
