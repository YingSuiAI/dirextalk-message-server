package provider

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

func TestEC2DeploymentDestroyProviderRequiresAbsentReadBack(t *testing.T) {
	client := &destroyEC2Fake{instanceState: types.InstanceStateNameRunning, interfaces: map[string]bool{"eni-0aaaaaaaaaaaaaaaa": true}, volumes: map[string]bool{"vol-0aaaaaaaaaaaaaaaa": true}}
	provider, err := NewEC2DeploymentDestroyProvider(client)
	if err != nil {
		t.Fatal(err)
	}
	spec := api.DeploymentDestroySpec{InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}}
	if complete, err := provider.EnsureVerifiedDestroyed(context.Background(), spec); complete || providerErrorCode(err) != "deployment_destroy_in_progress" {
		t.Fatalf("running instance complete=%v err=%v", complete, err)
	}
	if client.deleteCalls != 0 {
		t.Fatal("dependencies were deleted before the instance terminated")
	}
	client.instanceState = types.InstanceStateNameTerminated
	if complete, err := provider.EnsureVerifiedDestroyed(context.Background(), spec); !complete || err != nil {
		t.Fatalf("verified destroy complete=%v err=%v", complete, err)
	}
	if client.deleteCalls != 2 || client.interfaces[spec.NetworkInterfaceIDs[0]] || client.volumes[spec.VolumeIDs[0]] {
		t.Fatalf("dependency deletion state calls=%d interfaces=%v volumes=%v", client.deleteCalls, client.interfaces, client.volumes)
	}
}

type destroyEC2Fake struct {
	instanceState types.InstanceStateName
	interfaces    map[string]bool
	volumes       map[string]bool
	deleteCalls   int
}

func (fake *destroyEC2Fake) TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	return &ec2.TerminateInstancesOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: []types.Instance{{State: &types.InstanceState{Name: fake.instanceState}}}}}}, nil
}
func (fake *destroyEC2Fake) DeleteNetworkInterface(_ context.Context, input *ec2.DeleteNetworkInterfaceInput, _ ...func(*ec2.Options)) (*ec2.DeleteNetworkInterfaceOutput, error) {
	fake.deleteCalls++
	delete(fake.interfaces, aws.ToString(input.NetworkInterfaceId))
	return &ec2.DeleteNetworkInterfaceOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeNetworkInterfaces(_ context.Context, input *ec2.DescribeNetworkInterfacesInput, _ ...func(*ec2.Options)) (*ec2.DescribeNetworkInterfacesOutput, error) {
	if fake.interfaces[input.NetworkInterfaceIds[0]] {
		return &ec2.DescribeNetworkInterfacesOutput{NetworkInterfaces: []types.NetworkInterface{{NetworkInterfaceId: aws.String(input.NetworkInterfaceIds[0])}}}, nil
	}
	return nil, &smithy.GenericAPIError{Code: "InvalidNetworkInterfaceID.NotFound", Message: "gone"}
}
func (fake *destroyEC2Fake) DeleteVolume(_ context.Context, input *ec2.DeleteVolumeInput, _ ...func(*ec2.Options)) (*ec2.DeleteVolumeOutput, error) {
	fake.deleteCalls++
	delete(fake.volumes, aws.ToString(input.VolumeId))
	return &ec2.DeleteVolumeOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeVolumes(_ context.Context, input *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	if fake.volumes[input.VolumeIds[0]] {
		return &ec2.DescribeVolumesOutput{Volumes: []types.Volume{{VolumeId: aws.String(input.VolumeIds[0])}}}, nil
	}
	return nil, &smithy.GenericAPIError{Code: "InvalidVolume.NotFound", Message: "gone"}
}
