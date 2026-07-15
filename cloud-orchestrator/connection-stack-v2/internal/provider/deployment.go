package provider

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

type EC2DeploymentAPI interface {
	RunInstances(ctx context.Context, params *ec2.RunInstancesInput, optFns ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error)
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DescribeVolumes(ctx context.Context, params *ec2.DescribeVolumesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
}

type EC2DeploymentProvider struct{ client EC2DeploymentAPI }

func NewEC2DeploymentProvider(client EC2DeploymentAPI) (*EC2DeploymentProvider, error) {
	if client == nil {
		return nil, api.NewError("deployment_provider_unavailable", 503)
	}
	return &EC2DeploymentProvider{client: client}, nil
}

func (p *EC2DeploymentProvider) EnsureCreated(ctx context.Context, spec api.DeploymentSpec) (string, error) {
	if !validDeploymentSpec(spec) {
		return "", api.NewError("deployment_spec_invalid", 500)
	}
	input := &ec2.RunInstancesInput{ImageId: aws.String(spec.AMIId), InstanceType: ec2types.InstanceType(spec.InstanceType), MinCount: aws.Int32(1), MaxCount: aws.Int32(1), ClientToken: aws.String(spec.ClientToken), Placement: &ec2types.Placement{AvailabilityZone: aws.String(spec.AvailabilityZone)}, InstanceInitiatedShutdownBehavior: ec2types.ShutdownBehaviorStop, MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{HttpEndpoint: ec2types.InstanceMetadataEndpointStateEnabled, HttpTokens: ec2types.HttpTokensStateRequired, HttpPutResponseHopLimit: aws.Int32(1)}, NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{DeviceIndex: aws.Int32(0), SubnetId: aws.String(spec.SubnetID), Groups: []string{spec.SecurityGroupID}, AssociatePublicIpAddress: aws.Bool(false), DeleteOnTermination: aws.Bool(false)}}, BlockDeviceMappings: []ec2types.BlockDeviceMapping{{DeviceName: aws.String("/dev/xvda"), Ebs: &ec2types.EbsBlockDevice{Encrypted: aws.Bool(true), DeleteOnTermination: aws.Bool(false), VolumeSize: aws.Int32(int32(spec.DiskGiB)), VolumeType: ec2types.VolumeTypeGp3}}}, TagSpecifications: deploymentTagSpecifications(spec)}
	output, err := p.client.RunInstances(ctx, input)
	if err != nil {
		return "", api.NewError("deployment_provider_unavailable", 503)
	}
	if len(output.Instances) != 1 || output.Instances[0].InstanceId == nil {
		return "", api.NewError("provider_readback_invalid", 502)
	}
	return *output.Instances[0].InstanceId, nil
}

func (p *EC2DeploymentProvider) ReadBack(ctx context.Context, spec api.DeploymentSpec, instanceID string) (api.DeploymentEvidence, error) {
	if !validDeploymentSpec(spec) || instanceID == "" {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
	}
	output, err := p.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	if err != nil || len(output.Reservations) != 1 || len(output.Reservations[0].Instances) != 1 {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_unavailable", 503)
	}
	instance := output.Reservations[0].Instances[0]
	if aws.ToString(instance.InstanceId) != instanceID || aws.ToString(instance.ImageId) != spec.AMIId || string(instance.InstanceType) != spec.InstanceType || aws.ToString(instance.SubnetId) != spec.SubnetID || aws.ToString(instance.VpcId) != spec.VPCID || instance.Placement == nil || aws.ToString(instance.Placement.AvailabilityZone) != spec.AvailabilityZone || instance.PublicIpAddress != nil || instance.IamInstanceProfile != nil || len(instance.SecurityGroups) != 1 || aws.ToString(instance.SecurityGroups[0].GroupId) != spec.SecurityGroupID || !hasDeploymentTags(instance.Tags, spec) {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
	}
	volumeIDs := []string{}
	for _, device := range instance.BlockDeviceMappings {
		if device.Ebs != nil && device.Ebs.VolumeId != nil {
			volumeIDs = append(volumeIDs, *device.Ebs.VolumeId)
		}
	}
	interfaceIDs := []string{}
	for _, network := range instance.NetworkInterfaces {
		if network.NetworkInterfaceId == nil || network.Association != nil {
			return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
		}
		interfaceIDs = append(interfaceIDs, *network.NetworkInterfaceId)
	}
	if len(volumeIDs) == 0 || len(interfaceIDs) == 0 {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
	}
	volumes, err := p.client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: volumeIDs})
	if err != nil {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_unavailable", 503)
	}
	if len(volumes.Volumes) != len(volumeIDs) {
		return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
	}
	for _, volume := range volumes.Volumes {
		if volume.VolumeId == nil || volume.Encrypted == nil || !*volume.Encrypted || volume.Size == nil || int64(*volume.Size) < spec.DiskGiB || volume.VolumeType != ec2types.VolumeTypeGp3 || !hasDeploymentTags(volume.Tags, spec) {
			return api.DeploymentEvidence{}, api.NewError("provider_readback_invalid", 502)
		}
	}
	sort.Strings(volumeIDs)
	sort.Strings(interfaceIDs)
	return api.DeploymentEvidence{InstanceID: instanceID, VolumeIDs: volumeIDs, NetworkInterfaceIDs: interfaceIDs}, nil
}

func deploymentTagSpecifications(spec api.DeploymentSpec) []ec2types.TagSpecification {
	tags := deploymentTags(spec)
	return []ec2types.TagSpecification{{ResourceType: ec2types.ResourceTypeInstance, Tags: tags}, {ResourceType: ec2types.ResourceTypeVolume, Tags: tags}}
}
func deploymentTags(spec api.DeploymentSpec) []ec2types.Tag {
	return []ec2types.Tag{{Key: aws.String("dirextalk:managed"), Value: aws.String("true")}, {Key: aws.String("dirextalk:connection-id"), Value: aws.String(spec.ConnectionID)}, {Key: aws.String("dirextalk:deployment-id"), Value: aws.String(spec.DeploymentID)}}
}
func hasDeploymentTags(tags []ec2types.Tag, spec api.DeploymentSpec) bool {
	values := map[string]string{}
	for _, tag := range tags {
		values[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return values["dirextalk:managed"] == "true" && values["dirextalk:connection-id"] == spec.ConnectionID && values["dirextalk:deployment-id"] == spec.DeploymentID
}
func validDeploymentSpec(spec api.DeploymentSpec) bool {
	return spec.ConnectionID != "" && spec.DeploymentID != "" && len(spec.ClientToken) == 64 && spec.AMIId != "" && spec.InstanceType != "" && (spec.Architecture == "amd64" || spec.Architecture == "arm64") && spec.DiskGiB >= 8 && spec.DiskGiB <= 16384 && spec.VPCID != "" && spec.SubnetID != "" && spec.AvailabilityZone != "" && spec.SecurityGroupID != ""
}
