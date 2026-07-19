package provider

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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

func TestDeploymentDestroyDeletesSecretsOnlyAfterEC2AndRequiresNotFoundReadBack(t *testing.T) {
	ec2Client := &destroyEC2Fake{instanceState: types.InstanceStateNameTerminated, interfaces: map[string]bool{}, volumes: map[string]bool{}}
	secretClient := &destroySecretsFake{present: true}
	provider, err := NewEC2DeploymentDestroyProvider(ec2Client, DeploymentSecretDestroyConfig{Client: secretClient, ConnectionID: "connection-0001"})
	if err != nil {
		t.Fatal(err)
	}
	spec := api.DeploymentDestroySpec{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:model-token"}}
	if complete, destroyErr := provider.EnsureVerifiedDestroyed(context.Background(), spec); !complete || destroyErr != nil || secretClient.deleteCalls != 1 || secretClient.describeCalls != 1 || !secretClient.force {
		t.Fatalf("secret destroy complete=%v err=%v client=%#v", complete, destroyErr, secretClient)
	}
	if complete, replayErr := provider.EnsureVerifiedDestroyed(context.Background(), spec); !complete || replayErr != nil || secretClient.deleteCalls != 2 {
		t.Fatalf("response-loss replay complete=%v err=%v calls=%d", complete, replayErr, secretClient.deleteCalls)
	}
	secretClient.errCode = "AccessDeniedException"
	if complete, deniedErr := provider.EnsureVerifiedDestroyed(context.Background(), spec); complete || providerErrorCode(deniedErr) != "deployment_destroy_forbidden" {
		t.Fatalf("AccessDenied complete=%v err=%v", complete, deniedErr)
	}
}

func TestDeploymentDestroyMapsEC2AccessDeniedToForbidden(t *testing.T) {
	for _, operation := range []string{"terminate", "describe-instance", "delete-eni", "delete-volume", "describe-eni", "describe-volume"} {
		t.Run(operation, func(t *testing.T) {
			client := &destroyEC2Fake{instanceState: types.InstanceStateNameTerminated, interfaces: map[string]bool{}, volumes: map[string]bool{}, deniedOperation: operation}
			provider, err := NewEC2DeploymentDestroyProvider(client)
			if err != nil {
				t.Fatal(err)
			}
			spec := api.DeploymentDestroySpec{InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}}
			complete, destroyErr := provider.EnsureVerifiedDestroyed(context.Background(), spec)
			if complete || providerErrorCode(destroyErr) != "deployment_destroy_forbidden" {
				t.Fatalf("AccessDenied operation=%s complete=%v err=%v", operation, complete, destroyErr)
			}
		})
	}
}

type destroySecretsFake struct {
	present                    bool
	errCode                    string
	deleteCalls, describeCalls int
	force                      bool
}

func (f *destroySecretsFake) DeleteSecret(_ context.Context, in *secretsmanager.DeleteSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error) {
	f.deleteCalls++
	f.force = aws.ToBool(in.ForceDeleteWithoutRecovery)
	if f.errCode != "" {
		return nil, &smithy.GenericAPIError{Code: f.errCode, Message: "denied"}
	}
	f.present = false
	return &secretsmanager.DeleteSecretOutput{}, nil
}

func (f *destroySecretsFake) DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	f.describeCalls++
	if f.errCode != "" {
		return nil, &smithy.GenericAPIError{Code: f.errCode, Message: "denied"}
	}
	if !f.present {
		return nil, &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: "gone"}
	}
	return &secretsmanager.DescribeSecretOutput{}, nil
}

type destroyEC2Fake struct {
	instanceState   types.InstanceStateName
	interfaces      map[string]bool
	volumes         map[string]bool
	deleteCalls     int
	deniedOperation string
}

func (fake *destroyEC2Fake) TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	if fake.deniedOperation == "terminate" {
		return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "denied"}
	}
	return &ec2.TerminateInstancesOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	if fake.deniedOperation == "describe-instance" {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
	}
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: []types.Instance{{State: &types.InstanceState{Name: fake.instanceState}}}}}}, nil
}
func (fake *destroyEC2Fake) DeleteNetworkInterface(_ context.Context, input *ec2.DeleteNetworkInterfaceInput, _ ...func(*ec2.Options)) (*ec2.DeleteNetworkInterfaceOutput, error) {
	if fake.deniedOperation == "delete-eni" {
		return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "denied"}
	}
	fake.deleteCalls++
	delete(fake.interfaces, aws.ToString(input.NetworkInterfaceId))
	return &ec2.DeleteNetworkInterfaceOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeNetworkInterfaces(_ context.Context, input *ec2.DescribeNetworkInterfacesInput, _ ...func(*ec2.Options)) (*ec2.DescribeNetworkInterfacesOutput, error) {
	if fake.deniedOperation == "describe-eni" {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
	}
	if fake.interfaces[input.NetworkInterfaceIds[0]] {
		return &ec2.DescribeNetworkInterfacesOutput{NetworkInterfaces: []types.NetworkInterface{{NetworkInterfaceId: aws.String(input.NetworkInterfaceIds[0])}}}, nil
	}
	return nil, &smithy.GenericAPIError{Code: "InvalidNetworkInterfaceID.NotFound", Message: "gone"}
}
func (fake *destroyEC2Fake) DeleteVolume(_ context.Context, input *ec2.DeleteVolumeInput, _ ...func(*ec2.Options)) (*ec2.DeleteVolumeOutput, error) {
	if fake.deniedOperation == "delete-volume" {
		return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "denied"}
	}
	fake.deleteCalls++
	delete(fake.volumes, aws.ToString(input.VolumeId))
	return &ec2.DeleteVolumeOutput{}, nil
}
func (fake *destroyEC2Fake) DescribeVolumes(_ context.Context, input *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	if fake.deniedOperation == "describe-volume" {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
	}
	if fake.volumes[input.VolumeIds[0]] {
		return &ec2.DescribeVolumesOutput{Volumes: []types.Volume{{VolumeId: aws.String(input.VolumeIds[0])}}}, nil
	}
	return nil, &smithy.GenericAPIError{Code: "InvalidVolume.NotFound", Message: "gone"}
}
