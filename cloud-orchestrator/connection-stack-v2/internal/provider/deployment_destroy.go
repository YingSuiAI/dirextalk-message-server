package provider

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

type EC2DestroyAPI interface {
	TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DeleteNetworkInterface(context.Context, *ec2.DeleteNetworkInterfaceInput, ...func(*ec2.Options)) (*ec2.DeleteNetworkInterfaceOutput, error)
	DescribeNetworkInterfaces(context.Context, *ec2.DescribeNetworkInterfacesInput, ...func(*ec2.Options)) (*ec2.DescribeNetworkInterfacesOutput, error)
	DeleteVolume(context.Context, *ec2.DeleteVolumeInput, ...func(*ec2.Options)) (*ec2.DeleteVolumeOutput, error)
	DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
}

type SecretsManagerDestroyAPI interface {
	DeleteSecret(context.Context, *secretsmanager.DeleteSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error)
	DescribeSecret(context.Context, *secretsmanager.DescribeSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
}

type DeploymentSecretDestroyConfig struct {
	Client       SecretsManagerDestroyAPI
	ConnectionID string
}

type EC2DeploymentDestroyProvider struct {
	client       EC2DestroyAPI
	secretClient SecretsManagerDestroyAPI
	connectionID string
}

func NewEC2DeploymentDestroyProvider(client EC2DestroyAPI, secretConfig ...DeploymentSecretDestroyConfig) (*EC2DeploymentDestroyProvider, error) {
	if client == nil {
		return nil, api.NewError("deployment_destroy_provider_unavailable", 503)
	}
	provider := &EC2DeploymentDestroyProvider{client: client}
	if len(secretConfig) > 1 || (len(secretConfig) == 1 && (secretConfig[0].Client == nil || secretConfig[0].ConnectionID == "")) {
		return nil, api.NewError("deployment_destroy_provider_unavailable", 503)
	}
	if len(secretConfig) == 1 {
		provider.secretClient, provider.connectionID = secretConfig[0].Client, secretConfig[0].ConnectionID
	}
	return provider, nil
}

// EnsureVerifiedDestroyed advances the fixed dependency order and returns
// only after AWS read-back proves the instance, ENIs, and volumes absent.
// Retriable AWS transition states never become a successful receipt.
func (provider *EC2DeploymentDestroyProvider) EnsureVerifiedDestroyed(ctx context.Context, spec api.DeploymentDestroySpec) (bool, error) {
	if spec.InstanceID == "" || len(spec.VolumeIDs) == 0 || len(spec.NetworkInterfaceIDs) == 0 || (len(spec.SecretRefs) != 0 && (provider.secretClient == nil || spec.ConnectionID != provider.connectionID || spec.DeploymentID == "")) {
		return false, api.NewError("deployment_destroy_spec_invalid", 500)
	}
	if _, err := provider.client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{spec.InstanceID}}); err != nil && !awsNotFound(err, "InvalidInstanceID.NotFound") {
		return false, deploymentDestroyError(err)
	}
	instances, err := provider.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{spec.InstanceID}})
	if err != nil {
		if !awsNotFound(err, "InvalidInstanceID.NotFound") {
			return false, deploymentDestroyError(err)
		}
	} else if len(instances.Reservations) != 1 || len(instances.Reservations[0].Instances) != 1 || instances.Reservations[0].Instances[0].State == nil || string(instances.Reservations[0].Instances[0].State.Name) != "terminated" {
		return false, api.NewError("deployment_destroy_in_progress", 409)
	}
	for _, interfaceID := range spec.NetworkInterfaceIDs {
		if _, err := provider.client.DeleteNetworkInterface(ctx, &ec2.DeleteNetworkInterfaceInput{NetworkInterfaceId: aws.String(interfaceID)}); err != nil && !awsNotFound(err, "InvalidNetworkInterfaceID.NotFound") {
			if awsTransitionPending(err, "InvalidParameterValue", "DependencyViolation") {
				return false, api.NewError("deployment_destroy_in_progress", 409)
			}
			return false, deploymentDestroyError(err)
		}
	}
	for _, volumeID := range spec.VolumeIDs {
		if _, err := provider.client.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: aws.String(volumeID)}); err != nil && !awsNotFound(err, "InvalidVolume.NotFound") {
			if awsTransitionPending(err, "VolumeInUse", "IncorrectState") {
				return false, api.NewError("deployment_destroy_in_progress", 409)
			}
			return false, deploymentDestroyError(err)
		}
	}
	for _, interfaceID := range spec.NetworkInterfaceIDs {
		interfaces, err := provider.client.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{NetworkInterfaceIds: []string{interfaceID}})
		if err != nil {
			if !awsNotFound(err, "InvalidNetworkInterfaceID.NotFound") {
				return false, deploymentDestroyError(err)
			}
		} else if len(interfaces.NetworkInterfaces) != 0 {
			return false, api.NewError("deployment_destroy_in_progress", 409)
		}
	}
	for _, volumeID := range spec.VolumeIDs {
		volumes, err := provider.client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{volumeID}})
		if err != nil {
			if !awsNotFound(err, "InvalidVolume.NotFound") {
				return false, deploymentDestroyError(err)
			}
		} else if len(volumes.Volumes) != 0 {
			return false, api.NewError("deployment_destroy_in_progress", 409)
		}
	}
	for _, secretRef := range spec.SecretRefs {
		name := serviceSecretName(spec.ConnectionID, spec.DeploymentID, secretRef)
		if _, err := provider.secretClient.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{SecretId: aws.String(name), ForceDeleteWithoutRecovery: aws.Bool(true)}); err != nil && !awsNotFound(err, "ResourceNotFoundException") {
			if !awsTransitionPending(err, "InvalidRequestException") {
				return false, deploymentDestroyError(err)
			}
		}
	}
	for _, secretRef := range spec.SecretRefs {
		name := serviceSecretName(spec.ConnectionID, spec.DeploymentID, secretRef)
		if _, err := provider.secretClient.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(name)}); err == nil {
			return false, api.NewError("deployment_destroy_in_progress", 409)
		} else if !awsNotFound(err, "ResourceNotFoundException") {
			return false, deploymentDestroyError(err)
		}
	}
	return true, nil
}

func deploymentDestroyError(err error) error {
	if awsTransitionPending(err, "AccessDenied", "AccessDeniedException", "UnauthorizedOperation") {
		return api.NewError("deployment_destroy_forbidden", 403)
	}
	return api.NewError("deployment_destroy_provider_unavailable", 503)
}

func awsNotFound(err error, codes ...string) bool { return awsTransitionPending(err, codes...) }

func awsTransitionPending(err error, codes ...string) bool {
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	for _, code := range codes {
		if apiError.ErrorCode() == code {
			return true
		}
	}
	return false
}
