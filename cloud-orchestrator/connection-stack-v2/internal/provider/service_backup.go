package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

type EC2BackupAPI interface {
	CreateImage(context.Context, *ec2.CreateImageInput, ...func(*ec2.Options)) (*ec2.CreateImageOutput, error)
	DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
}

type EC2ServiceBackupProvider struct{ client EC2BackupAPI }

func NewEC2ServiceBackupProvider(client EC2BackupAPI) (*EC2ServiceBackupProvider, error) {
	if client == nil {
		return nil, api.NewError("service_backup_provider_unavailable", 503)
	}
	return &EC2ServiceBackupProvider{client: client}, nil
}

// EnsureBackup creates one EBS-backed AMI with a deterministic account/region
// unique name. EC2's unique AMI name is the mutation fence: a lost response or
// concurrent replay cannot create a second image. The receipt still exposes
// and validates every encrypted EBS snapshot plus the retained AMI resource.
func (provider *EC2ServiceBackupProvider) EnsureBackup(ctx context.Context, spec api.ServiceBackupSpec) (contract.ServiceBackupEvidence, bool, error) {
	if spec.ConnectionID == "" || spec.BackupID == "" || spec.ServiceID == "" || spec.DeploymentID == "" || spec.InstanceID == "" || len(spec.VolumeIDs) == 0 {
		return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_spec_invalid", 500)
	}
	volumeIDs := append([]string(nil), spec.VolumeIDs...)
	sort.Strings(volumeIDs)
	name := serviceBackupImageName(spec.ConnectionID, spec.BackupID)
	_, createErr := provider.client.CreateImage(ctx, &ec2.CreateImageInput{
		InstanceId: aws.String(spec.InstanceID), Name: aws.String(name), Description: aws.String("Dirextalk managed service backup " + spec.BackupID), NoReboot: aws.Bool(true),
		TagSpecifications: []types.TagSpecification{{ResourceType: types.ResourceTypeImage, Tags: serviceBackupTags(spec)}, {ResourceType: types.ResourceTypeSnapshot, Tags: serviceBackupTags(spec)}},
	})
	if createErr != nil && !awsTransitionPending(createErr, "InvalidAMIName.Duplicate") {
		return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_provider_unavailable", 503)
	}
	images, err := provider.client.DescribeImages(ctx, &ec2.DescribeImagesInput{Owners: []string{"self"}, Filters: []types.Filter{{Name: aws.String("name"), Values: []string{name}}}})
	if err != nil {
		return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_provider_unavailable", 503)
	}
	if len(images.Images) == 0 {
		return contract.ServiceBackupEvidence{}, false, nil
	}
	if len(images.Images) != 1 {
		return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
	}
	image := images.Images[0]
	if aws.ToString(image.ImageId) == "" || aws.ToString(image.Name) != name || !serviceBackupTagsMatch(image.Tags, spec) {
		return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
	}
	snapshotIDs := make([]string, 0, len(image.BlockDeviceMappings))
	for _, mapping := range image.BlockDeviceMappings {
		if mapping.Ebs != nil && aws.ToString(mapping.Ebs.SnapshotId) != "" {
			snapshotIDs = append(snapshotIDs, aws.ToString(mapping.Ebs.SnapshotId))
		}
	}
	if len(snapshotIDs) == 0 {
		return contract.ServiceBackupEvidence{}, false, nil
	}
	if len(snapshotIDs) != len(volumeIDs) {
		return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
	}
	readback, err := provider.client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: snapshotIDs})
	if err != nil {
		return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_provider_unavailable", 503)
	}
	byVolume := make(map[string]types.Snapshot, len(readback.Snapshots))
	for _, snapshot := range readback.Snapshots {
		volumeID := aws.ToString(snapshot.VolumeId)
		if _, duplicate := byVolume[volumeID]; duplicate {
			return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
		}
		byVolume[volumeID] = snapshot
	}
	evidence := contract.ServiceBackupEvidence{BackupID: spec.BackupID, ServiceID: spec.ServiceID, DeploymentID: spec.DeploymentID, InstanceID: spec.InstanceID, RetentionPolicy: "manual", ImageID: aws.ToString(image.ImageId)}
	complete := string(image.State) == "available"
	if string(image.State) == "failed" || string(image.State) == "error" {
		return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_failed", 502)
	}
	for _, volumeID := range volumeIDs {
		snapshot, ok := byVolume[volumeID]
		if !ok || !serviceBackupTagsMatch(snapshot.Tags, spec) {
			return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
		}
		state := string(snapshot.State)
		if state == "error" {
			return contract.ServiceBackupEvidence{}, false, api.NewError("service_backup_failed", 502)
		}
		if state != "completed" {
			complete = false
		}
		if !aws.ToBool(snapshot.Encrypted) && state == "completed" {
			return contract.ServiceBackupEvidence{}, false, api.NewError("provider_readback_invalid", 502)
		}
		evidence.Snapshots = append(evidence.Snapshots, contract.ServiceBackupSnapshot{VolumeID: volumeID, SnapshotID: aws.ToString(snapshot.SnapshotId), State: state, Encrypted: aws.ToBool(snapshot.Encrypted)})
	}
	return evidence, complete, nil
}

func serviceBackupImageName(connectionID, backupID string) string {
	sum := sha256.Sum256([]byte(connectionID + "\x00" + backupID))
	return "dirextalk-backup-" + hex.EncodeToString(sum[:24])
}

func serviceBackupTags(spec api.ServiceBackupSpec) []types.Tag {
	return []types.Tag{
		{Key: aws.String("DirextalkConnectionId"), Value: aws.String(spec.ConnectionID)}, {Key: aws.String("DirextalkBackupId"), Value: aws.String(spec.BackupID)},
		{Key: aws.String("DirextalkServiceId"), Value: aws.String(spec.ServiceID)}, {Key: aws.String("DirextalkDeploymentId"), Value: aws.String(spec.DeploymentID)},
		{Key: aws.String("DirextalkRetention"), Value: aws.String("manual")},
	}
}

func serviceBackupTagsMatch(tags []types.Tag, spec api.ServiceBackupSpec) bool {
	values := make(map[string]string, len(tags))
	for _, tag := range tags {
		values[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	for _, expected := range serviceBackupTags(spec) {
		if values[aws.ToString(expected.Key)] != aws.ToString(expected.Value) {
			return false
		}
	}
	return true
}
