package provider

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

func TestEC2ServiceBackupProviderUsesUniqueImageFenceAndEncryptedReadBack(t *testing.T) {
	fake := &backupEC2Fake{state: types.ImageStatePending, snapshotState: types.SnapshotStatePending}
	provider, e := NewEC2ServiceBackupProvider(fake)
	if e != nil {
		t.Fatal(e)
	}
	spec := api.ServiceBackupSpec{ConnectionID: "connection-backup-0001", BackupID: "backup-0001", ServiceID: "service-backup-0001", DeploymentID: "deployment-backup-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}}
	if _, complete, e := provider.EnsureBackup(t.Context(), spec); e != nil || complete {
		t.Fatalf("pending complete=%v err=%v", complete, e)
	}
	if !fake.noReboot || len(fake.tagResources) != 2 || fake.tagResources[0] != types.ResourceTypeImage || fake.tagResources[1] != types.ResourceTypeSnapshot {
		t.Fatalf("create image boundary no_reboot=%v tag_resources=%v", fake.noReboot, fake.tagResources)
	}
	fake.duplicate = true
	fake.state = types.ImageStateAvailable
	fake.snapshotState = types.SnapshotStateCompleted
	fake.encrypted = true
	evidence, complete, e := provider.EnsureBackup(t.Context(), spec)
	if e != nil || !complete || evidence.ImageID != "ami-0123456789abcdef0" || len(evidence.Snapshots) != 1 {
		t.Fatalf("complete=%v evidence=%#v err=%v", complete, evidence, e)
	}
	if fake.names[0] != fake.names[1] {
		t.Fatalf("image fence drifted: %v", fake.names)
	}
	fake.encrypted = false
	if _, complete, e = provider.EnsureBackup(t.Context(), spec); complete || providerErrorCode(e) != "provider_readback_invalid" {
		t.Fatalf("unencrypted complete=%v err=%v", complete, e)
	}
}

type backupEC2Fake struct {
	state                types.ImageState
	snapshotState        types.SnapshotState
	encrypted, duplicate bool
	names                []string
	tags                 []types.Tag
	noReboot             bool
	tagResources         []types.ResourceType
}

func (f *backupEC2Fake) CreateImage(_ context.Context, in *ec2.CreateImageInput, _ ...func(*ec2.Options)) (*ec2.CreateImageOutput, error) {
	f.names = append(f.names, aws.ToString(in.Name))
	f.tags = in.TagSpecifications[0].Tags
	f.noReboot = aws.ToBool(in.NoReboot)
	f.tagResources = f.tagResources[:0]
	for _, tags := range in.TagSpecifications {
		f.tagResources = append(f.tagResources, tags.ResourceType)
	}
	if f.duplicate {
		return nil, &smithy.GenericAPIError{Code: "InvalidAMIName.Duplicate", Message: "exists"}
	}
	return &ec2.CreateImageOutput{ImageId: aws.String("ami-0123456789abcdef0")}, nil
}
func (f *backupEC2Fake) DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	return &ec2.DescribeImagesOutput{Images: []types.Image{{ImageId: aws.String("ami-0123456789abcdef0"), Name: aws.String(f.names[len(f.names)-1]), State: f.state, Tags: f.tags, BlockDeviceMappings: []types.BlockDeviceMapping{{Ebs: &types.EbsBlockDevice{SnapshotId: aws.String("snap-0123456789abcdef0")}}}}}}, nil
}
func (f *backupEC2Fake) DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	return &ec2.DescribeSnapshotsOutput{Snapshots: []types.Snapshot{{SnapshotId: aws.String("snap-0123456789abcdef0"), VolumeId: aws.String("vol-0123456789abcdef0"), State: f.snapshotState, Encrypted: aws.Bool(f.encrypted), Tags: f.tags}}}, nil
}
