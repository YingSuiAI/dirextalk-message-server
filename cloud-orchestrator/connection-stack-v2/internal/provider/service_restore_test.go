package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestEC2ServiceRestoreProviderConvergesExactInPlaceSwap(t *testing.T) {
	fake := newRestoreEC2Fake()
	provider, err := NewEC2ServiceRestoreProvider(fake)
	if err != nil {
		t.Fatal(err)
	}
	evidence := runRestoreUntilComplete(t, provider, restoreProviderSpec(), 12)
	if evidence.Outcome != "restored" || evidence.InstanceState != "running" || evidence.FallbackVerified || len(evidence.Replacements) != 1 || evidence.Replacements[0].State != "attached_current" {
		t.Fatalf("unexpected restore evidence: %#v", evidence)
	}
	if fake.createTokens[0] != restoreVolumeClientToken(restoreProviderSpec(), restoreProviderSpec().VolumeSwaps[0]) {
		t.Fatal("replacement volume did not use deterministic client token")
	}
	if fake.mappingVolume != fake.replacementID || !fake.deleteOnTermination {
		t.Fatalf("replacement mapping not read back: volume=%s dot=%v", fake.mappingVolume, fake.deleteOnTermination)
	}
}

func TestEC2ServiceRestoreProviderReattachesOriginalAfterPartialFailure(t *testing.T) {
	fake := newRestoreEC2Fake()
	fake.failReplacementAttach = true
	provider, err := NewEC2ServiceRestoreProvider(fake)
	if err != nil {
		t.Fatal(err)
	}
	evidence := runRestoreUntilComplete(t, provider, restoreProviderSpec(), 12)
	if evidence.Outcome != "original_restored" || !evidence.FallbackVerified || evidence.InstanceState != "running" || evidence.Replacements[0].State != "retained_detached" {
		t.Fatalf("fallback was not independently verified: %#v", evidence)
	}
	if fake.mappingVolume != restoreProviderSpec().VolumeSwaps[0].OriginalVolumeID || fake.phase != restorePhaseFallback {
		t.Fatalf("original mapping or durable fallback marker missing: volume=%s phase=%s", fake.mappingVolume, fake.phase)
	}
}

func runRestoreUntilComplete(t *testing.T, provider *EC2ServiceRestoreProvider, spec api.ServiceRestoreSpec, attempts int) contract.ServiceRestoreAWSEvidence {
	t.Helper()
	for range attempts {
		evidence, complete, err := provider.EnsureRestore(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			return evidence
		}
	}
	t.Fatal("restore did not converge")
	return contract.ServiceRestoreAWSEvidence{}
}

func restoreProviderSpec() api.ServiceRestoreSpec {
	return api.ServiceRestoreSpec{
		ConnectionID:     "connection-restore-0001",
		RestoreID:        "restore-provider-0001",
		ServiceID:        "service-restore-0001",
		DeploymentID:     "deployment-restore-0001",
		BackupID:         "backup-restore-0001",
		InstanceID:       "i-0123456789abcdef0",
		Region:           "ap-south-1",
		AvailabilityZone: "ap-south-1a",
		VolumeSwaps: []contract.ServiceRestoreVolumeSwap{{
			OriginalVolumeID:    "vol-0123456789abcdef0",
			SnapshotID:          "snap-0123456789abcdef0",
			DeviceName:          "/dev/xvda",
			VolumeType:          "gp3",
			SizeGiB:             80,
			IOPS:                3000,
			ThroughputMiB:       125,
			Encrypted:           true,
			DeleteOnTermination: true,
		}},
	}
}

type restoreEC2Fake struct {
	instanceState         types.InstanceStateName
	mappingVolume         string
	mappingDevice         string
	replacementID         string
	replacement           *types.Volume
	phase                 string
	deleteOnTermination   bool
	failReplacementAttach bool
	createTokens          []string
}

func newRestoreEC2Fake() *restoreEC2Fake {
	return &restoreEC2Fake{instanceState: types.InstanceStateNameRunning, mappingVolume: "vol-0123456789abcdef0", mappingDevice: "/dev/xvda", replacementID: "vol-0fedcba9876543210", deleteOnTermination: true}
}

func (f *restoreEC2Fake) CreateVolume(_ context.Context, in *ec2.CreateVolumeInput, _ ...func(*ec2.Options)) (*ec2.CreateVolumeOutput, error) {
	f.createTokens = append(f.createTokens, aws.ToString(in.ClientToken))
	tags := append([]types.Tag(nil), in.TagSpecifications[0].Tags...)
	f.replacement = &types.Volume{VolumeId: aws.String(f.replacementID), SnapshotId: in.SnapshotId, AvailabilityZone: in.AvailabilityZone, Encrypted: in.Encrypted, State: types.VolumeStateAvailable, Tags: tags, VolumeType: in.VolumeType, Size: in.Size, Iops: in.Iops, Throughput: in.Throughput}
	return &ec2.CreateVolumeOutput{VolumeId: aws.String(f.replacementID)}, nil
}

func (f *restoreEC2Fake) DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	if f.replacement == nil {
		return &ec2.DescribeVolumesOutput{}, nil
	}
	copyVolume := *f.replacement
	copyVolume.Tags = append([]types.Tag(nil), f.replacement.Tags...)
	if f.phase != "" {
		copyVolume.Tags = append(copyVolume.Tags, types.Tag{Key: aws.String("DirextalkRestorePhase"), Value: aws.String(f.phase)})
	}
	if f.mappingVolume == f.replacementID {
		copyVolume.State = types.VolumeStateInUse
		copyVolume.Attachments = []types.VolumeAttachment{{Device: aws.String(f.mappingDevice), InstanceId: aws.String("i-0123456789abcdef0"), State: types.VolumeAttachmentStateAttached, VolumeId: aws.String(f.replacementID)}}
	} else {
		copyVolume.State = types.VolumeStateAvailable
		copyVolume.Attachments = nil
	}
	return &ec2.DescribeVolumesOutput{Volumes: []types.Volume{copyVolume}}, nil
}

func (f *restoreEC2Fake) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	instance := types.Instance{
		InstanceId: aws.String("i-0123456789abcdef0"),
		State:      &types.InstanceState{Name: f.instanceState},
		Placement:  &types.Placement{AvailabilityZone: aws.String("ap-south-1a")},
		Tags: []types.Tag{
			{Key: aws.String("dirextalk:managed"), Value: aws.String("true")},
			{Key: aws.String("dirextalk:connection-id"), Value: aws.String("connection-restore-0001")},
			{Key: aws.String("dirextalk:deployment-id"), Value: aws.String("deployment-restore-0001")},
		},
	}
	if f.mappingVolume != "" {
		instance.BlockDeviceMappings = []types.InstanceBlockDeviceMapping{{DeviceName: aws.String(f.mappingDevice), Ebs: &types.EbsInstanceBlockDevice{VolumeId: aws.String(f.mappingVolume), DeleteOnTermination: aws.Bool(f.deleteOnTermination)}}}
	}
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: []types.Instance{instance}}}}, nil
}

func (f *restoreEC2Fake) StopInstances(context.Context, *ec2.StopInstancesInput, ...func(*ec2.Options)) (*ec2.StopInstancesOutput, error) {
	f.instanceState = types.InstanceStateNameStopped
	return &ec2.StopInstancesOutput{}, nil
}

func (f *restoreEC2Fake) StartInstances(context.Context, *ec2.StartInstancesInput, ...func(*ec2.Options)) (*ec2.StartInstancesOutput, error) {
	f.instanceState = types.InstanceStateNameRunning
	return &ec2.StartInstancesOutput{}, nil
}

func (f *restoreEC2Fake) DetachVolume(_ context.Context, in *ec2.DetachVolumeInput, _ ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error) {
	if f.mappingVolume == aws.ToString(in.VolumeId) {
		f.mappingVolume = ""
	}
	return &ec2.DetachVolumeOutput{}, nil
}

func (f *restoreEC2Fake) AttachVolume(_ context.Context, in *ec2.AttachVolumeInput, _ ...func(*ec2.Options)) (*ec2.AttachVolumeOutput, error) {
	volumeID := aws.ToString(in.VolumeId)
	if volumeID == f.replacementID && f.failReplacementAttach {
		f.failReplacementAttach = false
		return nil, errors.New("injected replacement attach failure")
	}
	f.mappingVolume = volumeID
	f.mappingDevice = aws.ToString(in.Device)
	return &ec2.AttachVolumeOutput{}, nil
}

func (f *restoreEC2Fake) ModifyInstanceAttribute(_ context.Context, in *ec2.ModifyInstanceAttributeInput, _ ...func(*ec2.Options)) (*ec2.ModifyInstanceAttributeOutput, error) {
	f.deleteOnTermination = aws.ToBool(in.BlockDeviceMappings[0].Ebs.DeleteOnTermination)
	return &ec2.ModifyInstanceAttributeOutput{}, nil
}

func (f *restoreEC2Fake) CreateTags(_ context.Context, in *ec2.CreateTagsInput, _ ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	f.phase = aws.ToString(in.Tags[0].Value)
	return &ec2.CreateTagsOutput{}, nil
}
