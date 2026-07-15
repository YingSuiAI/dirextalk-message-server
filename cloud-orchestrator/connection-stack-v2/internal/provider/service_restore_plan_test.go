package provider

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pricing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestServiceRestorePlanProviderBindsCurrentMappingSnapshotAndStoragePrice(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	fake := restorePlanAWSFake{}
	provider, err := NewServiceRestorePlanProvider(fake, fake)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.ServiceRestorePlanRequest{Schema: contract.ServiceRestorePlanSchema, RestorePlanID: "restore-plan-0001", ServiceID: "service-restore-0001", DeploymentID: "deployment-restore-0001", BackupID: "backup-restore-0001", InstanceID: "i-0123456789abcdef0", Region: "us-east-1", ImageID: "ami-0123456789abcdef0", SnapshotRefs: []contract.ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0"}}}
	command := restorePlanCommand(t, request, now)
	plan, err := provider.Plan(t.Context(), command, request, now)
	if err != nil || plan.AvailabilityZone != "us-east-1a" || plan.EstimatedThirtyDayMinor != 640 || plan.EstimatedHourlyMinor != 1 || len(plan.VolumeSwaps) != 1 || plan.VolumeSwaps[0].DeviceName != "/dev/xvda" || !plan.VolumeSwaps[0].DeleteOnTermination {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}

	fake.imageDevice = "/dev/xvdf"
	provider, _ = NewServiceRestorePlanProvider(fake, fake)
	if _, err = provider.Plan(t.Context(), command, request, now); providerErrorCode(err) != "service_restore_readback_invalid" {
		t.Fatalf("AMI mapping drift err=%v", err)
	}
}

type restorePlanAWSFake struct{ imageDevice string }

func (f restorePlanAWSFake) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: []types.Instance{{InstanceId: aws.String("i-0123456789abcdef0"), State: &types.InstanceState{Name: types.InstanceStateNameRunning}, Placement: &types.Placement{AvailabilityZone: aws.String("us-east-1a")}, BlockDeviceMappings: []types.InstanceBlockDeviceMapping{{DeviceName: aws.String("/dev/xvda"), Ebs: &types.EbsInstanceBlockDevice{VolumeId: aws.String("vol-0123456789abcdef0"), DeleteOnTermination: aws.Bool(true)}}}}}}}}, nil
}
func (f restorePlanAWSFake) DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	return &ec2.DescribeVolumesOutput{Volumes: []types.Volume{{VolumeId: aws.String("vol-0123456789abcdef0"), AvailabilityZone: aws.String("us-east-1a"), Encrypted: aws.Bool(true), State: types.VolumeStateInUse, VolumeType: types.VolumeTypeGp3, Size: aws.Int32(80), Iops: aws.Int32(3000), Throughput: aws.Int32(125), Attachments: []types.VolumeAttachment{{InstanceId: aws.String("i-0123456789abcdef0"), Device: aws.String("/dev/xvda"), State: types.VolumeAttachmentStateAttached}}}}}, nil
}
func (f restorePlanAWSFake) DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	return &ec2.DescribeSnapshotsOutput{Snapshots: []types.Snapshot{{SnapshotId: aws.String("snap-0123456789abcdef0"), VolumeId: aws.String("vol-0123456789abcdef0"), State: types.SnapshotStateCompleted, Encrypted: aws.Bool(true)}}}, nil
}
func (f restorePlanAWSFake) DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	device := f.imageDevice
	if device == "" {
		device = "/dev/xvda"
	}
	return &ec2.DescribeImagesOutput{Images: []types.Image{{ImageId: aws.String("ami-0123456789abcdef0"), State: types.ImageStateAvailable, BlockDeviceMappings: []types.BlockDeviceMapping{{DeviceName: aws.String(device), Ebs: &types.EbsBlockDevice{SnapshotId: aws.String("snap-0123456789abcdef0")}}}}}}, nil
}
func (f restorePlanAWSFake) GetProducts(context.Context, *pricing.GetProductsInput, ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	raw := fmt.Sprintf(`{"product":{"productFamily":"Storage","attributes":{"regionCode":"us-east-1","volumeApiName":"gp3"}},"terms":{"OnDemand":{"term":{"priceDimensions":{"dimension":{"unit":"GB-Mo","beginRange":"0","endRange":"Inf","pricePerUnit":{"USD":"0.08"}}}}}}}`)
	return &pricing.GetProductsOutput{PriceList: []string{raw}}, nil
}

func restorePlanCommand(t *testing.T, request contract.ServiceRestorePlanRequest, now time.Time) contract.Command {
	t.Helper()
	// A zeroed signature is structurally valid here; node verification belongs
	// to the API boundary tests and the provider consumes an already verified command.
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return contract.Command{Schema: contract.CommandSchema, ConnectionID: "connection-0001", CommandID: "command-restore-plan-0001", NodeKeyID: "node-key-1", IssuedAt: contract.CanonicalInstant(now), ExpiresAt: contract.CanonicalInstant(now.Add(5 * time.Minute)), ExpectedGeneration: 1, NodeCounter: 1, Action: contract.ActionServiceRestorePlan, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(sum[:]), SignatureB64: base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
}
