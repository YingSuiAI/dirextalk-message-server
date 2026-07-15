package workerimage

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAWSProviderUsesVersionedS3AndLockedDownBuilder(t *testing.T) {
	ec2Fake := &fakeEC2API{}
	s3Fake := &fakeS3API{}
	provider := &AWSProvider{ec2: ec2Fake, s3: s3Fake, presign: fakePresigner{}}
	filePath := filepath.Join(t.TempDir(), "artifact.tar")
	raw := []byte("deterministic archive")
	if err := os.WriteFile(filePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	file, _ := os.Open(filePath)
	defer file.Close()
	digest := namedBytes(raw)
	upload, err := provider.PutArtifact(context.Background(), "versioned-bucket", "worker/artifact.tar", "v1.2.0-stage-t.1", file, int64(len(raw)), digest)
	if err != nil || upload.VersionID != "s3-version-1" || s3Fake.input == nil || aws.ToString(s3Fake.input.ChecksumSHA256) == "" {
		t.Fatalf("PutArtifact=%#v err=%v", upload, err)
	}
	launch, err := provider.LaunchBuilder(context.Background(), LaunchSpec{Name: "builder", ClientToken: "token", BaseAMIID: "ami-base", SubnetID: "subnet-private", SecurityGroupID: "sg-zero-ingress", InstanceType: "m7i.large", UserData: "#!/bin/bash\nfixed", Tags: map[string]string{"dirextalk:managed": "true"}})
	if err != nil || launch.InstanceID == "" {
		t.Fatalf("LaunchBuilder=%#v err=%v", launch, err)
	}
	input := ec2Fake.runInput
	if input == nil || len(input.NetworkInterfaces) != 1 || aws.ToBool(input.NetworkInterfaces[0].AssociatePublicIpAddress) || input.IamInstanceProfile != nil || input.MetadataOptions == nil || input.MetadataOptions.HttpTokens != ec2types.HttpTokensStateRequired || len(input.BlockDeviceMappings) != 1 || !aws.ToBool(input.BlockDeviceMappings[0].Ebs.Encrypted) || !aws.ToBool(input.BlockDeviceMappings[0].Ebs.DeleteOnTermination) {
		t.Fatalf("unsafe RunInstances: %#v", input)
	}
	decoded, err := base64.StdEncoding.DecodeString(aws.ToString(input.UserData))
	if err != nil || string(decoded) != "#!/bin/bash\nfixed" {
		t.Fatal("user-data was not exact fixed payload")
	}
}

type fakeS3API struct{ input *s3.PutObjectInput }

func (fake *fakeS3API) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	fake.input = input
	_, _ = io.Copy(io.Discard, input.Body)
	return &s3.PutObjectOutput{VersionId: aws.String("s3-version-1")}, nil
}
func (fake *fakeS3API) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

type fakePresigner struct{}

func (fakePresigner) PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error) {
	return &awsv4.PresignedHTTPRequest{URL: "https://example.invalid/object?X-Amz-Signature=redacted"}, nil
}

type fakeEC2API struct{ runInput *ec2.RunInstancesInput }

func (fake *fakeEC2API) RunInstances(_ context.Context, input *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	fake.runInput = input
	return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{{InstanceId: aws.String("i-0123456789abcdef0"), State: &ec2types.InstanceState{Name: ec2types.InstanceStateNamePending}, Tags: []ec2types.Tag{{Key: aws.String("Name"), Value: aws.String("builder")}}}}}, nil
}
func (fake *fakeEC2API) DescribeSecurityGroups(context.Context, *ec2.DescribeSecurityGroupsInput, ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{{GroupId: aws.String("sg-zero-ingress"), IpPermissions: []ec2types.IpPermission{}}}}, nil
}
func (fake *fakeEC2API) DescribeImages(_ context.Context, input *ec2.DescribeImagesInput, _ ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	if len(input.ImageIds) == 1 && input.ImageIds[0] == "ami-base" {
		return &ec2.DescribeImagesOutput{Images: []ec2types.Image{{ImageId: aws.String("ami-base"), RootDeviceName: aws.String("/dev/sda1")}}}, nil
	}
	return &ec2.DescribeImagesOutput{}, nil
}
func (fake *fakeEC2API) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return &ec2.DescribeInstancesOutput{}, nil
}
func (fake *fakeEC2API) GetConsoleOutput(context.Context, *ec2.GetConsoleOutputInput, ...func(*ec2.Options)) (*ec2.GetConsoleOutputOutput, error) {
	return &ec2.GetConsoleOutputOutput{}, nil
}
func (fake *fakeEC2API) TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	return &ec2.TerminateInstancesOutput{}, nil
}
func (fake *fakeEC2API) DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	return &ec2.DescribeSnapshotsOutput{}, nil
}
func (fake *fakeEC2API) CreateImage(context.Context, *ec2.CreateImageInput, ...func(*ec2.Options)) (*ec2.CreateImageOutput, error) {
	return &ec2.CreateImageOutput{}, nil
}
func (fake *fakeEC2API) DeregisterImage(context.Context, *ec2.DeregisterImageInput, ...func(*ec2.Options)) (*ec2.DeregisterImageOutput, error) {
	return &ec2.DeregisterImageOutput{}, nil
}
func (fake *fakeEC2API) DeleteSnapshot(context.Context, *ec2.DeleteSnapshotInput, ...func(*ec2.Options)) (*ec2.DeleteSnapshotOutput, error) {
	return &ec2.DeleteSnapshotOutput{}, nil
}
