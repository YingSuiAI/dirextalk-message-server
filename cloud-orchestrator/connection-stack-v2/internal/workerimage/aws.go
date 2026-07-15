package workerimage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type ec2API interface {
	RunInstances(context.Context, *ec2.RunInstancesInput, ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error)
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DescribeSecurityGroups(context.Context, *ec2.DescribeSecurityGroupsInput, ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	GetConsoleOutput(context.Context, *ec2.GetConsoleOutputInput, ...func(*ec2.Options)) (*ec2.GetConsoleOutputOutput, error)
	TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
	CreateImage(context.Context, *ec2.CreateImageInput, ...func(*ec2.Options)) (*ec2.CreateImageOutput, error)
	DeregisterImage(context.Context, *ec2.DeregisterImageInput, ...func(*ec2.Options)) (*ec2.DeregisterImageOutput, error)
	DeleteSnapshot(context.Context, *ec2.DeleteSnapshotInput, ...func(*ec2.Options)) (*ec2.DeleteSnapshotOutput, error)
}
type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}
type s3Presigner interface {
	PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error)
}

type AWSProvider struct {
	ec2     ec2API
	s3      s3API
	presign s3Presigner
}

func NewAWSProvider(ec2Client *ec2.Client, s3Client *s3.Client) (*AWSProvider, error) {
	if ec2Client == nil || s3Client == nil {
		return nil, ErrInvalidConfig
	}
	return &AWSProvider{ec2: ec2Client, s3: s3Client, presign: s3.NewPresignClient(s3Client)}, nil
}

func (provider *AWSProvider) PutArtifact(ctx context.Context, bucket, key, version string, file *os.File, size int64, digest string) (ArtifactUpload, error) {
	if provider == nil || file == nil || size <= 0 || !digestPattern.MatchString(digest) {
		return ArtifactUpload{}, ErrInvalidConfig
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if err != nil || len(decoded) != sha256.Size {
		return ArtifactUpload{}, ErrInvalidConfig
	}
	output, err := provider.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: file, ContentLength: aws.Int64(size), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(decoded)), Metadata: map[string]string{"dirextalk-artifact-version": version, "dirextalk-archive-sha256": digest}})
	if err != nil {
		return ArtifactUpload{}, err
	}
	if aws.ToString(output.VersionId) == "" {
		return ArtifactUpload{}, ErrBuildFailed
	}
	return ArtifactUpload{VersionID: aws.ToString(output.VersionId)}, nil
}
func (provider *AWSProvider) PresignArtifactGET(ctx context.Context, bucket, key, version string, ttl time.Duration) (string, error) {
	if ttl <= 0 || ttl > 15*time.Minute {
		return "", ErrInvalidConfig
	}
	request, err := provider.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(version)}, func(options *s3.PresignOptions) { options.Expires = ttl })
	if err != nil {
		return "", err
	}
	return request.URL, nil
}
func (provider *AWSProvider) DeleteArtifact(ctx context.Context, bucket, key, version string) error {
	_, err := provider.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(version)})
	return err
}

func (provider *AWSProvider) FindBuilder(ctx context.Context, name string) (BuilderObservation, bool, error) {
	output, err := provider.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{{Name: aws.String("tag:Name"), Values: []string{name}}, {Name: aws.String("tag:dirextalk:managed"), Values: []string{"true"}}, {Name: aws.String("instance-state-name"), Values: []string{"pending", "running", "stopping", "stopped", "shutting-down"}}}})
	if err != nil {
		return BuilderObservation{}, false, err
	}
	instances := flattenInstances(output)
	if len(instances) == 0 {
		return BuilderObservation{}, false, nil
	}
	if len(instances) != 1 || aws.ToString(instances[0].PublicIpAddress) != "" {
		return BuilderObservation{}, false, ErrBuildFailed
	}
	return builderObservation(instances[0]), true, nil
}
func (provider *AWSProvider) LaunchBuilder(ctx context.Context, spec LaunchSpec) (BuilderObservation, error) {
	groups, err := provider.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{spec.SecurityGroupID}})
	if err != nil || len(groups.SecurityGroups) != 1 || len(groups.SecurityGroups[0].IpPermissions) != 0 {
		return BuilderObservation{}, ErrBuildFailed
	}
	images, err := provider.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{spec.BaseAMIID}})
	if err != nil || len(images.Images) != 1 || aws.ToString(images.Images[0].RootDeviceName) == "" {
		return BuilderObservation{}, ErrBuildFailed
	}
	tags := awsTags(spec.Tags)
	tags = append(tags, ec2types.Tag{Key: aws.String("Name"), Value: aws.String(spec.Name)})
	output, err := provider.ec2.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(spec.BaseAMIID), InstanceType: ec2types.InstanceType(spec.InstanceType), MinCount: aws.Int32(1), MaxCount: aws.Int32(1), ClientToken: aws.String(spec.ClientToken), InstanceInitiatedShutdownBehavior: ec2types.ShutdownBehaviorStop, MetadataOptions: &ec2types.InstanceMetadataOptionsRequest{HttpEndpoint: ec2types.InstanceMetadataEndpointStateEnabled, HttpTokens: ec2types.HttpTokensStateRequired, HttpPutResponseHopLimit: aws.Int32(1), InstanceMetadataTags: ec2types.InstanceMetadataTagsStateDisabled}, NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{DeviceIndex: aws.Int32(0), SubnetId: aws.String(spec.SubnetID), Groups: []string{spec.SecurityGroupID}, AssociatePublicIpAddress: aws.Bool(false), DeleteOnTermination: aws.Bool(true)}}, BlockDeviceMappings: []ec2types.BlockDeviceMapping{{DeviceName: images.Images[0].RootDeviceName, Ebs: &ec2types.EbsBlockDevice{Encrypted: aws.Bool(true), DeleteOnTermination: aws.Bool(true), VolumeSize: aws.Int32(30), VolumeType: ec2types.VolumeTypeGp3}}}, UserData: aws.String(base64.StdEncoding.EncodeToString([]byte(spec.UserData))), TagSpecifications: []ec2types.TagSpecification{{ResourceType: ec2types.ResourceTypeInstance, Tags: tags}, {ResourceType: ec2types.ResourceTypeVolume, Tags: tags}}})
	if err != nil {
		return BuilderObservation{}, err
	}
	if len(output.Instances) != 1 {
		return BuilderObservation{}, ErrBuildFailed
	}
	return builderObservation(output.Instances[0]), nil
}
func (provider *AWSProvider) ObserveBuilder(ctx context.Context, id string) (BuilderObservation, error) {
	output, err := provider.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
	if err != nil {
		return BuilderObservation{}, err
	}
	instances := flattenInstances(output)
	if len(instances) != 1 {
		return BuilderObservation{}, ErrBuildFailed
	}
	return builderObservation(instances[0]), nil
}
func (provider *AWSProvider) ConsoleOutput(ctx context.Context, id string) (string, error) {
	output, err := provider.ec2.GetConsoleOutput(ctx, &ec2.GetConsoleOutputInput{InstanceId: aws.String(id), Latest: aws.Bool(true)})
	if err != nil {
		return "", err
	}
	encoded := aws.ToString(output.Output)
	if encoded == "" {
		return "", ErrBuildFailed
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", ErrBuildFailed
	}
	return string(raw), nil
}
func (provider *AWSProvider) TerminateBuilder(ctx context.Context, id string) error {
	_, err := provider.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	waitContext, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		output, describeErr := provider.ec2.DescribeInstances(waitContext, &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
		if isNotFound(describeErr) {
			return nil
		}
		if describeErr != nil {
			return describeErr
		}
		instances := flattenInstances(output)
		if len(instances) == 1 && instances[0].State != nil && instances[0].State.Name == ec2types.InstanceStateNameTerminated {
			return nil
		}
		select {
		case <-waitContext.Done():
			return waitContext.Err()
		case <-ticker.C:
		}
	}
}
func (provider *AWSProvider) FindImageByName(ctx context.Context, name string) (ImageObservation, bool, error) {
	return provider.findImage(ctx, &ec2.DescribeImagesInput{Owners: []string{"self"}, Filters: []ec2types.Filter{{Name: aws.String("name"), Values: []string{name}}}})
}
func (provider *AWSProvider) FindImageByID(ctx context.Context, id string) (ImageObservation, bool, error) {
	return provider.findImage(ctx, &ec2.DescribeImagesInput{ImageIds: []string{id}, Owners: []string{"self"}})
}
func (provider *AWSProvider) findImage(ctx context.Context, input *ec2.DescribeImagesInput) (ImageObservation, bool, error) {
	output, err := provider.ec2.DescribeImages(ctx, input)
	if isNotFound(err) {
		return ImageObservation{}, false, nil
	}
	if err != nil {
		return ImageObservation{}, false, err
	}
	if len(output.Images) == 0 {
		return ImageObservation{}, false, nil
	}
	if len(output.Images) != 1 {
		return ImageObservation{}, false, ErrBuildFailed
	}
	image := output.Images[0]
	snapshotIDs := []string{}
	for _, mapping := range image.BlockDeviceMappings {
		if mapping.Ebs != nil && aws.ToString(mapping.Ebs.SnapshotId) != "" {
			snapshotIDs = append(snapshotIDs, aws.ToString(mapping.Ebs.SnapshotId))
		}
	}
	if len(snapshotIDs) == 0 {
		return ImageObservation{}, false, ErrBuildFailed
	}
	snapshots, err := provider.ec2.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: snapshotIDs, OwnerIds: []string{"self"}})
	if err != nil {
		return ImageObservation{}, false, err
	}
	encrypted := len(snapshots.Snapshots) == len(snapshotIDs)
	for _, snapshot := range snapshots.Snapshots {
		encrypted = encrypted && aws.ToBool(snapshot.Encrypted) && hasRequiredTags(snapshot.Tags, tagsMap(image.Tags))
	}
	sort.Strings(snapshotIDs)
	return ImageObservation{ImageID: aws.ToString(image.ImageId), Name: aws.ToString(image.Name), State: string(image.State), SnapshotIDs: snapshotIDs, SnapshotsEncrypted: encrypted, Tags: tagsMap(image.Tags)}, true, nil
}
func (provider *AWSProvider) CreateImage(ctx context.Context, instanceID, name string, tags map[string]string) (string, error) {
	awsTagSet := awsTags(tags)
	output, err := provider.ec2.CreateImage(ctx, &ec2.CreateImageInput{InstanceId: aws.String(instanceID), Name: aws.String(name), Description: aws.String("Dirextalk immutable Worker image " + name), NoReboot: aws.Bool(true), TagSpecifications: []ec2types.TagSpecification{{ResourceType: ec2types.ResourceTypeImage, Tags: awsTagSet}, {ResourceType: ec2types.ResourceTypeSnapshot, Tags: awsTagSet}}})
	if err != nil {
		return "", err
	}
	if aws.ToString(output.ImageId) == "" {
		return "", ErrBuildFailed
	}
	return aws.ToString(output.ImageId), nil
}
func (provider *AWSProvider) DeregisterImage(ctx context.Context, id string) error {
	_, err := provider.ec2.DeregisterImage(ctx, &ec2.DeregisterImageInput{ImageId: aws.String(id)})
	if isNotFound(err) {
		return nil
	}
	return err
}

func (provider *AWSProvider) DeleteSnapshot(ctx context.Context, id string, expectedTags map[string]string) error {
	observed, err := provider.ec2.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: []string{id}, OwnerIds: []string{"self"}})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(observed.Snapshots) == 0 {
		return nil
	}
	if len(observed.Snapshots) != 1 || !aws.ToBool(observed.Snapshots[0].Encrypted) || !hasRequiredTags(observed.Snapshots[0].Tags, expectedTags) {
		return ErrBuildFailed
	}
	_, err = provider.ec2.DeleteSnapshot(ctx, &ec2.DeleteSnapshotInput{SnapshotId: aws.String(id)})
	if isNotFound(err) {
		return nil
	}
	return err
}

func flattenInstances(output *ec2.DescribeInstancesOutput) []ec2types.Instance {
	result := []ec2types.Instance{}
	if output != nil {
		for _, reservation := range output.Reservations {
			result = append(result, reservation.Instances...)
		}
	}
	return result
}
func builderObservation(instance ec2types.Instance) BuilderObservation {
	state := BuilderState("")
	if instance.State != nil {
		state = BuilderState(instance.State.Name)
	}
	return BuilderObservation{InstanceID: aws.ToString(instance.InstanceId), Name: tagValue(instance.Tags, "Name"), State: state}
}
func awsTags(values map[string]string) []ec2types.Tag {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ec2types.Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, ec2types.Tag{Key: aws.String(key), Value: aws.String(values[key])})
	}
	return result
}
func tagsMap(tags []ec2types.Tag) map[string]string {
	result := map[string]string{}
	for _, tag := range tags {
		key := aws.ToString(tag.Key)
		if key != "" {
			if _, exists := result[key]; exists {
				return map[string]string{}
			}
			result[key] = aws.ToString(tag.Value)
		}
	}
	return result
}
func tagValue(tags []ec2types.Tag, key string) string { return tagsMap(tags)[key] }
func hasRequiredTags(tags []ec2types.Tag, want map[string]string) bool {
	got := tagsMap(tags)
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	switch api.ErrorCode() {
	case "InvalidAMIID.NotFound", "InvalidInstanceID.NotFound", "InvalidSnapshot.NotFound":
		return true
	}
	return false
}
