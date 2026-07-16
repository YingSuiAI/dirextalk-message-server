package stackteardown

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type cloudFormationAPI interface {
	DescribeStacks(context.Context, *cloudformation.DescribeStacksInput, ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error)
	ListStackResources(context.Context, *cloudformation.ListStackResourcesInput, ...func(*cloudformation.Options)) (*cloudformation.ListStackResourcesOutput, error)
	DeleteStack(context.Context, *cloudformation.DeleteStackInput, ...func(*cloudformation.Options)) (*cloudformation.DeleteStackOutput, error)
}

type dynamoAPI interface {
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	DeleteTable(context.Context, *dynamodb.DeleteTableInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
}

type ec2API interface {
	DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	DeregisterImage(context.Context, *ec2.DeregisterImageInput, ...func(*ec2.Options)) (*ec2.DeregisterImageOutput, error)
	DescribeSnapshots(context.Context, *ec2.DescribeSnapshotsInput, ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
	DeleteSnapshot(context.Context, *ec2.DeleteSnapshotInput, ...func(*ec2.Options)) (*ec2.DeleteSnapshotOutput, error)
}

type s3API interface {
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	DeleteBucket(context.Context, *s3.DeleteBucketInput, ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
}

type kmsAPI interface {
	DescribeKey(context.Context, *kms.DescribeKeyInput, ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
	ListAliases(context.Context, *kms.ListAliasesInput, ...func(*kms.Options)) (*kms.ListAliasesOutput, error)
	DeleteAlias(context.Context, *kms.DeleteAliasInput, ...func(*kms.Options)) (*kms.DeleteAliasOutput, error)
	ScheduleKeyDeletion(context.Context, *kms.ScheduleKeyDeletionInput, ...func(*kms.Options)) (*kms.ScheduleKeyDeletionOutput, error)
}

type awsProvider struct {
	cloudFormation cloudFormationAPI
	dynamo         dynamoAPI
	ec2            ec2API
	s3             s3API
	kms            kmsAPI
}

// NewAWSService uses the standard AWS SDK credential chain. It deliberately
// has no credential flags and exposes no arbitrary AWS operation surface.
func NewAWSService(config aws.Config) *Service {
	return newService(&awsProvider{
		cloudFormation: cloudformation.NewFromConfig(config),
		dynamo:         dynamodb.NewFromConfig(config),
		ec2:            ec2.NewFromConfig(config),
		s3:             s3.NewFromConfig(config),
		kms:            kms.NewFromConfig(config),
	})
}

func (provider *awsProvider) FindStack(ctx context.Context, name string) (StackObservation, bool, error) {
	output, err := provider.cloudFormation.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(name)})
	if cloudFormationNotFound(err) {
		return StackObservation{}, false, nil
	}
	if err != nil {
		return StackObservation{}, false, providerError(err)
	}
	if len(output.Stacks) != 1 {
		return StackObservation{}, false, ErrProviderUnavailable
	}
	stack := output.Stacks[0]
	return StackObservation{ID: aws.ToString(stack.StackId), Name: aws.ToString(stack.StackName), Status: string(stack.StackStatus)}, true, nil
}

func (provider *awsProvider) StackResources(ctx context.Context, stackID string) ([]StackResourceObservation, error) {
	resources := []StackResourceObservation{}
	var token *string
	for page := 0; page < 1000; page++ {
		output, err := provider.cloudFormation.ListStackResources(ctx, &cloudformation.ListStackResourcesInput{StackName: aws.String(stackID), NextToken: token})
		if err != nil {
			return nil, providerError(err)
		}
		if output == nil {
			return nil, ErrProviderUnavailable
		}
		for _, resource := range output.StackResourceSummaries {
			resources = append(resources, StackResourceObservation{LogicalID: aws.ToString(resource.LogicalResourceId), Type: aws.ToString(resource.ResourceType), Identifier: aws.ToString(resource.PhysicalResourceId)})
		}
		if aws.ToString(output.NextToken) == "" {
			return resources, nil
		}
		token = output.NextToken
	}
	return nil, ErrProviderUnavailable
}

func (provider *awsProvider) DeleteStack(ctx context.Context, stackID string) error {
	_, err := provider.cloudFormation.DeleteStack(ctx, &cloudformation.DeleteStackInput{StackName: aws.String(stackID)})
	if cloudFormationNotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) Table(ctx context.Context, identifier string) (TableObservation, bool, error) {
	output, err := provider.dynamo.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(identifier)})
	if dynamoNotFound(err) {
		return TableObservation{}, false, nil
	}
	if err != nil || output.Table == nil {
		return TableObservation{}, false, providerError(err)
	}
	return TableObservation{Status: string(output.Table.TableStatus), DeletionProtectionEnabled: aws.ToBool(output.Table.DeletionProtectionEnabled)}, true, nil
}

func (provider *awsProvider) DisableTableDeletionProtection(ctx context.Context, identifier string) error {
	_, err := provider.dynamo.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String(identifier), DeletionProtectionEnabled: aws.Bool(false)})
	return providerError(err)
}

func (provider *awsProvider) DeleteTable(ctx context.Context, identifier string) error {
	_, err := provider.dynamo.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(identifier)})
	if dynamoNotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) BucketExists(ctx context.Context, identifier string) (bool, error) {
	_, err := provider.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(identifier)})
	if s3NotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, providerError(err)
	}
	return true, nil
}

func (provider *awsProvider) EmptyBucket(ctx context.Context, identifier string) error {
	var keyMarker, versionMarker *string
	for page := 0; page < 1000; page++ {
		output, err := provider.s3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(identifier), KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if s3NotFound(err) {
			return nil
		}
		if err != nil {
			return providerError(err)
		}
		objects, objectErr := versionObjects(output.Versions, output.DeleteMarkers)
		if objectErr != nil {
			return objectErr
		}
		if len(objects) > 0 {
			deleted, deleteErr := provider.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(identifier), Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
			if s3NotFound(deleteErr) {
				return nil
			}
			if deleteErr != nil {
				return providerError(deleteErr)
			}
			if deleted == nil || len(deleted.Errors) != 0 {
				return ErrProviderUnavailable
			}
		}
		if !aws.ToBool(output.IsTruncated) {
			return nil
		}
		if aws.ToString(output.NextKeyMarker) == "" || aws.ToString(output.NextVersionIdMarker) == "" {
			return ErrProviderUnavailable
		}
		keyMarker, versionMarker = output.NextKeyMarker, output.NextVersionIdMarker
	}
	return ErrProviderUnavailable
}

func versionObjects(versions []s3types.ObjectVersion, markers []s3types.DeleteMarkerEntry) ([]s3types.ObjectIdentifier, error) {
	objects := make([]s3types.ObjectIdentifier, 0, len(versions)+len(markers))
	for _, version := range versions {
		if aws.ToString(version.Key) == "" || aws.ToString(version.VersionId) == "" {
			return nil, ErrProviderUnavailable
		}
		objects = append(objects, s3types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
	}
	for _, marker := range markers {
		if aws.ToString(marker.Key) == "" || aws.ToString(marker.VersionId) == "" {
			return nil, ErrProviderUnavailable
		}
		objects = append(objects, s3types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
	}
	return objects, nil
}

func (provider *awsProvider) DeleteBucket(ctx context.Context, identifier string) error {
	_, err := provider.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(identifier)})
	if s3NotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) ManagedImages(ctx context.Context, connectionID string) ([]TaggedObservation, error) {
	filters := []ec2types.Filter{{Name: aws.String("tag:DirextalkConnectionId"), Values: []string{connectionID}}, {Name: aws.String("tag:DirextalkRetention"), Values: []string{"manual"}}}
	var token *string
	result := []TaggedObservation{}
	for page := 0; page < 1000; page++ {
		output, err := provider.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{Owners: []string{"self"}, Filters: filters, NextToken: token})
		if err != nil {
			return nil, providerError(err)
		}
		for _, image := range output.Images {
			result = append(result, TaggedObservation{ID: aws.ToString(image.ImageId), Tags: tags(image.Tags)})
		}
		if aws.ToString(output.NextToken) == "" {
			return result, nil
		}
		token = output.NextToken
	}
	return nil, ErrProviderUnavailable
}

func (provider *awsProvider) ManagedSnapshots(ctx context.Context, connectionID string) ([]TaggedObservation, error) {
	filters := []ec2types.Filter{{Name: aws.String("tag:DirextalkConnectionId"), Values: []string{connectionID}}, {Name: aws.String("tag:DirextalkRetention"), Values: []string{"manual"}}}
	var token *string
	result := []TaggedObservation{}
	for page := 0; page < 1000; page++ {
		output, err := provider.ec2.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{OwnerIds: []string{"self"}, Filters: filters, NextToken: token})
		if err != nil {
			return nil, providerError(err)
		}
		for _, snapshot := range output.Snapshots {
			result = append(result, TaggedObservation{ID: aws.ToString(snapshot.SnapshotId), Tags: tags(snapshot.Tags)})
		}
		if aws.ToString(output.NextToken) == "" {
			return result, nil
		}
		token = output.NextToken
	}
	return nil, ErrProviderUnavailable
}

func (provider *awsProvider) Image(ctx context.Context, identifier string) (TaggedObservation, bool, error) {
	output, err := provider.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{identifier}, Owners: []string{"self"}})
	if ec2NotFound(err) {
		return TaggedObservation{}, false, nil
	}
	if err != nil {
		return TaggedObservation{}, false, providerError(err)
	}
	if len(output.Images) == 0 {
		return TaggedObservation{}, false, nil
	}
	if len(output.Images) != 1 || aws.ToString(output.Images[0].ImageId) != identifier {
		return TaggedObservation{}, false, ErrProviderUnavailable
	}
	return TaggedObservation{ID: identifier, Tags: tags(output.Images[0].Tags)}, true, nil
}

func (provider *awsProvider) DeregisterImage(ctx context.Context, identifier string) error {
	_, err := provider.ec2.DeregisterImage(ctx, &ec2.DeregisterImageInput{ImageId: aws.String(identifier)})
	if ec2NotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) Snapshot(ctx context.Context, identifier string) (TaggedObservation, bool, error) {
	output, err := provider.ec2.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: []string{identifier}, OwnerIds: []string{"self"}})
	if ec2NotFound(err) {
		return TaggedObservation{}, false, nil
	}
	if err != nil {
		return TaggedObservation{}, false, providerError(err)
	}
	if len(output.Snapshots) == 0 {
		return TaggedObservation{}, false, nil
	}
	if len(output.Snapshots) != 1 || aws.ToString(output.Snapshots[0].SnapshotId) != identifier {
		return TaggedObservation{}, false, ErrProviderUnavailable
	}
	return TaggedObservation{ID: identifier, Tags: tags(output.Snapshots[0].Tags)}, true, nil
}

func (provider *awsProvider) DeleteSnapshot(ctx context.Context, identifier string) error {
	_, err := provider.ec2.DeleteSnapshot(ctx, &ec2.DeleteSnapshotInput{SnapshotId: aws.String(identifier)})
	if ec2NotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) Key(ctx context.Context, identifier string) (KeyObservation, bool, error) {
	output, err := provider.kms.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(identifier)})
	if kmsNotFound(err) {
		return KeyObservation{}, false, nil
	}
	if err != nil || output.KeyMetadata == nil {
		return KeyObservation{}, false, providerError(err)
	}
	return KeyObservation{State: string(output.KeyMetadata.KeyState)}, true, nil
}

func (provider *awsProvider) AliasExists(ctx context.Context, identifier, keyID string) (bool, error) {
	if keyID == "" {
		return false, ErrInvalidRequest
	}
	var marker *string
	for page := 0; page < 1000; page++ {
		output, err := provider.kms.ListAliases(ctx, &kms.ListAliasesInput{KeyId: aws.String(keyID), Marker: marker, Limit: aws.Int32(100)})
		if kmsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, providerError(err)
		}
		for _, alias := range output.Aliases {
			if aws.ToString(alias.AliasName) == identifier {
				return true, nil
			}
		}
		if !output.Truncated {
			return false, nil
		}
		if aws.ToString(output.NextMarker) == "" {
			return false, ErrProviderUnavailable
		}
		marker = output.NextMarker
	}
	return false, ErrProviderUnavailable
}

func (provider *awsProvider) DeleteAlias(ctx context.Context, identifier string) error {
	_, err := provider.kms.DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: aws.String(identifier)})
	if kmsNotFound(err) {
		return nil
	}
	return providerError(err)
}

func (provider *awsProvider) ScheduleKeyDeletion(ctx context.Context, identifier string, days int32) error {
	_, err := provider.kms.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: aws.String(identifier), PendingWindowInDays: aws.Int32(days)})
	if kmsNotFound(err) {
		return nil
	}
	return providerError(err)
}

func tags(values []ec2types.Tag) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		key := aws.ToString(value.Key)
		if key == "" {
			continue
		}
		if _, duplicate := result[key]; duplicate {
			return map[string]string{}
		}
		result[key] = aws.ToString(value.Value)
	}
	return result
}

func providerError(err error) error {
	if err == nil {
		return nil
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "UnrecognizedClientException", "InvalidClientTokenId":
			return ErrProviderForbidden
		}
	}
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) && responseError.HTTPStatusCode() == 403 {
		return ErrProviderForbidden
	}
	return fmt.Errorf("%w", ErrProviderUnavailable)
}

func cloudFormationNotFound(err error) bool {
	message := strings.ToLower(apiMessage(err))
	return apiCode(err, "ValidationError") && (strings.Contains(message, "does not exist") || strings.Contains(message, "not exist") || strings.Contains(message, "not found"))
}
func dynamoNotFound(err error) bool { return apiCode(err, "ResourceNotFoundException") }
func kmsNotFound(err error) bool    { return apiCode(err, "NotFoundException") }
func s3NotFound(err error) bool {
	if apiCode(err, "NoSuchBucket", "NotFound", "404") {
		return true
	}
	var responseError *smithyhttp.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == 404
}
func ec2NotFound(err error) bool {
	return apiCode(err, "InvalidAMIID.NotFound", "InvalidSnapshot.NotFound")
}

func apiCode(err error, values ...string) bool {
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	for _, value := range values {
		if apiError.ErrorCode() == value {
			return true
		}
	}
	return false
}

func apiMessage(err error) string {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		return apiError.ErrorMessage()
	}
	return ""
}
