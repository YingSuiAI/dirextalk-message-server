package connectionfoundation

import (
	"context"
	"errors"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// EC2FoundationAPI, S3FoundationAPI and KMSFoundationAPI are intentionally
// the exact AWS SDK seams used by the concrete Foundation provider. They are
// closed to the resources below; callers never obtain a generic SDK client.
// This also gives focused tests a small fake-provider boundary without making
// a real AWS mutation.
type EC2FoundationAPI interface {
	DescribeVpcs(context.Context, *ec2.DescribeVpcsInput, ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
	CreateVpc(context.Context, *ec2.CreateVpcInput, ...func(*ec2.Options)) (*ec2.CreateVpcOutput, error)
	ModifyVpcAttribute(context.Context, *ec2.ModifyVpcAttributeInput, ...func(*ec2.Options)) (*ec2.ModifyVpcAttributeOutput, error)
	DescribeVpcAttribute(context.Context, *ec2.DescribeVpcAttributeInput, ...func(*ec2.Options)) (*ec2.DescribeVpcAttributeOutput, error)
	DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	CreateSubnet(context.Context, *ec2.CreateSubnetInput, ...func(*ec2.Options)) (*ec2.CreateSubnetOutput, error)
	ModifySubnetAttribute(context.Context, *ec2.ModifySubnetAttributeInput, ...func(*ec2.Options)) (*ec2.ModifySubnetAttributeOutput, error)
	DescribeInternetGateways(context.Context, *ec2.DescribeInternetGatewaysInput, ...func(*ec2.Options)) (*ec2.DescribeInternetGatewaysOutput, error)
	CreateInternetGateway(context.Context, *ec2.CreateInternetGatewayInput, ...func(*ec2.Options)) (*ec2.CreateInternetGatewayOutput, error)
	AttachInternetGateway(context.Context, *ec2.AttachInternetGatewayInput, ...func(*ec2.Options)) (*ec2.AttachInternetGatewayOutput, error)
	DescribeRouteTables(context.Context, *ec2.DescribeRouteTablesInput, ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error)
	CreateRouteTable(context.Context, *ec2.CreateRouteTableInput, ...func(*ec2.Options)) (*ec2.CreateRouteTableOutput, error)
	CreateRoute(context.Context, *ec2.CreateRouteInput, ...func(*ec2.Options)) (*ec2.CreateRouteOutput, error)
	AssociateRouteTable(context.Context, *ec2.AssociateRouteTableInput, ...func(*ec2.Options)) (*ec2.AssociateRouteTableOutput, error)
	DescribeAddresses(context.Context, *ec2.DescribeAddressesInput, ...func(*ec2.Options)) (*ec2.DescribeAddressesOutput, error)
	AllocateAddress(context.Context, *ec2.AllocateAddressInput, ...func(*ec2.Options)) (*ec2.AllocateAddressOutput, error)
	DescribeNatGateways(context.Context, *ec2.DescribeNatGatewaysInput, ...func(*ec2.Options)) (*ec2.DescribeNatGatewaysOutput, error)
	CreateNatGateway(context.Context, *ec2.CreateNatGatewayInput, ...func(*ec2.Options)) (*ec2.CreateNatGatewayOutput, error)
	DescribeSecurityGroups(context.Context, *ec2.DescribeSecurityGroupsInput, ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	CreateSecurityGroup(context.Context, *ec2.CreateSecurityGroupInput, ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error)
	RevokeSecurityGroupEgress(context.Context, *ec2.RevokeSecurityGroupEgressInput, ...func(*ec2.Options)) (*ec2.RevokeSecurityGroupEgressOutput, error)
	AuthorizeSecurityGroupEgress(context.Context, *ec2.AuthorizeSecurityGroupEgressInput, ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupEgressOutput, error)
}

type S3FoundationAPI interface {
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	GetBucketTagging(context.Context, *s3.GetBucketTaggingInput, ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error)
	PutBucketTagging(context.Context, *s3.PutBucketTaggingInput, ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error)
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	PutBucketVersioning(context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
	GetPublicAccessBlock(context.Context, *s3.GetPublicAccessBlockInput, ...func(*s3.Options)) (*s3.GetPublicAccessBlockOutput, error)
	PutPublicAccessBlock(context.Context, *s3.PutPublicAccessBlockInput, ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error)
	GetBucketOwnershipControls(context.Context, *s3.GetBucketOwnershipControlsInput, ...func(*s3.Options)) (*s3.GetBucketOwnershipControlsOutput, error)
	PutBucketOwnershipControls(context.Context, *s3.PutBucketOwnershipControlsInput, ...func(*s3.Options)) (*s3.PutBucketOwnershipControlsOutput, error)
	GetBucketEncryption(context.Context, *s3.GetBucketEncryptionInput, ...func(*s3.Options)) (*s3.GetBucketEncryptionOutput, error)
	PutBucketEncryption(context.Context, *s3.PutBucketEncryptionInput, ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error)
}

type KMSFoundationAPI interface {
	ListAliases(context.Context, *kms.ListAliasesInput, ...func(*kms.Options)) (*kms.ListAliasesOutput, error)
	ListKeys(context.Context, *kms.ListKeysInput, ...func(*kms.Options)) (*kms.ListKeysOutput, error)
	CreateKey(context.Context, *kms.CreateKeyInput, ...func(*kms.Options)) (*kms.CreateKeyOutput, error)
	DescribeKey(context.Context, *kms.DescribeKeyInput, ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
	CreateAlias(context.Context, *kms.CreateAliasInput, ...func(*kms.Options)) (*kms.CreateAliasOutput, error)
	ListResourceTags(context.Context, *kms.ListResourceTagsInput, ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error)
}

// AWSProvider is the concrete, Go-only implementation of FoundationProvider.
// It is deliberately constructed from typed SDK clients, not credentials. The
// connection-bootstrap layer owns short-lived root-key use; this provider does
// not persist or expose any AWS credential material.
type AWSProvider struct {
	ec2    EC2FoundationAPI
	s3     S3FoundationAPI
	kms    KMSFoundationAPI
	region string
}

func NewAWSProvider(ec2Client EC2FoundationAPI, s3Client S3FoundationAPI, kmsClient KMSFoundationAPI) (*AWSProvider, error) {
	if ec2Client == nil || s3Client == nil || kmsClient == nil {
		return nil, ErrInvalidProvisionRequest
	}
	return &AWSProvider{ec2: ec2Client, s3: s3Client, kms: kmsClient}, nil
}

func (provider *AWSProvider) Observe(ctx context.Context, spec Spec) (Observation, bool, error) {
	if provider == nil || provider.ec2 == nil || provider.s3 == nil || provider.kms == nil || spec.Request.Validate() != nil || provider.region != "" && provider.region != spec.Request.Region {
		return Observation{}, false, ErrInvalidProvisionRequest
	}
	vpc, found, err := provider.findVPC(ctx, spec)
	if err != nil || !found {
		return Observation{}, found, err
	}
	publicSubnet, found, err := provider.findSubnet(ctx, spec, spec.Names.PublicSubnet, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	privateSubnet, found, err := provider.findSubnet(ctx, spec, spec.Names.PrivateSubnet, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	gateway, found, err := provider.findInternetGateway(ctx, spec, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	nat, found, err := provider.findNATGateway(ctx, spec)
	if err != nil || !found {
		return Observation{}, false, err
	}
	publicRouteTable, found, err := provider.findRouteTable(ctx, spec, spec.Names.PublicRouteTable, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	privateRouteTable, found, err := provider.findRouteTable(ctx, spec, spec.Names.PrivateRouteTable, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	securityGroup, found, err := provider.findSecurityGroup(ctx, spec, aws.ToString(vpc.VpcId))
	if err != nil || !found {
		return Observation{}, false, err
	}
	key, found, err := provider.findKMSKey(ctx, spec)
	if err != nil || !found {
		return Observation{}, false, err
	}
	bucket, found, err := provider.readBucket(ctx, spec)
	if err != nil || !found {
		return Observation{}, false, err
	}

	dnsSupport, err := provider.vpcBooleanAttribute(ctx, aws.ToString(vpc.VpcId), ec2types.VpcAttributeNameEnableDnsSupport)
	if err != nil {
		return Observation{}, false, err
	}
	dnsHostnames, err := provider.vpcBooleanAttribute(ctx, aws.ToString(vpc.VpcId), ec2types.VpcAttributeNameEnableDnsHostnames)
	if err != nil {
		return Observation{}, false, err
	}
	return Observation{
		VPC:                 VPCObservation{ID: aws.ToString(vpc.VpcId), CIDR: aws.ToString(vpc.CidrBlock), DNSSupport: dnsSupport, DNSHostnames: dnsHostnames, Tags: ec2Tags(vpc.Tags)},
		PublicSubnet:        subnetObservation(publicSubnet),
		PrivateSubnet:       subnetObservation(privateSubnet),
		InternetGateway:     gatewayObservation(gateway),
		NATGateway:          natObservation(nat),
		PublicRoute:         routeObservation(publicRouteTable, aws.ToString(publicSubnet.SubnetId)),
		PrivateRoute:        routeObservation(privateRouteTable, aws.ToString(privateSubnet.SubnetId)),
		WorkerSecurityGroup: securityGroupObservation(securityGroup),
		ArtifactBucket:      bucket,
		ArtifactKMSKey:      key,
	}, true, nil
}

func (provider *AWSProvider) Ensure(ctx context.Context, spec Spec) error {
	if provider == nil || provider.ec2 == nil || provider.s3 == nil || provider.kms == nil || spec.Request.Validate() != nil || provider.region != "" && provider.region != spec.Request.Region {
		return ErrInvalidProvisionRequest
	}
	vpcID, err := provider.ensureVPC(ctx, spec)
	if err != nil {
		return err
	}
	publicSubnetID, err := provider.ensureSubnet(ctx, spec, spec.Names.PublicSubnet, spec.Network.PublicSubnetCIDR, vpcID)
	if err != nil {
		return err
	}
	privateSubnetID, err := provider.ensureSubnet(ctx, spec, spec.Names.PrivateSubnet, spec.Network.PrivateSubnetCIDR, vpcID)
	if err != nil {
		return err
	}
	gatewayID, err := provider.ensureInternetGateway(ctx, spec, vpcID)
	if err != nil {
		return err
	}
	if _, err := provider.ensureRouteTable(ctx, spec, spec.Names.PublicRouteTable, publicSubnetID, vpcID, gatewayID, true); err != nil {
		return err
	}
	allocationID, err := provider.ensureEIP(ctx, spec)
	if err != nil {
		return err
	}
	natID, err := provider.ensureNATGateway(ctx, spec, publicSubnetID, allocationID)
	if err != nil {
		return err
	}
	if _, err := provider.ensureRouteTable(ctx, spec, spec.Names.PrivateRouteTable, privateSubnetID, vpcID, natID, false); err != nil {
		return err
	}
	if _, err := provider.ensureSecurityGroup(ctx, spec, vpcID); err != nil {
		return err
	}
	key, err := provider.ensureKMSKey(ctx, spec)
	if err != nil {
		return err
	}
	if err := provider.ensureBucket(ctx, spec, key.ARN); err != nil {
		return err
	}
	return nil
}

func NewAWSProviderFromConfig(config aws.Config) (*AWSProvider, error) {
	if strings.TrimSpace(config.Region) == "" {
		return nil, ErrInvalidProvisionRequest
	}
	provider, err := NewAWSProvider(ec2.NewFromConfig(config), s3.NewFromConfig(config), kms.NewFromConfig(config))
	if err != nil {
		return nil, err
	}
	provider.region = config.Region
	return provider, nil
}

func (provider *AWSProvider) findVPC(ctx context.Context, spec Spec) (ec2types.Vpc, bool, error) {
	output, err := provider.ec2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: taggedFilters(spec, spec.Names.VPC)})
	if err != nil {
		return ec2types.Vpc{}, false, err
	}
	if len(output.Vpcs) == 0 {
		return ec2types.Vpc{}, false, nil
	}
	if len(output.Vpcs) != 1 {
		return ec2types.Vpc{}, false, ErrFoundationReadback
	}
	return output.Vpcs[0], true, nil
}

func (provider *AWSProvider) findSubnet(ctx context.Context, spec Spec, name, vpcID string) (ec2types.Subnet, bool, error) {
	filters := append(taggedFilters(spec, name), ec2types.Filter{Name: aws.String("vpc-id"), Values: []string{vpcID}})
	output, err := provider.ec2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{Filters: filters})
	if err != nil {
		return ec2types.Subnet{}, false, err
	}
	if len(output.Subnets) == 0 {
		return ec2types.Subnet{}, false, nil
	}
	if len(output.Subnets) != 1 {
		return ec2types.Subnet{}, false, ErrFoundationReadback
	}
	return output.Subnets[0], true, nil
}

func (provider *AWSProvider) findInternetGateway(ctx context.Context, spec Spec, vpcID string) (ec2types.InternetGateway, bool, error) {
	output, err := provider.ec2.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{Filters: taggedFilters(spec, spec.Names.InternetGateway)})
	if err != nil {
		return ec2types.InternetGateway{}, false, err
	}
	if len(output.InternetGateways) == 0 {
		return ec2types.InternetGateway{}, false, nil
	}
	if len(output.InternetGateways) != 1 {
		return ec2types.InternetGateway{}, false, ErrFoundationReadback
	}
	if len(output.InternetGateways[0].Attachments) == 0 {
		return output.InternetGateways[0], false, nil
	}
	if !gatewayAttachedTo(output.InternetGateways[0], vpcID) {
		return ec2types.InternetGateway{}, false, ErrFoundationReadback
	}
	return output.InternetGateways[0], true, nil
}

func (provider *AWSProvider) findNATGateway(ctx context.Context, spec Spec) (ec2types.NatGateway, bool, error) {
	output, err := provider.ec2.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{Filter: taggedFilters(spec, spec.Names.NATGateway)})
	if err != nil {
		return ec2types.NatGateway{}, false, err
	}
	if len(output.NatGateways) == 0 {
		return ec2types.NatGateway{}, false, nil
	}
	if len(output.NatGateways) != 1 {
		return ec2types.NatGateway{}, false, ErrFoundationReadback
	}
	return output.NatGateways[0], true, nil
}

func (provider *AWSProvider) findRouteTable(ctx context.Context, spec Spec, name, vpcID string) (ec2types.RouteTable, bool, error) {
	filters := append(taggedFilters(spec, name), ec2types.Filter{Name: aws.String("vpc-id"), Values: []string{vpcID}})
	output, err := provider.ec2.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: filters})
	if err != nil {
		return ec2types.RouteTable{}, false, err
	}
	if len(output.RouteTables) == 0 {
		return ec2types.RouteTable{}, false, nil
	}
	if len(output.RouteTables) != 1 {
		return ec2types.RouteTable{}, false, ErrFoundationReadback
	}
	return output.RouteTables[0], true, nil
}

func (provider *AWSProvider) findSecurityGroup(ctx context.Context, spec Spec, vpcID string) (ec2types.SecurityGroup, bool, error) {
	filters := append(taggedFilters(spec, spec.Names.WorkerSecurityGrp), ec2types.Filter{Name: aws.String("vpc-id"), Values: []string{vpcID}})
	output, err := provider.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: filters})
	if err != nil {
		return ec2types.SecurityGroup{}, false, err
	}
	if len(output.SecurityGroups) == 0 {
		return ec2types.SecurityGroup{}, false, nil
	}
	if len(output.SecurityGroups) != 1 {
		return ec2types.SecurityGroup{}, false, ErrFoundationReadback
	}
	return output.SecurityGroups[0], true, nil
}

func (provider *AWSProvider) findKMSKey(ctx context.Context, spec Spec) (KMSKeyObservation, bool, error) {
	aliasName := "alias/" + spec.Names.ArtifactKMSKey
	var keyID string
	for marker := ""; ; {
		output, err := provider.kms.ListAliases(ctx, &kms.ListAliasesInput{Marker: optionalString(marker)})
		if err != nil {
			return KMSKeyObservation{}, false, err
		}
		for _, alias := range output.Aliases {
			if aws.ToString(alias.AliasName) == aliasName {
				keyID = aws.ToString(alias.TargetKeyId)
				break
			}
		}
		if keyID != "" || !output.Truncated {
			break
		}
		marker = aws.ToString(output.NextMarker)
		if marker == "" {
			return KMSKeyObservation{}, false, ErrFoundationReadback
		}
	}
	if keyID == "" {
		return KMSKeyObservation{}, false, nil
	}
	described, err := provider.kms.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyID)})
	if err != nil || described.KeyMetadata == nil {
		return KMSKeyObservation{}, false, err
	}
	tags, err := provider.kmsTags(ctx, aws.ToString(described.KeyMetadata.Arn))
	if err != nil {
		return KMSKeyObservation{}, false, err
	}
	return KMSKeyObservation{
		ARN:         aws.ToString(described.KeyMetadata.Arn),
		Enabled:     described.KeyMetadata.Enabled,
		KeySpec:     string(described.KeyMetadata.KeySpec),
		KeyUsage:    string(described.KeyMetadata.KeyUsage),
		MultiRegion: aws.ToBool(described.KeyMetadata.MultiRegion),
		Tags:        tags,
	}, true, nil
}

func (provider *AWSProvider) readBucket(ctx context.Context, spec Spec) (BucketObservation, bool, error) {
	bucket := spec.Names.ArtifactBucket
	if _, err := provider.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)}); err != nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	tagsOutput, err := provider.s3.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if err != nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	tags := s3Tags(tagsOutput.TagSet)
	if !sameRequiredTags(tags, tagsFor(spec, spec.Names.ArtifactBucket)) {
		return BucketObservation{Name: bucket, Tags: tags}, true, nil
	}
	versioning, err := provider.s3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if err != nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	publicAccess, err := provider.s3.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if err != nil || publicAccess.PublicAccessBlockConfiguration == nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	ownership, err := provider.s3.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if err != nil || ownership.OwnershipControls == nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	encryption, err := provider.s3.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if err != nil || encryption.ServerSideEncryptionConfiguration == nil {
		if isFoundationNotFound(err) {
			return BucketObservation{}, false, nil
		}
		return BucketObservation{}, false, err
	}
	allBlocked := aws.ToBool(publicAccess.PublicAccessBlockConfiguration.BlockPublicAcls) && aws.ToBool(publicAccess.PublicAccessBlockConfiguration.IgnorePublicAcls) && aws.ToBool(publicAccess.PublicAccessBlockConfiguration.BlockPublicPolicy) && aws.ToBool(publicAccess.PublicAccessBlockConfiguration.RestrictPublicBuckets)
	if versioning.Status != s3types.BucketVersioningStatusEnabled || !allBlocked || !bucketOwnerEnforced(ownership.OwnershipControls.Rules) || bucketKMSKeyARN(encryption.ServerSideEncryptionConfiguration.Rules) == "" {
		return BucketObservation{}, false, nil
	}
	return BucketObservation{
		Name:                   bucket,
		VersioningEnabled:      versioning.Status == s3types.BucketVersioningStatusEnabled,
		AllPublicAccessBlocked: allBlocked,
		BucketOwnerEnforced:    bucketOwnerEnforced(ownership.OwnershipControls.Rules),
		KMSKeyARN:              bucketKMSKeyARN(encryption.ServerSideEncryptionConfiguration.Rules),
		Tags:                   tags,
	}, true, nil
}

func (provider *AWSProvider) vpcBooleanAttribute(ctx context.Context, vpcID string, attribute ec2types.VpcAttributeName) (bool, error) {
	output, err := provider.ec2.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{VpcId: aws.String(vpcID), Attribute: attribute})
	if err != nil {
		return false, err
	}
	if attribute == ec2types.VpcAttributeNameEnableDnsSupport {
		return output.EnableDnsSupport != nil && aws.ToBool(output.EnableDnsSupport.Value), nil
	}
	if attribute == ec2types.VpcAttributeNameEnableDnsHostnames {
		return output.EnableDnsHostnames != nil && aws.ToBool(output.EnableDnsHostnames.Value), nil
	}
	return false, ErrInvalidProvisionRequest
}

func taggedFilters(spec Spec, name string) []ec2types.Filter {
	return []ec2types.Filter{
		{Name: aws.String("tag:Name"), Values: []string{name}},
		{Name: aws.String("tag:dirextalk:managed"), Values: []string{"true"}},
		{Name: aws.String("tag:dirextalk:component"), Values: []string{foundationComponent}},
		{Name: aws.String("tag:dirextalk:connection-id"), Values: []string{spec.Request.ConnectionID}},
	}
}

func gatewayAttachedTo(gateway ec2types.InternetGateway, vpcID string) bool {
	for _, attachment := range gateway.Attachments {
		if aws.ToString(attachment.VpcId) == vpcID && attachment.State == ec2types.AttachmentStatusAttached {
			return true
		}
	}
	return false
}

func subnetObservation(subnet ec2types.Subnet) SubnetObservation {
	return SubnetObservation{ID: aws.ToString(subnet.SubnetId), VPCID: aws.ToString(subnet.VpcId), AvailabilityZone: aws.ToString(subnet.AvailabilityZone), CIDR: aws.ToString(subnet.CidrBlock), MapPublicIPOnLaunch: aws.ToBool(subnet.MapPublicIpOnLaunch), Tags: ec2Tags(subnet.Tags)}
}

func gatewayObservation(gateway ec2types.InternetGateway) GatewayObservation {
	vpcID := ""
	if len(gateway.Attachments) == 1 {
		vpcID = aws.ToString(gateway.Attachments[0].VpcId)
	}
	return GatewayObservation{ID: aws.ToString(gateway.InternetGatewayId), VPCID: vpcID, Tags: ec2Tags(gateway.Tags)}
}

func natObservation(gateway ec2types.NatGateway) NATGatewayObservation {
	return NATGatewayObservation{ID: aws.ToString(gateway.NatGatewayId), SubnetID: aws.ToString(gateway.SubnetId), State: string(gateway.State), Tags: ec2Tags(gateway.Tags)}
}

func routeObservation(table ec2types.RouteTable, subnetID string) RouteObservation {
	result := RouteObservation{RouteTableID: aws.ToString(table.RouteTableId), Tags: ec2Tags(table.Tags)}
	for _, association := range table.Associations {
		if aws.ToString(association.SubnetId) == subnetID {
			result.AssociatedSubnetID = subnetID
			break
		}
	}
	for _, route := range table.Routes {
		if aws.ToString(route.DestinationCidrBlock) == "0.0.0.0/0" {
			result.DestinationCIDR = "0.0.0.0/0"
			if aws.ToString(route.GatewayId) != "" {
				result.TargetID = aws.ToString(route.GatewayId)
			} else {
				result.TargetID = aws.ToString(route.NatGatewayId)
			}
			break
		}
	}
	return result
}

func securityGroupObservation(group ec2types.SecurityGroup) SecurityGroupObservation {
	return SecurityGroupObservation{ID: aws.ToString(group.GroupId), VPCID: aws.ToString(group.VpcId), Ingress: ipPermissions(group.IpPermissions), Egress: ipPermissions(group.IpPermissionsEgress), Tags: ec2Tags(group.Tags)}
}

func ipPermissions(permissions []ec2types.IpPermission) []EgressRule {
	result := make([]EgressRule, 0, len(permissions))
	for _, permission := range permissions {
		if len(permission.IpRanges) != 1 || len(permission.Ipv6Ranges) != 0 || len(permission.PrefixListIds) != 0 || len(permission.UserIdGroupPairs) != 0 || permission.FromPort == nil || permission.ToPort == nil {
			return []EgressRule{{Protocol: "invalid"}}
		}
		result = append(result, EgressRule{Protocol: aws.ToString(permission.IpProtocol), FromPort: aws.ToInt32(permission.FromPort), ToPort: aws.ToInt32(permission.ToPort), CIDR: aws.ToString(permission.IpRanges[0].CidrIp)})
	}
	return result
}

func ec2Tags(tags []ec2types.Tag) map[string]string {
	result := make(map[string]string, len(tags))
	for _, tag := range tags {
		key := aws.ToString(tag.Key)
		if key == "" {
			continue
		}
		if _, exists := result[key]; exists {
			return map[string]string{}
		}
		result[key] = aws.ToString(tag.Value)
	}
	return result
}

func s3Tags(tags []s3types.Tag) map[string]string {
	result := make(map[string]string, len(tags))
	for _, tag := range tags {
		key := aws.ToString(tag.Key)
		if key == "" {
			continue
		}
		if _, exists := result[key]; exists {
			return map[string]string{}
		}
		result[key] = aws.ToString(tag.Value)
	}
	return result
}

func bucketOwnerEnforced(rules []s3types.OwnershipControlsRule) bool {
	return len(rules) == 1 && rules[0].ObjectOwnership == s3types.ObjectOwnershipBucketOwnerEnforced
}

func bucketKMSKeyARN(rules []s3types.ServerSideEncryptionRule) string {
	if len(rules) != 1 || rules[0].ApplyServerSideEncryptionByDefault == nil {
		return ""
	}
	defaultEncryption := rules[0].ApplyServerSideEncryptionByDefault
	if defaultEncryption.SSEAlgorithm != s3types.ServerSideEncryptionAwsKms {
		return ""
	}
	return aws.ToString(defaultEncryption.KMSMasterKeyID)
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return aws.String(value)
}

func isFoundationNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	switch apiError.ErrorCode() {
	case "NoSuchBucket", "NotFound", "404", "NoSuchTagSet", "NoSuchPublicAccessBlockConfiguration", "OwnershipControlsNotFoundError", "ServerSideEncryptionConfigurationNotFoundError", "NotFoundException":
		return true
	default:
		return false
	}
}

func (provider *AWSProvider) ensureVPC(ctx context.Context, spec Spec) (string, error) {
	vpc, found, err := provider.findVPC(ctx, spec)
	if err != nil {
		return "", err
	}
	if !found {
		if err := provider.rejectOverlappingVPC(ctx, spec.Network.VPCCIDR); err != nil {
			return "", err
		}
		created, createErr := provider.ec2.CreateVpc(ctx, &ec2.CreateVpcInput{
			CidrBlock:         aws.String(spec.Network.VPCCIDR),
			TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeVpc, tagsFor(spec, spec.Names.VPC))},
		})
		if createErr != nil || created.Vpc == nil {
			if createErr != nil {
				return "", createErr
			}
			return "", ErrFoundationProvisioning
		}
		vpc = *created.Vpc
	}
	vpcID := aws.ToString(vpc.VpcId)
	if vpcID == "" || aws.ToString(vpc.CidrBlock) != spec.Network.VPCCIDR {
		return "", ErrFoundationReadback
	}
	for _, attribute := range []ec2types.VpcAttributeName{ec2types.VpcAttributeNameEnableDnsSupport, ec2types.VpcAttributeNameEnableDnsHostnames} {
		input := &ec2.ModifyVpcAttributeInput{VpcId: aws.String(vpcID)}
		if attribute == ec2types.VpcAttributeNameEnableDnsSupport {
			input.EnableDnsSupport = &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}
		} else {
			input.EnableDnsHostnames = &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}
		}
		if _, err := provider.ec2.ModifyVpcAttribute(ctx, input); err != nil {
			return "", err
		}
	}
	return vpcID, nil
}

func (provider *AWSProvider) rejectOverlappingVPC(ctx context.Context, cidr string) error {
	desired, err := netip.ParsePrefix(cidr)
	if err != nil {
		return ErrInvalidProvisionRequest
	}
	var nextToken *string
	for {
		output, err := provider.ec2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{NextToken: nextToken})
		if err != nil {
			return err
		}
		for _, vpc := range output.Vpcs {
			for _, association := range vpc.CidrBlockAssociationSet {
				existing, parseErr := netip.ParsePrefix(aws.ToString(association.CidrBlock))
				if parseErr == nil && desired.Overlaps(existing) {
					return ErrFoundationReadback
				}
			}
		}
		if aws.ToString(output.NextToken) == "" {
			return nil
		}
		nextToken = output.NextToken
	}
}

func (provider *AWSProvider) ensureSubnet(ctx context.Context, spec Spec, name, cidr, vpcID string) (string, error) {
	subnet, found, err := provider.findSubnet(ctx, spec, name, vpcID)
	if err != nil {
		return "", err
	}
	if !found {
		created, createErr := provider.ec2.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:             aws.String(vpcID),
			CidrBlock:         aws.String(cidr),
			AvailabilityZone:  aws.String(spec.Request.AvailabilityZone),
			TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeSubnet, tagsFor(spec, name))},
		})
		if createErr != nil || created.Subnet == nil {
			if createErr != nil {
				return "", createErr
			}
			return "", ErrFoundationProvisioning
		}
		subnet = *created.Subnet
	}
	subnetID := aws.ToString(subnet.SubnetId)
	if subnetID == "" || aws.ToString(subnet.CidrBlock) != cidr || aws.ToString(subnet.AvailabilityZone) != spec.Request.AvailabilityZone {
		return "", ErrFoundationReadback
	}
	// Neither subnet automatically grants a public address. The NAT's EIP is
	// explicit; a Worker must remain private even if an accidental launch occurs.
	_, err = provider.ec2.ModifySubnetAttribute(ctx, &ec2.ModifySubnetAttributeInput{SubnetId: aws.String(subnetID), MapPublicIpOnLaunch: &ec2types.AttributeBooleanValue{Value: aws.Bool(false)}})
	return subnetID, err
}

func (provider *AWSProvider) ensureInternetGateway(ctx context.Context, spec Spec, vpcID string) (string, error) {
	gateway, found, err := provider.findInternetGateway(ctx, spec, vpcID)
	if err != nil {
		return "", err
	}
	if !found {
		// A tagged gateway attached to another VPC is not reusable. A tagged,
		// unattached gateway is the only safe response-loss recovery case.
		output, lookupErr := provider.ec2.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{Filters: taggedFilters(spec, spec.Names.InternetGateway)})
		if lookupErr != nil {
			return "", lookupErr
		}
		if len(output.InternetGateways) > 1 {
			return "", ErrFoundationReadback
		}
		if len(output.InternetGateways) == 1 {
			gateway = output.InternetGateways[0]
			gatewayID := aws.ToString(gateway.InternetGatewayId)
			if gatewayID == "" || len(gateway.Attachments) != 0 {
				return "", ErrFoundationReadback
			}
			if _, err := provider.ec2.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{InternetGatewayId: aws.String(gatewayID), VpcId: aws.String(vpcID)}); err != nil {
				return "", err
			}
			return gatewayID, nil
		}
		created, createErr := provider.ec2.CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeInternetGateway, tagsFor(spec, spec.Names.InternetGateway))}})
		if createErr != nil || created.InternetGateway == nil {
			if createErr != nil {
				return "", createErr
			}
			return "", ErrFoundationProvisioning
		}
		gateway = *created.InternetGateway
		gatewayID := aws.ToString(gateway.InternetGatewayId)
		if gatewayID == "" {
			return "", ErrFoundationProvisioning
		}
		if _, err := provider.ec2.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{InternetGatewayId: aws.String(gatewayID), VpcId: aws.String(vpcID)}); err != nil {
			return "", err
		}
		return gatewayID, nil
	}
	return aws.ToString(gateway.InternetGatewayId), nil
}

func (provider *AWSProvider) ensureRouteTable(ctx context.Context, spec Spec, name, subnetID, vpcID, targetID string, public bool) (string, error) {
	table, found, err := provider.findRouteTable(ctx, spec, name, vpcID)
	if err != nil {
		return "", err
	}
	if !found {
		created, createErr := provider.ec2.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: aws.String(vpcID), TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeRouteTable, tagsFor(spec, name))}})
		if createErr != nil || created.RouteTable == nil {
			if createErr != nil {
				return "", createErr
			}
			return "", ErrFoundationProvisioning
		}
		table = *created.RouteTable
	}
	tableID := aws.ToString(table.RouteTableId)
	if tableID == "" {
		return "", ErrFoundationReadback
	}
	if err := provider.ensureDefaultRoute(ctx, table, targetID, public); err != nil {
		return "", err
	}
	if err := provider.ensureRouteTableAssociation(ctx, tableID, subnetID); err != nil {
		return "", err
	}
	return tableID, nil
}

func (provider *AWSProvider) ensureDefaultRoute(ctx context.Context, table ec2types.RouteTable, targetID string, public bool) error {
	for _, route := range table.Routes {
		if aws.ToString(route.DestinationCidrBlock) != "0.0.0.0/0" {
			continue
		}
		if public && aws.ToString(route.GatewayId) == targetID || !public && aws.ToString(route.NatGatewayId) == targetID {
			return nil
		}
		return ErrFoundationReadback
	}
	input := &ec2.CreateRouteInput{RouteTableId: table.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0")}
	if public {
		input.GatewayId = aws.String(targetID)
	} else {
		input.NatGatewayId = aws.String(targetID)
	}
	_, err := provider.ec2.CreateRoute(ctx, input)
	return err
}

func (provider *AWSProvider) ensureRouteTableAssociation(ctx context.Context, tableID, subnetID string) error {
	output, err := provider.ec2.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []ec2types.Filter{{Name: aws.String("association.subnet-id"), Values: []string{subnetID}}}})
	if err != nil {
		return err
	}
	if len(output.RouteTables) > 1 {
		return ErrFoundationReadback
	}
	if len(output.RouteTables) == 1 {
		if aws.ToString(output.RouteTables[0].RouteTableId) != tableID {
			return ErrFoundationReadback
		}
		return nil
	}
	_, err = provider.ec2.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{RouteTableId: aws.String(tableID), SubnetId: aws.String(subnetID)})
	return err
}

func (provider *AWSProvider) ensureEIP(ctx context.Context, spec Spec) (string, error) {
	name := spec.Names.NATGateway + "-eip"
	output, err := provider.ec2.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{Filters: taggedFilters(spec, name)})
	if err != nil {
		return "", err
	}
	if len(output.Addresses) > 1 {
		return "", ErrFoundationReadback
	}
	if len(output.Addresses) == 1 {
		// A tagged EIP may already be attached to the deterministic NAT after a
		// response-loss. The following NAT readback still verifies the tagged
		// gateway/subnet; rejecting it here would cause recovery to fail before
		// that check and tempt a duplicate allocation.
		if allocationID := aws.ToString(output.Addresses[0].AllocationId); allocationID != "" {
			return allocationID, nil
		}
		return "", ErrFoundationReadback
	}
	allocated, err := provider.ec2.AllocateAddress(ctx, &ec2.AllocateAddressInput{Domain: ec2types.DomainTypeVpc, TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeElasticIp, tagsFor(spec, name))}})
	if err != nil {
		return "", err
	}
	if aws.ToString(allocated.AllocationId) == "" {
		return "", ErrFoundationProvisioning
	}
	return aws.ToString(allocated.AllocationId), nil
}

func (provider *AWSProvider) ensureNATGateway(ctx context.Context, spec Spec, subnetID, allocationID string) (string, error) {
	nat, found, err := provider.findNATGateway(ctx, spec)
	if err != nil {
		return "", err
	}
	if !found {
		created, createErr := provider.ec2.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{SubnetId: aws.String(subnetID), AllocationId: aws.String(allocationID), ClientToken: aws.String(spec.ClientToken("create-nat-gateway")), ConnectivityType: ec2types.ConnectivityTypePublic, TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeNatgateway, tagsFor(spec, spec.Names.NATGateway))}})
		if createErr != nil || created.NatGateway == nil {
			if createErr != nil {
				return "", createErr
			}
			return "", ErrFoundationProvisioning
		}
		nat = *created.NatGateway
	}
	if aws.ToString(nat.SubnetId) != subnetID {
		return "", ErrFoundationReadback
	}
	available, err := provider.waitForNATGateway(ctx, aws.ToString(nat.NatGatewayId))
	if err != nil {
		return "", err
	}
	if !natUsesAllocation(available, allocationID) {
		return "", ErrFoundationReadback
	}
	return aws.ToString(nat.NatGatewayId), nil
}

func (provider *AWSProvider) waitForNATGateway(ctx context.Context, natID string) (ec2types.NatGateway, error) {
	if natID == "" {
		return ec2types.NatGateway{}, ErrFoundationProvisioning
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		output, err := provider.ec2.DescribeNatGateways(waitCtx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{natID}})
		if err != nil {
			return ec2types.NatGateway{}, err
		}
		if len(output.NatGateways) != 1 {
			return ec2types.NatGateway{}, ErrFoundationReadback
		}
		switch output.NatGateways[0].State {
		case ec2types.NatGatewayStateAvailable:
			return output.NatGateways[0], nil
		case ec2types.NatGatewayStateFailed, ec2types.NatGatewayStateDeleting, ec2types.NatGatewayStateDeleted:
			return ec2types.NatGateway{}, ErrFoundationProvisioning
		}
		select {
		case <-waitCtx.Done():
			return ec2types.NatGateway{}, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func natUsesAllocation(gateway ec2types.NatGateway, allocationID string) bool {
	for _, address := range gateway.NatGatewayAddresses {
		if aws.ToString(address.AllocationId) == allocationID {
			return true
		}
	}
	return false
}

func (provider *AWSProvider) ensureSecurityGroup(ctx context.Context, spec Spec, vpcID string) (string, error) {
	group, found, err := provider.findSecurityGroup(ctx, spec, vpcID)
	if err != nil {
		return "", err
	}
	if found {
		// Existing groups are never "fixed" by widening or replacing rules. The
		// independent readback will reject drift, preserving the reviewed scope.
		return aws.ToString(group.GroupId), nil
	}
	created, createErr := provider.ec2.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:         aws.String(spec.Names.WorkerSecurityGrp),
		Description:       aws.String("Dirextalk private Worker egress-only security group"),
		VpcId:             aws.String(vpcID),
		TagSpecifications: []ec2types.TagSpecification{ec2TagSpecification(ec2types.ResourceTypeSecurityGroup, tagsFor(spec, spec.Names.WorkerSecurityGrp))},
	})
	if createErr != nil {
		return "", createErr
	}
	groupID := aws.ToString(created.GroupId)
	if groupID == "" {
		return "", ErrFoundationProvisioning
	}
	observed, err := provider.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{groupID}})
	if err != nil || len(observed.SecurityGroups) != 1 {
		if err != nil {
			return "", err
		}
		return "", ErrFoundationReadback
	}
	if len(observed.SecurityGroups[0].IpPermissions) != 0 {
		return "", ErrFoundationReadback
	}
	if len(observed.SecurityGroups[0].IpPermissionsEgress) > 0 {
		if _, err := provider.ec2.RevokeSecurityGroupEgress(ctx, &ec2.RevokeSecurityGroupEgressInput{GroupId: aws.String(groupID), IpPermissions: observed.SecurityGroups[0].IpPermissionsEgress}); err != nil {
			return "", err
		}
	}
	if _, err := provider.ec2.AuthorizeSecurityGroupEgress(ctx, &ec2.AuthorizeSecurityGroupEgressInput{GroupId: aws.String(groupID), IpPermissions: awsEgressPermissions(spec.Egress)}); err != nil {
		return "", err
	}
	return groupID, nil
}

func awsEgressPermissions(rules []EgressRule) []ec2types.IpPermission {
	permissions := make([]ec2types.IpPermission, 0, len(rules))
	for _, rule := range rules {
		permissions = append(permissions, ec2types.IpPermission{IpProtocol: aws.String(rule.Protocol), FromPort: aws.Int32(rule.FromPort), ToPort: aws.Int32(rule.ToPort), IpRanges: []ec2types.IpRange{{CidrIp: aws.String(rule.CIDR)}}})
	}
	return permissions
}

func (provider *AWSProvider) ensureKMSKey(ctx context.Context, spec Spec) (KMSKeyObservation, error) {
	key, found, err := provider.findKMSKey(ctx, spec)
	if err != nil {
		return KMSKeyObservation{}, err
	}
	if found {
		return key, nil
	}
	// CreateKey itself does not accept a client token. If its response was lost
	// before CreateAlias, recover only an exact tagged key and bind the
	// deterministic alias to it; never create a second ambiguous key.
	key, found, err = provider.findTaggedKMSKey(ctx, spec)
	if err != nil {
		return KMSKeyObservation{}, err
	}
	if found {
		if _, err := provider.kms.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/" + spec.Names.ArtifactKMSKey), TargetKeyId: aws.String(key.ARN)}); err != nil {
			return KMSKeyObservation{}, err
		}
		return key, nil
	}
	created, createErr := provider.kms.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("Dirextalk immutable Connection Foundation artifact key"),
		KeySpec:     kmstypes.KeySpecSymmetricDefault,
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
		MultiRegion: aws.Bool(false),
		Tags:        kmsTags(tagsFor(spec, spec.Names.ArtifactKMSKey)),
	})
	if createErr != nil || created.KeyMetadata == nil {
		if createErr != nil {
			return KMSKeyObservation{}, createErr
		}
		return KMSKeyObservation{}, ErrFoundationProvisioning
	}
	keyID := aws.ToString(created.KeyMetadata.KeyId)
	keyARN := aws.ToString(created.KeyMetadata.Arn)
	if keyID == "" || keyARN == "" {
		return KMSKeyObservation{}, ErrFoundationProvisioning
	}
	if _, err := provider.kms.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/" + spec.Names.ArtifactKMSKey), TargetKeyId: aws.String(keyID)}); err != nil {
		return KMSKeyObservation{}, err
	}
	return KMSKeyObservation{ARN: keyARN, Enabled: created.KeyMetadata.Enabled, KeySpec: string(created.KeyMetadata.KeySpec), KeyUsage: string(created.KeyMetadata.KeyUsage), MultiRegion: aws.ToBool(created.KeyMetadata.MultiRegion), Tags: kmsTagsToMap(kmsTags(tagsFor(spec, spec.Names.ArtifactKMSKey)))}, nil
}

func (provider *AWSProvider) findTaggedKMSKey(ctx context.Context, spec Spec) (KMSKeyObservation, bool, error) {
	required := tagsFor(spec, spec.Names.ArtifactKMSKey)
	var matches []KMSKeyObservation
	for marker := ""; ; {
		output, err := provider.kms.ListKeys(ctx, &kms.ListKeysInput{Marker: optionalString(marker)})
		if err != nil {
			return KMSKeyObservation{}, false, err
		}
		for _, entry := range output.Keys {
			arn := aws.ToString(entry.KeyArn)
			if arn == "" {
				continue
			}
			tags, tagsErr := provider.kmsTags(ctx, arn)
			if tagsErr != nil {
				return KMSKeyObservation{}, false, tagsErr
			}
			if !sameRequiredTags(tags, required) {
				continue
			}
			described, describeErr := provider.kms.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(arn)})
			if describeErr != nil || described.KeyMetadata == nil {
				if describeErr != nil {
					return KMSKeyObservation{}, false, describeErr
				}
				return KMSKeyObservation{}, false, ErrFoundationReadback
			}
			matches = append(matches, KMSKeyObservation{ARN: aws.ToString(described.KeyMetadata.Arn), Enabled: described.KeyMetadata.Enabled, KeySpec: string(described.KeyMetadata.KeySpec), KeyUsage: string(described.KeyMetadata.KeyUsage), MultiRegion: aws.ToBool(described.KeyMetadata.MultiRegion), Tags: tags})
		}
		if !output.Truncated {
			break
		}
		marker = aws.ToString(output.NextMarker)
		if marker == "" {
			return KMSKeyObservation{}, false, ErrFoundationReadback
		}
	}
	if len(matches) == 0 {
		return KMSKeyObservation{}, false, nil
	}
	if len(matches) != 1 {
		return KMSKeyObservation{}, false, ErrFoundationReadback
	}
	return matches[0], true, nil
}

func (provider *AWSProvider) kmsTags(ctx context.Context, keyARN string) (map[string]string, error) {
	result := map[string]string{}
	for marker := ""; ; {
		output, err := provider.kms.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: aws.String(keyARN), Marker: optionalString(marker)})
		if err != nil {
			return nil, err
		}
		for _, tag := range output.Tags {
			key := aws.ToString(tag.TagKey)
			if key == "" {
				return nil, ErrFoundationReadback
			}
			if _, exists := result[key]; exists {
				return nil, ErrFoundationReadback
			}
			result[key] = aws.ToString(tag.TagValue)
		}
		if !output.Truncated {
			return result, nil
		}
		marker = aws.ToString(output.NextMarker)
		if marker == "" {
			return nil, ErrFoundationReadback
		}
	}
}

func (provider *AWSProvider) ensureBucket(ctx context.Context, spec Spec, keyARN string) error {
	bucket := spec.Names.ArtifactBucket
	_, headErr := provider.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
	if headErr != nil && !isFoundationNotFound(headErr) {
		return headErr
	}
	if isFoundationNotFound(headErr) {
		input := &s3.CreateBucketInput{Bucket: aws.String(bucket), ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced}
		if spec.Request.Region != "us-east-1" {
			input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraint(spec.Request.Region)}
		}
		if _, err := provider.s3.CreateBucket(ctx, input); err != nil {
			return err
		}
		if _, err := provider.s3.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID), Tagging: &s3types.Tagging{TagSet: s3TagSet(tagsFor(spec, spec.Names.ArtifactBucket))}}); err != nil {
			return err
		}
	} else {
		// A pre-existing bucket is usable only after its deterministic provenance
		// tags read back exactly. This avoids claiming an unrelated same-account
		// bucket merely because a globally unique name collided.
		tags, err := provider.s3.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID)})
		if err != nil || !sameRequiredTags(s3Tags(tags.TagSet), tagsFor(spec, spec.Names.ArtifactBucket)) {
			return ErrFoundationReadback
		}
	}
	// These are all monotonic hardening controls. Reapplying them after a
	// response-loss is safe, while readback rejects a pre-existing foreign or
	// broadened bucket before it can be used as an artifact source.
	if _, err := provider.s3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		return err
	}
	if _, err := provider.s3.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID), PublicAccessBlockConfiguration: &s3types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(true), IgnorePublicAcls: aws.Bool(true), BlockPublicPolicy: aws.Bool(true), RestrictPublicBuckets: aws.Bool(true)}}); err != nil {
		return err
	}
	if _, err := provider.s3.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID), OwnershipControls: &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced}}}}); err != nil {
		return err
	}
	if _, err := provider.s3.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ExpectedBucketOwner: aws.String(spec.Request.AccountID), ServerSideEncryptionConfiguration: &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAwsKms, KMSMasterKeyID: aws.String(keyARN)}, BucketKeyEnabled: aws.Bool(true)}}}}); err != nil {
		return err
	}
	return nil
}

func ec2TagSpecification(resourceType ec2types.ResourceType, tags map[string]string) ec2types.TagSpecification {
	return ec2types.TagSpecification{ResourceType: resourceType, Tags: ec2TagSet(tags)}
}

func ec2TagSet(values map[string]string) []ec2types.Tag {
	keys := sortedTagKeys(values)
	result := make([]ec2types.Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, ec2types.Tag{Key: aws.String(key), Value: aws.String(values[key])})
	}
	return result
}

func s3TagSet(values map[string]string) []s3types.Tag {
	keys := sortedTagKeys(values)
	result := make([]s3types.Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, s3types.Tag{Key: aws.String(key), Value: aws.String(values[key])})
	}
	return result
}

func kmsTags(values map[string]string) []kmstypes.Tag {
	keys := sortedTagKeys(values)
	result := make([]kmstypes.Tag, 0, len(keys))
	for _, key := range keys {
		result = append(result, kmstypes.Tag{TagKey: aws.String(key), TagValue: aws.String(values[key])})
	}
	return result
}

func kmsTagsToMap(tags []kmstypes.Tag) map[string]string {
	result := make(map[string]string, len(tags))
	for _, tag := range tags {
		result[aws.ToString(tag.TagKey)] = aws.ToString(tag.TagValue)
	}
	return result
}

func sortedTagKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
