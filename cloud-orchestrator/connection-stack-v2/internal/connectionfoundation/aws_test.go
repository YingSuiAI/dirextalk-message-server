package connectionfoundation

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestAWSProviderObserveReadsExistingFoundationWithoutMutation(t *testing.T) {
	spec, err := deriveSpec(testProvisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	ec2Fake := &awsObserveEC2{spec: spec}
	s3Fake := &awsObserveS3{spec: spec}
	kmsFake := &awsObserveKMS{spec: spec}
	provider, err := NewAWSProvider(ec2Fake, s3Fake, kmsFake)
	if err != nil {
		t.Fatal(err)
	}

	observation, found, err := provider.Observe(context.Background(), spec)
	if err != nil || !found {
		t.Fatalf("Observe() found=%v err=%v", found, err)
	}
	facts, err := factsFromObservation(spec, observation)
	if err != nil {
		t.Fatal(err)
	}
	if facts.ArtifactKMSKeyARN != kmsFake.keyARN() || facts.WorkerSecurityGroupID == "" {
		t.Fatalf("unexpected readback facts: %#v", facts)
	}
	if ec2Fake.mutations != 0 || s3Fake.mutations != 0 || kmsFake.mutations != 0 {
		t.Fatalf("Observe unexpectedly mutated provider: ec2=%d s3=%d kms=%d", ec2Fake.mutations, s3Fake.mutations, kmsFake.mutations)
	}
}

func TestAWSProviderObserveKeepsIncompleteTaggedBucketRecoverable(t *testing.T) {
	spec, err := deriveSpec(testProvisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewAWSProvider(&awsObserveEC2{spec: spec}, &awsObserveS3{spec: spec, incompleteBucket: true}, &awsObserveKMS{spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := provider.Observe(context.Background(), spec); err != nil || found {
		t.Fatalf("incomplete tagged bucket should request recovery, found=%v err=%v", found, err)
	}
}

func TestAWSProviderObservationRejectsUnexpectedInboundRule(t *testing.T) {
	spec, err := deriveSpec(testProvisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewAWSProvider(&awsObserveEC2{spec: spec, injectIngress: true}, &awsObserveS3{spec: spec}, &awsObserveKMS{spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	observation, found, err := provider.Observe(context.Background(), spec)
	if err != nil || !found {
		t.Fatalf("Observe() found=%v err=%v", found, err)
	}
	if _, err := factsFromObservation(spec, observation); err != ErrFoundationReadback {
		t.Fatalf("unexpected inbound readback error=%v", err)
	}
}

func TestAWSProviderEnsureCreatesOnlyClosedFoundationCapabilities(t *testing.T) {
	spec, err := deriveSpec(testProvisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	ec2Fake := &awsEnsureEC2{spec: spec}
	s3Fake := &awsEnsureS3{}
	kmsFake := &awsEnsureKMS{spec: spec}
	provider, err := NewAWSProvider(ec2Fake, s3Fake, kmsFake)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Ensure(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if ec2Fake.natInput == nil || aws.ToString(ec2Fake.natInput.ClientToken) != spec.ClientToken("create-nat-gateway") || aws.ToString(ec2Fake.natInput.SubnetId) != "subnet-0123456789abcdef0" {
		t.Fatalf("NAT request did not use deterministic private foundation input: %#v", ec2Fake.natInput)
	}
	if ec2Fake.egressInput == nil || len(ec2Fake.egressInput.IpPermissions) != 3 {
		t.Fatalf("expected exactly three closed egress rules: %#v", ec2Fake.egressInput)
	}
	for _, permission := range ec2Fake.egressInput.IpPermissions {
		if aws.ToString(permission.IpProtocol) == "-1" || len(permission.IpRanges) != 1 || aws.ToString(permission.IpRanges[0].CidrIp) == "" {
			t.Fatalf("unexpected broad egress permission: %#v", permission)
		}
	}
	if s3Fake.create == nil || s3Fake.encryption == nil || kmsFake.create == nil || aws.ToString(s3Fake.encryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID) != kmsFake.keyARN() {
		t.Fatalf("bucket/KMS requests did not stay bound to the generated key")
	}
}

func TestAWSProviderRecoversTaggedKMSAfterCreateResponseLoss(t *testing.T) {
	spec, err := deriveSpec(testProvisionRequest())
	if err != nil {
		t.Fatal(err)
	}
	kmsFake := &awsEnsureKMS{spec: spec, recoverTagged: true}
	provider, err := NewAWSProvider(&awsEnsureEC2{spec: spec}, &awsEnsureS3{}, kmsFake)
	if err != nil {
		t.Fatal(err)
	}
	key, err := provider.ensureKMSKey(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if key.ARN != kmsFake.keyARN() || kmsFake.create != nil || !kmsFake.aliasCreated {
		t.Fatalf("KMS recovery created a second key or missed deterministic alias: key=%#v created=%v alias=%v", key, kmsFake.create != nil, kmsFake.aliasCreated)
	}
}

// The embedded interfaces make unsupported SDK calls fail loudly if the
// adapter unexpectedly expands its capability. These fakes only implement
// the exact read-only calls exercised by Observe.
type awsObserveEC2 struct {
	EC2FoundationAPI
	spec          Spec
	injectIngress bool
	mutations     int
}

func (fake *awsObserveEC2) DescribeVpcs(_ context.Context, _ *ec2.DescribeVpcsInput, _ ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	return &ec2.DescribeVpcsOutput{Vpcs: []ec2types.Vpc{{VpcId: aws.String("vpc-0123456789abcdef0"), CidrBlock: aws.String(fake.spec.Network.VPCCIDR), Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.VPC))}}}, nil
}

func (fake *awsObserveEC2) DescribeVpcAttribute(_ context.Context, input *ec2.DescribeVpcAttributeInput, _ ...func(*ec2.Options)) (*ec2.DescribeVpcAttributeOutput, error) {
	if input.Attribute == ec2types.VpcAttributeNameEnableDnsSupport {
		return &ec2.DescribeVpcAttributeOutput{EnableDnsSupport: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}}, nil
	}
	return &ec2.DescribeVpcAttributeOutput{EnableDnsHostnames: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}}, nil
}

func (fake *awsObserveEC2) DescribeSubnets(_ context.Context, input *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	name := filterValue(input.Filters, "tag:Name")
	if name == fake.spec.Names.PublicSubnet {
		return &ec2.DescribeSubnetsOutput{Subnets: []ec2types.Subnet{{SubnetId: aws.String("subnet-0123456789abcdef0"), VpcId: aws.String("vpc-0123456789abcdef0"), AvailabilityZone: aws.String(fake.spec.Request.AvailabilityZone), CidrBlock: aws.String(fake.spec.Network.PublicSubnetCIDR), MapPublicIpOnLaunch: aws.Bool(false), Tags: ec2TagSet(tagsFor(fake.spec, name))}}}, nil
	}
	return &ec2.DescribeSubnetsOutput{Subnets: []ec2types.Subnet{{SubnetId: aws.String("subnet-0123456789abcdee0"), VpcId: aws.String("vpc-0123456789abcdef0"), AvailabilityZone: aws.String(fake.spec.Request.AvailabilityZone), CidrBlock: aws.String(fake.spec.Network.PrivateSubnetCIDR), MapPublicIpOnLaunch: aws.Bool(false), Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.PrivateSubnet))}}}, nil
}

func (fake *awsObserveEC2) DescribeInternetGateways(_ context.Context, _ *ec2.DescribeInternetGatewaysInput, _ ...func(*ec2.Options)) (*ec2.DescribeInternetGatewaysOutput, error) {
	return &ec2.DescribeInternetGatewaysOutput{InternetGateways: []ec2types.InternetGateway{{InternetGatewayId: aws.String("igw-0123456789abcdef0"), Attachments: []ec2types.InternetGatewayAttachment{{VpcId: aws.String("vpc-0123456789abcdef0"), State: ec2types.AttachmentStatusAttached}}, Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.InternetGateway))}}}, nil
}

func (fake *awsObserveEC2) DescribeNatGateways(_ context.Context, _ *ec2.DescribeNatGatewaysInput, _ ...func(*ec2.Options)) (*ec2.DescribeNatGatewaysOutput, error) {
	return &ec2.DescribeNatGatewaysOutput{NatGateways: []ec2types.NatGateway{{NatGatewayId: aws.String("nat-0123456789abcdef0"), SubnetId: aws.String("subnet-0123456789abcdef0"), State: ec2types.NatGatewayStateAvailable, Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.NATGateway))}}}, nil
}

func (fake *awsObserveEC2) DescribeRouteTables(_ context.Context, input *ec2.DescribeRouteTablesInput, _ ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error) {
	name := filterValue(input.Filters, "tag:Name")
	if name == fake.spec.Names.PublicRouteTable {
		return &ec2.DescribeRouteTablesOutput{RouteTables: []ec2types.RouteTable{{RouteTableId: aws.String("rtb-0123456789abcdef0"), Associations: []ec2types.RouteTableAssociation{{SubnetId: aws.String("subnet-0123456789abcdef0")}}, Routes: []ec2types.Route{{DestinationCidrBlock: aws.String("0.0.0.0/0"), GatewayId: aws.String("igw-0123456789abcdef0")}}, Tags: ec2TagSet(tagsFor(fake.spec, name))}}}, nil
	}
	return &ec2.DescribeRouteTablesOutput{RouteTables: []ec2types.RouteTable{{RouteTableId: aws.String("rtb-0123456789abcdee0"), Associations: []ec2types.RouteTableAssociation{{SubnetId: aws.String("subnet-0123456789abcdee0")}}, Routes: []ec2types.Route{{DestinationCidrBlock: aws.String("0.0.0.0/0"), NatGatewayId: aws.String("nat-0123456789abcdef0")}}, Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.PrivateRouteTable))}}}, nil
}

func (fake *awsObserveEC2) DescribeSecurityGroups(_ context.Context, _ *ec2.DescribeSecurityGroupsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	ingress := []ec2types.IpPermission(nil)
	if fake.injectIngress {
		ingress = []ec2types.IpPermission{{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(22), ToPort: aws.Int32(22), IpRanges: []ec2types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}
	}
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{{GroupId: aws.String("sg-0123456789abcdef0"), VpcId: aws.String("vpc-0123456789abcdef0"), IpPermissions: ingress, IpPermissionsEgress: awsEgressPermissions(fake.spec.Egress), Tags: ec2TagSet(tagsFor(fake.spec, fake.spec.Names.WorkerSecurityGrp))}}}, nil
}

type awsObserveS3 struct {
	S3FoundationAPI
	spec             Spec
	incompleteBucket bool
	mutations        int
}

func (fake *awsObserveS3) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, nil
}

func (fake *awsObserveS3) GetBucketTagging(context.Context, *s3.GetBucketTaggingInput, ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error) {
	return &s3.GetBucketTaggingOutput{TagSet: s3TagSet(tagsFor(fake.spec, fake.spec.Names.ArtifactBucket))}, nil
}

func (fake *awsObserveS3) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	if fake.incompleteBucket {
		return &s3.GetBucketVersioningOutput{}, nil
	}
	return &s3.GetBucketVersioningOutput{Status: s3types.BucketVersioningStatusEnabled}, nil
}

func (fake *awsObserveS3) GetPublicAccessBlock(context.Context, *s3.GetPublicAccessBlockInput, ...func(*s3.Options)) (*s3.GetPublicAccessBlockOutput, error) {
	return &s3.GetPublicAccessBlockOutput{PublicAccessBlockConfiguration: &s3types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(true), IgnorePublicAcls: aws.Bool(true), BlockPublicPolicy: aws.Bool(true), RestrictPublicBuckets: aws.Bool(true)}}, nil
}

func (fake *awsObserveS3) GetBucketOwnershipControls(context.Context, *s3.GetBucketOwnershipControlsInput, ...func(*s3.Options)) (*s3.GetBucketOwnershipControlsOutput, error) {
	return &s3.GetBucketOwnershipControlsOutput{OwnershipControls: &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced}}}}, nil
}

func (fake *awsObserveS3) GetBucketEncryption(context.Context, *s3.GetBucketEncryptionInput, ...func(*s3.Options)) (*s3.GetBucketEncryptionOutput, error) {
	return &s3.GetBucketEncryptionOutput{ServerSideEncryptionConfiguration: &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAwsKms, KMSMasterKeyID: aws.String("arn:aws:kms:" + fake.spec.Request.Region + ":" + fake.spec.Request.AccountID + ":key/01234567-89ab-cdef-0123-456789abcdef")}}}}}, nil
}

type awsObserveKMS struct {
	KMSFoundationAPI
	spec      Spec
	mutations int
}

func (fake *awsObserveKMS) keyARN() string {
	return "arn:aws:kms:" + fake.spec.Request.Region + ":" + fake.spec.Request.AccountID + ":key/01234567-89ab-cdef-0123-456789abcdef"
}

func (fake *awsObserveKMS) ListAliases(context.Context, *kms.ListAliasesInput, ...func(*kms.Options)) (*kms.ListAliasesOutput, error) {
	return &kms.ListAliasesOutput{Aliases: []kmstypes.AliasListEntry{{AliasName: aws.String("alias/" + fake.spec.Names.ArtifactKMSKey), TargetKeyId: aws.String("01234567-89ab-cdef-0123-456789abcdef")}}}, nil
}

func (fake *awsObserveKMS) DescribeKey(context.Context, *kms.DescribeKeyInput, ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	return &kms.DescribeKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{Arn: aws.String(fake.keyARN()), KeyId: aws.String("01234567-89ab-cdef-0123-456789abcdef"), Enabled: true, KeySpec: kmstypes.KeySpecSymmetricDefault, KeyUsage: kmstypes.KeyUsageTypeEncryptDecrypt, MultiRegion: aws.Bool(false)}}, nil
}

func (fake *awsObserveKMS) ListResourceTags(context.Context, *kms.ListResourceTagsInput, ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error) {
	return &kms.ListResourceTagsOutput{Tags: kmsTags(tagsFor(fake.spec, fake.spec.Names.ArtifactKMSKey))}, nil
}

func filterValue(filters []ec2types.Filter, name string) string {
	for _, filter := range filters {
		if aws.ToString(filter.Name) == name && len(filter.Values) == 1 {
			return filter.Values[0]
		}
	}
	return ""
}

type awsEnsureEC2 struct {
	EC2FoundationAPI
	spec        Spec
	natInput    *ec2.CreateNatGatewayInput
	egressInput *ec2.AuthorizeSecurityGroupEgressInput
}

func (fake *awsEnsureEC2) DescribeVpcs(context.Context, *ec2.DescribeVpcsInput, ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	return &ec2.DescribeVpcsOutput{}, nil
}

func (fake *awsEnsureEC2) CreateVpc(_ context.Context, input *ec2.CreateVpcInput, _ ...func(*ec2.Options)) (*ec2.CreateVpcOutput, error) {
	return &ec2.CreateVpcOutput{Vpc: &ec2types.Vpc{VpcId: aws.String("vpc-0123456789abcdef0"), CidrBlock: input.CidrBlock}}, nil
}

func (fake *awsEnsureEC2) ModifyVpcAttribute(context.Context, *ec2.ModifyVpcAttributeInput, ...func(*ec2.Options)) (*ec2.ModifyVpcAttributeOutput, error) {
	return &ec2.ModifyVpcAttributeOutput{}, nil
}

func (fake *awsEnsureEC2) DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	return &ec2.DescribeSubnetsOutput{}, nil
}

func (fake *awsEnsureEC2) CreateSubnet(_ context.Context, input *ec2.CreateSubnetInput, _ ...func(*ec2.Options)) (*ec2.CreateSubnetOutput, error) {
	id := "subnet-0123456789abcdef0"
	if aws.ToString(input.CidrBlock) == fake.spec.Network.PrivateSubnetCIDR {
		id = "subnet-0123456789abcdee0"
	}
	return &ec2.CreateSubnetOutput{Subnet: &ec2types.Subnet{SubnetId: aws.String(id), CidrBlock: input.CidrBlock, AvailabilityZone: input.AvailabilityZone}}, nil
}

func (fake *awsEnsureEC2) ModifySubnetAttribute(context.Context, *ec2.ModifySubnetAttributeInput, ...func(*ec2.Options)) (*ec2.ModifySubnetAttributeOutput, error) {
	return &ec2.ModifySubnetAttributeOutput{}, nil
}

func (fake *awsEnsureEC2) DescribeInternetGateways(context.Context, *ec2.DescribeInternetGatewaysInput, ...func(*ec2.Options)) (*ec2.DescribeInternetGatewaysOutput, error) {
	return &ec2.DescribeInternetGatewaysOutput{}, nil
}

func (fake *awsEnsureEC2) CreateInternetGateway(context.Context, *ec2.CreateInternetGatewayInput, ...func(*ec2.Options)) (*ec2.CreateInternetGatewayOutput, error) {
	return &ec2.CreateInternetGatewayOutput{InternetGateway: &ec2types.InternetGateway{InternetGatewayId: aws.String("igw-0123456789abcdef0")}}, nil
}

func (fake *awsEnsureEC2) AttachInternetGateway(context.Context, *ec2.AttachInternetGatewayInput, ...func(*ec2.Options)) (*ec2.AttachInternetGatewayOutput, error) {
	return &ec2.AttachInternetGatewayOutput{}, nil
}

func (fake *awsEnsureEC2) DescribeRouteTables(context.Context, *ec2.DescribeRouteTablesInput, ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error) {
	return &ec2.DescribeRouteTablesOutput{}, nil
}

func (fake *awsEnsureEC2) CreateRouteTable(context.Context, *ec2.CreateRouteTableInput, ...func(*ec2.Options)) (*ec2.CreateRouteTableOutput, error) {
	return &ec2.CreateRouteTableOutput{RouteTable: &ec2types.RouteTable{RouteTableId: aws.String("rtb-0123456789abcdef0")}}, nil
}

func (fake *awsEnsureEC2) CreateRoute(context.Context, *ec2.CreateRouteInput, ...func(*ec2.Options)) (*ec2.CreateRouteOutput, error) {
	return &ec2.CreateRouteOutput{Return: aws.Bool(true)}, nil
}

func (fake *awsEnsureEC2) AssociateRouteTable(context.Context, *ec2.AssociateRouteTableInput, ...func(*ec2.Options)) (*ec2.AssociateRouteTableOutput, error) {
	return &ec2.AssociateRouteTableOutput{}, nil
}

func (fake *awsEnsureEC2) DescribeAddresses(context.Context, *ec2.DescribeAddressesInput, ...func(*ec2.Options)) (*ec2.DescribeAddressesOutput, error) {
	return &ec2.DescribeAddressesOutput{}, nil
}

func (fake *awsEnsureEC2) AllocateAddress(context.Context, *ec2.AllocateAddressInput, ...func(*ec2.Options)) (*ec2.AllocateAddressOutput, error) {
	return &ec2.AllocateAddressOutput{AllocationId: aws.String("eipalloc-0123456789abcdef0")}, nil
}

func (fake *awsEnsureEC2) DescribeNatGateways(_ context.Context, input *ec2.DescribeNatGatewaysInput, _ ...func(*ec2.Options)) (*ec2.DescribeNatGatewaysOutput, error) {
	if len(input.NatGatewayIds) == 1 {
		return &ec2.DescribeNatGatewaysOutput{NatGateways: []ec2types.NatGateway{{NatGatewayId: aws.String(input.NatGatewayIds[0]), State: ec2types.NatGatewayStateAvailable, NatGatewayAddresses: []ec2types.NatGatewayAddress{{AllocationId: aws.String("eipalloc-0123456789abcdef0")}}}}}, nil
	}
	return &ec2.DescribeNatGatewaysOutput{}, nil
}

func (fake *awsEnsureEC2) CreateNatGateway(_ context.Context, input *ec2.CreateNatGatewayInput, _ ...func(*ec2.Options)) (*ec2.CreateNatGatewayOutput, error) {
	fake.natInput = input
	return &ec2.CreateNatGatewayOutput{NatGateway: &ec2types.NatGateway{NatGatewayId: aws.String("nat-0123456789abcdef0"), SubnetId: input.SubnetId}}, nil
}

func (fake *awsEnsureEC2) DescribeSecurityGroups(_ context.Context, input *ec2.DescribeSecurityGroupsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	if len(input.GroupIds) == 1 {
		return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{{GroupId: aws.String(input.GroupIds[0]), IpPermissionsEgress: []ec2types.IpPermission{{IpProtocol: aws.String("-1"), IpRanges: []ec2types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}}}}, nil
	}
	return &ec2.DescribeSecurityGroupsOutput{}, nil
}

func (fake *awsEnsureEC2) CreateSecurityGroup(context.Context, *ec2.CreateSecurityGroupInput, ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	return &ec2.CreateSecurityGroupOutput{GroupId: aws.String("sg-0123456789abcdef0")}, nil
}

func (fake *awsEnsureEC2) RevokeSecurityGroupEgress(context.Context, *ec2.RevokeSecurityGroupEgressInput, ...func(*ec2.Options)) (*ec2.RevokeSecurityGroupEgressOutput, error) {
	return &ec2.RevokeSecurityGroupEgressOutput{}, nil
}

func (fake *awsEnsureEC2) AuthorizeSecurityGroupEgress(_ context.Context, input *ec2.AuthorizeSecurityGroupEgressInput, _ ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupEgressOutput, error) {
	fake.egressInput = input
	return &ec2.AuthorizeSecurityGroupEgressOutput{}, nil
}

type awsEnsureS3 struct {
	S3FoundationAPI
	create     *s3.CreateBucketInput
	encryption *s3.PutBucketEncryptionInput
}

func (fake *awsEnsureS3) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "missing"}
}

func (fake *awsEnsureS3) CreateBucket(_ context.Context, input *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	fake.create = input
	return &s3.CreateBucketOutput{}, nil
}

func (fake *awsEnsureS3) PutBucketTagging(context.Context, *s3.PutBucketTaggingInput, ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error) {
	return &s3.PutBucketTaggingOutput{}, nil
}

func (fake *awsEnsureS3) PutBucketVersioning(context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error) {
	return &s3.PutBucketVersioningOutput{}, nil
}

func (fake *awsEnsureS3) PutPublicAccessBlock(context.Context, *s3.PutPublicAccessBlockInput, ...func(*s3.Options)) (*s3.PutPublicAccessBlockOutput, error) {
	return &s3.PutPublicAccessBlockOutput{}, nil
}

func (fake *awsEnsureS3) PutBucketOwnershipControls(context.Context, *s3.PutBucketOwnershipControlsInput, ...func(*s3.Options)) (*s3.PutBucketOwnershipControlsOutput, error) {
	return &s3.PutBucketOwnershipControlsOutput{}, nil
}

func (fake *awsEnsureS3) PutBucketEncryption(_ context.Context, input *s3.PutBucketEncryptionInput, _ ...func(*s3.Options)) (*s3.PutBucketEncryptionOutput, error) {
	fake.encryption = input
	return &s3.PutBucketEncryptionOutput{}, nil
}

type awsEnsureKMS struct {
	KMSFoundationAPI
	spec          Spec
	create        *kms.CreateKeyInput
	recoverTagged bool
	aliasCreated  bool
}

func (fake *awsEnsureKMS) keyARN() string {
	return "arn:aws:kms:" + fake.spec.Request.Region + ":" + fake.spec.Request.AccountID + ":key/01234567-89ab-cdef-0123-456789abcdef"
}

func (fake *awsEnsureKMS) ListAliases(context.Context, *kms.ListAliasesInput, ...func(*kms.Options)) (*kms.ListAliasesOutput, error) {
	return &kms.ListAliasesOutput{}, nil
}

func (fake *awsEnsureKMS) ListKeys(context.Context, *kms.ListKeysInput, ...func(*kms.Options)) (*kms.ListKeysOutput, error) {
	if fake.recoverTagged {
		return &kms.ListKeysOutput{Keys: []kmstypes.KeyListEntry{{KeyArn: aws.String(fake.keyARN()), KeyId: aws.String("01234567-89ab-cdef-0123-456789abcdef")}}}, nil
	}
	return &kms.ListKeysOutput{}, nil
}

func (fake *awsEnsureKMS) DescribeKey(context.Context, *kms.DescribeKeyInput, ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	return &kms.DescribeKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{Arn: aws.String(fake.keyARN()), KeyId: aws.String("01234567-89ab-cdef-0123-456789abcdef"), Enabled: true, KeySpec: kmstypes.KeySpecSymmetricDefault, KeyUsage: kmstypes.KeyUsageTypeEncryptDecrypt, MultiRegion: aws.Bool(false)}}, nil
}

func (fake *awsEnsureKMS) ListResourceTags(context.Context, *kms.ListResourceTagsInput, ...func(*kms.Options)) (*kms.ListResourceTagsOutput, error) {
	return &kms.ListResourceTagsOutput{Tags: kmsTags(tagsFor(fake.spec, fake.spec.Names.ArtifactKMSKey))}, nil
}

func (fake *awsEnsureKMS) CreateKey(_ context.Context, input *kms.CreateKeyInput, _ ...func(*kms.Options)) (*kms.CreateKeyOutput, error) {
	fake.create = input
	return &kms.CreateKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{Arn: aws.String(fake.keyARN()), KeyId: aws.String("01234567-89ab-cdef-0123-456789abcdef"), Enabled: true, KeySpec: kmstypes.KeySpecSymmetricDefault, KeyUsage: kmstypes.KeyUsageTypeEncryptDecrypt, MultiRegion: aws.Bool(false)}}, nil
}

func (fake *awsEnsureKMS) CreateAlias(context.Context, *kms.CreateAliasInput, ...func(*kms.Options)) (*kms.CreateAliasOutput, error) {
	fake.aliasCreated = true
	return &kms.CreateAliasOutput{}, nil
}
